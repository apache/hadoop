/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3AEncryptionMethods.*;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.test.LambdaTestUtils.*;

/**
 * Test SSE setup operations and errors raised.
 * Tests related to secret providers and AWS credentials are also
 * included, as they share some common setup operations.
 */
public class TestSSEConfiguration extends Assert {

  /** Bucket to use for per-bucket options. */
  public static final String BUCKET = "dataset-1";

  @Rule
  public Timeout testTimeout = new Timeout(
      S3ATestConstants.S3A_TEST_TIMEOUT
  );

  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  @Test
  public void testSSECNoKey() throws Throwable {
    assertGetAlgorithmFails(SSE_C_NO_KEY_ERROR, SSE_C.getMethod(), null);
  }

  @Test
  public void testSSECBlankKey() throws Throwable {
    assertGetAlgorithmFails(SSE_C_NO_KEY_ERROR, SSE_C.getMethod(), "");
  }

  @Test
  public void testSSECGoodKey() throws Throwable {
    assertEquals(SSE_C, getAlgorithm(SSE_C, "sseckey"));
  }

  @Test
  public void testKMSGoodKey() throws Throwable {
    assertEquals(SSE_KMS, getAlgorithm(SSE_KMS, "kmskey"));
  }

  @Test
  public void testAESKeySet() throws Throwable {
    assertGetAlgorithmFails(SSE_S3_WITH_KEY_ERROR,
        SSE_S3.getMethod(), "setkey");
  }

  @Test
  public void testSSEEmptyKey() {
    // test the internal logic of the test setup code
    Configuration c = buildConf(SSE_C.getMethod(), "");
    assertEquals("", getServerSideEncryptionKey(BUCKET, c));
  }

  @Test
  public void testSSEKeyNull() throws Throwable {
    // test the internal logic of the test setup code
    final Configuration c = buildConf(SSE_C.getMethod(), null);
    assertEquals("", getServerSideEncryptionKey(BUCKET, c));

    intercept(IOException.class, SSE_C_NO_KEY_ERROR,
        () -> getEncryptionAlgorithm(BUCKET, c));
  }

  @Test
  public void testSSEKeyFromCredentialProvider() throws Exception {
    // set up conf to have a cred provider
    final Configuration conf = confWithProvider();
    String key = "provisioned";
    setProviderOption(conf, SERVER_SIDE_ENCRYPTION_KEY, key);
    // let's set the password in config and ensure that it uses the credential
    // provider provisioned value instead.
    conf.set(SERVER_SIDE_ENCRYPTION_KEY, "keyInConfObject");

    String sseKey = getServerSideEncryptionKey(BUCKET, conf);
    assertNotNull("Proxy password should not retrun null.", sseKey);
    assertEquals("Proxy password override did NOT work.", key, sseKey);
  }

  /**
   * Add a temp file provider to the config.
   * @param conf config
   * @throws Exception failure
   */
  private void addFileProvider(Configuration conf)
      throws Exception {
    final File file = tempDir.newFile("test.jks");
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
        file.toURI());
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        jks.toString());
  }

  /**
   * Set the an option under the configuration via the
   * {@link CredentialProviderFactory} APIs.
   * @param conf config
   * @param option option name
   * @param value value to set option to.
   * @throws Exception failure
   */
  void setProviderOption(final Configuration conf,
      String option, String value) throws Exception {
    // add our password to the provider
    final CredentialProvider provider =
        CredentialProviderFactory.getProviders(conf).get(0);
    provider.createCredentialEntry(option,
        value.toCharArray());
    provider.flush();
  }

  /**
   * Assert that the exception text from {@link #getAlgorithm(String, String)}
   * is as expected.
   * @param expected expected substring in error
   * @param alg algorithm to ask for
   * @param key optional key value
   * @throws Exception anything else which gets raised
   */
  public void assertGetAlgorithmFails(String expected,
      final String alg, final String key) throws Exception {
    intercept(IOException.class, expected,
        () -> getAlgorithm(alg, key));
  }

  private S3AEncryptionMethods getAlgorithm(S3AEncryptionMethods algorithm,
      String key)
      throws IOException {
    return getAlgorithm(algorithm.getMethod(), key);
  }

  private S3AEncryptionMethods getAlgorithm(String algorithm, String key)
      throws IOException {
    return getEncryptionAlgorithm(BUCKET, buildConf(algorithm, key));
  }

  /**
   * Build a new configuration with the given S3-SSE algorithm
   * and key.
   * @param algorithm  algorithm to use, may be null
   * @param key key, may be null
   * @return the new config.
   */
  private Configuration buildConf(String algorithm, String key) {
    Configuration conf = emptyConf();
    if (algorithm != null) {
      conf.set(SERVER_SIDE_ENCRYPTION_ALGORITHM, algorithm);
    } else {
      conf.unset(SERVER_SIDE_ENCRYPTION_ALGORITHM);
    }
    if (key != null) {
      conf.set(SERVER_SIDE_ENCRYPTION_KEY, key);
    } else {
      conf.unset(SERVER_SIDE_ENCRYPTION_KEY);
    }
    return conf;
  }

  /**
   * Create an empty conf: no -default or -site values.
   * @return an empty configuration
   */
  private Configuration emptyConf() {
    return new Configuration(false);
  }

  /**
   * Create a configuration with no defaults and bonded to a file
   * provider, so that
   * {@link #setProviderOption(Configuration, String, String)}
   * can be used to set a secret.
   * @return the configuration
   * @throws Exception any failure
   */
  private Configuration confWithProvider() throws Exception {
    final Configuration conf = emptyConf();
    addFileProvider(conf);
    return conf;
  }


  private static final String SECRET = "*secret*";

  private static final String BUCKET_PATTERN = FS_S3A_BUCKET_PREFIX + "%s.%s";

  @Test
  public void testGetPasswordFromConf() throws Throwable {
    final Configuration conf = emptyConf();
    conf.set(SECRET_KEY, SECRET);
    assertEquals(SECRET, lookupPassword(conf, SECRET_KEY, ""));
    assertEquals(SECRET, lookupPassword(conf, SECRET_KEY, "defVal"));
  }

  @Test
  public void testGetPasswordFromProvider() throws Throwable {
    final Configuration conf = confWithProvider();
    setProviderOption(conf, SECRET_KEY, SECRET);
    assertEquals(SECRET, lookupPassword(conf, SECRET_KEY, ""));
    assertSecretKeyEquals(conf, null, SECRET, "");
    assertSecretKeyEquals(conf, null, "overidden", "overidden");
  }

  @Test
  public void testGetBucketPasswordFromProvider() throws Throwable {
    final Configuration conf = confWithProvider();
    URI bucketURI = new URI("s3a://"+ BUCKET +"/");
    setProviderOption(conf, SECRET_KEY, "unbucketed");

    String bucketedKey = String.format(BUCKET_PATTERN, BUCKET, SECRET_KEY);
    setProviderOption(conf, bucketedKey, SECRET);
    String overrideVal;
    overrideVal = "";
    assertSecretKeyEquals(conf, BUCKET, SECRET, overrideVal);
    assertSecretKeyEquals(conf, bucketURI.getHost(), SECRET, "");
    assertSecretKeyEquals(conf, bucketURI.getHost(), "overidden", "overidden");
  }

  /**
   * Assert that a secret key is as expected.
   * @param conf configuration to examine
   * @param bucket bucket name
   * @param expected expected value
   * @param overrideVal override value in {@code S3AUtils.lookupPassword()}
   * @throws IOException IO problem
   */
  private void assertSecretKeyEquals(Configuration conf,
      String bucket,
      String expected, String overrideVal) throws IOException {
    assertEquals(expected,
        S3AUtils.lookupPassword(bucket, conf, SECRET_KEY, overrideVal, null));
  }

  @Test
  public void testGetBucketPasswordFromProviderShort() throws Throwable {
    final Configuration conf = confWithProvider();
    URI bucketURI = new URI("s3a://"+ BUCKET +"/");
    setProviderOption(conf, SECRET_KEY, "unbucketed");

    String bucketedKey = String.format(BUCKET_PATTERN, BUCKET, "secret.key");
    setProviderOption(conf, bucketedKey, SECRET);
    assertSecretKeyEquals(conf, BUCKET, SECRET, "");
    assertSecretKeyEquals(conf, bucketURI.getHost(), SECRET, "");
    assertSecretKeyEquals(conf, bucketURI.getHost(), "overidden", "overidden");
  }

  @Test
  public void testUnknownEncryptionMethod() throws Throwable {
    intercept(IOException.class, UNKNOWN_ALGORITHM,
        () -> S3AEncryptionMethods.getMethod("SSE-ROT13"));
  }

  @Test
  public void testClientEncryptionMethod() throws Throwable {
    S3AEncryptionMethods method = getMethod("CSE-KMS");
    assertEquals(CSE_KMS, method);
    assertFalse("shouldn't be server side " + method, method.isServerSide());
  }

  @Test
  public void testCSEKMSEncryptionMethod() throws Throwable {
    S3AEncryptionMethods method = getMethod("CSE-CUSTOM");
    assertEquals(CSE_CUSTOM, method);
    assertFalse("shouldn't be server side " + method, method.isServerSide());
  }

  @Test
  public void testNoEncryptionMethod() throws Throwable {
    assertEquals(NONE, getMethod(" "));
  }

}
