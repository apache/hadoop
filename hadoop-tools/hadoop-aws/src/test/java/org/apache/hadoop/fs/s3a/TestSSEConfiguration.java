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
import java.util.concurrent.Callable;

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
 */
public class TestSSEConfiguration extends Assert {

  @Rule
  public Timeout testTimeout = new Timeout(
      S3ATestConstants.S3A_TEST_TIMEOUT
  );

  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  @Test
  public void testSSECNoKey() throws Throwable {
    assertExceptionTextEquals(SSE_C_NO_KEY_ERROR, SSE_C.getMethod(), null);
  }

  @Test
  public void testSSECBlankKey() throws Throwable {
    assertExceptionTextEquals(SSE_C_NO_KEY_ERROR, SSE_C.getMethod(), "");
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
  public void testKMSGoodOldOptionName() throws Throwable {
    Configuration conf = new Configuration(false);
    conf.set(SERVER_SIDE_ENCRYPTION_ALGORITHM, SSE_KMS.getMethod());
    conf.set(OLD_S3A_SERVER_SIDE_ENCRYPTION_KEY, "kmskeyID");
    // verify key round trip
    assertEquals("kmskeyID", getServerSideEncryptionKey(conf));
    // and that KMS lookup finds it
    assertEquals(SSE_KMS, getEncryptionAlgorithm(conf));
  }

  @Test
  public void testAESKeySet() throws Throwable {
    assertExceptionTextEquals(SSE_S3_WITH_KEY_ERROR,
        SSE_S3.getMethod(), "setkey");
  }

  @Test
  public void testSSEEmptyKey() throws Throwable {
    // test the internal logic of the test setup code
    Configuration c = buildConf(SSE_C.getMethod(), "");
    assertEquals("", getServerSideEncryptionKey(c));
  }

  @Test
  public void testSSEKeyNull() throws Throwable {
    // test the internal logic of the test setup code
    final Configuration c = buildConf(SSE_C.getMethod(), null);
    assertNull("", getServerSideEncryptionKey(c));

    intercept(IOException.class, SSE_C_NO_KEY_ERROR,
        new Callable<S3AEncryptionMethods>() {
          @Override
          public S3AEncryptionMethods call() throws Exception {
            return getEncryptionAlgorithm(c);
          }
        });
  }

  @Test
  public void testSSEKeyFromCredentialProvider() throws Exception {
    // set up conf to have a cred provider
    final Configuration conf = new Configuration();
    addFileProvider(conf);
    String key = "provisioned";
    provisionSSEKey(conf, SERVER_SIDE_ENCRYPTION_KEY, key);
    // let's set the password in config and ensure that it uses the credential
    // provider provisioned value instead.
    conf.set(SERVER_SIDE_ENCRYPTION_KEY, "keyInConfObject");

    String sseKey = getServerSideEncryptionKey(conf);
    assertNotNull("Proxy password should not retrun null.", sseKey);
    assertEquals("Proxy password override did NOT work.", key, sseKey);
  }

  /**
   * Very that the old key is picked up via the properties
   * @throws Exception failure
   */
  @Test
  public void testOldKeyFromCredentialProvider() throws Exception {
    // set up conf to have a cred provider
    final Configuration conf = new Configuration();
    addFileProvider(conf);
    String key = "provisioned";
    provisionSSEKey(conf, OLD_S3A_SERVER_SIDE_ENCRYPTION_KEY, key);
    // let's set the password in config and ensure that it uses the credential
    // provider provisioned value instead.
    //conf.set(OLD_S3A_SERVER_SIDE_ENCRYPTION_KEY, "oldKeyInConf");
    String sseKey = getServerSideEncryptionKey(conf);
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
   * Set the SSE Key via the provision API, not the config itself.
   * @param conf config
   * @param option option name
   * @param key key to set
   * @throws Exception failure
   */
  void provisionSSEKey(final Configuration conf,
      String option, String key) throws Exception {
    // add our password to the provider
    final CredentialProvider provider =
        CredentialProviderFactory.getProviders(conf).get(0);
    provider.createCredentialEntry(option,
        key.toCharArray());
    provider.flush();
  }

  /**
   * Assert that the exception text from a config contains the expected string
   * @param expected expected substring
   * @param alg algorithm to ask for
   * @param key optional key value
   * @throws Exception anything else which gets raised
   */
  public void assertExceptionTextEquals(String expected,
      final String alg, final String key) throws Exception {
    intercept(IOException.class, expected,
        new Callable<S3AEncryptionMethods>() {
          @Override
          public S3AEncryptionMethods call() throws Exception {
            return getAlgorithm(alg, key);
          }
        });
  }

  private S3AEncryptionMethods getAlgorithm(S3AEncryptionMethods algorithm,
      String key)
      throws IOException {
    return getAlgorithm(algorithm.getMethod(), key);
  }

  private S3AEncryptionMethods getAlgorithm(String algorithm, String key)
      throws IOException {
    return getEncryptionAlgorithm(buildConf(algorithm, key));
  }

  private Configuration buildConf(String algorithm, String key) {
    Configuration conf = new Configuration(false);
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
}
