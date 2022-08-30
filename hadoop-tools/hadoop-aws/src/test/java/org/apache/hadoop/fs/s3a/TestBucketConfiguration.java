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
import java.net.URI;
import java.util.Collection;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.auth.delegation.EncryptionSecrets;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.s3a.Constants.CHANGE_DETECT_MODE;
import static org.apache.hadoop.fs.s3a.Constants.CHANGE_DETECT_MODE_CLIENT;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_BUCKET_PREFIX;
import static org.apache.hadoop.fs.s3a.Constants.S3A_SECURITY_CREDENTIAL_PROVIDER_PATH;
import static org.apache.hadoop.fs.s3a.Constants.S3_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.s3a.Constants.S3_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.s3a.Constants.USER_AGENT_PREFIX;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assertOptionEquals;
import static org.apache.hadoop.fs.s3a.S3AUtils.CREDENTIAL_PROVIDER_PATH;
import static org.apache.hadoop.fs.s3a.S3AUtils.clearBucketOption;
import static org.apache.hadoop.fs.s3a.S3AUtils.getEncryptionAlgorithm;
import static org.apache.hadoop.fs.s3a.S3AUtils.patchSecurityCredentialProviders;
import static org.apache.hadoop.fs.s3a.S3AUtils.propagateBucketOptions;
import static org.apache.hadoop.fs.s3a.S3AUtils.setBucketOption;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * S3A tests for configuration option propagation.
 */
@SuppressWarnings("deprecation")
public class TestBucketConfiguration extends AbstractHadoopTestBase {

  private static final String NEW_ALGORITHM_KEY_GLOBAL = "CSE-KMS";
  private static final String OLD_ALGORITHM_KEY_BUCKET = "SSE-KMS";
  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  /**
   * Setup: create the contract then init it.
   * @throws Exception on any failure
   */
  @Before
  public void setup() throws Exception {
    // forces in deprecation wireup, even when this test method is running isolated
    S3AFileSystem.initializeClass();
  }

  @Test
  public void testBucketConfigurationPropagation() throws Throwable {
    Configuration config = new Configuration(false);
    setBucketOption(config, "b", "base", "1024");
    String basekey = "fs.s3a.base";
    assertOptionEquals(config, basekey, null);
    String bucketKey = "fs.s3a.bucket.b.base";
    assertOptionEquals(config, bucketKey, "1024");
    Configuration updated = propagateBucketOptions(config, "b");
    assertOptionEquals(updated, basekey, "1024");
    // original conf is not updated
    assertOptionEquals(config, basekey, null);

    String[] sources = updated.getPropertySources(basekey);
    assertEquals(1, sources.length);
    Assertions.assertThat(sources)
        .describedAs("base key property sources")
        .hasSize(1);
    Assertions.assertThat(sources[0])
        .describedAs("Property source")
        .contains(bucketKey);
  }

  @Test
  public void testBucketConfigurationPropagationResolution() throws Throwable {
    Configuration config = new Configuration(false);
    String basekey = "fs.s3a.base";
    String baseref = "fs.s3a.baseref";
    String baseref2 = "fs.s3a.baseref2";
    config.set(basekey, "orig");
    config.set(baseref2, "${fs.s3a.base}");
    setBucketOption(config, "b", basekey, "1024");
    setBucketOption(config, "b", baseref, "${fs.s3a.base}");
    Configuration updated = propagateBucketOptions(config, "b");
    assertOptionEquals(updated, basekey, "1024");
    assertOptionEquals(updated, baseref, "1024");
    assertOptionEquals(updated, baseref2, "1024");
  }

  @Test
  public void testMultipleBucketConfigurations() throws Throwable {
    Configuration config = new Configuration(false);
    setBucketOption(config, "b", USER_AGENT_PREFIX, "UA-b");
    setBucketOption(config, "c", USER_AGENT_PREFIX, "UA-c");
    config.set(USER_AGENT_PREFIX, "UA-orig");
    Configuration updated = propagateBucketOptions(config, "c");
    assertOptionEquals(updated, USER_AGENT_PREFIX, "UA-c");
  }

  @Test
  public void testClearBucketOption() throws Throwable {
    Configuration config = new Configuration();
    config.set(USER_AGENT_PREFIX, "base");
    setBucketOption(config, "bucket", USER_AGENT_PREFIX, "overridden");
    clearBucketOption(config, "bucket", USER_AGENT_PREFIX);
    Configuration updated = propagateBucketOptions(config, "c");
    assertOptionEquals(updated, USER_AGENT_PREFIX, "base");
  }

  @Test
  public void testBucketConfigurationSkipsUnmodifiable() throws Throwable {
    Configuration config = new Configuration(false);
    String impl = "fs.s3a.impl";
    config.set(impl, "orig");
    setBucketOption(config, "b", impl, "b");
    String changeDetectionMode = CHANGE_DETECT_MODE;
    String client = CHANGE_DETECT_MODE_CLIENT;
    setBucketOption(config, "b", changeDetectionMode, client);
    setBucketOption(config, "b", "impl2", "b2");
    setBucketOption(config, "b", "bucket.b.loop", "b3");
    assertOptionEquals(config, "fs.s3a.bucket.b.impl", "b");

    Configuration updated = propagateBucketOptions(config, "b");
    assertOptionEquals(updated, impl, "orig");
    assertOptionEquals(updated, "fs.s3a.impl2", "b2");
    assertOptionEquals(updated, changeDetectionMode, client);
    assertOptionEquals(updated, "fs.s3a.bucket.b.loop", null);
  }

  @Test
  public void testSecurityCredentialPropagationNoOverride() throws Exception {
    Configuration config = new Configuration();
    config.set(CREDENTIAL_PROVIDER_PATH, "base");
    patchSecurityCredentialProviders(config);
    assertOptionEquals(config, CREDENTIAL_PROVIDER_PATH,
        "base");
  }

  @Test
  public void testSecurityCredentialPropagationOverrideNoBase()
      throws Exception {
    Configuration config = new Configuration();
    config.unset(CREDENTIAL_PROVIDER_PATH);
    config.set(S3A_SECURITY_CREDENTIAL_PROVIDER_PATH, "override");
    patchSecurityCredentialProviders(config);
    assertOptionEquals(config, CREDENTIAL_PROVIDER_PATH,
        "override");
  }

  @Test
  public void testSecurityCredentialPropagationOverride() throws Exception {
    Configuration config = new Configuration();
    config.set(CREDENTIAL_PROVIDER_PATH, "base");
    config.set(S3A_SECURITY_CREDENTIAL_PROVIDER_PATH, "override");
    patchSecurityCredentialProviders(config);
    assertOptionEquals(config, CREDENTIAL_PROVIDER_PATH,
        "override,base");
    Collection<String> all = config.getStringCollection(
        CREDENTIAL_PROVIDER_PATH);
    assertTrue(all.contains("override"));
    assertTrue(all.contains("base"));
  }

  @Test
  public void testSecurityCredentialPropagationEndToEnd() throws Exception {
    Configuration config = new Configuration();
    config.set(CREDENTIAL_PROVIDER_PATH, "base");
    setBucketOption(config, "b", S3A_SECURITY_CREDENTIAL_PROVIDER_PATH,
        "override");
    Configuration updated = propagateBucketOptions(config, "b");

    patchSecurityCredentialProviders(updated);
    assertOptionEquals(updated, CREDENTIAL_PROVIDER_PATH,
        "override,base");
  }

  /**
   * This test shows that a per-bucket value of the older key takes priority
   * over a global value of a new key in XML configuration file.
   */
  @Test
  public void testBucketConfigurationDeprecatedEncryptionAlgorithm()
      throws Throwable {
    Configuration config = new Configuration(false);
    config.set(S3_ENCRYPTION_ALGORITHM, NEW_ALGORITHM_KEY_GLOBAL);
    setBucketOption(config, "b", SERVER_SIDE_ENCRYPTION_ALGORITHM,
        OLD_ALGORITHM_KEY_BUCKET);
    Configuration updated = propagateBucketOptions(config, "b");

    // Get the encryption method and verify that the value is per-bucket of
    // old keys.
    String value = getEncryptionAlgorithm("b", updated).getMethod();
    Assertions.assertThat(value)
        .describedAs("lookupPassword(%s)", S3_ENCRYPTION_ALGORITHM)
        .isEqualTo(OLD_ALGORITHM_KEY_BUCKET);
  }

  @Test
  public void testJceksDeprecatedEncryptionAlgorithm() throws Exception {
    // set up conf to have a cred provider
    final Configuration conf = new Configuration(false);
    final File file = tempDir.newFile("test.jks");
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
        file.toURI());
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        jks.toString());

    // add our creds to the provider
    final CredentialProvider provider =
        CredentialProviderFactory.getProviders(conf).get(0);
    provider.createCredentialEntry(S3_ENCRYPTION_ALGORITHM,
        NEW_ALGORITHM_KEY_GLOBAL.toCharArray());
    provider.createCredentialEntry(S3_ENCRYPTION_KEY,
        "global s3 encryption key".toCharArray());
    provider.createCredentialEntry(
        FS_S3A_BUCKET_PREFIX + "b." + SERVER_SIDE_ENCRYPTION_ALGORITHM,
        OLD_ALGORITHM_KEY_BUCKET.toCharArray());
    final String bucketKey = "bucket-server-side-encryption-key";
    provider.createCredentialEntry(
        FS_S3A_BUCKET_PREFIX + "b." + SERVER_SIDE_ENCRYPTION_KEY,
        bucketKey.toCharArray());
    provider.flush();

    // Get the encryption method and verify that the value is per-bucket of
    // old keys.
    final EncryptionSecrets secrets = S3AUtils.buildEncryptionSecrets("b", conf);
    Assertions.assertThat(secrets.getEncryptionMethod().getMethod())
        .describedAs("buildEncryptionSecrets() encryption algorithm resolved to %s", secrets)
        .isEqualTo(OLD_ALGORITHM_KEY_BUCKET);

    Assertions.assertThat(secrets.getEncryptionKey())
        .describedAs("buildEncryptionSecrets() encryption key resolved to %s", secrets)
        .isEqualTo(bucketKey);

  }
}
