/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import java.io.IOException;
import java.nio.file.AccessDeniedException;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.auth.delegation.EncryptionSecrets;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.Constants.S3_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.s3a.Constants.S3_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfEncryptionTestsDisabled;
import static org.apache.hadoop.fs.s3a.S3AUtils.getEncryptionAlgorithm;
import static org.apache.hadoop.fs.s3a.S3AUtils.getS3EncryptionKey;

/**
 * Test whether or not encryption works by turning it on. Some checks
 * are made for different file sizes as there have been reports that the
 * file length may be rounded up to match word boundaries.
 */
public abstract class AbstractTestS3AEncryption extends AbstractS3ATestBase {

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    skipIfEncryptionTestsDisabled(conf);
    S3ATestUtils.disableFilesystemCaching(conf);
    patchConfigurationEncryptionSettings(conf);
    return conf;
  }

  /**
   * This removes the encryption settings from the
   * configuration and then sets the
   * fs.s3a.encryption.algorithm value to
   * be that of {@code getSSEAlgorithm()}.
   * Called in {@code createConfiguration()}.
   * @param conf configuration to patch.
   */
  @SuppressWarnings("deprecation")
  protected void patchConfigurationEncryptionSettings(
      final Configuration conf) {
    removeBaseAndBucketOverrides(conf,
        S3_ENCRYPTION_ALGORITHM,
        S3_ENCRYPTION_KEY,
        SERVER_SIDE_ENCRYPTION_ALGORITHM,
        SERVER_SIDE_ENCRYPTION_KEY);
    conf.set(S3_ENCRYPTION_ALGORITHM,
            getSSEAlgorithm().getMethod());
  }

  private static final int[] SIZES = {
      0, 1, 2, 3, 4, 5, 254, 255, 256, 257, 2 ^ 12 - 1
  };

  /**
   * Skips the tests if encryption is not enabled in configuration.
   *
   * @implNote We can use {@link #createConfiguration()} here since
   * it does not depend on any per-bucket based configuration.
   * Otherwise, we would need to grab the configuration from an
   * instance of {@link S3AFileSystem}.
   */
  protected void requireEncryptedFileSystem() {
    skipIfEncryptionTestsDisabled(createConfiguration());
  }

  /**
   * Skipping tests when running against mandatory encryption bucket
   * which allows only certain encryption method.
   * S3 throw AmazonS3Exception with status 403 AccessDenied
   * then it is translated into AccessDeniedException by S3AUtils.translateException(...)
   */
  @Override
  public void setup() throws Exception {
    try {
      requireEncryptedFileSystem();
      super.setup();
    } catch (AccessDeniedException e) {
      skip("Bucket does not allow " + getSSEAlgorithm() + " encryption method");
    }
  }

  /**
   * This examines how encryption settings propagate better.
   * If the settings are actually in a JCEKS file, then the
   * test override will fail; this is here to help debug the problem.
   */
  @Test
  public void testEncryptionSettingPropagation() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    S3AEncryptionMethods algorithm = getEncryptionAlgorithm(
        fs.getBucket(), fs.getConf());
    assertEquals("Configuration has wrong encryption algorithm",
        getSSEAlgorithm(), algorithm);
  }

  @Test
  public void testEncryption() throws Throwable {
    requireEncryptedFileSystem();
    validateEncryptionSecrets(getFileSystem().getEncryptionSecrets());
    for (int size: SIZES) {
      validateEncryptionForFilesize(size);
    }
  }

  @Test
  public void testEncryptionOverRename() throws Throwable {
    Path src = path(createFilename(1024));
    byte[] data = dataset(1024, 'a', 'z');
    S3AFileSystem fs = getFileSystem();
    EncryptionSecrets secrets = fs.getEncryptionSecrets();
    validateEncryptionSecrets(secrets);
    writeDataset(fs, src, data, data.length, 1024 * 1024, true);
    ContractTestUtils.verifyFileContents(fs, src, data);
    // this file will be encrypted
    assertEncrypted(src);

    Path targetDir = path("target");
    mkdirs(targetDir);
    fs.rename(src, targetDir);
    Path renamedFile = new Path(targetDir, src.getName());
    ContractTestUtils.verifyFileContents(fs, renamedFile, data);
    assertEncrypted(renamedFile);
  }

  /**
   * Verify that the filesystem encryption secrets match expected.
   * This makes sure that the settings have propagated properly.
   * @param secrets encryption secrets of the filesystem.
   */
  protected void validateEncryptionSecrets(final EncryptionSecrets secrets) {
    assertNotNull("No encryption secrets for filesystem", secrets);
    S3AEncryptionMethods sseAlgorithm = getSSEAlgorithm();
    assertEquals("Filesystem has wrong encryption algorithm",
        sseAlgorithm, secrets.getEncryptionMethod());
  }

  protected void validateEncryptionForFilesize(int len) throws IOException {
    describe("Create an encrypted file of size " + len);
    String src = createFilename(len);
    Path path = writeThenReadFile(src, len);
    assertEncrypted(path);
    rm(getFileSystem(), path, false, false);
  }

  protected String createFilename(int len) {
    return String.format("%s-%04x", methodName.getMethodName(), len);
  }

  protected String createFilename(String name) {
    return String.format("%s-%s", methodName.getMethodName(), name);
  }

  /**
   * Assert that at path references an encrypted blob.
   * @param path path
   * @throws IOException on a failure
   */
  protected void assertEncrypted(Path path) throws IOException {
    //S3 will return full arn of the key, so specify global arn in properties
    String kmsKeyArn =
        getS3EncryptionKey(getTestBucketName(getConfiguration()),
            getConfiguration());
    S3AEncryptionMethods algorithm = getSSEAlgorithm();
    EncryptionTestUtils.assertEncrypted(getFileSystem(),
            path,
            algorithm,
            kmsKeyArn);
  }

  protected abstract S3AEncryptionMethods getSSEAlgorithm();

}
