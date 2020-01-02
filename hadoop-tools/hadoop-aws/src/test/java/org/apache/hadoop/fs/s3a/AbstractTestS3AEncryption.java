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

import com.amazonaws.services.s3.model.ObjectMetadata;
import org.junit.Test;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.auth.delegation.EncryptionSecrets;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfEncryptionTestsDisabled;
import static org.apache.hadoop.fs.s3a.S3AUtils.getEncryptionAlgorithm;

/**
 * Test whether or not encryption works by turning it on. Some checks
 * are made for different file sizes as there have been reports that the
 * file length may be rounded up to match word boundaries.
 */
public abstract class AbstractTestS3AEncryption extends AbstractS3ATestBase {

  protected static final String AWS_KMS_SSE_ALGORITHM = "aws:kms";

  protected static final String SSE_C_ALGORITHM = "AES256";

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    S3ATestUtils.disableFilesystemCaching(conf);
    removeBaseAndBucketOverrides(conf,
        SERVER_SIDE_ENCRYPTION_ALGORITHM,
        SERVER_SIDE_ENCRYPTION_KEY);
    conf.set(SERVER_SIDE_ENCRYPTION_ALGORITHM,
            getSSEAlgorithm().getMethod());
    return conf;
  }

  private static final int[] SIZES = {
      0, 1, 2, 3, 4, 5, 254, 255, 256, 257, 2 ^ 12 - 1
  };

  protected void requireEncryptedFileSystem() {
    skipIfEncryptionTestsDisabled(getFileSystem().getConf());
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    requireEncryptedFileSystem();
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
    validateEncrytionSecrets(getFileSystem().getEncryptionSecrets());
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
    validateEncrytionSecrets(secrets);
    writeDataset(fs, src, data, data.length, 1024 * 1024, true);
    ContractTestUtils.verifyFileContents(fs, src, data);
    Path dest = path(src.getName() + "-copy");
    fs.rename(src, dest);
    ContractTestUtils.verifyFileContents(fs, dest, data);
    assertEncrypted(dest);
  }

  /**
   * Verify that the filesystem encryption secrets match expected.
   * This makes sure that the settings have propagated properly.
   * @param secrets encryption secrets of the filesystem.
   */
  protected void validateEncrytionSecrets(final EncryptionSecrets secrets) {
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
    ObjectMetadata md = getFileSystem().getObjectMetadata(path);
    switch(getSSEAlgorithm()) {
    case SSE_C:
      assertNull("Metadata algorithm should have been null",
          md.getSSEAlgorithm());
      assertEquals("Wrong SSE-C algorithm", SSE_C_ALGORITHM, md.getSSECustomerAlgorithm());
      String md5Key = convertKeyToMd5();
      assertEquals("getSSECustomerKeyMd5() wrong", md5Key, md.getSSECustomerKeyMd5());
      break;
    case SSE_KMS:
      assertEquals(AWS_KMS_SSE_ALGORITHM, md.getSSEAlgorithm());
      //S3 will return full arn of the key, so specify global arn in properties
      assertEquals(this.getConfiguration().
          getTrimmed(SERVER_SIDE_ENCRYPTION_KEY),
          md.getSSEAwsKmsKeyId());
      break;
    default:
      assertEquals("AES256", md.getSSEAlgorithm());
    }
  }

  /**
   * Decodes the SERVER_SIDE_ENCRYPTION_KEY from base64 into an AES key, then
   * gets the md5 of it, then encodes it in base64 so it will match the version
   * that AWS returns to us.
   *
   * @return md5'd base64 encoded representation of the server side encryption
   * key
   */
  private String convertKeyToMd5() {
    String base64Key = getFileSystem().getConf().getTrimmed(
        SERVER_SIDE_ENCRYPTION_KEY
    );
    byte[] key = Base64.decodeBase64(base64Key);
    byte[] md5 =  DigestUtils.md5(key);
    return Base64.encodeBase64String(md5).trim();
  }

  protected abstract S3AEncryptionMethods getSSEAlgorithm();

}
