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

import org.junit.Ignore;
import org.junit.Test;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.auth.delegation.EncryptionSecrets;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.s3a.EncryptionTestUtils.AWS_KMS_SSE_ALGORITHM;
import static org.apache.hadoop.fs.s3a.S3AEncryptionMethods.SSE_KMS;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;

/**
 * Concrete class that extends {@link AbstractTestS3AEncryption}
 * and tests already configured bucket level encryption using s3 console.
 * This requires the SERVER_SIDE_ENCRYPTION_KEY
 * to be set in auth-keys.xml for it to run. The value should match with the
 * kms key set for the bucket.
 * See HADOOP-16794.
 */
public class ITestS3AEncryptionWithDefaultS3Settings extends
        AbstractTestS3AEncryption {

  @Override
  public void setup() throws Exception {
    super.setup();
    // get the KMS key for this test.
    S3AFileSystem fs = getFileSystem();
    Configuration c = fs.getConf();
    String kmsKey = c.get(SERVER_SIDE_ENCRYPTION_KEY);
    if (StringUtils.isBlank(kmsKey)) {
      skip(SERVER_SIDE_ENCRYPTION_KEY + " is not set for " +
          SSE_KMS.getMethod());
    }
  }

  @Override
  protected void patchConfigurationEncryptionSettings(
      final Configuration conf) {
    removeBaseAndBucketOverrides(conf,
        SERVER_SIDE_ENCRYPTION_ALGORITHM);
    conf.set(SERVER_SIDE_ENCRYPTION_ALGORITHM,
            getSSEAlgorithm().getMethod());
  }

  /**
   * Setting this to NONE as we don't want to overwrite
   * already configured encryption settings.
   * @return the algorithm
   */
  @Override
  protected S3AEncryptionMethods getSSEAlgorithm() {
    return S3AEncryptionMethods.NONE;
  }

  /**
   * The check here is that the object is encrypted
   * <i>and</i> that the encryption key is the KMS key
   * provided, not any default key.
   * @param path path
   */
  @Override
  protected void assertEncrypted(Path path) throws IOException {
    S3AFileSystem fs = getFileSystem();
    Configuration c = fs.getConf();
    String kmsKey = c.getTrimmed(SERVER_SIDE_ENCRYPTION_KEY);
    EncryptionTestUtils.assertEncrypted(fs, path, SSE_KMS, kmsKey);
  }

  @Override
  @Ignore
  @Test
  public void testEncryptionSettingPropagation() throws Throwable {
  }

  @Override
  @Ignore
  @Test
  public void testEncryption() throws Throwable {
  }

  /**
   * Skipping if the test bucket is not configured with
   * aws:kms encryption algorithm.
   */
  @Override
  public void testEncryptionOverRename() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    Path path = path(getMethodName() + "find-encryption-algo");
    ContractTestUtils.touch(fs, path);
    String sseAlgorithm = fs.getObjectMetadata(path).getSSEAlgorithm();
    if(StringUtils.isBlank(sseAlgorithm) ||
            !sseAlgorithm.equals(AWS_KMS_SSE_ALGORITHM)) {
      skip("Test bucket is not configured with " + AWS_KMS_SSE_ALGORITHM);
    }
    super.testEncryptionOverRename();
  }

  @Test
  public void testEncryptionOverRename2() throws Throwable {
    S3AFileSystem fs = getFileSystem();

    // write the file with the unencrypted FS.
    // this will pick up whatever defaults we have.
    Path src = path(createFilename(1024));
    byte[] data = dataset(1024, 'a', 'z');
    EncryptionSecrets secrets = fs.getEncryptionSecrets();
    validateEncrytionSecrets(secrets);
    writeDataset(fs, src, data, data.length, 1024 * 1024, true);
    ContractTestUtils.verifyFileContents(fs, src, data);

    // fs2 conf will always use SSE-KMS
    Configuration fs2Conf = new Configuration(fs.getConf());
    fs2Conf.set(SERVER_SIDE_ENCRYPTION_ALGORITHM,
        S3AEncryptionMethods.SSE_KMS.getMethod());
    try (FileSystem kmsFS = FileSystem.newInstance(fs.getUri(), fs2Conf)) {
      Path targetDir = path("target");
      kmsFS.mkdirs(targetDir);
      ContractTestUtils.rename(kmsFS, src, targetDir);
      Path renamedFile = new Path(targetDir, src.getName());
      ContractTestUtils.verifyFileContents(fs, renamedFile, data);
      String kmsKey = fs2Conf.getTrimmed(SERVER_SIDE_ENCRYPTION_KEY);
      // we assert that the renamed file has picked up the KMS key of our FS
      EncryptionTestUtils.assertEncrypted(fs, renamedFile, SSE_KMS, kmsKey);
    }
  }
}
