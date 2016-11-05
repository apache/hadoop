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

import com.amazonaws.services.s3.model.ObjectMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;

/**
 * Test whether or not encryption works by turning it on. Some checks
 * are made for different file sizes as there have been reports that the
 * file length may be rounded up to match word boundaries.
 */
public class ITestS3AEncryption extends AbstractS3ATestBase {
  private static final String AES256 = Constants.SERVER_SIDE_ENCRYPTION_AES256;

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    S3ATestUtils.disableFilesystemCaching(conf);
    conf.set(Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM,
        AES256);
    return conf;
  }

  private static final int[] SIZES = {
      0, 1, 2, 3, 4, 5, 254, 255, 256, 257, 2 ^ 12 - 1
  };

  @Test
  public void testEncryption() throws Throwable {
    for (int size: SIZES) {
      validateEncryptionForFilesize(size);
    }
  }

  @Test
  public void testEncryptionOverRename() throws Throwable {
    skipIfEncryptionTestsDisabled(getConfiguration());
    Path src = path(createFilename(1024));
    byte[] data = dataset(1024, 'a', 'z');
    S3AFileSystem fs = getFileSystem();
    writeDataset(fs, src, data, data.length, 1024 * 1024, true);
    ContractTestUtils.verifyFileContents(fs, src, data);
    Path dest = path(src.getName() + "-copy");
    fs.rename(src, dest);
    ContractTestUtils.verifyFileContents(fs, dest, data);
    assertEncrypted(dest);
  }

  protected void validateEncryptionForFilesize(int len) throws IOException {
    skipIfEncryptionTestsDisabled(getConfiguration());
    describe("Create an encrypted file of size " + len);
    String src = createFilename(len);
    Path path = writeThenReadFile(src, len);
    assertEncrypted(path);
    rm(getFileSystem(), path, false, false);
  }

  private String createFilename(int len) {
    return String.format("%s-%04x", methodName.getMethodName(), len);
  }

  /**
   * Assert that at path references an encrypted blob.
   * @param path path
   * @throws IOException on a failure
   */
  private void assertEncrypted(Path path) throws IOException {
    ObjectMetadata md = getFileSystem().getObjectMetadata(path);
    assertEquals(AES256, md.getSSEAlgorithm());
  }

}
