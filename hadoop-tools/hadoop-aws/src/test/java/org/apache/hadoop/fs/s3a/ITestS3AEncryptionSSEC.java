/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.rm;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfEncryptionTestsDisabled;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.contract.s3a.S3AContract;
import org.junit.Test;

/**
 * Concrete class that extends {@link AbstractTestS3AEncryption}
 * and tests SSE-C encryption.
 */
public class ITestS3AEncryptionSSEC extends AbstractTestS3AEncryption {

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    S3ATestUtils.disableFilesystemCaching(conf);
    conf.set(Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM,
        getSSEAlgorithm().getMethod());
    conf.set(Constants.SERVER_SIDE_ENCRYPTION_KEY,
        "4niV/jPK5VFRHY+KNb6wtqYd4xXyMgdJ9XQJpcQUVbs=");
    return conf;
  }

  /**
   * This will create and write to a file using encryption key A, then attempt
   * to read from it again with encryption key B.  This will not work as it
   * cannot decrypt the file.
   * @throws Exception
   */
  @Test
  public void testCreateFileAndReadWithDifferentEncryptionKey() throws
    Exception {
    final Path[] path = new Path[1];
    intercept(java.nio.file.AccessDeniedException.class,
        "Service: Amazon S3; Status Code: 403;", () -> {

        int len = 2048;
        skipIfEncryptionTestsDisabled(getConfiguration());
        describe("Create an encrypted file of size " + len);
        String src = createFilename(len);
        path[0] = writeThenReadFile(src, len);

        Configuration conf = this.createConfiguration();
        conf.set(Constants.SERVER_SIDE_ENCRYPTION_KEY,
            "kX7SdwVc/1VXJr76kfKnkQ3ONYhxianyL2+C3rPVT9s=");

        S3AContract contract = (S3AContract) createContract(conf);
        contract.init();
        //skip tests if they aren't enabled
        assumeEnabled();
        //extract the test FS
        FileSystem fileSystem = contract.getTestFileSystem();
        byte[] data = dataset(len, 'a', 'z');
        ContractTestUtils.verifyFileContents(fileSystem, path[0], data);
        throw new Exception("Fail");
      });
    rm(getFileSystem(), path[0], false, false);
  }

  @Override
  protected S3AEncryptionMethods getSSEAlgorithm() {
    return S3AEncryptionMethods.SSE_C;
  }
}
