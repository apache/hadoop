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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;

/**
 * Test whether or not encryption settings propagate by choosing an invalid
 * one. We expect the write to fail with a 400 bad request error
 */
public class ITestS3AEncryptionAlgorithmPropagation
    extends AbstractS3ATestBase {

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    S3ATestUtils.disableFilesystemCaching(conf);
    conf.set(Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM,
        "DES");
    return conf;
  }

  @Test
  public void testEncrypt0() throws Throwable {
    writeThenReadFileToFailure(0);
  }

  @Test
  public void testEncrypt256() throws Throwable {
    writeThenReadFileToFailure(256);
  }

  /**
   * Make this a no-op so test setup doesn't fail.
   * @param path path path
   * @throws IOException on any failure
   */
  @Override
  protected void mkdirs(Path path) throws IOException {

  }

  protected void writeThenReadFileToFailure(int len) throws IOException {
    skipIfEncryptionTestsDisabled(getConfiguration());
    describe("Create an encrypted file of size " + len);
    try {
      writeThenReadFile(methodName.getMethodName() + '-' + len, len);
      fail("Expected an exception about an illegal encryption algorithm");
    } catch (AWSS3IOException e) {
      assertStatusCode(e, 400);
    }
  }

}
