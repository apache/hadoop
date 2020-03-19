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

package org.apache.hadoop.fs.s3a.scale;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.EncryptionTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.s3a.S3AEncryptionMethods.SSE_KMS;

/**
 * Class to test SSE_KMS encryption settings for huge files.
 * Tests will only run if value of {@link Constants#SERVER_SIDE_ENCRYPTION_KEY}
 * is set in the configuration. The testing bucket must be configured with this
 * same key else test might fail.
 */
public class ITestS3AHugeFilesEncryption extends AbstractSTestS3AHugeFiles {

  @Override
  public void setup() throws Exception {
    Configuration c = new Configuration();
    String kmsKey = c.get(SERVER_SIDE_ENCRYPTION_KEY);
    if (StringUtils.isBlank(kmsKey)) {
      skip(SERVER_SIDE_ENCRYPTION_KEY + " is not set for " +
              SSE_KMS.getMethod());
    }
    super.setup();
  }

  @Override
  protected String getBlockOutputBufferName() {
    return Constants.FAST_UPLOAD_BUFFER_ARRAY;
  }

  /**
   * @param fileSystem
   * @return true if {@link Constants#SERVER_SIDE_ENCRYPTION_KEY} is set
   * in the config.
   */
  @Override
  protected boolean isEncrypted(S3AFileSystem fileSystem) {
    Configuration c = new Configuration();
    return c.get(SERVER_SIDE_ENCRYPTION_KEY) != null;
  }

  @Override
  protected void assertEncrypted(Path hugeFile) throws IOException {
    Configuration c = new Configuration();
    String kmsKey = c.get(SERVER_SIDE_ENCRYPTION_KEY);
    EncryptionTestUtils.assertEncrypted(getFileSystem(), hugeFile,
            SSE_KMS, kmsKey);
  }
}
