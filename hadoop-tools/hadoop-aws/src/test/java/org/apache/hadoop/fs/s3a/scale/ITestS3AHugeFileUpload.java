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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.Constants;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.IO_CHUNK_BUFFER_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_SIZE;

public class ITestS3AHugeFileUpload extends S3AScaleTestBase{
  final private Logger LOG = LoggerFactory.getLogger(
      ITestS3AHugeFileUpload.class.getName());

  private long fileSize = Integer.MAX_VALUE * 2L;
  @Override
  protected Configuration createScaleConfiguration() {
    Configuration configuration = super.createScaleConfiguration();
    configuration.setBoolean(Constants.ALLOW_MULTIPART_UPLOADS, false);
    configuration.setLong(MULTIPART_SIZE, 53687091200L);
    configuration.setInt(KEY_TEST_TIMEOUT, 36000);
    configuration.setInt(IO_CHUNK_BUFFER_SIZE, 655360);
    configuration.set("fs.s3a.connection.request.timeout", "1h");
    return configuration;
  }

  @Test
  public void uploadFileSinglePut() throws IOException {
    LOG.info("Creating file with size : {}", fileSize);
    ContractTestUtils.createAndVerifyFile(getFileSystem(),
        getTestPath(), fileSize );
  }
}
