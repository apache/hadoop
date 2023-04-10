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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;

import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.Constants;

import static org.apache.hadoop.fs.contract.ContractTestUtils.IO_CHUNK_BUFFER_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.REQUEST_TIMEOUT;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestPropertyBytes;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_PUT_REQUESTS;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticCounter;

/**
 * Test a file upload using a single PUT operation. Multipart uploads will
 * be disabled in the test.
 */
public class ITestS3AHugeFileUploadSinglePut extends S3AScaleTestBase{
  final private Logger LOG = LoggerFactory.getLogger(
      ITestS3AHugeFileUploadSinglePut.class.getName());

  private long fileSize;
  @Override
  protected Configuration createScaleConfiguration() {
    Configuration configuration = super.createScaleConfiguration();
    configuration.setBoolean(Constants.MULTIPART_UPLOADS_ENABLED, false);
    configuration.setLong(MULTIPART_SIZE, 53687091200L);
    configuration.setInt(KEY_TEST_TIMEOUT, 36000);
    configuration.setInt(IO_CHUNK_BUFFER_SIZE, 655360);
    configuration.set(REQUEST_TIMEOUT, "1h");
    fileSize = getTestPropertyBytes(configuration, KEY_HUGE_FILESIZE,
        DEFAULT_HUGE_FILESIZE);
    return configuration;
  }

  @Test
  public void uploadFileSinglePut() throws IOException {
    LOG.info("Creating file with size : {}", fileSize);
    S3AFileSystem fs = getFileSystem();
    ContractTestUtils.createAndVerifyFile(fs,
        getTestPath(), fileSize);
    //No more than three put requests should be made during the upload of the file
    //First one being the creation of test/ directory marker
    //Second being the creation of the file with tests3ascale/<file-name>
    //Third being the creation of directory marker tests3ascale/ on the file delete
    assertThatStatisticCounter(fs.getIOStatistics(), OBJECT_PUT_REQUESTS.getSymbol())
        .isEqualTo(3);
  }
}
