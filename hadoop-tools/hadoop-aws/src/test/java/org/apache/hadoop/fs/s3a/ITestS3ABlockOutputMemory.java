/**
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.apache.hadoop.fs.s3a.Constants.*;

import java.io.IOException;

/**
 * Tests regular and multi-part upload functionality for
 * {@link S3ABlockOutputStream} with the block buffered in memory.
 *
 * File sizes are kept small to reduce test duration on slow connections.
 */
public class ITestS3ABlockOutputMemory extends AbstractS3ATestBase {

  @Rule
  public Timeout testTimeout = new Timeout(30 * 60 * 1000);

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    S3ATestUtils.disableFilesystemCaching(conf);
    conf.setLong(MIN_MULTIPART_THRESHOLD, MULTIPART_MIN_SIZE);
    conf.setInt(MULTIPART_SIZE, MULTIPART_MIN_SIZE);
    conf.setBoolean(BLOCK_OUTPUT, true);
    conf.set(BLOCK_OUTPUT_BUFFER, getBlockOutputBufferName());
    return conf;
  }

  protected String getBlockOutputBufferName() {
    return BLOCK_OUTPUT_BUFFER_ARRAY;
  }

  @Test
  public void testRegularUpload() throws IOException {
    verifyUpload("regular", 1024);
  }

  @Test
  public void testMultiPartUpload() throws IOException {
    verifyUpload("multipart", MULTIPART_MIN_SIZE + 1024 * 1024);
  }

  public void verifyUpload(String name, int fileSize) throws IOException {
    Path dest = path(name);
    describe(name + " upload to " + dest);
    ContractTestUtils.createAndVerifyFile(
        getFileSystem(),
        dest,
        fileSize);
  }
}
