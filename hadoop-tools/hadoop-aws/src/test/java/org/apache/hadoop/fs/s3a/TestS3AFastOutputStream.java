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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Tests regular and multi-part upload functionality for S3AFastOutputStream.
 * File sizes are kept small to reduce test duration on slow connections
 */
public class TestS3AFastOutputStream {
  private FileSystem fs;


  @Rule
  public Timeout testTimeout = new Timeout(30 * 60 * 1000);

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(Constants.MIN_MULTIPART_THRESHOLD, 5 * 1024 * 1024);
    conf.setInt(Constants.MULTIPART_SIZE, 5 * 1024 * 1024);
    conf.setBoolean(Constants.FAST_UPLOAD, true);
    fs = S3ATestUtils.createTestFileSystem(conf);
  }

  @After
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.delete(getTestPath(), true);
    }
  }

  protected Path getTestPath() {
    return new Path("/tests3a");
  }

  @Test
  public void testRegularUpload() throws IOException {
    ContractTestUtils.createAndVerifyFile(fs, getTestPath(), 1024 * 1024);
  }

  @Test
  public void testMultiPartUpload() throws IOException {
    ContractTestUtils.createAndVerifyFile(fs, getTestPath(), 6 * 1024 *
        1024);
  }
}
