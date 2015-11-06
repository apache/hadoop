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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Demonstrate that the threadpool blocks additional client requests if
 * its queue is full (rather than throwing an exception) by initiating an
 * upload consisting of 4 parts with 2 threads and 1 spot in the queue. The
 * 4th part should not trigger an exception as it would with a
 * non-blocking threadpool.
 */
public class TestS3ABlockingThreadPool {

  private Configuration conf;
  private S3AFileSystem fs;

  @Rule
  public Timeout testTimeout = new Timeout(30 * 60 * 1000);

  protected Path getTestPath() {
    return new Path("/tests3a");
  }

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setLong(Constants.MIN_MULTIPART_THRESHOLD, 5 * 1024 * 1024);
    conf.setLong(Constants.MULTIPART_SIZE, 5 * 1024 * 1024);
    conf.setInt(Constants.MAX_THREADS, 2);
    conf.setInt(Constants.MAX_TOTAL_TASKS, 1);
  }

  @After
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.delete(getTestPath(), true);
    }
  }

  @Test
  public void testRegularMultiPartUpload() throws Exception {
    fs = S3ATestUtils.createTestFileSystem(conf);
    ContractTestUtils.createAndVerifyFile(fs, getTestPath(), 16 * 1024 *
        1024);
  }

  @Test
  public void testFastMultiPartUpload() throws Exception {
    conf.setBoolean(Constants.FAST_UPLOAD, true);
    fs = S3ATestUtils.createTestFileSystem(conf);
    ContractTestUtils.createAndVerifyFile(fs, getTestPath(), 16 * 1024 *
        1024);

  }
}
