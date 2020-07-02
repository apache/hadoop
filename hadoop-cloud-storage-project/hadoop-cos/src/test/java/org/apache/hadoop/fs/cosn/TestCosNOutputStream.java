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
package org.apache.hadoop.fs.cosn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.junit.rules.Timeout;

import java.io.IOException;

/**
 * CosNOutputStream Tester.
 * <p>
 * If the test.fs.cosn.name property is not set, all test case will fail.
 */
public class TestCosNOutputStream {
  private FileSystem fs;
  private Path testRootDir;

  @Rule
  public Timeout timeout = new Timeout(3600 * 1000);

  @Before
  public void setUp() throws Exception {
    Configuration configuration = new Configuration();
    configuration.setInt(
        CosNConfigKeys.COSN_BLOCK_SIZE_KEY, 2 * Unit.MB);
    configuration.setLong(
        CosNConfigKeys.COSN_UPLOAD_BUFFER_SIZE_KEY,
        CosNConfigKeys.DEFAULT_UPLOAD_BUFFER_SIZE);
    this.fs = CosNTestUtils.createTestFileSystem(configuration);
    this.testRootDir = new Path("/test");
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testEmptyFileUpload() throws IOException {
    ContractTestUtils.createAndVerifyFile(this.fs, this.testRootDir, 0);
  }

  @Test
  public void testSingleFileUpload() throws IOException {
    ContractTestUtils.createAndVerifyFile(
        this.fs, this.testRootDir, 1 * Unit.MB - 1);
    ContractTestUtils.createAndVerifyFile(
        this.fs, this.testRootDir, 1 * Unit.MB);
    ContractTestUtils.createAndVerifyFile(
        this.fs, this.testRootDir, 2 * Unit.MB - 1);
  }

  @Test
  public void testLargeFileUpload() throws IOException {
    ContractTestUtils.createAndVerifyFile(
        this.fs, this.testRootDir, 2 * Unit.MB);
    ContractTestUtils.createAndVerifyFile(
        this.fs, this.testRootDir, 2 * Unit.MB + 1);
    ContractTestUtils.createAndVerifyFile(
        this.fs, this.testRootDir, 100 * Unit.MB);
    // In principle, a maximum boundary test (file size: 2MB * 10000 - 1)
    // should be provided here,
    // but it is skipped due to network bandwidth and test time constraints.
  }
}
