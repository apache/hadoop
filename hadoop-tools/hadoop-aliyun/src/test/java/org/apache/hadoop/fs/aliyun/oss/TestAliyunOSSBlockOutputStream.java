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

package org.apache.hadoop.fs.aliyun.oss;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;

import static org.apache.hadoop.fs.aliyun.oss.Constants.MULTIPART_UPLOAD_PART_SIZE_DEFAULT;
import static org.apache.hadoop.fs.contract.ContractTestUtils.IO_CHUNK_BUFFER_SIZE;
import static org.junit.Assert.assertEquals;

/**
 * Tests regular and multi-part upload functionality for
 * AliyunOSSBlockOutputStream.
 */
public class TestAliyunOSSBlockOutputStream {
  private FileSystem fs;
  private static String testRootPath =
      AliyunOSSTestUtils.generateUniqueTestPath();

  @Rule
  public Timeout testTimeout = new Timeout(30 * 60 * 1000);

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(Constants.MULTIPART_UPLOAD_PART_SIZE_KEY, 1024 * 1024);
    conf.setInt(IO_CHUNK_BUFFER_SIZE,
        conf.getInt(Constants.MULTIPART_UPLOAD_PART_SIZE_KEY, 0));
    conf.setInt(Constants.UPLOAD_ACTIVE_BLOCKS_KEY, 20);
    fs = AliyunOSSTestUtils.createTestFileSystem(conf);
  }

  @After
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.delete(new Path(testRootPath), true);
    }
  }

  private Path getTestPath() {
    return new Path(testRootPath + "/test-aliyun-oss");
  }

  @Test
  public void testZeroByteUpload() throws IOException {
    ContractTestUtils.createAndVerifyFile(fs, getTestPath(), 0);
  }

  @Test
  public void testRegularUpload() throws IOException {
    FileSystem.clearStatistics();
    long size = 1024 * 1024;
    FileSystem.Statistics statistics =
        FileSystem.getStatistics("oss", AliyunOSSFileSystem.class);
    // This test is a little complicated for statistics, lifecycle is
    // generateTestFile
    //   fs.create(getFileStatus)    read 1
    //   output stream write         write 1
    // path exists(fs.exists)        read 1
    // verifyReceivedData
    //   fs.open(getFileStatus)      read 1
    //   input stream read           read 2(part size is 512K)
    // fs.delete
    //   getFileStatus & delete & exists & create fake dir read 2, write 2
    ContractTestUtils.createAndVerifyFile(fs, getTestPath(), size - 1);
    assertEquals(7, statistics.getReadOps());
    assertEquals(size - 1, statistics.getBytesRead());
    assertEquals(3, statistics.getWriteOps());
    assertEquals(size - 1, statistics.getBytesWritten());

    ContractTestUtils.createAndVerifyFile(fs, getTestPath(), size);
    assertEquals(14, statistics.getReadOps());
    assertEquals(2 * size - 1, statistics.getBytesRead());
    assertEquals(6, statistics.getWriteOps());
    assertEquals(2 * size - 1, statistics.getBytesWritten());

    ContractTestUtils.createAndVerifyFile(fs, getTestPath(), size + 1);

    assertEquals(22, statistics.getReadOps());
    assertEquals(3 * size, statistics.getBytesRead());
    assertEquals(10, statistics.getWriteOps());
    assertEquals(3 * size, statistics.getBytesWritten());
  }

  @Test
  public void testMultiPartUpload() throws IOException {
    long size = 6 * 1024 * 1024;
    FileSystem.clearStatistics();
    FileSystem.Statistics statistics =
        FileSystem.getStatistics("oss", AliyunOSSFileSystem.class);
    ContractTestUtils.createAndVerifyFile(fs, getTestPath(), size - 1);
    assertEquals(17, statistics.getReadOps());
    assertEquals(size - 1, statistics.getBytesRead());
    assertEquals(8, statistics.getWriteOps());
    assertEquals(size - 1, statistics.getBytesWritten());

    ContractTestUtils.createAndVerifyFile(fs, getTestPath(), size);
    assertEquals(34, statistics.getReadOps());
    assertEquals(2 * size - 1, statistics.getBytesRead());
    assertEquals(16, statistics.getWriteOps());
    assertEquals(2 * size - 1, statistics.getBytesWritten());

    ContractTestUtils.createAndVerifyFile(fs, getTestPath(), size + 1);
    assertEquals(52, statistics.getReadOps());
    assertEquals(3 * size, statistics.getBytesRead());
    assertEquals(25, statistics.getWriteOps());
    assertEquals(3 * size, statistics.getBytesWritten());
  }

  @Test
  public void testMultiPartUploadConcurrent() throws IOException {
    FileSystem.clearStatistics();
    long size = 50 * 1024 * 1024 - 1;
    ContractTestUtils.createAndVerifyFile(fs, getTestPath(), size);
    FileSystem.Statistics statistics =
        FileSystem.getStatistics("oss", AliyunOSSFileSystem.class);
    assertEquals(105, statistics.getReadOps());
    assertEquals(size, statistics.getBytesRead());
    assertEquals(52, statistics.getWriteOps());
    assertEquals(size, statistics.getBytesWritten());
  }

  @Test
  public void testHugeUpload() throws IOException {
    ContractTestUtils.createAndVerifyFile(fs, getTestPath(),
        MULTIPART_UPLOAD_PART_SIZE_DEFAULT - 1);
    ContractTestUtils.createAndVerifyFile(fs, getTestPath(),
        MULTIPART_UPLOAD_PART_SIZE_DEFAULT);
    ContractTestUtils.createAndVerifyFile(fs, getTestPath(),
        MULTIPART_UPLOAD_PART_SIZE_DEFAULT + 1);
  }

  @Test
  public void testMultiPartUploadLimit() throws IOException {
    long partSize1 = AliyunOSSUtils.calculatePartSize(10 * 1024, 100 * 1024);
    assert(10 * 1024 / partSize1 < Constants.MULTIPART_UPLOAD_PART_NUM_LIMIT);

    long partSize2 = AliyunOSSUtils.calculatePartSize(200 * 1024, 100 * 1024);
    assert(200 * 1024 / partSize2 < Constants.MULTIPART_UPLOAD_PART_NUM_LIMIT);

    long partSize3 = AliyunOSSUtils.calculatePartSize(10000 * 100 * 1024,
        100 * 1024);
    assert(10000 * 100 * 1024 / partSize3
        < Constants.MULTIPART_UPLOAD_PART_NUM_LIMIT);

    long partSize4 = AliyunOSSUtils.calculatePartSize(10001 * 100 * 1024,
        100 * 1024);
    assert(10001 * 100 * 1024 / partSize4
        < Constants.MULTIPART_UPLOAD_PART_NUM_LIMIT);
  }
}
