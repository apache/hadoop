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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.io.IOUtils;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Random;

/**
 * CosNInputStream Tester.
 */
public class TestCosNInputStream {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestCosNInputStream.class);

  private FileSystem fs;

  private Path testRootDir;

  @Before
  public void setUp() throws IOException {
    Configuration configuration = new Configuration();
    this.fs = CosNTestUtils.createTestFileSystem(configuration);
    this.testRootDir = CosNTestUtils.createTestPath(new Path("/test"));
    LOG.info("test root dir: " + this.testRootDir);
  }

  @After
  public void tearDown() throws IOException {
    if (null != this.fs) {
      this.fs.delete(this.testRootDir, true);
    }
  }

  /**
   * Method: seek(long pos).
   */
  @Test
  public void testSeek() throws Exception {
    Path seekTestFilePath = new Path(this.testRootDir + "/"
        + "seekTestFile");
    long fileSize = 5 * Unit.MB;

    ContractTestUtils.generateTestFile(
        this.fs, seekTestFilePath, fileSize, 256, 255);
    LOG.info("5MB file for seek test has created.");

    FSDataInputStream inputStream = this.fs.open(seekTestFilePath);
    int seekTimes = 5;
    for (int i = 0; i != seekTimes; i++) {
      long pos = fileSize / (seekTimes - i) - 1;
      inputStream.seek(pos);
      assertTrue("expected position at: " +
              pos + ", but got: " + inputStream.getPos(),
          inputStream.getPos() == pos);
      LOG.info("completed seeking at pos: " + inputStream.getPos());
    }
    LOG.info("begin to random position seeking test...");
    Random random = new Random();
    for (int i = 0; i < seekTimes; i++) {
      long pos = Math.abs(random.nextLong()) % fileSize;
      LOG.info("seeking for pos: " + pos);
      inputStream.seek(pos);
      assertTrue("expected position at: " +
              pos + ", but got: " + inputStream.getPos(),
          inputStream.getPos() == pos);
      LOG.info("completed seeking at pos: " + inputStream.getPos());
    }
  }

  /**
   * Method: getPos().
   */
  @Test
  public void testGetPos() throws Exception {
    Path seekTestFilePath = new Path(this.testRootDir + "/" +
        "seekTestFile");
    long fileSize = 5 * Unit.MB;
    ContractTestUtils.generateTestFile(
        this.fs, seekTestFilePath, fileSize, 256, 255);
    LOG.info("5MB file for getPos test has created.");

    FSDataInputStream inputStream = this.fs.open(seekTestFilePath);
    Random random = new Random();
    long pos = Math.abs(random.nextLong()) % fileSize;
    inputStream.seek(pos);
    assertTrue("expected position at: " +
            pos + ", but got: " + inputStream.getPos(),
        inputStream.getPos() == pos);
    LOG.info("completed get pos tests.");
  }

  /**
   * Method: seekToNewSource(long targetPos).
   */
  @Ignore("Not ready yet")
  public void testSeekToNewSource() throws Exception {
    LOG.info("Currently it is not supported to " +
        "seek the offset in a new source.");
  }

  /**
   * Method: read().
   */
  @Test
  public void testRead() throws Exception {
    final int bufLen = 256;
    Path readTestFilePath = new Path(this.testRootDir + "/"
        + "testReadSmallFile.txt");
    long fileSize = 5 * Unit.MB;

    ContractTestUtils.generateTestFile(
        this.fs, readTestFilePath, fileSize, 256, 255);
    LOG.info("read test file: " + readTestFilePath + " has created.");

    FSDataInputStream inputStream = this.fs.open(readTestFilePath);
    byte[] buf = new byte[bufLen];
    long bytesRead = 0;
    while (bytesRead < fileSize) {
      int bytes = 0;
      if (fileSize - bytesRead < bufLen) {
        int remaining = (int) (fileSize - bytesRead);
        bytes = inputStream.read(buf, 0, remaining);
      } else {
        bytes = inputStream.read(buf, 0, bufLen);
      }
      bytesRead += bytes;

      if (bytesRead % (1 * Unit.MB) == 0) {
        int available = inputStream.available();
        assertTrue("expected remaining: " + (fileSize - bytesRead) +
            " but got: " + available, (fileSize - bytesRead) == available);
        LOG.info("Bytes read: " +
            Math.round((double) bytesRead / Unit.MB) + "MB");
      }
    }

    assertTrue(inputStream.available() == 0);
    IOUtils.closeStream(inputStream);
  }
}
