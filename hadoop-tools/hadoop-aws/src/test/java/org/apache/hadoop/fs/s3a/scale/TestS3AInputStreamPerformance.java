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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AInstrumentation;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;

/**
 * Look at the performance of S3a operations.
 */
public class TestS3AInputStreamPerformance extends S3AScaleTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestS3AInputStreamPerformance.class);

  private S3AFileSystem s3aFS;
  private Path testData;
  private S3AFileStatus testDataStatus;
  private FSDataInputStream in;
  private S3AInstrumentation.InputStreamStatistics streamStatistics;
  public static final int BLOCK_SIZE = 32 * 1024;
  public static final int BIG_BLOCK_SIZE = 256 * 1024;

  /** Tests only run if the there is a named test file that can be read */
  private boolean testDataAvailable = true;
  private String assumptionMessage = "test file";

  /**
   * Open the FS and the test data. The input stream is always set up here.
   * @throws IOException
   */
  @Before
  public void openFS() throws IOException {
    Configuration conf = getConf();
    String testFile =  conf.getTrimmed(KEY_CSVTEST_FILE, DEFAULT_CSVTEST_FILE);
    if (testFile.isEmpty()) {
      assumptionMessage = "Empty test property: " + KEY_CSVTEST_FILE;
      testDataAvailable = false;
    } else {
      testData = new Path(testFile);
      s3aFS = (S3AFileSystem) FileSystem.newInstance(testData.toUri(), conf);
      try {
        testDataStatus = s3aFS.getFileStatus(testData);
      } catch (IOException e) {
        LOG.warn("Failed to read file {} specified in {}",
            testFile, KEY_CSVTEST_FILE, e);
        throw e;
      }
    }
  }

  /**
   * Cleanup: close the stream, close the FS.
   */
  @After
  public void cleanup() {
    IOUtils.closeStream(in);
    IOUtils.closeStream(s3aFS);
  }

  /**
   * Declare that the test requires the CSV test dataset
   */
  private void requireCSVTestData() {
    Assume.assumeTrue(assumptionMessage, testDataAvailable);
  }

  /**
   * Open the test file with the read buffer specified in the setting
   * {@link #KEY_READ_BUFFER_SIZE}
   * @return the stream, wrapping an S3a one
   * @throws IOException
   */
  FSDataInputStream openTestFile() throws IOException {
    int bufferSize = getConf().getInt(KEY_READ_BUFFER_SIZE,
        DEFAULT_READ_BUFFER_SIZE);
    FSDataInputStream stream = s3aFS.open(testData, bufferSize);
    streamStatistics = getInputStreamStatistics(stream);
    return stream;
  }

  /**
   * assert tha the stream was only ever opened once
   */
  protected void assertStreamOpenedExactlyOnce() {
    assertOpenOperationCount(1);
  }

  /**
   * Make an assertion count about the number of open operations
   * @param expected the expected number
   */
  private void assertOpenOperationCount(int expected) {
    assertEquals("open operations in " + streamStatistics,
        expected, streamStatistics.openOperations);
  }

  /**
   * Log how long an IOP took, by dividing the total time by the
   * count of operations, printing in a human-readable form
   * @param timer timing data
   * @param count IOP count.
   */
  protected void logTimePerIOP(NanoTimer timer, long count) {
    LOG.info("Time per IOP: {} nS", toHuman(timer.duration() / count));
  }

  @Test
  public void testTimeToOpenAndReadWholeFileByByte() throws Throwable {
    requireCSVTestData();
    describe("Open the test file %s and read it byte by byte", testData);
    long len = testDataStatus.getLen();
    NanoTimer timeOpen = new NanoTimer();
    in = openTestFile();
    timeOpen.end("Open stream");
    NanoTimer readTimer = new NanoTimer();
    long count = 0;
    while (in.read() >= 0) {
      count ++;
    }
    readTimer.end("Time to read %d bytes", len);
    bandwidth(readTimer, count);
    assertEquals("Not enough bytes were read)", len, count);
    long nanosPerByte = readTimer.nanosPerOperation(count);
    LOG.info("An open() call has the equivalent duration of reading {} bytes",
        toHuman( timeOpen.duration() / nanosPerByte));
  }

  @Test
  public void testTimeToOpenAndReadWholeFileBlocks() throws Throwable {
    requireCSVTestData();
    describe("Open the test file %s and read it in blocks of size %d",
        testData, BLOCK_SIZE);
    long len = testDataStatus.getLen();
    in = openTestFile();
    byte[] block = new byte[BLOCK_SIZE];
    NanoTimer timer2 = new NanoTimer();
    long count = 0;
    // implicitly rounding down here
    long blockCount = len / BLOCK_SIZE;
    for (long i = 0; i < blockCount; i++) {
      int offset = 0;
      int remaining = BLOCK_SIZE;
      NanoTimer blockTimer = new NanoTimer();
      int reads = 0;
      while (remaining > 0) {
        int bytesRead = in.read(block, offset, remaining);
        reads ++;
        if (bytesRead == 1) {
          break;
        }
        remaining -= bytesRead;
        offset += bytesRead;
        count += bytesRead;
      }
      blockTimer.end("Reading block %d in %d reads", i, reads);
    }
    timer2.end("Time to read %d bytes in %d blocks", len, blockCount );
    bandwidth(timer2, count);
    LOG.info("{}", streamStatistics);
  }

  @Test
  public void testLazySeekEnabled() throws Throwable {
    requireCSVTestData();
    describe("Verify that seeks do not trigger any IO");
    long len = testDataStatus.getLen();
    in = openTestFile();
    NanoTimer timer = new NanoTimer();
    long blockCount = len / BLOCK_SIZE;
    for (long i = 0; i < blockCount; i++) {
      in.seek(in.getPos() + BLOCK_SIZE - 1);
    }
    in.seek(0);
    blockCount++;
    timer.end("Time to execute %d seeks", blockCount);
    logTimePerIOP(timer, blockCount);
    LOG.info("{}", streamStatistics);
    assertOpenOperationCount(0);
    assertEquals("bytes read", 0, streamStatistics.bytesRead);
  }

  @Test
  public void testReadAheadDefault() throws Throwable {
    requireCSVTestData();
    describe("Verify that a series of forward skips within the readahead" +
        " range do not close and reopen the stream");
    executeSeekReadSequence(BLOCK_SIZE, Constants.DEFAULT_READAHEAD_RANGE);
    assertStreamOpenedExactlyOnce();
  }

  @Test
  public void testReadaheadOutOfRange() throws Throwable {
    requireCSVTestData();
    try {
      in = openTestFile();
      in.setReadahead(-1L);
      fail("Stream should have rejected the request "+ in);
    } catch (IllegalArgumentException e) {
      // expected
    }

  }

  @Test
  public void testReadBigBlocksAvailableReadahead() throws Throwable {
    requireCSVTestData();
    describe("set readahead to available bytes only");
    executeSeekReadSequence(BIG_BLOCK_SIZE, 0);
    // expect that the stream will have had lots of opens
    assertTrue("not enough open operations in " + streamStatistics,
        streamStatistics.openOperations > 1);
  }

  @Test
  public void testReadBigBlocksBigReadahead() throws Throwable {
    requireCSVTestData();
    describe("Read big blocks with a big readahead");
    executeSeekReadSequence(BIG_BLOCK_SIZE, BIG_BLOCK_SIZE * 2);
    assertStreamOpenedExactlyOnce();
  }

  /**
   * Execute a seek+read sequence
   * @param blockSize block size for seeks
   * @param readahead what the readahead value of the stream should be
   * @throws IOException IO problems
   */
  protected void executeSeekReadSequence(long blockSize,
      long readahead) throws IOException {
    requireCSVTestData();
    long len = testDataStatus.getLen();
    in = openTestFile();
    in.setReadahead(readahead);
    NanoTimer timer = new NanoTimer();
    long blockCount = len / blockSize;
    LOG.info("Reading {} blocks, readahead = {}",
        blockCount, readahead);
    for (long i = 0; i < blockCount; i++) {
      in.seek(in.getPos() + blockSize - 1);
      // this is the read
      assertTrue(in.read() >= 0);
    }
    timer.end("Time to execute %d seeks of distance %d with readahead = %d",
        blockCount,
        blockSize,
        readahead);
    logTimePerIOP(timer, blockCount);
    LOG.info("Effective bandwidth {} MB/S",
        timer.bandwidthDescription(streamStatistics.bytesRead -
            streamStatistics.bytesSkippedOnSeek));
    LOG.info("{}", streamStatistics);
  }

}
