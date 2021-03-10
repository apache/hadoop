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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AInputPolicy;
import org.apache.hadoop.fs.s3a.S3AInputStream;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.MeanStatistic;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.LineReader;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assume;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticMinimum;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.lookupMaximumStatistic;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.lookupMeanStatistic;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsSourceToString;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToString;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.snapshotIOStatistics;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.ACTION_HTTP_GET_REQUEST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_MAX;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_MEAN;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_MIN;

/**
 * Look at the performance of S3a operations.
 */
public class ITestS3AInputStreamPerformance extends S3AScaleTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(
      ITestS3AInputStreamPerformance.class);

  private S3AFileSystem s3aFS;
  private Path testData;
  private FileStatus testDataStatus;
  private FSDataInputStream in;
  private S3AInputStreamStatistics streamStatistics;
  public static final int BLOCK_SIZE = 32 * 1024;
  public static final int BIG_BLOCK_SIZE = 256 * 1024;

  private static final IOStatisticsSnapshot IOSTATS = snapshotIOStatistics();


  /** Tests only run if the there is a named test file that can be read. */
  private boolean testDataAvailable = true;
  private String assumptionMessage = "test file";

  /**
   * Open the FS and the test data. The input stream is always set up here.
   * @throws IOException IO Problems.
   */
  @Before
  public void openFS() throws IOException {
    Configuration conf = getConf();
    conf.setInt(SOCKET_SEND_BUFFER, 16 * 1024);
    conf.setInt(SOCKET_RECV_BUFFER, 16 * 1024);
    String testFile =  conf.getTrimmed(KEY_CSVTEST_FILE, DEFAULT_CSVTEST_FILE);
    if (testFile.isEmpty()) {
      assumptionMessage = "Empty test property: " + KEY_CSVTEST_FILE;
      LOG.warn(assumptionMessage);
      testDataAvailable = false;
    } else {
      testData = new Path(testFile);
      LOG.info("Using {} as input stream source", testData);
      Path path = this.testData;
      bindS3aFS(path);
      try {
        testDataStatus = s3aFS.getFileStatus(this.testData);
      } catch (IOException e) {
        LOG.warn("Failed to read file {} specified in {}",
            testFile, KEY_CSVTEST_FILE, e);
        throw e;
      }
    }
  }

  private void bindS3aFS(Path path) throws IOException {
    s3aFS = (S3AFileSystem) FileSystem.newInstance(path.toUri(), getConf());
  }

  /**
   * Cleanup: close the stream, close the FS.
   */
  @After
  public void cleanup() {
    describe("cleanup");
    IOUtils.closeStream(in);
    if (in != null) {
      LOG.info("Stream statistics {}",
          ioStatisticsSourceToString(in));
      IOSTATS.aggregate(in.getIOStatistics());
    }
    if (s3aFS != null) {
      LOG.info("FileSystem statistics {}",
          ioStatisticsSourceToString(s3aFS));
      FILESYSTEM_IOSTATS.aggregate(s3aFS.getIOStatistics());
      IOUtils.closeStream(s3aFS);
    }
  }

  @AfterClass
  public static void dumpIOStatistics() {
    LOG.info("Aggregate Stream Statistics {}", IOSTATS);
  }

  /**
   * Declare that the test requires the CSV test dataset.
   */
  private void requireCSVTestData() {
    assume(assumptionMessage, testDataAvailable);
  }

  /**
   * Open the test file with the read buffer specified in the setting.
   * {@link #KEY_READ_BUFFER_SIZE}; use the {@code Normal} policy
   * @return the stream, wrapping an S3a one
   * @throws IOException IO problems
   */
  FSDataInputStream openTestFile() throws IOException {
    return openTestFile(S3AInputPolicy.Normal, 0);
  }

  /**
   * Open the test file with the read buffer specified in the setting
   * {@link #KEY_READ_BUFFER_SIZE}.
   * This includes the {@link #requireCSVTestData()} assumption; so
   * if called before any FS op, will automatically skip the test
   * if the CSV file is absent.
   *
   * @param inputPolicy input policy to use
   * @param readahead readahead/buffer size
   * @return the stream, wrapping an S3a one
   * @throws IOException IO problems
   */
  FSDataInputStream openTestFile(S3AInputPolicy inputPolicy, long readahead)
      throws IOException {
    requireCSVTestData();
    return openDataFile(s3aFS, this.testData, inputPolicy, readahead);
  }

  /**
   * Open a test file with the read buffer specified in the setting
   * {@link org.apache.hadoop.fs.s3a.S3ATestConstants#KEY_READ_BUFFER_SIZE}.
   *
   * @param path path to open
   * @param inputPolicy input policy to use
   * @param readahead readahead/buffer size
   * @return the stream, wrapping an S3a one
   * @throws IOException IO problems
   */
  private FSDataInputStream openDataFile(S3AFileSystem fs,
      Path path,
      S3AInputPolicy inputPolicy,
      long readahead) throws IOException {
    int bufferSize = getConf().getInt(KEY_READ_BUFFER_SIZE,
        DEFAULT_READ_BUFFER_SIZE);
    S3AInputPolicy policy = fs.getInputPolicy();
    fs.setInputPolicy(inputPolicy);
    try {
      FSDataInputStream stream = fs.open(path, bufferSize);
      if (readahead >= 0) {
        stream.setReadahead(readahead);
      }
      streamStatistics = getInputStreamStatistics(stream);
      return stream;
    } finally {
      fs.setInputPolicy(policy);
    }
  }

  /**
   * Assert that the stream was only ever opened once.
   */
  protected void assertStreamOpenedExactlyOnce() {
    assertOpenOperationCount(1);
  }

  /**
   * Make an assertion count about the number of open operations.
   * @param expected the expected number
   */
  private void assertOpenOperationCount(long expected) {
    assertEquals("open operations in\n" + in,
        expected, streamStatistics.getOpenOperations());
  }

  /**
   * Log how long an IOP took, by dividing the total time by the
   * count of operations, printing in a human-readable form.
   * @param operation operation being measured
   * @param timer timing data
   * @param count IOP count.
   */
  protected void logTimePerIOP(String operation,
      NanoTimer timer,
      long count) {
    LOG.info("Time per {}: {} nS",
        operation, toHuman(timer.duration() / count));
  }

  @Test
  public void testTimeToOpenAndReadWholeFileBlocks() throws Throwable {
    requireCSVTestData();
    int blockSize = _1MB;
    describe("Open the test file %s and read it in blocks of size %d",
        testData, blockSize);
    long len = testDataStatus.getLen();
    in = openTestFile();
    byte[] block = new byte[blockSize];
    NanoTimer timer2 = new NanoTimer();
    long count = 0;
    // implicitly rounding down here
    long blockCount = len / blockSize;
    long totalToRead = blockCount * blockSize;
    long minimumBandwidth = 128 * 1024;
    int maxResetCount = 4;
    int resetCount = 0;
    for (long i = 0; i < blockCount; i++) {
      int offset = 0;
      int remaining = blockSize;
      long blockId = i + 1;
      NanoTimer blockTimer = new NanoTimer();
      int reads = 0;
      while (remaining > 0) {
        NanoTimer readTimer = new NanoTimer();
        int bytesRead = in.read(block, offset, remaining);
        reads++;
        if (bytesRead == 1) {
          break;
        }
        remaining -= bytesRead;
        offset += bytesRead;
        count += bytesRead;
        readTimer.end();
        if (bytesRead != 0) {
          LOG.debug("Bytes in read #{}: {} , block bytes: {}," +
                  " remaining in block: {}" +
                  " duration={} nS; ns/byte: {}, bandwidth={} MB/s",
              reads, bytesRead, blockSize - remaining, remaining,
              readTimer.duration(),
              readTimer.nanosPerOperation(bytesRead),
              readTimer.bandwidthDescription(bytesRead));
        } else {
          LOG.warn("0 bytes returned by read() operation #{}", reads);
        }
      }
      blockTimer.end("Reading block %d in %d reads", blockId, reads);
      String bw = blockTimer.bandwidthDescription(blockSize);
      LOG.info("Bandwidth of block {}: {} MB/s: ", blockId, bw);
      if (bandwidth(blockTimer, blockSize) < minimumBandwidth) {
        LOG.warn("Bandwidth {} too low on block {}: resetting connection",
            bw, blockId);
        Assert.assertTrue("Bandwidth of " + bw +" too low after  "
            + resetCount + " attempts", resetCount <= maxResetCount);
        resetCount++;
        // reset the connection
        getS3AInputStream(in).resetConnection();
      }
    }
    timer2.end("Time to read %d bytes in %d blocks", totalToRead, blockCount);
    LOG.info("Overall Bandwidth {} MB/s; reset connections {}",
        timer2.bandwidth(totalToRead), resetCount);
    logStreamStatistics();
  }

  /**
   * Work out the bandwidth in bytes/second.
   * @param timer timer measuring the duration
   * @param bytes bytes
   * @return the number of bytes/second of the recorded operation
   */
  public static double bandwidth(NanoTimer timer, long bytes) {
    return bytes * 1.0e9 / timer.duration();
  }

  @Test
  public void testLazySeekEnabled() throws Throwable {
    describe("Verify that seeks do not trigger any IO");
    in = openTestFile();
    long len = testDataStatus.getLen();
    NanoTimer timer = new NanoTimer();
    long blockCount = len / BLOCK_SIZE;
    for (long i = 0; i < blockCount; i++) {
      in.seek(in.getPos() + BLOCK_SIZE - 1);
    }
    in.seek(0);
    blockCount++;
    timer.end("Time to execute %d seeks", blockCount);
    logTimePerIOP("seek()", timer, blockCount);
    logStreamStatistics();
    assertOpenOperationCount(0);
    assertEquals("bytes read", 0, streamStatistics.getBytesRead());
  }

  @Test
  public void testReadaheadOutOfRange() throws Throwable {
    try {
      in = openTestFile();
      in.setReadahead(-1L);
      fail("Stream should have rejected the request "+ in);
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testReadWithNormalPolicy() throws Throwable {
    describe("Read big blocks with a big readahead");
    executeSeekReadSequence(BIG_BLOCK_SIZE, BIG_BLOCK_SIZE * 2,
        S3AInputPolicy.Normal);
    assertStreamOpenedExactlyOnce();
  }

  @Test
  public void testDecompressionSequential128K() throws Throwable {
    describe("Decompress with a 128K readahead");
    executeDecompression(128 * _1KB, S3AInputPolicy.Sequential);
    assertStreamOpenedExactlyOnce();
  }

  /**
   * Execute a decompression + line read with the given input policy.
   * @param readahead byte readahead
   * @param inputPolicy read policy
   * @throws IOException IO Problems
   */
  private void executeDecompression(long readahead,
      S3AInputPolicy inputPolicy) throws IOException {
    CompressionCodecFactory factory
        = new CompressionCodecFactory(getConf());
    CompressionCodec codec = factory.getCodec(testData);
    long bytesRead = 0;
    int lines = 0;

    FSDataInputStream objectIn = openTestFile(inputPolicy, readahead);
    IOStatistics readerStatistics = null;
    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    try (LineReader lineReader = new LineReader(
        codec.createInputStream(objectIn), getConf())) {
      readerStatistics = lineReader.getIOStatistics();
      Text line = new Text();
      int read;
      while ((read = lineReader.readLine(line)) > 0) {
        bytesRead += read;
        lines++;
      }
    } catch (EOFException eof) {
      // done
    }
    timer.end("Time to read %d lines [%d bytes expanded, %d raw]" +
        " with readahead = %d",
        lines,
        bytesRead,
        testDataStatus.getLen(),
        readahead);
    logTimePerIOP("line read", timer, lines);
    logStreamStatistics();
    assertNotNull("No IOStatistics through line reader", readerStatistics);
    LOG.info("statistics from reader {}",
        ioStatisticsToString(readerStatistics));
  }

  private void logStreamStatistics() {
    LOG.info(String.format("Stream Statistics%n{}"), streamStatistics);
  }

  /**
   * Execute a seek+read sequence.
   * @param blockSize block size for seeks
   * @param readahead what the readahead value of the stream should be
   * @throws IOException IO problems
   */
  protected void executeSeekReadSequence(long blockSize,
      long readahead,
      S3AInputPolicy policy) throws IOException {
    in = openTestFile(policy, readahead);
    long len = testDataStatus.getLen();
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
    logTimePerIOP("seek(pos + " + blockCount+"); read()", timer, blockCount);
    LOG.info("Effective bandwidth {} MB/S",
        timer.bandwidthDescription(streamStatistics.getBytesRead() -
            streamStatistics.getBytesSkippedOnSeek()));
    logStreamStatistics();
  }

  public static final int _4K = 4 * 1024;
  public static final int _8K = 8 * 1024;
  public static final int _16K = 16 * 1024;
  public static final int _32K = 32 * 1024;
  public static final int _64K = 64 * 1024;
  public static final int _128K = 128 * 1024;
  public static final int _256K = 256 * 1024;
  public static final int _1MB = 1024 * 1024;
  public static final int _2MB = 2 * _1MB;
  public static final int _10MB = _1MB * 10;
  public static final int _5MB = _1MB * 5;

  private static final int[][] RANDOM_IO_SEQUENCE = {
      {_2MB, _128K},
      {_128K, _128K},
      {_5MB, _64K},
      {_1MB, _1MB},
  };

  @Test
  public void testRandomIORandomPolicy() throws Throwable {
    executeRandomIO(S3AInputPolicy.Random, (long) RANDOM_IO_SEQUENCE.length);
    assertEquals("streams aborted in " + streamStatistics,
        0, streamStatistics.getAborted());
  }

  @Test
  public void testRandomIONormalPolicy() throws Throwable {
    long expectedOpenCount = RANDOM_IO_SEQUENCE.length;
    executeRandomIO(S3AInputPolicy.Normal, expectedOpenCount);
    assertEquals("streams aborted in " + streamStatistics,
        1, streamStatistics.getAborted());
    assertEquals("policy changes in " + streamStatistics,
        2, streamStatistics.getPolicySetCount());
    assertEquals("input policy in " + streamStatistics,
        S3AInputPolicy.Random.ordinal(),
        streamStatistics.getInputPolicy());
    IOStatistics ioStatistics = streamStatistics.getIOStatistics();
    verifyStatisticCounterValue(
        ioStatistics,
        StreamStatisticNames.STREAM_READ_ABORTED,
        1);
    verifyStatisticCounterValue(
        ioStatistics,
        StreamStatisticNames.STREAM_READ_SEEK_POLICY_CHANGED,
        2);
  }

  /**
   * Execute the random IO {@code readFully(pos, bytes[])} sequence defined by
   * {@link #RANDOM_IO_SEQUENCE}. The stream is closed afterwards; that's used
   * in the timing too
   * @param policy read policy
   * @param expectedOpenCount expected number of stream openings
   * @throws IOException IO problems
   * @return the timer
   */
  private ContractTestUtils.NanoTimer executeRandomIO(S3AInputPolicy policy,
      long expectedOpenCount)
      throws IOException {
    describe("Random IO with policy \"%s\"", policy);
    byte[] buffer = new byte[_1MB];
    long totalBytesRead = 0;

    in = openTestFile(policy, 0);
    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    for (int[] action : RANDOM_IO_SEQUENCE) {
      int position = action[0];
      int range = action[1];
      in.readFully(position, buffer, 0, range);
      totalBytesRead += range;
    }
    int reads = RANDOM_IO_SEQUENCE.length;
    timer.end("Time to execute %d reads of total size %d bytes",
        reads,
        totalBytesRead);
    in.close();
    assertOpenOperationCount(expectedOpenCount);
    logTimePerIOP("byte read", timer, totalBytesRead);
    LOG.info("Effective bandwidth {} MB/S",
        timer.bandwidthDescription(streamStatistics.getBytesRead() -
            streamStatistics.getBytesSkippedOnSeek()));
    logStreamStatistics();
    IOStatistics iostats = in.getIOStatistics();
    long maxHttpGet = lookupMaximumStatistic(iostats,
        ACTION_HTTP_GET_REQUEST + SUFFIX_MAX);
    assertThatStatisticMinimum(iostats,
        ACTION_HTTP_GET_REQUEST + SUFFIX_MIN)
        .isGreaterThan(0)
        .isLessThan(maxHttpGet);
    MeanStatistic getMeanStat = lookupMeanStatistic(iostats,
        ACTION_HTTP_GET_REQUEST + SUFFIX_MEAN);
    Assertions.assertThat(getMeanStat.getSamples())
        .describedAs("sample count of %s", getMeanStat)
        .isEqualTo(expectedOpenCount);

    return timer;
  }

  S3AInputStream getS3aStream() {
    return (S3AInputStream) in.getWrappedStream();
  }

  @Test
  public void testRandomReadOverBuffer() throws Throwable {
    describe("read over a buffer, making sure that the requests" +
        " spans readahead ranges");
    int datasetLen = _32K;
    S3AFileSystem fs = getFileSystem();
    Path dataFile = path("testReadOverBuffer.bin");
    byte[] sourceData = dataset(datasetLen, 0, 64);
    // relies on the field 'fs' referring to the R/W FS
    writeDataset(fs, dataFile, sourceData, datasetLen, _16K, true);
    byte[] buffer = new byte[datasetLen];
    int readahead = _8K;
    int halfReadahead = _4K;
    in = openDataFile(fs, dataFile, S3AInputPolicy.Random, readahead);

    LOG.info("Starting initial reads");
    S3AInputStream s3aStream = getS3aStream();
    assertEquals(readahead, s3aStream.getReadahead());
    byte[] oneByte = new byte[1];
    assertEquals(1, in.read(0, oneByte, 0, 1));
    // make some assertions about the current state
    assertEquals("remaining in\n" + in,
        readahead - 1, s3aStream.remainingInCurrentRequest());
    assertEquals("range start in\n" + in,
        0, s3aStream.getContentRangeStart());
    assertEquals("range finish in\n" + in,
        readahead, s3aStream.getContentRangeFinish());

    assertStreamOpenedExactlyOnce();

    describe("Starting sequence of positioned read calls over\n%s", in);
    NanoTimer readTimer = new NanoTimer();
    int currentPos = halfReadahead;
    int offset = currentPos;
    int bytesRead = 0;
    int readOps = 0;

    // make multiple read() calls
    while (bytesRead < halfReadahead) {
      int length = buffer.length - offset;
      int read = in.read(currentPos, buffer, offset, length);
      bytesRead += read;
      offset += read;
      readOps++;
      assertEquals("open operations on request #" + readOps
              + " after reading " + bytesRead
              + " current position in stream " + currentPos
              + " in\n" + fs
              + "\n " + in,
          1, streamStatistics.getOpenOperations());
      for (int i = currentPos; i < currentPos + read; i++) {
        assertEquals("Wrong value from byte " + i,
            sourceData[i], buffer[i]);
      }
      currentPos += read;
    }
    assertStreamOpenedExactlyOnce();
    // assert at the end of the original block
    assertEquals(readahead, currentPos);
    readTimer.end("read %d in %d operations", bytesRead, readOps);
    bandwidth(readTimer, bytesRead);
    LOG.info("Time per byte(): {} nS",
        toHuman(readTimer.nanosPerOperation(bytesRead)));
    LOG.info("Time per read(): {} nS",
        toHuman(readTimer.nanosPerOperation(readOps)));

    describe("read last byte");
    // read one more
    int read = in.read(currentPos, buffer, bytesRead, 1);
    assertTrue("-1 from last read", read >= 0);
    assertOpenOperationCount(2);
    assertEquals("Wrong value from read ", sourceData[currentPos],
        (int) buffer[currentPos]);
    currentPos++;


    // now scan all the way to the end of the file, using single byte read()
    // calls
    describe("read() to EOF over \n%s", in);
    long readCount = 0;
    NanoTimer timer = new NanoTimer();
    LOG.info("seeking");
    in.seek(currentPos);
    LOG.info("reading");
    while(currentPos < datasetLen) {
      int r = in.read();
      assertTrue("Negative read() at position " + currentPos + " in\n" + in,
          r >= 0);
      buffer[currentPos] = (byte)r;
      assertEquals("Wrong value from read from\n" + in,
          sourceData[currentPos], r);
      currentPos++;
      readCount++;
    }
    timer.end("read %d bytes", readCount);
    bandwidth(timer, readCount);
    LOG.info("Time per read(): {} nS",
        toHuman(timer.nanosPerOperation(readCount)));

    assertEquals("last read in " + in, -1, in.read());
  }
}
