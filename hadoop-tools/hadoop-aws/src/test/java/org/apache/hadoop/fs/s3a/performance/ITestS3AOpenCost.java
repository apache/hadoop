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

package org.apache.hadoop.fs.s3a.performance;


import java.io.EOFException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.RangeNotSatisfiableEOFException;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.statistics.IOStatistics;

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_RANDOM;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_LENGTH;
import static org.apache.hadoop.fs.contract.ContractTestUtils.readStream;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeTextFile;
import static org.apache.hadoop.fs.s3a.Statistic.STREAM_READ_BYTES_READ_CLOSE;
import static org.apache.hadoop.fs.s3a.Statistic.STREAM_READ_OPENED;
import static org.apache.hadoop.fs.s3a.Statistic.STREAM_READ_SEEK_BYTES_SKIPPED;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.NO_HEAD_OR_LIST;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertDurationRange;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.extractStatistics;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.demandStringifyIOStatistics;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.ACTION_FILE_OPENED;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Cost of openFile().
 */
public class ITestS3AOpenCost extends AbstractS3ACostTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3AOpenCost.class);

  public static final String TEXT = "0123456789ABCDEF";

  private Path testFile;

  private FileStatus testFileStatus;

  private long fileLength;

  public ITestS3AOpenCost() {
    super(true);
  }

  /**
   * Setup creates a test file, saves is status and length
   * to fields.
   */
  @Override
  public void setup() throws Exception {
    super.setup();
    S3AFileSystem fs = getFileSystem();
    testFile = methodPath();

    writeTextFile(fs, testFile, TEXT, true);
    testFileStatus = fs.getFileStatus(testFile);
    fileLength = testFileStatus.getLen();
  }

  /**
   * Test when openFile() performs GET requests when file status
   * and length options are passed down.
   * Note that the input streams only update the FS statistics
   * in close(), so metrics cannot be verified until all operations
   * on a stream are complete.
   * This is slightly less than ideal.
   */
  @Test
  public void testOpenFileWithStatusOfOtherFS() throws Throwable {
    describe("Test cost of openFile with/without status; raw only");
    S3AFileSystem fs = getFileSystem();

    // now read that file back in using the openFile call.
    // with a new FileStatus and a different path.
    // this verifies that any FileStatus class/subclass is used
    // as a source of the file length.
    FileStatus st2 = new FileStatus(
        fileLength, false,
        testFileStatus.getReplication(),
        testFileStatus.getBlockSize(),
        testFileStatus.getModificationTime(),
        testFileStatus.getAccessTime(),
        testFileStatus.getPermission(),
        testFileStatus.getOwner(),
        testFileStatus.getGroup(),
        new Path("gopher:///localhost/" + testFile.getName()));

    // no IO in open
    FSDataInputStream in = verifyMetrics(() ->
            fs.openFile(testFile)
                .withFileStatus(st2)
                .build()
                .get(),
        always(NO_HEAD_OR_LIST),
        with(STREAM_READ_OPENED, 0));

    // the stream gets opened during read
    long readLen = verifyMetrics(() ->
            readStream(in),
        always(NO_HEAD_OR_LIST),
        with(STREAM_READ_OPENED, 1));
    assertEquals("bytes read from file", fileLength, readLen);
  }

  @Test
  public void testOpenFileShorterLength() throws Throwable {
    // do a second read with the length declared as short.
    // we now expect the bytes read to be shorter.
    S3AFileSystem fs = getFileSystem();

    S3ATestUtils.MetricDiff bytesDiscarded =
        new S3ATestUtils.MetricDiff(fs, STREAM_READ_BYTES_READ_CLOSE);
    int offset = 2;
    long shortLen = fileLength - offset;
    // open the file
    FSDataInputStream in2 = openFile(shortLen,
            FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL);

    // verify that the statistics are in range
    IOStatistics ioStatistics = extractStatistics(in2);
    Object statsString = demandStringifyIOStatistics(ioStatistics);
    LOG.info("Statistics of open stream {}", statsString);
    verifyStatisticCounterValue(ioStatistics, ACTION_FILE_OPENED, 1);
    // no network IO happened, duration is 0. There's a very small
    // risk of some delay making it positive just from scheduling delays
    assertDurationRange(ioStatistics, ACTION_FILE_OPENED, 0, 0);
    // now read it
    long r2 = verifyMetrics(() ->
            readStream(in2),
        always(NO_HEAD_OR_LIST),
        with(STREAM_READ_OPENED, 1),
        with(STREAM_READ_BYTES_READ_CLOSE, 0),
        with(STREAM_READ_SEEK_BYTES_SKIPPED, 0));

    LOG.info("Statistics of read stream {}", statsString);

    assertEquals("bytes read from file", shortLen, r2);
    // no bytes were discarded.
    bytesDiscarded.assertDiffEquals(0);
  }

  @Test
  public void testOpenFileLongerLengthReadFully() throws Throwable {
    // do a read with the length declared as longer
    // than it is.
    // An EOF will be read on readFully(), -1 on a read()

    final int extra = 10;
    long longLen = fileLength + extra;


    // assert behaviors of seeking/reading past the file length.
    // there is no attempt at recovery.
    verifyMetrics(() -> {
          try (FSDataInputStream in = openFile(longLen,
              FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL)) {
            byte[] out = new byte[(int) (longLen)];
            intercept(EOFException.class,
                () -> in.readFully(0, out));
            in.seek(longLen - 1);
            assertEquals("read past real EOF on " + in,
                -1, in.read());
            return in.toString();
          }
        },
        // two GET calls were made, one for readFully,
        // the second on the read() past the EOF
        // the operation has got as far as S3
        with(STREAM_READ_OPENED, 2));

    // now on a new stream, try a full read from after the EOF
    verifyMetrics(() -> {
          try (FSDataInputStream in = openFile(longLen,
              FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL)) {
            byte[] out = new byte[extra];
            intercept(EOFException.class,
                () -> in.readFully(fileLength, out));
            return in.toString();
          }
        },
        // two GET calls were made, one for readFully,
        // the second on the read() past the EOF
        // the operation has got as far as S3
        with(STREAM_READ_OPENED, 2));


  }

  /**
   * Open a file.
   * @param longLen length to declare
   * @param policy read policy
   * @return file handle
   */
  private FSDataInputStream openFile(final long longLen, String policy)
      throws Exception {
    S3AFileSystem fs = getFileSystem();
    // set a length past the actual file length
    return verifyMetrics(() ->
            fs.openFile(testFile)
                .must(FS_OPTION_OPENFILE_READ_POLICY, policy)
                .mustLong(FS_OPTION_OPENFILE_LENGTH, longLen)
                .build()
                .get(),
        always(NO_HEAD_OR_LIST));
  }

  /**
   * Read a file read() by read and expect it all to work through the file, -1 afterwards.
   */
  @Test
  public void testOpenFileLongerLengthReadCalls() throws Throwable {

    // set a length past the actual file length
    final int extra = 10;
    long longLen = fileLength + extra;
    try (FSDataInputStream in = openFile(longLen,
        FS_OPTION_OPENFILE_READ_POLICY_RANDOM)) {
      for (int i = 0; i < fileLength; i++) {
        assertEquals("read() at " + i,
            TEXT.charAt(i), in.read());
      }
    }

    // now open and read after the EOF; this is
    // expected to return -1 on each read; there's a GET per call.
    // as the counters are updated on close(), the stream must be closed
    // within the verification clause.
    // note how there's no attempt to alter file expected length...
    // instead the call always goes to S3.
    // there's no information in the exception from the SDK
    describe("reading past the end of the file");

    verifyMetrics(() -> {
          try (FSDataInputStream in =
                   openFile(longLen, FS_OPTION_OPENFILE_READ_POLICY_RANDOM)) {
            for (int i = 0; i < extra; i++) {
              final long p = fileLength + i;
              in.seek(p);

              assertEquals("read() at " + p,
                  -1, in.read());
            }
            return in.toString();
          }

        },
        with(Statistic.ACTION_HTTP_GET_REQUEST, extra));

    // now, next corner case. Do a block read() of more bytes than the file length.
    try (FSDataInputStream in =
                       openFile(longLen, FS_OPTION_OPENFILE_READ_POLICY_RANDOM)) {

    }

  }
}
