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
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.statistics.IOStatistics;

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY;
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

    writeTextFile(fs, testFile, "openfile", true);
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
    FSDataInputStream in2 = verifyMetrics(() ->
            fs.openFile(testFile)
                .must(FS_OPTION_OPENFILE_READ_POLICY,
                    FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL)
                .mustLong(FS_OPTION_OPENFILE_LENGTH, shortLen)
                .build()
                .get(),
        always(NO_HEAD_OR_LIST),
        with(STREAM_READ_OPENED, 0));

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
  public void testOpenFileLongerLength() throws Throwable {
    // do a second read with the length declared as longer
    // than it is.
    // An EOF will be read on readFully(), -1 on a read()

    S3AFileSystem fs = getFileSystem();
    // set a length past the actual file length
    long longLen = fileLength + 10;
    FSDataInputStream in3 = verifyMetrics(() ->
            fs.openFile(testFile)
                .must(FS_OPTION_OPENFILE_READ_POLICY,
                    FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL)
                .mustLong(FS_OPTION_OPENFILE_LENGTH, longLen)
                .build()
                .get(),
        always(NO_HEAD_OR_LIST));

    // assert behaviors of seeking/reading past the file length.
    // there is no attempt at recovery.
    verifyMetrics(() -> {
      byte[] out = new byte[(int) longLen];
      intercept(EOFException.class,
          () -> in3.readFully(0, out));
      in3.seek(longLen - 1);
      assertEquals("read past real EOF on " + in3,
          -1, in3.read());
      in3.close();
      return in3.toString();
    },
        // two GET calls were made, one for readFully,
        // the second on the read() past the EOF
        // the operation has got as far as S3
        with(STREAM_READ_OPENED, 2));

  }
}
