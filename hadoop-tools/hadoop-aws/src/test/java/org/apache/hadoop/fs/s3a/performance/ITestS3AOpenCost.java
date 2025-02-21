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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AInputStream;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.impl.streams.InputStreamType;
import org.apache.hadoop.fs.statistics.IOStatistics;

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_FOOTER_CACHE;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_RANDOM;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_LENGTH;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_WHOLE_FILE;
import static org.apache.hadoop.fs.contract.ContractTestUtils.readStream;
import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeTextFile;
import static org.apache.hadoop.fs.s3a.Constants.CHECKSUM_VALIDATION;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assertStreamIsNotChecksummed;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableFilesystemCaching;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getS3AInputStream;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.streamType;
import static org.apache.hadoop.fs.s3a.Statistic.STREAM_READ_BYTES_READ_CLOSE;
import static org.apache.hadoop.fs.s3a.Statistic.STREAM_READ_OPENED;
import static org.apache.hadoop.fs.s3a.Statistic.STREAM_READ_SEEK_BYTES_SKIPPED;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.NO_HEAD_OR_LIST;
import static org.apache.hadoop.fs.s3a.performance.OperationCostValidator.probe;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertDurationRange;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.extractStatistics;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.demandStringifyIOStatistics;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.ACTION_FILE_OPENED;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.test.LambdaTestUtils.interceptFuture;

/**
 * Cost of openFile().
 */
public class ITestS3AOpenCost extends AbstractS3ACostTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3AOpenCost.class);

  public static final String TEXT = "0123456789ABCDEF";

  private Path testFile;

  private FileStatus testFileStatus;

  private int fileLength;

  /**
   * Is prefetching enabled?
   */
  private boolean prefetching;

  @Override
  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    removeBaseAndBucketOverrides(conf,
        CHECKSUM_VALIDATION);
    conf.setBoolean(CHECKSUM_VALIDATION, false);
    disableFilesystemCaching(conf);
    return conf;
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
    fileLength = (int)testFileStatus.getLen();
    prefetching = prefetching();
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
  public void testStreamIsNotChecksummed() throws Throwable {
    describe("Verify that an opened stream is not checksummed");

    // if prefetching is enabled, skip this test
    assumeNoPrefetching();
    S3AFileSystem fs = getFileSystem();

    // open the file
    try (FSDataInputStream in = verifyMetrics(() ->
            fs.openFile(testFile)
                .must(FS_OPTION_OPENFILE_READ_POLICY,
                    FS_OPTION_OPENFILE_READ_POLICY_WHOLE_FILE)
                .must(FS_OPTION_OPENFILE_FOOTER_CACHE, false)
                .mustLong(FS_OPTION_OPENFILE_LENGTH, fileLength)
                .build()
                .get(),
        always(NO_HEAD_OR_LIST),
        with(STREAM_READ_OPENED, 0))) {

      // open the stream.
      in.read();
      // now examine the innermost stream and make sure it doesn't have a checksum
      assertStreamIsNotChecksummed(getS3AInputStream(in));
    }
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
        intercept(EOFException.class, () -> {
          in.readFully(0, out);
          return in;
        });
        in.seek(longLen - 1);
        assertEquals("read past real EOF on " + in, -1, in.read());
        return in.toString();
      }
    },
        always(),
        // two GET calls were made, one for readFully,
        // the second on the read() past the EOF
        // the operation has got as far as S3
        probe(!prefetching(), STREAM_READ_OPENED, 1 + 1));

    // now on a new stream, try a full read from after the EOF
    verifyMetrics(() -> {
      try (FSDataInputStream in =
               openFile(longLen, FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL)) {
        byte[] out = new byte[extra];
        intercept(EOFException.class, () -> in.readFully(fileLength, out));
        return in.toString();
      }
    },
        // two GET calls were made, one for readFully,
        // the second on the read() past the EOF
        // the operation has got as far as S3

        with(STREAM_READ_OPENED, 1));
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
   * Open a file with a length declared as longer than the actual file length.
   * Validate input stream.read() semantics.
   */
  @Test
  public void testReadPastEOF() throws Throwable {

    // set a length past the actual file length
    describe("read() up to the end of the real file");
    assumeNoPrefetching();

    final int extra = 10;
    int longLen = fileLength + extra;
    try (FSDataInputStream in = openFile(longLen,
        FS_OPTION_OPENFILE_READ_POLICY_RANDOM)) {
      for (int i = 0; i < fileLength; i++) {
        Assertions.assertThat(in.read())
            .describedAs("read() at %d from stream %s", i, in)
            .isEqualTo(TEXT.charAt(i));
      }
      LOG.info("Statistics after EOF {}", ioStatisticsToPrettyString(in.getIOStatistics()));
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
          final int p = fileLength + i;
          in.seek(p);
          Assertions.assertThat(in.read())
              .describedAs("read() at %d", p)
              .isEqualTo(-1);
        }
        LOG.info("Statistics after EOF {}", ioStatisticsToPrettyString(in.getIOStatistics()));
        return in.toString();
      }
    },
        always(),
        probe(!prefetching, Statistic.ACTION_HTTP_GET_REQUEST, extra));
  }

  /**
   * Test {@code PositionedReadable.readFully()} past EOF in a file.
   */
  @Test
  public void testPositionedReadableReadFullyPastEOF() throws Throwable {
    // now, next corner case. Do a readFully() of more bytes than the file length.
    // we expect failure.
    // this codepath does a GET to the end of the (expected) file length, and when
    // that GET returns -1 from the read because the bytes returned is less than
    // expected then the readFully call fails.
    describe("PositionedReadable.readFully() past the end of the file");
    // set a length past the actual file length
    final int extra = 10;
    int longLen = fileLength + extra;
    verifyMetrics(() -> {
      try (FSDataInputStream in =
               openFile(longLen, FS_OPTION_OPENFILE_READ_POLICY_RANDOM)) {
        byte[] buf = new byte[(int) (longLen + 1)];
        // readFully will fail
        intercept(EOFException.class, () -> {
          in.readFully(0, buf);
          return in;
        });
        assertS3StreamClosed(in);
        return "readFully past EOF with statistics"
            + ioStatisticsToPrettyString(in.getIOStatistics());
      }
    },
        always(),
        probe(!prefetching, Statistic.ACTION_HTTP_GET_REQUEST, 1)); // no attempt to re-open
  }

  /**
   * Test {@code PositionedReadable.read()} past EOF in a file.
   */
  @Test
  public void testPositionedReadableReadPastEOF() throws Throwable {

    // set a length past the actual file length
    final int extra = 10;
    int longLen = fileLength + extra;

    describe("PositionedReadable.read() past the end of the file");
    assumeNoPrefetching();

    verifyMetrics(() -> {
      try (FSDataInputStream in =
               openFile(longLen, FS_OPTION_OPENFILE_READ_POLICY_RANDOM)) {
        byte[] buf = new byte[(int) (longLen + 1)];

        // readFully will read to the end of the file
        Assertions.assertThat(in.read(0, buf, 0, buf.length))
            .isEqualTo(fileLength);
        assertS3StreamOpen(in);

        // now attempt to read after EOF
        Assertions.assertThat(in.read(fileLength, buf, 0, buf.length))
            .describedAs("PositionedReadable.read() past EOF")
            .isEqualTo(-1);
        // stream is closed as part of this failure
        assertS3StreamClosed(in);

        return "PositionedReadable.read()) past EOF with " + in;
      }
    },
        always(),
        probe(!prefetching, Statistic.ACTION_HTTP_GET_REQUEST, 1)); // no attempt to re-open
  }

  /**
   * Test Vector Read past EOF in a file.
   * See related tests in {@code ITestS3AContractVectoredRead}
   */
  @Test
  public void testVectorReadPastEOF() throws Throwable {

    // set a length past the actual file length
    final int extra = 10;
    int longLen = fileLength + extra;

    describe("Vector read past the end of the file, expecting an EOFException");

    verifyMetrics(() -> {
      try (FSDataInputStream in =
               openFile(longLen, FS_OPTION_OPENFILE_READ_POLICY_RANDOM)) {
        assertS3StreamClosed(in);
        byte[] buf = new byte[longLen];
        ByteBuffer bb = ByteBuffer.wrap(buf);
        final FileRange range = FileRange.createFileRange(0, longLen);
        in.readVectored(Arrays.asList(range), (i) -> bb);
        interceptFuture(EOFException.class,
            "",
            ContractTestUtils.VECTORED_READ_OPERATION_TEST_TIMEOUT_SECONDS,
            TimeUnit.SECONDS,
            range.getData());
        assertS3StreamClosed(in);
        return "vector read past EOF with " + in;
      }
    },
        always(),
        probe(!prefetching, Statistic.ACTION_HTTP_GET_REQUEST, 1));
  }

  /**
   * Probe the FS for supporting prefetching.
   * @return true if the fs has prefetching enabled.
   */
  private boolean prefetching()  {
    return InputStreamType.Prefetch == streamType(getFileSystem());
  }

  /**
   * Skip the test if prefetching is enabled.
   */
  private void assumeNoPrefetching(){
    if (prefetching) {
      skip("Prefetching is enabled");
    }
  }

  /**
   * Assert that the inner S3 Stream is closed.
   * @param in input stream
   */
  private static void assertS3StreamClosed(final FSDataInputStream in) {
    final InputStream wrapped = in.getWrappedStream();
    if (wrapped instanceof S3AInputStream) {
      S3AInputStream s3ain = (S3AInputStream) wrapped;
      Assertions.assertThat(s3ain.isObjectStreamOpen())
          .describedAs("stream is open: %s", s3ain)
          .isFalse();
    }
  }

  /**
   * Assert that the inner S3 Stream is closed.
   * @param in input stream
   */
  private static void assertS3StreamOpen(final FSDataInputStream in) {
    final InputStream wrapped = in.getWrappedStream();
    if (wrapped instanceof S3AInputStream) {
      S3AInputStream s3ain = (S3AInputStream) wrapped;
      Assertions.assertThat(s3ain.isObjectStreamOpen())
          .describedAs("stream is closed: %s", s3ain)
          .isTrue();
    }
  }
}
