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

package org.apache.hadoop.fs.contract;

import java.util.Collections;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.extractStatistics;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.demandStringifyIOStatisticsSource;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.snapshotIOStatistics;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_BYTES;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_WRITE_BYTES;

/**
 * Tests {@link IOStatistics} support in input and output streams.
 * <p>
 * Requires both the input and output streams to offer the basic
 * bytes read/written statistics.
 * </p>
 * If the IO is buffered, that information must be provided,
 * especially the input buffer size.
 */
public abstract class AbstractContractStreamIOStatisticsTest
    extends AbstractFSContractTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractContractStreamIOStatisticsTest.class);

  /**
   * FileSystem statistics are collected across every test case.
   */
  protected static final IOStatisticsSnapshot FILESYSTEM_IOSTATS =
      snapshotIOStatistics();

  @Override
  public void teardown() throws Exception {
    final FileSystem fs = getFileSystem();
    if (fs instanceof IOStatisticsSource) {
      FILESYSTEM_IOSTATS.aggregate(((IOStatisticsSource)fs).getIOStatistics());
    }
    super.teardown();
  }

  /**
   * Dump the filesystem statistics after the class if contains any values.
   */
  @AfterClass
  public static void dumpFileSystemIOStatistics() {
    if (!FILESYSTEM_IOSTATS.counters().isEmpty()) {
      // if there is at least one counter
      LOG.info("Aggregate FileSystem Statistics {}",
          ioStatisticsToPrettyString(FILESYSTEM_IOSTATS));
    }
  }

  @Test
  public void testOutputStreamStatisticKeys() throws Throwable {
    describe("Look at the statistic keys of an output stream");
    Path path = methodPath();
    FileSystem fs = getFileSystem();
    fs.mkdirs(path.getParent());
    try (FSDataOutputStream out = fs.create(path, true)) {
      IOStatistics statistics = extractStatistics(out);
      final List<String> keys = outputStreamStatisticKeys();
      Assertions.assertThat(statistics.counters().keySet())
          .describedAs("statistic keys of %s", statistics)
          .containsAll(keys);
      Assertions.assertThat(keys)
          .describedAs("Statistics supported by the stream %s", out)
          .contains(STREAM_WRITE_BYTES);
    } finally {
      fs.delete(path, false);
    }
  }

  /**
   * If the stream writes in blocks, then counters during the write may be
   * zero until a whole block is written -or the write has finished.
   * @return true if writes are buffered into whole blocks.
   */
  public boolean streamWritesInBlocks() {
    return false;
  }

  @Test
  public void testWriteSingleByte() throws Throwable {
    describe("Write a byte to a file and verify"
        + " the stream statistics are updated");
    Path path = methodPath();
    FileSystem fs = getFileSystem();
    fs.mkdirs(path.getParent());
    boolean writesInBlocks = streamWritesInBlocks();
    try (FSDataOutputStream out = fs.create(path, true)) {
      IOStatistics statistics = extractStatistics(out);
      // before a write, no bytes
      verifyStatisticCounterValue(statistics, STREAM_WRITE_BYTES, 0);
      out.write('0');
      verifyStatisticCounterValue(statistics, STREAM_WRITE_BYTES,
          writesInBlocks ? 0 : 1);
      // close the stream
      out.close();
      // statistics are still valid after the close
      // always call the output stream to check that behavior
      statistics = extractStatistics(out);
      final String strVal = statistics.toString();
      LOG.info("Statistics = {}", strVal);
      verifyStatisticCounterValue(statistics, STREAM_WRITE_BYTES, 1);
    } finally {
      fs.delete(path, false);
    }
  }

  @Test
  public void testWriteByteArrays() throws Throwable {
    describe("Write byte arrays to a file and verify"
        + " the stream statistics are updated");
    Path path = methodPath();
    FileSystem fs = getFileSystem();
    fs.mkdirs(path.getParent());
    boolean writesInBlocks = streamWritesInBlocks();
    try (FSDataOutputStream out = fs.create(path, true)) {
      Object demandStatsString = demandStringifyIOStatisticsSource(out);
      // before a write, no bytes
      final byte[] bytes = ContractTestUtils.toAsciiByteArray(
          "statistically-speaking");
      final long len = bytes.length;
      out.write(bytes);
      out.flush();
      LOG.info("stats {}", demandStatsString);
      IOStatistics statistics = extractStatistics(out);
      verifyStatisticCounterValue(statistics, STREAM_WRITE_BYTES,
          writesInBlocks ? 0 : len);
      out.write(bytes);
      out.flush();
      verifyStatisticCounterValue(statistics, STREAM_WRITE_BYTES,
          writesInBlocks ? 0 : len * 2);
      // close the stream
      out.close();
      LOG.info("stats {}", demandStatsString);
      // statistics are still valid after the close
      // always call the output stream to check that behavior
      statistics = extractStatistics(out);
      verifyStatisticCounterValue(statistics, STREAM_WRITE_BYTES, len * 2);
      // the to string value must contain the same counterHiCable you mean
      Assertions.assertThat(demandStatsString.toString())
          .contains(Long.toString(len * 2));
    } finally {
      fs.delete(path, false);
    }
  }

  @Test
  public void testInputStreamStatisticKeys() throws Throwable {
    describe("Look at the statistic keys of an input stream");
    Path path = methodPath();
    FileSystem fs = getFileSystem();
    ContractTestUtils.touch(fs, path);
    try (FSDataInputStream in = fs.open(path)) {
      IOStatistics statistics = extractStatistics(in);
      final List<String> keys = inputStreamStatisticKeys();
      Assertions.assertThat(statistics.counters().keySet())
          .describedAs("statistic keys of %s", statistics)
          .containsAll(keys);
      Assertions.assertThat(keys)
          .describedAs("Statistics supported by the stream %s", in)
          .contains(STREAM_READ_BYTES);
      verifyStatisticCounterValue(statistics, STREAM_READ_BYTES, 0);
    } finally {
      fs.delete(path, false);
    }
  }

  @Test
  public void testInputStreamStatisticRead() throws Throwable {
    describe("Read Data from an input stream");
    Path path = methodPath();
    FileSystem fs = getFileSystem();
    final int fileLen = 1024;
    final byte[] ds = dataset(fileLen, 'a', 26);
    ContractTestUtils.writeDataset(fs, path, ds, fileLen, 8_000, true);

    try (FSDataInputStream in = fs.open(path)) {
      long current = 0;
      IOStatistics statistics = extractStatistics(in);
      verifyStatisticCounterValue(statistics, STREAM_READ_BYTES, 0);
      Assertions.assertThat(in.read()).isEqualTo('a');
      int bufferSize = readBufferSize();
      // either a single byte was read or a whole block
      current = verifyBytesRead(statistics, current, 1, bufferSize);
      final int bufferLen = 128;
      byte[] buf128 = new byte[bufferLen];
      in.read(buf128);
      current = verifyBytesRead(statistics, current, bufferLen, bufferSize);
      in.readFully(buf128);
      current = verifyBytesRead(statistics, current, bufferLen, bufferSize);
      in.readFully(0, buf128);
      current = verifyBytesRead(statistics, current, bufferLen, bufferSize);
      // seek must not increment the read counter
      in.seek(256);
      verifyBytesRead(statistics, current, 0, bufferSize);

      // if a stream implements lazy-seek the seek operation
      // may be postponed until the read
      final int sublen = 32;
      Assertions.assertThat(in.read(buf128, 0, sublen))
          .isEqualTo(sublen);
      current = verifyBytesRead(statistics, current, sublen, bufferSize);

      // perform some read operations near the end of the file such that
      // the buffer will not be completely read.
      // skip these tests for buffered IO as it is too complex to work out
      if (bufferSize == 0) {
        final int pos = fileLen - sublen;
        in.seek(pos);
        Assertions.assertThat(in.read(buf128))
            .describedAs("Read overlapping EOF")
            .isEqualTo(sublen);
        current = verifyStatisticCounterValue(statistics, STREAM_READ_BYTES,
            current + sublen);
        Assertions.assertThat(in.read(pos, buf128, 0, bufferLen))
            .describedAs("Read(buffer) overlapping EOF")
            .isEqualTo(sublen);
        verifyStatisticCounterValue(statistics, STREAM_READ_BYTES,
            current + sublen);
      }
    } finally {
      fs.delete(path, false);
    }
  }

  /**
   * Verify the bytes read value, taking into account block size.
   * @param statistics stats
   * @param current current count
   * @param bytesRead bytes explicitly read
   * @param bufferSize buffer size of stream
   * @return the current count of bytes read <i>ignoring block size</i>
   */
  public long verifyBytesRead(final IOStatistics statistics,
      final long current,
      final int bytesRead, final int bufferSize) {
    // final position. for unbuffered read, this is the expected value
    long finalPos = current + bytesRead;
    long expected = finalPos;
    if (bufferSize > 0) {
      // buffered. count of read is number of buffers already read
      // plus the current buffer, multiplied by that buffer size
      expected = bufferSize * (1 + (current / bufferSize));
    }
    verifyStatisticCounterValue(statistics, STREAM_READ_BYTES, expected);
    return finalPos;
  }

  /**
   * Buffer size for reads.
   * Filesystems performing block reads (checksum, etc)
   * must return their buffer value is
   * @return buffer capacity; 0 for unbuffered
   */
  public int readBufferSize() {
    return 0;
  }

  /**
   * Keys which the output stream must support.
   * @return a list of keys
   */
  public List<String> outputStreamStatisticKeys() {
    return Collections.singletonList(STREAM_WRITE_BYTES);
  }

  /**
   * Keys which the input stream must support.
   * @return a list of keys
   */
  public List<String> inputStreamStatisticKeys() {
    return Collections.singletonList(STREAM_READ_BYTES);
  }

}
