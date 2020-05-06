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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatistics;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.extractStatistics;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticValue;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_BYTES;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_WRITE_BYTES;

/**
 * Tests {@link IOStatistics} support in input streams.
 * Requires both the input and output streams to offer statistics.
 */
public abstract class AbstractContractStreamIOStatisticsTest
    extends AbstractFSContractTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractContractStreamIOStatisticsTest.class);

  @Test
  public void testOutputStreamStatisticKeys() throws Throwable {
    describe("Look at the statistic keys of an output stream");
    Path path = methodPath();
    FileSystem fs = getFileSystem();
    fs.mkdirs(path.getParent());
    try (FSDataOutputStream out = fs.create(path, true)) {
      IOStatistics statistics = extractStatistics(out);
      final List<String> keys = outputStreamStatisticKeys();
      Assertions.assertThat(statistics.keys())
          .describedAs("statistic keys of %s", statistics)
          .containsAll(keys);
      Assertions.assertThat(keys)
          .describedAs("Statistics supported by the stream %s", out)
          .contains(STREAM_WRITE_BYTES);
    } finally {
      fs.delete(path, false);
    }
  }

  @Test
  public void testWriteSingleByte() throws Throwable {
    describe("Write a byte to a file and verify"
        + " the stream statistics are updated");
    Path path = methodPath();
    FileSystem fs = getFileSystem();
    fs.mkdirs(path.getParent());
    try (FSDataOutputStream out = fs.create(path, true)) {
      IOStatistics statistics = extractStatistics(out);
      // before a write, no bytes
      verifyStatisticValue(statistics, STREAM_WRITE_BYTES, 0);
      out.write('0');
      verifyStatisticValue(statistics, STREAM_WRITE_BYTES, 1);
      // close the stream
      out.close();
      // statistics are still valid after the close
      // always call the output stream to check that behavior
      statistics = extractStatistics(out);
      final String strVal = statistics.toString();
      LOG.info("Statistics = {}", strVal);
      verifyStatisticValue(statistics, STREAM_WRITE_BYTES, 1);
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
    try (FSDataOutputStream out = fs.create(path, true)) {
      // before a write, no bytes
      final byte[] bytes = ContractTestUtils.toAsciiByteArray(
          "statistically-speaking");
      final int len = bytes.length;
      out.write(bytes);
      IOStatistics statistics = extractStatistics(out);
      verifyStatisticValue(statistics, STREAM_WRITE_BYTES, len);
      out.write(bytes);
      verifyStatisticValue(statistics, STREAM_WRITE_BYTES, len * 2);
      // close the stream
      out.close();
      // statistics are still valid after the close
      // always call the output stream to check that behavior
      statistics = extractStatistics(out);
      verifyStatisticValue(statistics, STREAM_WRITE_BYTES, len * 2);
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
      Assertions.assertThat(statistics.keys())
          .describedAs("statistic keys of %s", statistics)
          .containsAll(keys);
      Assertions.assertThat(keys)
          .describedAs("Statistics supported by the stream %s", in)
          .contains(STREAM_READ_BYTES);
      verifyStatisticValue(statistics, STREAM_READ_BYTES, 0);
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
      verifyStatisticValue(statistics, STREAM_READ_BYTES, 0);
      Assertions.assertThat(in.read()).isEqualTo('a');
      current = verifyStatisticValue(statistics, STREAM_READ_BYTES, 1);
      final int bufferLen = 128;
      byte[] buf128 = new byte[bufferLen];
      in.read(buf128);
      current = verifyStatisticValue(statistics, STREAM_READ_BYTES, current +
          +bufferLen);
      in.readFully(buf128);
      current = verifyStatisticValue(statistics, STREAM_READ_BYTES, current
          + bufferLen);
      in.readFully(0, buf128);
      current = verifyStatisticValue(statistics, STREAM_READ_BYTES, current
          + bufferLen);
      // seek must not increment the read counter
      in.seek(256);
      verifyStatisticValue(statistics, STREAM_READ_BYTES, current);

      // if a stream implements lazy-seek the seek operation
      // may be postponed until the read
      final int sublen = 32;
      Assertions.assertThat(in.read(buf128, 0, sublen))
          .isEqualTo(sublen);
      current = verifyStatisticValue(statistics, STREAM_READ_BYTES,
          current + sublen);

      // perform some read operations near the end of the file such that
      // the buffer will not be completely read.
      final int pos = fileLen - sublen;
      in.seek(pos);
      Assertions.assertThat(in.read(buf128))
          .describedAs("Read overlapping EOF")
          .isEqualTo(sublen);
      current = verifyStatisticValue(statistics, STREAM_READ_BYTES,
          current + sublen);
      Assertions.assertThat(in.read(pos, buf128, 0, bufferLen))
          .describedAs("Read(buffer) overlapping EOF")
          .isEqualTo(sublen);
      current = verifyStatisticValue(statistics, STREAM_READ_BYTES,
          current + sublen);
    } finally {
      fs.delete(path, false);
    }
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
