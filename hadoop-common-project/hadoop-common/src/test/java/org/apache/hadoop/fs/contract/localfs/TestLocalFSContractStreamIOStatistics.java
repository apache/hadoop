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

package org.apache.hadoop.fs.contract.localfs;

import java.util.Arrays;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractContractStreamIOStatisticsTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.statistics.IOStatistics;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.extractStatistics;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticValue;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_BYTES;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_EXCEPTIONS;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_SEEK_OPERATIONS;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_SKIP_BYTES;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_SKIP_OPERATIONS;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_WRITE_BYTES;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_WRITE_EXCEPTIONS;

/**
 * Test IOStatistics through the local FS.
 */
public class TestLocalFSContractStreamIOStatistics extends
    AbstractContractStreamIOStatisticsTest {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new LocalFSContract(conf);
  }

  /**
   * Keys which the input stream must support.
   * @return a list of keys
   */
  public List<String> inputStreamStatisticKeys() {
    return Arrays.asList(STREAM_READ_BYTES,
        STREAM_READ_EXCEPTIONS,
        STREAM_READ_SEEK_OPERATIONS,
        STREAM_READ_SKIP_OPERATIONS,
        STREAM_READ_SKIP_BYTES);
  }

  /**
   * Keys which the output stream must support.
   * @return a list of keys
   */
  @Override
  public List<String> outputStreamStatisticKeys() {
    return Arrays.asList(STREAM_WRITE_BYTES,
        STREAM_WRITE_EXCEPTIONS);
  }

  @Override
  public int readBlockSize() {
    return 1024;
  }

  @Override
  public boolean streamWritesInBlocks() {
    return true;
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
      int blockSize = readBlockSize();
      current = verifyStatisticValue(statistics, STREAM_READ_BYTES, blockSize);
    }
  }
}
