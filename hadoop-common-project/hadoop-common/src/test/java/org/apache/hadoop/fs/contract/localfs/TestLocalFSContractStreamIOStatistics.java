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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractStreamIOStatisticsTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

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
  public int readBufferSize() {
    return 1024;
  }

  @Override
  public boolean streamWritesInBlocks() {
    return true;
  }


}
