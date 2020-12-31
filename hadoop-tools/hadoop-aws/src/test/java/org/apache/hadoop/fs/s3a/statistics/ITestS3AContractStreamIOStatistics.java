/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.s3a.statistics;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractStreamIOStatisticsTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.s3a.S3AContract;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.maybeEnableS3Guard;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.*;

/**
 * Test the S3A Streams IOStatistics support.
 */
public class ITestS3AContractStreamIOStatistics extends
    AbstractContractStreamIOStatisticsTest {

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    // patch in S3Guard options
    maybeEnableS3Guard(conf);
    return conf;
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }

  /**
   * Keys which the input stream must support.
   * @return a list of keys
   */
  public List<String> inputStreamStatisticKeys() {
    return Arrays.asList(
        StreamStatisticNames.STREAM_READ_ABORTED,
        StreamStatisticNames.STREAM_READ_BYTES_DISCARDED_ABORT,
        StreamStatisticNames.STREAM_READ_CLOSED,
        StreamStatisticNames.STREAM_READ_BYTES_DISCARDED_CLOSE,
        StreamStatisticNames.STREAM_READ_CLOSE_OPERATIONS,
        StreamStatisticNames.STREAM_READ_OPENED,
        StreamStatisticNames.STREAM_READ_BYTES,
        StreamStatisticNames.STREAM_READ_EXCEPTIONS,
        StreamStatisticNames.STREAM_READ_FULLY_OPERATIONS,
        StreamStatisticNames.STREAM_READ_OPERATIONS,
        StreamStatisticNames.STREAM_READ_OPERATIONS_INCOMPLETE,
        StreamStatisticNames.STREAM_READ_VERSION_MISMATCHES,
        StreamStatisticNames.STREAM_READ_SEEK_OPERATIONS,
        StreamStatisticNames.STREAM_READ_SEEK_BACKWARD_OPERATIONS,
        StreamStatisticNames.STREAM_READ_SEEK_FORWARD_OPERATIONS,
        StreamStatisticNames.STREAM_READ_SEEK_BYTES_BACKWARDS,
        StreamStatisticNames.STREAM_READ_SEEK_BYTES_DISCARDED,
        StreamStatisticNames.STREAM_READ_SEEK_BYTES_SKIPPED
    );
  }

  /**
   * Keys which the output stream must support.
   * @return a list of keys
   */
  @Override
  public List<String> outputStreamStatisticKeys() {
    return Arrays.asList(STREAM_WRITE_BYTES,
        STREAM_WRITE_BLOCK_UPLOADS,
        STREAM_WRITE_EXCEPTIONS);
  }

}
