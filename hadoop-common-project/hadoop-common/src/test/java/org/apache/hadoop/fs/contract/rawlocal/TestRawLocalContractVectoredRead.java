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

package org.apache.hadoop.fs.contract.rawlocal;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.contract.AbstractContractVectoredReadTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.statistics.IOStatistics;

import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticCounter;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_BYTES;
import static org.assertj.core.api.Assertions.assertThat;

@ParameterizedClass(name = "buffer-{0}")
@MethodSource("params")
public class TestRawLocalContractVectoredRead extends AbstractContractVectoredReadTest {

  private long initialBytesRead;

  public TestRawLocalContractVectoredRead(final String bufferType) {
    super(bufferType);
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new RawlocalFSContract(conf);
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    initialBytesRead = getBytesRead();
  }

  /**
   * API is deprecated, but Spark uses it, and it's how the regression was found.
   * this is how the production code looks at our stats.
   * @return counter of bytes read across all stores. Never reset.
   */
  private static long getBytesRead() {
    AtomicLong bytes = new AtomicLong();
    FileSystem.getAllStatistics().forEach(st -> bytes.addAndGet(st.getBytesRead()));
    return bytes.get();
  }

  /**
   * Add some custom checks of bytes read counts.
   * @param in active input stream.
   */
  @Override
  protected void assertionsWithinTestVectoredReadMultipleRanges(final FSDataInputStream in) {
    long currentBytesRead = getBytesRead();
    assertThat(currentBytesRead)
        .describedAs("bytes read in stream %s", in)
        .isGreaterThan(initialBytesRead);
    final long diff = currentBytesRead - initialBytesRead;
    final IOStatistics stats = in.getIOStatistics();
    assertThatStatisticCounter(stats, STREAM_READ_BYTES)
        .describedAs(STREAM_READ_BYTES + " in bytes read in stream %s", stats)
        .isEqualTo(diff);

  }
}
