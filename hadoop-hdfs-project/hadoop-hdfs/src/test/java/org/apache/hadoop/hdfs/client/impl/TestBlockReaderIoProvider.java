/**
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
package org.apache.hadoop.hdfs.client.impl;

import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.metrics.BlockReaderIoProvider;
import org.apache.hadoop.hdfs.client.impl.metrics.BlockReaderLocalMetrics;
import org.apache.hadoop.util.FakeTimer;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.times;

/**
 * Tests {@link BlockReaderIoProvider}'s profiling of short circuit read
 * latencies.
 */
public class TestBlockReaderIoProvider {

  private static final long SLOW_READ_THRESHOLD = 5000;

  private static final FakeTimer TIMER = new FakeTimer();

  @Test(timeout = 300_000)
  public void testSlowShortCircuitReadsIsRecorded() throws IOException {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setInt(HdfsClientConfigKeys.Read.ShortCircuit
        .METRICS_SAMPLING_PERCENTAGE_KEY, 100);
    DfsClientConf clientConf = new DfsClientConf(conf);

    BlockReaderLocalMetrics metrics = Mockito.mock(
        BlockReaderLocalMetrics.class);

    FileChannel dataIn = Mockito.mock(FileChannel.class);
    Mockito.when(dataIn.read(any(ByteBuffer.class), anyLong())).thenAnswer(
        new Answer<Object>() {
          @Override
          public Object answer(InvocationOnMock invocation) throws Throwable {
            TIMER.advance(SLOW_READ_THRESHOLD);
            return 0;
          }
        });

    BlockReaderIoProvider blockReaderIoProvider = new BlockReaderIoProvider(
        clientConf.getShortCircuitConf(), metrics, TIMER);

    blockReaderIoProvider.read(dataIn, any(ByteBuffer.class), anyLong());

    Mockito.verify(metrics, times(1)).addShortCircuitReadLatency(anyLong());
  }
}
