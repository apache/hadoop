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
package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests for the improved flushOrSync slow-log message (HDFS-17805).
 *
 * The fix separates the combined flushTotalNanos into distinct flushNanos and
 * syncNanos fields in the WARN log, enabling operators to pinpoint whether
 * latency originates from flush or sync operations.
 */
public class TestBlockReceiverSlowLog {

  /**
   * HDFS-17805: When flushOrSync exceeds the slow-IO threshold the WARN log
   * must contain both {@code flushNanos=} and {@code syncNanos=} fields so
   * that operators can distinguish flush latency from sync latency.
   */
  @Test
  @Timeout(value = 60)
  public void testFlushOrSyncSlowLogContainsSeparateFlushAndSyncNanos()
      throws IOException, InterruptedException {
    Configuration conf = new HdfsConfiguration();
    // Set threshold to 0 ms so every flushOrSync call triggers the WARN log.
    conf.setLong(DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_KEY, 0);

    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();

      GenericTestUtils.LogCapturer logs =
          GenericTestUtils.LogCapturer.captureLogs(BlockReceiver.LOG);

      // Write data and hsync to trigger flushOrSync(isSync=true).
      try (FSDataOutputStream out = cluster.getFileSystem()
          .create(new Path("/test-slow-log.dat"))) {
        out.write(new byte[1024]);
        out.hsync();
      }

      String captured = logs.getOutput();

      // The improved log must expose flush and sync timings separately.
      assertTrue(captured.contains("flushNanos="),
          "Slow flushOrSync log must contain 'flushNanos=' for flush timing. "
              + "Got: " + captured);
      assertTrue(captured.contains("syncNanos="),
          "Slow flushOrSync log must contain 'syncNanos=' for sync timing. "
              + "Got: " + captured);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
