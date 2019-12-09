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

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.junit.Test;

/**
 * This class tests various cases where faults are injected to DataNode.
 */
public class TestDataNodeFaultInjector {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestDataNodeFaultInjector.class);

  private static class MetricsDataNodeFaultInjector
      extends DataNodeFaultInjector {

    public static final long DELAY = 2000;
    private long delayMs = 0;
    private final String err = "Interrupted while sleeping. Bailing out.";
    private long delayTries = 1;

    void delayOnce() throws IOException {
      if (delayTries > 0) {
        delayTries--;
        try {
          Thread.sleep(DELAY);
        } catch (InterruptedException ie) {
          throw new IOException(err);
        }
      }
    }

    long getDelayMs() {
      return delayMs;
    }

    void logDelay(final long duration) {
      /**
       * delay should be at least longer than DELAY, otherwise, delayXYZ is
       * no-op
       */
      if (duration >= DELAY) {
        this.delayMs = duration;
      }
    }
  }

  @Test(timeout = 60000)
  public void testDelaySendingAckToUpstream() throws Exception {
    final MetricsDataNodeFaultInjector mdnFaultInjector =
        new MetricsDataNodeFaultInjector() {
          @Override
          public void delaySendingAckToUpstream(final String upstreamAddr)
              throws IOException {
            delayOnce();
          }

          @Override
          public void logDelaySendingAckToUpstream(final String upstreamAddr,
              final long delayMs) throws IOException {
            logDelay(delayMs);
          }
        };
    verifyFaultInjectionDelayPipeline(mdnFaultInjector);
  }

  @Test(timeout = 60000)
  public void testDelaySendingPacketDownstream() throws Exception {
    final MetricsDataNodeFaultInjector mdnFaultInjector =
        new MetricsDataNodeFaultInjector() {
          @Override
          public void stopSendingPacketDownstream(final String mirrAddr)
              throws IOException {
            delayOnce();
          }

          @Override
          public void logDelaySendingPacketDownstream(final String mirrAddr,
              final long delayMs) throws IOException {
            logDelay(delayMs);
          }
        };
    verifyFaultInjectionDelayPipeline(mdnFaultInjector);
  }

  private void verifyFaultInjectionDelayPipeline(
      final MetricsDataNodeFaultInjector mdnFaultInjector) throws Exception {

    final Path baseDir = new Path(
        PathUtils.getTestDir(getClass()).getPath(),
        GenericTestUtils.getMethodName());
    final DataNodeFaultInjector oldDnInjector = DataNodeFaultInjector.get();
    DataNodeFaultInjector.set(mdnFaultInjector);

    final Configuration conf = new HdfsConfiguration();

    /*
     * MetricsDataNodeFaultInjector.DELAY/2 ms is viewed as slow.
     */
    final long datanodeSlowLogThresholdMs = MetricsDataNodeFaultInjector.DELAY
        / 2;
    conf.setLong(DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_KEY,
        datanodeSlowLogThresholdMs);
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.toString());

    /**
     * configure to avoid resulting in pipeline failure due to read socket
     * timeout
     */
    conf.setLong(HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY,
        MetricsDataNodeFaultInjector.DELAY * 2);
    conf.setBoolean(
        HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY,
        true);
    conf.set(
        HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY,
        "ALWAYS");

    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
      cluster.waitActive();

      final FileSystem fs = cluster.getFileSystem();
      try (FSDataOutputStream out = fs
          .create(new Path(baseDir, "test.data"), (short) 2)) {
        out.write(0x31);
        out.hflush();
        out.hsync();
      }
      LOG.info("delay info: " + mdnFaultInjector.getDelayMs() + ":"
          + datanodeSlowLogThresholdMs);
      assertTrue("Injected delay should be longer than the configured one",
          mdnFaultInjector.getDelayMs() > datanodeSlowLogThresholdMs);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      DataNodeFaultInjector.set(oldDnInjector);
    }
  }
}
