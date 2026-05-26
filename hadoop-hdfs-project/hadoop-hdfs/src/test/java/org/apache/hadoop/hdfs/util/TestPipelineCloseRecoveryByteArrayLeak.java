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
package org.apache.hadoop.hdfs.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClientFaultInjector;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.util.ByteArrayManager.ManagerMap;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Regression test for HDFS-17916: when the streamer hits an error in the
 * PIPELINE_CLOSE stage and recovers via processDatanodeOrExternalError(),
 * the end-of-block DFSPacket's buffer must be returned to the
 * {@link ByteArrayManager}; otherwise it is leaked.
 */
public class TestPipelineCloseRecoveryByteArrayLeak {

  @Test
  @Timeout(120)
  public void itReleasesEndOfBlockBufferAfterPipelineCloseRecovery()
      throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(
        HdfsClientConfigKeys.Write.ByteArrayManager.ENABLED_KEY, true);
    // Threshold 0 means the FixedLengthManager is created on the very first
    // allocation of a given length. The end-of-block DFSPacket uses its own
    // (smaller) buffer length, distinct from data packets, so we need every
    // allocation to be tracked from the start.
    conf.setInt(
        HdfsClientConfigKeys.Write.ByteArrayManager.COUNT_THRESHOLD_KEY, 0);
    conf.setInt(
        HdfsClientConfigKeys.BlockWrite.LOCATEFOLLOWINGBLOCK_RETRIES_KEY, 3);

    MiniDFSCluster cluster = null;
    DFSClientFaultInjector originalFaultInjector = null;
    try {
      originalFaultInjector = DFSClientFaultInjector.get();
      DFSClientFaultInjector faultInjector = new DFSClientFaultInjector() {
        @Override
        public boolean failPacket() {
          // Force every last-in-block ack to be reported as failed; this is what
          // drives the streamer through processDatanodeOrExternalError() with
          // stage == PIPELINE_CLOSE.
          return true;
        }
      };
      DFSClientFaultInjector.set(faultInjector);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();

      Path file = new Path("/pipelineCloseRecoveryLeak.dat");
      DFSTestUtil.createFile(fs, file, 1024 * 1024L, (short) 3, 0L);

      ByteArrayManager bam =
          fs.getClient().getClientContext().getByteArrayManager();
      assertThat(bam)
          .describedAs("expected bounded ByteArrayManager")
          .isInstanceOf(ByteArrayManager.Impl.class);

      ManagerMap managers = ((ByteArrayManager.Impl) bam).getManagers();
      // After the writer closes, every DFSPacket that the streamer pulled
      // off the dataQueue must have had its buffer recycled. Without the
      // HDFS-17916 fix, the end-of-block packet from the PIPELINE_CLOSE
      // recovery path leaks one buffer, so countAllocated() stays > 0.
      // release() is performed by the streamer thread, so allow a brief
      // moment for that thread to settle after close() returns.
      GenericTestUtils.waitFor(() -> managers.countAllocated() == 0, 50, 5000);
      assertThat(managers.countAllocated())
          .describedAs("count allocated")
          .isEqualTo(0);
    } finally {
      if (originalFaultInjector != null) {
        DFSClientFaultInjector.set(originalFaultInjector);
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
