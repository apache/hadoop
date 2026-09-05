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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for the DataNode block-transfer inactivity timeout
 * ({@code dfs.datanode.last.packet.receive.timeout.ms}).
 *
 * <p>The timeout is intentionally configured smaller than the client socket
 * timeout so that the stall threshold ({@code timeout/2 = 2.5s}) is shorter than
 * the client's idle-heartbeat interval ({@code socketTimeout/2 = 5s}); this
 * makes the abort fire deterministically before any heartbeat could reset it.
 * The abort test uses a single DataNode with datanode replacement disabled so
 * that a broken pipeline cannot be silently recovered, making the client-visible
 * failure deterministic.
 */
public class TestBlockReceiverTransferTimeout {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestBlockReceiverTransferTimeout.class);

  /** Timeout used by the tests; stall threshold is TIMEOUT_MS/2 = 2.5s. */
  private static final long TIMEOUT_MS = 5000;
  /** Client socket read timeout; idle heartbeat interval is half of this. */
  private static final int SOCKET_TIMEOUT_MS = 10000;

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;

  @AfterEach
  public void tearDown() {
    IOUtils.closeStream(fs);
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private Configuration baseConf(long timeoutMs) {
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_DATANODE_LAST_PACKET_RECEIVE_TIMEOUT_MS,
        timeoutMs);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024 * 1024); // 1 MB
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 512);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, SOCKET_TIMEOUT_MS);
    return conf;
  }

  private void startCluster(Configuration conf, int numDataNodes)
      throws IOException {
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numDataNodes)
        .build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
  }

  private static byte[] payload(int size) {
    byte[] data = new byte[size];
    for (int i = 0; i < size; i++) {
      data[i] = (byte) (i % 256);
    }
    return data;
  }

  /**
   * A genuinely stalled client (writes then stops sending, without closing) must
   * be aborted by the DataNode: the client's pipeline breaks and any further use
   * of the stream fails. This is asserted unconditionally.
   */
  @Test
  @Timeout(60)
  public void testStuckClientIsAborted() throws Exception {
    Configuration conf = baseConf(TIMEOUT_MS);
    // A single DataNode with replacement disabled means a broken pipeline cannot
    // be recovered, so the abort is deterministically visible to the client.
    conf.setBoolean(
        "dfs.client.block.write.replace-datanode-on-failure.enable", false);
    startCluster(conf, 1);

    Path testFile = new Path("/testStuckClient");
    FSDataOutputStream out = fs.create(testFile, (short) 1);
    try {
      // Send a partial block and stop; the DataNode receives no further packets.
      out.write(payload(512 * 1024));
      out.hflush();
      LOG.info("Data flushed; simulating a stuck client (no more packets).");

      // Stall threshold is 2.5s and the check polls every 2.5s, so the abort
      // fires within ~5s; wait comfortably beyond that.
      Thread.sleep(TIMEOUT_MS + TIMEOUT_MS / 2 + 3000);

      // The transfer was aborted, so continued use of the stream must fail.
      try {
        out.write(payload(512 * 1024));
        out.hflush();
        out.close();
        fail("Expected an IOException after the stalled transfer was aborted");
      } catch (IOException expected) {
        LOG.info("Got expected failure after abort: {}",
            expected.getMessage());
      }
    } finally {
      IOUtils.closeStream(out);
    }
  }

  /** A complete transfer must succeed and be readable with the timeout on. */
  @Test
  @Timeout(30)
  public void testNormalBlockTransferWithTimeoutEnabled() throws Exception {
    startCluster(baseConf(TIMEOUT_MS), 3);
    Path testFile = new Path("/testNormalTransfer");
    byte[] data = payload(512 * 1024);
    DFSTestUtil.writeFile(fs, testFile, data);

    assertTrue(fs.exists(testFile), "File should exist");
    assertEquals(data.length, fs.getFileStatus(testFile).getLen(), "File size should match");
    assertArrayEquals(data, DFSTestUtil.readFileAsBytes(fs, testFile), "File content should match");
  }

  /**
   * A slow-but-steady writer sends packets every 300ms, always within the 2.5s
   * stall threshold, so the timer keeps resetting and the transfer completes.
   */
  @Test
  @Timeout(30)
  public void testSlowWriterCompletesBeforeTimeout() throws Exception {
    startCluster(baseConf(TIMEOUT_MS), 3);
    Path testFile = new Path("/testSlowWriter");
    FSDataOutputStream out = fs.create(testFile, (short) 3);
    try {
      byte[] chunk = payload(10 * 1024);
      for (int i = 0; i < 10; i++) {
        out.write(chunk);
        out.hflush();
        Thread.sleep(300); // well within the 2.5s threshold
      }
      out.close();
      assertTrue(fs.exists(testFile), "File should exist");
      assertEquals(chunk.length * 10, fs.getFileStatus(testFile).getLen(),
          "File size should match");
    } finally {
      IOUtils.closeStream(out);
    }
  }

  /**
   * Packets arriving every 2s stay within the 2.5s stall threshold, so even
   * though the total transfer time (&gt; 6s) exceeds the 5s timeout, the timer
   * resets on each packet and the transfer completes without a false abort.
   */
  @Test
  @Timeout(40)
  public void testTimerResetsWithPacketsNearThreshold() throws Exception {
    startCluster(baseConf(TIMEOUT_MS), 3);
    Path testFile = new Path("/testTimerReset");
    FSDataOutputStream out = fs.create(testFile, (short) 3);
    try {
      byte[] chunk = payload(10 * 1024);
      // 4 chunks, 2s apart => ~6s total (> 5s timeout) but each gap < 2.5s.
      for (int i = 0; i < 4; i++) {
        out.write(chunk);
        out.hflush();
        if (i < 3) {
          Thread.sleep(2000); // 2s < 2.5s threshold => timer resets
        }
      }
      out.close();
      assertTrue(fs.exists(testFile), "File should exist");
      assertEquals(chunk.length * 4, fs.getFileStatus(testFile).getLen(), "File size should match");
    } finally {
      IOUtils.closeStream(out);
    }
  }

  /**
   * With the timeout disabled (0), even a long idle gap must not abort the
   * transfer: the stream can resume writing and close successfully.
   */
  @Test
  @Timeout(30)
  public void testTimeoutDisabledDoesNotAbortIdleStream() throws Exception {
    startCluster(baseConf(0), 3);
    Path testFile = new Path("/testTimeoutDisabled");
    FSDataOutputStream out = fs.create(testFile, (short) 3);
    try {
      byte[] chunk = payload(64 * 1024);
      out.write(chunk);
      out.hflush();
      // Idle well past what would be the stall threshold if enabled.
      Thread.sleep(TIMEOUT_MS + 2000);
      // Must still be usable because the feature is disabled.
      out.write(chunk);
      out.hflush();
      out.close();
      assertTrue(fs.exists(testFile), "File should exist");
      assertEquals(chunk.length * 2, fs.getFileStatus(testFile).getLen(), "File size should match");
    } finally {
      IOUtils.closeStream(out);
    }
  }

  /** Several full transfers with the timeout enabled must all succeed. */
  @Test
  @Timeout(30)
  public void testMultipleConcurrentTransfersWithTimeout() throws Exception {
    startCluster(baseConf(TIMEOUT_MS), 3);
    int numFiles = 5;
    for (int i = 0; i < numFiles; i++) {
      Path f = new Path("/testConcurrent" + i);
      DFSTestUtil.writeFile(fs, f, payload(256 * 1024));
      assertTrue(fs.exists(f), "File " + i + " should exist");
    }
  }
}
