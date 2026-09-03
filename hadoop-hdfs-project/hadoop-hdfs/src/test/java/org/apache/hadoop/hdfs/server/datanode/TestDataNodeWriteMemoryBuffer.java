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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for DataNode write memory buffer configuration based on number of volumes.
 */
public class TestDataNodeWriteMemoryBuffer {

  private MiniDFSCluster cluster;

  @AfterEach
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Test that write memory buffer is enabled when config is true and minVolumes is 0.
   */
  @Test
  @Timeout(60)
  public void testWriteBufferEnabledWithMinVolumesZero() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_ENABLED, true);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_MIN_VOLUMES, 0);

    // Create cluster with 2 volumes
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .storagesPerDatanode(2)
        .build();
    cluster.waitActive();

    DataNode dn = cluster.getDataNodes().get(0);
    assertTrue(dn.isWriteMemoryBufferEnabled(),
        "Write memory buffer should be enabled when minVolumes is 0");
  }

  /**
   * Test that write memory buffer is disabled when config is false regardless of volumes.
   */
  @Test
  @Timeout(60)
  public void testWriteBufferDisabledWhenConfigFalse() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_ENABLED, false);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_MIN_VOLUMES, 0);

    // Create cluster with 3 volumes
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .storagesPerDatanode(3)
        .build();
    cluster.waitActive();

    DataNode dn = cluster.getDataNodes().get(0);
    assertFalse(dn.isWriteMemoryBufferEnabled(),
        "Write memory buffer should be disabled when config is false");
  }

  /**
   * Test that write memory buffer is enabled when volumes > minVolumes.
   */
  @Test
  @Timeout(60)
  public void testWriteBufferEnabledWhenVolumesGreaterThanMin() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_ENABLED, true);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_MIN_VOLUMES, 2);

    // Create cluster with 3 volumes (3 > 2)
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .storagesPerDatanode(3)
        .build();
    cluster.waitActive();

    DataNode dn = cluster.getDataNodes().get(0);
    assertTrue(dn.isWriteMemoryBufferEnabled(),
        "Write memory buffer should be enabled when volumes (3) > "
            + "minVolumes (2)");
  }

  /**
   * Test that write memory buffer is disabled when volumes <= minVolumes.
   */
  @Test
  @Timeout(60)
  public void testWriteBufferDisabledWhenVolumesEqualToMin() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_ENABLED, true);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_MIN_VOLUMES, 2);

    // Create cluster with 2 volumes (2 == 2, not greater than)
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .storagesPerDatanode(2)
        .build();
    cluster.waitActive();

    DataNode dn = cluster.getDataNodes().get(0);
    assertFalse(dn.isWriteMemoryBufferEnabled(),
        "Write memory buffer should be disabled when volumes (2) == "
            + "minVolumes (2)");
  }

  /**
   * Test that write memory buffer is disabled when volumes < minVolumes.
   */
  @Test
  @Timeout(60)
  public void testWriteBufferDisabledWhenVolumesLessThanMin() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_ENABLED, true);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_MIN_VOLUMES, 3);

    // Create cluster with 2 volumes (2 < 3)
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .storagesPerDatanode(2)
        .build();
    cluster.waitActive();

    DataNode dn = cluster.getDataNodes().get(0);
    assertFalse(dn.isWriteMemoryBufferEnabled(),
        "Write memory buffer should be disabled when volumes (2) < "
            + "minVolumes (3)");
  }

  /**
   * Test that write memory buffer is enabled when minVolumes is negative (ignored).
   */
  @Test
  @Timeout(60)
  public void testWriteBufferEnabledWithNegativeMinVolumes() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_ENABLED, true);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_MIN_VOLUMES, -1);

    // Create cluster with 1 volume
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .storagesPerDatanode(1)
        .build();
    cluster.waitActive();

    DataNode dn = cluster.getDataNodes().get(0);
    assertTrue(dn.isWriteMemoryBufferEnabled(),
        "Write memory buffer should be enabled when minVolumes is negative "
            + "(ignored)");
  }

  /**
   * Test edge case: exactly one more volume than minimum.
   */
  @Test
  @Timeout(60)
  public void testWriteBufferEnabledWithOneMoreVolume() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_ENABLED, true);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_MIN_VOLUMES, 4);

    // Create cluster with 5 volumes (5 > 4 by exactly 1)
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .storagesPerDatanode(5)
        .build();
    cluster.waitActive();

    DataNode dn = cluster.getDataNodes().get(0);
    assertTrue(dn.isWriteMemoryBufferEnabled(),
        "Write memory buffer should be enabled when volumes (5) > "
            + "minVolumes (4)");
  }

  /**
   * Regression for codex review on PR #2012 (DataNode.java line 660):
   * {@code initWriteBufferSemaphore} must apply the same 1 MB floor to
   * {@code dfs.datanode.write.buffer.size.bytes} that
   * {@code FsVolumeImpl.initBufferWriteResource} already applies before
   * computing the permit count. Otherwise sub-1 MB configs cause the
   * semaphore to over-issue permits relative to the actual (clamped)
   * per-buffer allocation, blowing past the configured memory budget.
   *
   * <p>Scenario:
   * <ul>
   *   <li>{@code dfs.datanode.write.memory.buffer.max.capacity.mb=100}
   *       → total budget 100 MB.</li>
   *   <li>{@code dfs.datanode.write.buffer.size.bytes=524288} (512 KB,
   *       sub-1 MB) → FsVolumeImpl clamps the per-buffer allocation to
   *       1 MB.</li>
   *   <li>Permit count must be {@code 100 MB / 1 MB = 100} — NOT
   *       {@code 100 MB / 512 KB = 200} which would correspond to
   *       200 × 1 MB = 200 MB actual memory, twice the configured
   *       budget.</li>
   * </ul>
   */
  @Test
  @Timeout(60)
  public void testSemaphorePermitsMatchClampedBufferSizeForSubMbConfig()
      throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_ENABLED, true);
    // 100 MB total memory budget for buffers.
    conf.setInt(
        DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_MAX_CAPACITY_MB, 100);
    // Per-buffer config = 512 KB, sub-1 MB. FsVolumeImpl clamps to 1 MB.
    conf.setInt(DFSConfigKeys.DFS_DATANODE_WRITE_BUFFER_SIZE_BYTES,
        512 * 1024);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_MIN_VOLUMES, 0);

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();

    DataNode dn = cluster.getDataNodes().get(0);
    assertTrue(dn.isWriteMemoryBufferEnabled());
    assertEquals(100, dn.getMaxConcurrentWriteBuffers().availablePermits(),
        "initWriteBufferSemaphore must apply the same 1 MB floor as "
            + "FsVolumeImpl. With max-capacity=100 MB and a sub-1 MB "
            + "per-buffer config, permits must be 100 (=100 MB / 1 MB), "
            + "not 200 (=100 MB / 512 KB) which would correspond to "
            + "200 MB of actual memory.");
  }

  /**
   * Safe-rollout: with
   * {@link DFSConfigKeys#DFS_DATANODE_WRITE_MEMORY_BUFFER_LAST_REPLICA_ONLY}=true
   * and replication 3, the buffered-write path must run on exactly one
   * DataNode per block — the terminal (last) replica in the pipeline. The
   * earlier two replicas must use the legacy non-buffered path. This bounds
   * blast radius to a single replica if the buffered code regresses.
   *
   * <p>BlockReceiver emits "During closing buffer" on {@code DataNode.LOG}
   * only when {@code useWriteBuffer} is true for that block. With 3 DNs in
   * one JVM (MiniDFSCluster), all writes share a single logger — so a count
   * of 1 across the cluster equals "exactly the last DN used the buffer".</p>
   */
  @Test
  @Timeout(120)
  public void testWriteBufferOnlyLastReplicaInPipeline() throws Exception {
    final String bufferedPathMarker = "During closing buffer";

    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_ENABLED, true);
    conf.setBoolean(
        DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_LAST_REPLICA_ONLY,
        true);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_MIN_VOLUMES, 0);

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .build();
    cluster.waitActive();

    for (DataNode dn : cluster.getDataNodes()) {
      assertTrue(dn.isWriteMemoryBufferEnabled(),
          "Each DataNode must have writeMemoryBufferEnabled = true");
      assertTrue(dn.isWriteMemoryBufferLastReplicaOnly(),
          "Each DataNode must have writeMemoryBufferLastReplicaOnly = true");
    }

    LogCapturer logCapturer = LogCapturer.captureLogs(DataNode.LOG);
    try {
      FileSystem fs = cluster.getFileSystem();
      Path p = new Path("/test/last-replica-only.txt");
      byte[] payload =
          "safe rollout: only last replica uses buffered path"
              .getBytes(StandardCharsets.UTF_8);
      try (FSDataOutputStream out = fs.create(p, (short) 3)) {
        out.write(payload);
      }
      byte[] read = new byte[payload.length];
      try (FSDataInputStream in = fs.open(p)) {
        in.readFully(read);
      }
      assertArrayEquals(payload, read, "Write/read must succeed across the pipeline");

      int bufferedCloses = countOccurrences(
          logCapturer.getOutput(), bufferedPathMarker);
      assertEquals(1, bufferedCloses, "With last-replica-only=true and replication 3, exactly one "
              + "DataNode (the terminal replica) must exercise the buffered "
              + "path; got " + bufferedCloses + " '"
              + bufferedPathMarker + "' log lines");
    } finally {
      logCapturer.stopCapturing();
    }
  }

  /**
   * Control test: with the master flag on and
   * last-replica-only=false (default), ALL three replicas use the buffered
   * path. Confirms the gating in
   * {@code testWriteBufferOnlyLastReplicaInPipeline} is the new flag's
   * behaviour, not an unrelated bug.
   */
  @Test
  @Timeout(120)
  public void testWriteBufferAllReplicasWhenLastReplicaOnlyDisabled()
      throws Exception {
    final String bufferedPathMarker = "During closing buffer";

    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_ENABLED, true);
    // Explicitly disable last-replica-only (its default is true) so all
    // replicas exercise the buffered path.
    conf.setBoolean(
        DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_LAST_REPLICA_ONLY, false);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_MIN_VOLUMES, 0);

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .build();
    cluster.waitActive();

    for (DataNode dn : cluster.getDataNodes()) {
      assertFalse(dn.isWriteMemoryBufferLastReplicaOnly(),
          "last-replica-only must be off for this test");
    }

    LogCapturer logCapturer = LogCapturer.captureLogs(DataNode.LOG);
    try {
      FileSystem fs = cluster.getFileSystem();
      Path p = new Path("/test/all-replicas-buffered.txt");
      byte[] payload =
          "all replicas use the buffered path".getBytes(StandardCharsets.UTF_8);
      try (FSDataOutputStream out = fs.create(p, (short) 3)) {
        out.write(payload);
      }

      int bufferedCloses = countOccurrences(
          logCapturer.getOutput(), bufferedPathMarker);
      assertEquals(3, bufferedCloses, "With last-replica-only=false and replication 3, all three "
              + "DataNodes must exercise the buffered path; got "
              + bufferedCloses + " '" + bufferedPathMarker + "' log lines");
    } finally {
      logCapturer.stopCapturing();
    }
  }

  /**
   * Guards the rolled-out default: {@code last-replica-only} now defaults to
   * {@code true}, so when the feature is enabled without explicitly setting it,
   * only the terminal DataNode in a pipeline takes the buffered-write path.
   */
  @Test
  @Timeout(60)
  public void testLastReplicaOnlyDefaultsToTrue() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_ENABLED, true);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_MIN_VOLUMES, 0);
    // Intentionally do NOT set last-replica-only: rely on the default.

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();

    DataNode dn = cluster.getDataNodes().get(0);
    assertTrue(dn.isWriteMemoryBufferEnabled(), "Write memory buffer should be enabled");
    assertTrue(dn.isWriteMemoryBufferLastReplicaOnly(), "last-replica-only must default to true");
  }

  /**
   * Refactor guard: when the write-memory-buffer feature does not produce a
   * buffer for a block, {@code BlockReceiver.buffer} is left {@code null} (the
   * old {@code NO_OP_INSTANCE} was removed) and the receiver must fall back to
   * the direct stream write path. Here the master flag is ON but the feature is
   * gated OFF at the DataNode (configured volumes do not exceed min.volumes),
   * so no buffer is initialized. The write/read round-trip — including read-path
   * checksum verification — must still succeed, and the buffered-write marker
   * must never appear (proving the buffer was not used).
   */
  @Test
  @Timeout(60)
  public void testWriteReadWorksWhenBufferNotInitialized() throws Exception {
    final String bufferedPathMarker = "During closing buffer";

    Configuration conf = new HdfsConfiguration();
    // Master flag ON ...
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_ENABLED, true);
    // ... but gated OFF: require more volumes than are configured (1 storage).
    conf.setInt(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_MIN_VOLUMES, 5);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 512);

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .storagesPerDatanode(1)
        .build();
    cluster.waitActive();

    DataNode dn = cluster.getDataNodes().get(0);
    assertFalse(dn.isWriteMemoryBufferEnabled(),
        "Feature must be gated off at the DN (volumes <= min.volumes), so the "
            + "buffer is never initialized");

    // Deterministic, multi-packet payload to exercise receivePacket's
    // null-buffer branch repeatedly.
    final byte[] payload = new byte[512 * 1024];
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) ((i * 31 + 7) % 251);
    }

    LogCapturer logs = LogCapturer.captureLogs(DataNode.LOG);
    final Path p = new Path("/test/buffer-not-initialized.dat");
    try {
      FileSystem fs = cluster.getFileSystem();
      try (FSDataOutputStream out = fs.create(p, (short) 1)) {
        out.write(payload);
      }

      // Read back through the HDFS read path (verifies CRCs against the meta
      // file; a corrupt/mismatched block would throw here).
      byte[] readBack = new byte[payload.length];
      try (FSDataInputStream in = fs.open(p)) {
        in.readFully(readBack);
        assertEquals(-1, in.read(),
            "No bytes expected beyond the written payload");
      }
      assertArrayEquals(payload, readBack,
          "Data must round-trip intact through the non-buffered write path");
      assertEquals(payload.length, fs.getFileStatus(p).getLen(),
          "File length must match what was written");

      assertEquals(0, countOccurrences(logs.getOutput(), bufferedPathMarker),
          "Buffer must not be used when it is not initialized; the buffered "
              + "close marker must not appear");
    } finally {
      logs.stopCapturing();
    }
  }

  /**
   * Regression test for the hflush visibility break: with write buffering,
   * bytes still sitting in the in-memory buffer must NOT be exposed as the RBW
   * replica's visible/acked length. Otherwise a concurrent reader (BlockSender,
   * short-circuit, replica-pinned) would read past the physical end of the
   * block file and hit a short read / checksum mismatch / stale bytes.
   *
   * <p>The invariant asserted here: the replica's acked (visible) length is
   * never greater than the number of bytes actually flushed to the block file.
   * A sub-buffer write followed by hflush (which does not fill or fsync the
   * buffer, and with the idle flush disabled) leaves the data purely in memory,
   * so the visible length must stay at or below the on-disk length.
   */
  @Test
  @Timeout(60)
  public void testAckedLengthNeverExceedsOnDiskLength() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_ENABLED, true);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_MIN_VOLUMES, 0);
    // Disable the idle flush so buffered data is not persisted behind our back.
    conf.setLong(DFSConfigKeys.DFS_DATANODE_WRITE_BUFFER_IDLE_FLUSH_TIMEOUT_MS, 0);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 512);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 16 * 1024 * 1024);

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();
    DataNode dn = cluster.getDataNodes().get(0);
    assertTrue(dn.isWriteMemoryBufferEnabled(), "Write memory buffer must be enabled");

    // 256 KB: well under the 8 MB buffer, so it stays in memory after hflush.
    final int size = 256 * 1024;
    final byte[] payload = new byte[size];
    for (int i = 0; i < size; i++) {
      payload[i] = (byte) (i % 251);
    }

    final Path path = new Path("/testAckedVsOnDisk");
    try (FSDataOutputStream out = fs.create(path, (short) 1)) {
      out.write(payload);
      out.hflush();

      org.apache.hadoop.hdfs.protocol.LocatedBlocks lbs =
          cluster.getFileSystem().getClient()
              .getLocatedBlocks(path.toString(), 0L);
      assertFalse(lbs.getLocatedBlocks().isEmpty(),
          "Block must be allocated after hflush");
      org.apache.hadoop.hdfs.protocol.ExtendedBlock block =
          lbs.getLocatedBlocks().get(0).getBlock();

      ReplicaInfo r = DataNodeTestUtils.fetchReplicaInfo(
          dn, block.getBlockPoolId(), block.getBlockId());
      assertTrue(r instanceof ReplicaInPipeline,
          "Replica must be a ReplicaInPipeline while being written");
      long ackedLen = ((ReplicaInPipeline) r).getBytesAcked();
      long onDiskLen = r.getBlockDataLength();

      // The core invariant: never expose more than what is physically on disk.
      assertTrue(ackedLen <= onDiskLen,
          "Acked/visible length (" + ackedLen + ") must not exceed the "
              + "on-disk block file length (" + onDiskLen + ") for buffered, "
              + "un-synced data");
    }

    // After close everything is flushed and finalized: full data reads back.
    final byte[] readBack = new byte[size];
    try (FSDataInputStream in = fs.open(path)) {
      in.readFully(readBack);
    }
    assertArrayEquals(payload, readBack, "Data must round-trip intact after close");
    assertEquals(size, fs.getFileStatus(path).getLen(),
        "Finalized file length must equal bytes written");
  }

  /**
   * Correctness test for buffered writes across a non-chunk-aligned hflush.
   * A client writes an unaligned amount, hflushes (which does NOT flush the
   * write buffer), then keeps writing across the next chunk boundary. This
   * exercises {@code BufferedBlockWriter.writeData} with unaligned on-disk
   * offsets and the checksum-rewrite path, and verifies the data round-trips
   * intact after close.
   */
  @Test
  @Timeout(60)
  public void testBufferedWriteRoundTripAcrossUnalignedHflush() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_ENABLED, true);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_MIN_VOLUMES, 0);
    // Disable the idle flush so buffered data is not persisted behind our back.
    conf.setLong(DFSConfigKeys.DFS_DATANODE_WRITE_BUFFER_IDLE_FLUSH_TIMEOUT_MS, 0);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 512);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 16 * 1024 * 1024);

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();
    DataNode dn = cluster.getDataNodes().get(0);
    assertTrue(dn.isWriteMemoryBufferEnabled(), "Write memory buffer must be enabled");

    // 700 bytes: not a multiple of bytesPerChecksum (512), so the on-disk tail
    // is a partial chunk after the first hflush.
    final int firstLen = 700;
    // 1300 more bytes: continues past the partial chunk and across the next
    // chunk boundary, forcing the DataNode to recompute the partial-chunk CRC.
    final int secondLen = 1300;
    final int total = firstLen + secondLen;
    final byte[] payload = new byte[total];
    for (int i = 0; i < total; i++) {
      payload[i] = (byte) (i % 251);
    }

    final Path path = new Path("/testBufferedUnalignedHflush");
    try (FSDataOutputStream out = fs.create(path, (short) 1)) {
      out.write(payload, 0, firstLen);
      // hflush at an unaligned offset. With buffering, this does NOT flush the
      // buffer, so the partial chunk stays in memory even though bytesOnDisk
      // advances.
      out.hflush();
      // Continue writing across the next chunk boundary. The buffered writer
      // must handle the unaligned on-disk tail and checksum rewrite correctly.
      out.write(payload, firstLen, secondLen);
      out.hflush();
    }

    final byte[] readBack = new byte[total];
    try (FSDataInputStream in = fs.open(path)) {
      in.readFully(readBack);
    }
    assertArrayEquals(payload, readBack, "Data must round-trip intact across an unaligned hflush "
        + "followed by more writes");
    assertEquals(total, fs.getFileStatus(path).getLen(),
        "Finalized file length must equal bytes written");
  }

  private static int countOccurrences(String haystack, String needle) {
    int count = 0;
    int idx = 0;
    while ((idx = haystack.indexOf(needle, idx)) != -1) {
      count++;
      idx += needle.length();
    }
    return count;
  }
}
