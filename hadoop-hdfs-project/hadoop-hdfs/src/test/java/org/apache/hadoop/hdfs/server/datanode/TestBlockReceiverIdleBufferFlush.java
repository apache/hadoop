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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for the DataNode idle buffer-flush task
 * ({@code dfs.datanode.write.buffer.idle.flush.timeout.ms}).
 *
 * <p>
 * Scenario exercised (the data-loss window this feature closes): the
 * write-memory-buffer is enabled. A client writes data, the DataNode buffers
 * it in memory,
 * and then the client stalls without sending the final packet. Without this
 * feature the buffered bytes would never reach disk until close, so a DataNode
 * crash would lose them. With the feature, an idle period triggers a flush of
 * just the in-memory buffer to the (DSYNC) block file.
 * </p>
 *
 * <p>
 * The test asserts both that the flush actually persists bytes to the on-disk
 * block file mid-write, and that interleaving more writes after a flush does
 * not corrupt the block: after close, the file reads back byte-for-byte and
 * HDFS read-path checksum verification passes.
 * </p>
 */
public class TestBlockReceiverIdleBufferFlush {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestBlockReceiverIdleBufferFlush.class);

  // Large enough that the "buffered bytes are not on disk yet" check right
  // after hflush cannot race a premature idle flush (the timer only fires this
  // long after the last packet), while still well under the 120s test timeout.
  private static final long IDLE_FLUSH_MS = 10000;
  private static final int CHUNK = 256 * 1024; // 256 KB, well under 8 MB buffer
  private static final String FLUSH_MARKER = "Idle-flushed write buffer";

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;

  @AfterEach
  public void tearDown() throws IOException {
    if (fs != null) {
      fs.close();
      fs = null;
    }
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * End-to-end: stalled write -> idle flush persists data -> resume write ->
   * idle flush again -> close -> read back and validate integrity.
   */
  @Test
  @Timeout(120)
  public void testIdleFlushPersistsBufferedDataWithoutCorruption()
      throws Exception {
    Configuration conf = new HdfsConfiguration();
    // Feature under test: buffer ON and idle-flush ON.
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_ENABLED, true);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_MIN_VOLUMES, 0);
    conf.setLong(DFSConfigKeys.DFS_DATANODE_WRITE_BUFFER_IDLE_FLUSH_TIMEOUT_MS,
        IDLE_FLUSH_MS);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 512);
    // Keep all data in a single block so one buffer/replica is exercised.
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 16 * 1024 * 1024);

    // Single DataNode => deterministic single replica; replication 1.
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();

    DataNode dn = cluster.getDataNodes().get(0);
    assertTrue(dn.isWriteMemoryBufferEnabled(),
        "Write memory buffer must be enabled for this test");

    LogCapturer logs = LogCapturer.captureLogs(DataNode.LOG);
    final Path path = new Path("/testIdleBufferFlush");

    final byte[] data1 = newPattern(CHUNK, 0);
    final byte[] data2 = newPattern(CHUNK, 7);

    FSDataOutputStream out = null;
    try {
      out = fs.create(path, (short) 1);

      // --- Phase 1: write data1, push it to the DN, then stall. ---
      // hflush guarantees the packets are received and processed by the DN
      // (acked) into the in-memory buffer. The buffered-write path returns
      // early on a non-sync flush, so the data is NOT yet on the block file.
      out.write(data1);
      out.hflush();

      final ExtendedBlock block = firstBlock(path);
      assertTrue(block != null, "Block should be allocated after hflush");

      // Right after hflush, before the idle window elapses, the bytes are in
      // memory only: the on-disk block file is still empty.
      long beforeFlush = onDiskLength(dn, block);
      assertTrue(beforeFlush < data1.length,
          "Buffered data must not be on disk before the idle flush "
              + "(on-disk length was " + beforeFlush + ")");

      // --- Phase 2: wait for the idle flush to persist data1. ---
      GenericTestUtils.waitFor(() -> {
        try {
          return onDiskLength(dn, block) >= data1.length;
        } catch (IOException e) {
          return false;
        }
      }, 100, 30000);
      assertEquals(data1.length, onDiskLength(dn, block),
          "Idle flush must persist exactly the buffered bytes");
      assertTrue(countOccurrences(logs.getOutput(), FLUSH_MARKER) >= 1,
          "Idle-flush log marker must be present after first flush");

      // --- Phase 3: resume writing data2 to confirm no corruption. ---
      // Writing more after an off-thread flush must not throw or corrupt the
      // stream/checksum state.
      out.write(data2);
      out.hflush();

      // --- Phase 4: wait for a second idle flush covering data1 + data2. ---
      GenericTestUtils.waitFor(() -> {
        try {
          return onDiskLength(dn, block) >= (long) data1.length + data2.length;
        } catch (IOException e) {
          return false;
        }
      }, 100, 30000);
      assertEquals((long) data1.length + data2.length,
          onDiskLength(dn, block),
          "Second idle flush must persist all buffered bytes");
      assertTrue(countOccurrences(logs.getOutput(), FLUSH_MARKER) >= 2,
          "Idle-flush log marker must appear at least twice");

      // --- Phase 5: close the stream normally. ---
      out.close();
      out = null;
    } finally {
      if (out != null) {
        try {
          out.close();
        } catch (IOException e) {
          LOG.warn("Error closing output stream", e);
        }
      }
      logs.stopCapturing();
    }

    // --- Phase 6: read back and validate integrity + checksums. ---
    // fs.open + readFully runs the HDFS read path, which verifies CRCs against
    // the meta file; a corrupt block would throw ChecksumException here.
    final byte[] expected = new byte[data1.length + data2.length];
    System.arraycopy(data1, 0, expected, 0, data1.length);
    System.arraycopy(data2, 0, expected, data1.length, data2.length);

    assertEquals(expected.length, fs.getFileStatus(path).getLen(),
        "File length must equal total bytes written");

    final byte[] readBack = new byte[expected.length];
    try (FSDataInputStream in = fs.open(path)) {
      in.readFully(readBack);
      // EOF expected right after the last byte.
      assertEquals(-1, in.read(), "No extra bytes expected beyond written data");
    }
    assertArrayEquals(expected, readBack, "Read-back data must match written data exactly "
        + "(checksums verified by the read path)");
  }

  /**
   * Regression: the one-shot flush timer must rearm on EVERY firing, not only
   * after a successful flush. If it only rescheduled after flushing, a tick
   * that fires while the client is still actively writing (idle &lt; timeout)
   * would return without rearming and permanently disable idle-flush for the
   * block — so a later stall would never be caught.
   *
   * <p>
   * This drives a phase of active writes spaced shorter than the idle timeout
   * (forcing no-op ticks), then stalls and asserts the accumulated buffer is
   * still flushed to disk. With the "rearm only after flush" bug the timer
   * dies during the active phase and this flush never happens.
   * </p>
   */
  @Test
  @Timeout(120)
  public void testTimerRearmsAfterNoOpTickThenStall() throws Exception {
    final long idleMs = 1000;
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_ENABLED, true);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_MIN_VOLUMES, 0);
    conf.setLong(DFSConfigKeys.DFS_DATANODE_WRITE_BUFFER_IDLE_FLUSH_TIMEOUT_MS,
        idleMs);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 512);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 16 * 1024 * 1024);

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    DataNode dn = cluster.getDataNodes().get(0);
    assertTrue(dn.isWriteMemoryBufferEnabled());

    final Path path = new Path("/testTimerRearm");
    final byte[] data1 = newPattern(CHUNK, 0);
    final int smallSize = 64 * 1024;

    FSDataOutputStream out = null;
    long totalWritten = 0;
    try {
      out = fs.create(path, (short) 1);
      out.write(data1);
      out.hflush();
      totalWritten += data1.length;
      final ExtendedBlock block = firstBlock(path);
      assertTrue(block != null, "Block should be allocated");

      // First idle flush persists data1 (timer rearms after this success).
      final long afterData1 = totalWritten;
      GenericTestUtils.waitFor(() -> safeOnDisk(dn, block) >= afterData1,
          100, 30000);

      // Active phase: write every idleMs/2 for several timer intervals. Each
      // timer firing sees idle < idleMs => a no-op tick that must still rearm.
      for (int i = 0; i < 6; i++) {
        byte[] small = newPattern(smallSize, i + 1);
        out.write(small);
        out.hflush();
        totalWritten += smallSize;
        Thread.sleep(idleMs / 2);
      }

      // Now stall. If the timer is still armed it fires within idleMs and
      // flushes the buffered bytes accumulated during the active phase.
      final long expectedOnDisk = totalWritten;
      GenericTestUtils.waitFor(() -> safeOnDisk(dn, block) >= expectedOnDisk,
          100, 30000);
      assertEquals(expectedOnDisk, onDiskLength(dn, block),
          "Idle flush must persist all buffered bytes after a stall "
              + "that follows no-op timer ticks");

      out.close();
      out = null;
    } finally {
      if (out != null) {
        try {
          out.close();
        } catch (IOException e) {
          LOG.warn("Error closing output stream", e);
        }
      }
    }

    // Integrity check on the full file.
    assertEquals(totalWritten, fs.getFileStatus(path).getLen());
    final byte[] readBack = new byte[(int) totalWritten];
    try (FSDataInputStream in = fs.open(path)) {
      in.readFully(readBack);
    }
    final byte[] prefix = new byte[data1.length];
    System.arraycopy(readBack, 0, prefix, 0, data1.length);
    assertArrayEquals(data1, prefix, "data1 prefix must round-trip intact");
  }

  /** {@link #onDiskLength} swallowing IOException, for use in waitFor lambdas. */
  private static long safeOnDisk(DataNode dn, ExtendedBlock block) {
    try {
      return onDiskLength(dn, block);
    } catch (IOException e) {
      return -1;
    }
  }

  /** On-disk length of the block data file for {@code block} on {@code dn}. */
  private static long onDiskLength(DataNode dn, ExtendedBlock block)
      throws IOException {
    ReplicaInfo r = DataNodeTestUtils.fetchReplicaInfo(
        dn, block.getBlockPoolId(), block.getBlockId());
    return r == null ? -1 : r.getBlockDataLength();
  }

  /** First located block of an (open-for-write) file. */
  private ExtendedBlock firstBlock(Path path) throws IOException {
    LocatedBlocks lbs =
        fs.getClient().getLocatedBlocks(path.toString(), 0L);
    if (lbs == null || lbs.getLocatedBlocks().isEmpty()) {
      return null;
    }
    return lbs.getLocatedBlocks().get(0).getBlock();
  }

  /** Deterministic, position-dependent byte pattern with a per-chunk offset. */
  private static byte[] newPattern(int size, int seed) {
    byte[] b = new byte[size];
    for (int i = 0; i < size; i++) {
      b[i] = (byte) ((i + seed) % 251);
    }
    return b;
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
