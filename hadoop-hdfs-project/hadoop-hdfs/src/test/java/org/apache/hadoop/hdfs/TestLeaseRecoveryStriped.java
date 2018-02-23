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
package org.apache.hadoop.hdfs;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.server.datanode.BlockRecoveryWorker;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

public class TestLeaseRecoveryStriped {
  public static final Logger LOG = LoggerFactory
      .getLogger(TestLeaseRecoveryStriped.class);

  private final ErasureCodingPolicy ecPolicy =
      StripedFileTestUtil.getDefaultECPolicy();
  private final int dataBlocks = ecPolicy.getNumDataUnits();
  private final int parityBlocks = ecPolicy.getNumParityUnits();
  private final int cellSize = ecPolicy.getCellSize();
  private final int stripeSize = dataBlocks * cellSize;
  private final int stripesPerBlock = 4;
  private final int blockSize = cellSize * stripesPerBlock;
  private final int blockGroupSize = blockSize * dataBlocks;
  private static final int bytesPerChecksum = 512;

  static {
    GenericTestUtils.setLogLevel(DataNode.LOG, Level.ALL);
    GenericTestUtils.setLogLevel(DFSStripedOutputStream.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(BlockRecoveryWorker.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(DataStreamer.LOG, Level.DEBUG);
  }

  static private final String fakeUsername = "fakeUser1";
  static private final String fakeGroup = "supergroup";

  private MiniDFSCluster cluster;
  private DistributedFileSystem dfs;
  private Configuration conf;
  private final Path dir = new Path("/" + this.getClass().getSimpleName());
  final Path p = new Path(dir, "testfile");
  private final int testFileLength = (stripesPerBlock - 1) * stripeSize;

  @Before
  public void setup() throws IOException {
    conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setLong(HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 60000L);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY,
        false);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 0);
    final int numDNs = dataBlocks + parityBlocks;
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
    dfs.enableErasureCodingPolicy(ecPolicy.getName());
    dfs.mkdirs(dir);
    dfs.setErasureCodingPolicy(dir, ecPolicy.getName());
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private static class BlockLengths {
    private final int[] blockLengths;
    private final long safeLength;

    BlockLengths(ErasureCodingPolicy policy, int[] blockLengths) {
      this.blockLengths = blockLengths;
      long[] longArray = Arrays.stream(blockLengths).asLongStream().toArray();
      this.safeLength = StripedBlockUtil.getSafeLength(policy, longArray);
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this)
          .append("blockLengths", getBlockLengths())
          .append("safeLength", getSafeLength())
          .toString();
    }

    /**
     * Length of each block in a block group.
     */
    public int[] getBlockLengths() {
      return blockLengths;
    }

    /**
     * Safe length, calculated by the block lengths.
     */
    public long getSafeLength() {
      return safeLength;
    }
  }

  private BlockLengths[] getBlockLengthsSuite() {
    final int groups = 4;
    final int minNumCell = 1;
    final int maxNumCell = stripesPerBlock;
    final int minNumDelta = -4;
    final int maxNumDelta = 2;
    BlockLengths[] suite = new BlockLengths[groups];
    Random random = ThreadLocalRandom.current();
    for (int i = 0; i < groups; i++) {
      int[] blockLengths = new int[dataBlocks + parityBlocks];
      for (int j = 0; j < blockLengths.length; j++) {
        // Choose a random number of cells for the block
        int numCell = random.nextInt(maxNumCell - minNumCell + 1) + minNumCell;
        // For data blocks, jitter the length a bit
        int numDelta = 0;
        if (i == groups - 1 && j < dataBlocks) {
          numDelta = random.nextInt(maxNumDelta - minNumDelta + 1) +
              minNumDelta;
        }
        blockLengths[j] = (cellSize * numCell) + (bytesPerChecksum * numDelta);
      }
      suite[i] = new BlockLengths(ecPolicy, blockLengths);
    }
    return suite;
  }

  private final BlockLengths[] blockLengthsSuite = getBlockLengthsSuite();

  @Test
  public void testLeaseRecovery() throws Exception {
    LOG.info("blockLengthsSuite: " +
        Arrays.toString(blockLengthsSuite));
    for (int i = 0; i < blockLengthsSuite.length; i++) {
      BlockLengths blockLengths = blockLengthsSuite[i];
      try {
        runTest(blockLengths.getBlockLengths(), blockLengths.getSafeLength());
      } catch (Throwable e) {
        String msg = "failed testCase at i=" + i + ", blockLengths="
            + blockLengths + "\n"
            + StringUtils.stringifyException(e);
        Assert.fail(msg);
      }
    }
  }

  private void runTest(int[] blockLengths, long safeLength) throws Exception {
    writePartialBlocks(blockLengths);

    int checkDataLength = Math.min(testFileLength, (int)safeLength);

    recoverLease();

    List<Long> oldGS = new ArrayList<>();
    oldGS.add(1001L);
    StripedFileTestUtil.checkData(dfs, p, checkDataLength,
        new ArrayList<DatanodeInfo>(), oldGS, blockGroupSize);
    // After recovery, storages are reported by primary DN. we should verify
    // storages reported by blockReport.
    cluster.restartNameNode(true);
    cluster.waitFirstBRCompleted(0, 10000);
    StripedFileTestUtil.checkData(dfs, p, checkDataLength,
        new ArrayList<DatanodeInfo>(), oldGS, blockGroupSize);
  }

  /**
   * Write a file with blocks of different lengths.
   *
   * This method depends on completing before the DFS socket timeout.
   * Otherwise, the client will mark timed-out streamers as failed, and the
   * write will fail if there are too many failed streamers.
   *
   * @param blockLengths lengths of blocks to write
   * @throws Exception
   */
  private void writePartialBlocks(int[] blockLengths) throws Exception {
    final FSDataOutputStream out = dfs.create(p);
    final DFSStripedOutputStream stripedOut = (DFSStripedOutputStream) out
        .getWrappedStream();
    int[] posToKill = getPosToKill(blockLengths);
    int checkingPos = nextCheckingPos(posToKill, 0);
    Set<Integer> stoppedStreamerIndexes = new HashSet<>();
    try {
      for (int pos = 0; pos < testFileLength; pos++) {
        out.write(StripedFileTestUtil.getByte(pos));
        if (pos == checkingPos) {
          for (int index : getIndexToStop(posToKill, pos)) {
            out.flush();
            stripedOut.enqueueAllCurrentPackets();
            LOG.info("Stopping block stream idx {} at file offset {} block " +
                    "length {}", index, pos, blockLengths[index]);
            StripedDataStreamer s = stripedOut.getStripedDataStreamer(index);
            waitStreamerAllAcked(s);
            waitByteSent(s, blockLengths[index]);
            stopBlockStream(s);
            stoppedStreamerIndexes.add(index);
          }
          checkingPos = nextCheckingPos(posToKill, pos);
        }
      }
    } finally {
      // Flush everything
      out.flush();
      stripedOut.enqueueAllCurrentPackets();
      // Wait for streamers that weren't killed above to be written out
      for (int i=0; i< blockLengths.length; i++) {
        if (stoppedStreamerIndexes.contains(i)) {
          continue;
        }
        StripedDataStreamer s = stripedOut.getStripedDataStreamer(i);
        LOG.info("Waiting for block stream idx {} to reach length {}", i,
            blockLengths[i]);
        waitStreamerAllAcked(s);
      }
      DFSTestUtil.abortStream(stripedOut);
    }
  }

  private int nextCheckingPos(int[] posToKill, int curPos) {
    int checkingPos = Integer.MAX_VALUE;
    for (int i = 0; i < posToKill.length; i++) {
      if (posToKill[i] > curPos) {
        checkingPos = Math.min(checkingPos, posToKill[i]);
      }
    }
    return checkingPos;
  }

  private int[] getPosToKill(int[] blockLengths) {
    int[] posToKill = new int[dataBlocks + parityBlocks];
    for (int i = 0; i < dataBlocks; i++) {
      int numStripe = (blockLengths[i] - 1) / cellSize;
      posToKill[i] = numStripe * stripeSize + i * cellSize
          + blockLengths[i] % cellSize;
      if (blockLengths[i] % cellSize == 0) {
        posToKill[i] += cellSize;
      }
    }
    for (int i = dataBlocks; i < dataBlocks
        + parityBlocks; i++) {
      Preconditions.checkArgument(blockLengths[i] % cellSize == 0);
      int numStripe = (blockLengths[i]) / cellSize;
      posToKill[i] = numStripe * stripeSize;
    }
    return posToKill;
  }

  private List<Integer> getIndexToStop(int[] posToKill, int pos) {
    List<Integer> indices = new LinkedList<>();
    for (int i = 0; i < posToKill.length; i++) {
      if (pos == posToKill[i]) {
        indices.add(i);
      }
    }
    return indices;
  }

  private void waitByteSent(final StripedDataStreamer s, final long byteSent)
      throws Exception {
    try {
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          return s.bytesSent >= byteSent;
        }
      }, 100, 30000);
    } catch (TimeoutException e) {
      throw new IOException("Timeout waiting for streamer " + s + ". Sent="
          + s.bytesSent + ", expected=" + byteSent);
    }
  }

  /**
   * Stop the block stream without immediately inducing a hard failure.
   * Packets can continue to be queued until the streamer hits a socket timeout.
   *
   * @param s
   * @throws Exception
   */
  private void stopBlockStream(StripedDataStreamer s) throws Exception {
    IOUtils.NullOutputStream nullOutputStream = new IOUtils.NullOutputStream();
    Whitebox.setInternalState(s, "blockStream",
        new DataOutputStream(nullOutputStream));
  }

  private void recoverLease() throws Exception {
    final DistributedFileSystem dfs2 =
        (DistributedFileSystem) getFSAsAnotherUser(conf);
    try {
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          try {
            return dfs2.recoverLease(p);
          } catch (IOException e) {
            return false;
          }
        }
      }, 5000, 24000);
    } catch (TimeoutException e) {
      throw new IOException("Timeout waiting for recoverLease()");
    }
  }

  private FileSystem getFSAsAnotherUser(final Configuration c)
      throws IOException, InterruptedException {
    return FileSystem.get(FileSystem.getDefaultUri(c), c,
        UserGroupInformation
            .createUserForTesting(fakeUsername, new String[] { fakeGroup })
            .getUserName());
  }

  public static void waitStreamerAllAcked(DataStreamer s) throws IOException {
    long toWaitFor = s.getLastQueuedSeqno();
    s.waitForAckedSeqno(toWaitFor);
  }
}
