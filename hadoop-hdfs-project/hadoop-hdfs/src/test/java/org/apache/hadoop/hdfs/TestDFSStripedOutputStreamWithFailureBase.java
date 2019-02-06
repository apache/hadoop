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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.SecurityTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.io.erasurecode.ErasureCodeNative;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawErasureCoderFactory;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Base class for test striped file write operation.
 */
public class TestDFSStripedOutputStreamWithFailureBase {
  public static final Logger LOG = LoggerFactory.getLogger(
      TestDFSStripedOutputStreamWithFailureBase.class);
  static {
    GenericTestUtils.setLogLevel(DFSOutputStream.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(DataStreamer.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(DFSClient.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(
        LoggerFactory.getLogger(BlockPlacementPolicy.class), Level.TRACE);
  }

  protected final int cellSize = 64 * 1024; // 8k
  protected final int stripesPerBlock = 4;
  protected ErasureCodingPolicy ecPolicy;
  protected int dataBlocks;
  protected int parityBlocks;
  protected int blockSize;
  protected int blockGroupSize;
  private int[][] dnIndexSuite;
  protected List<Integer> lengths;
  protected static final Random RANDOM = new Random();
  MiniDFSCluster cluster;
  DistributedFileSystem dfs;
  final Path dir = new Path("/"
      + TestDFSStripedOutputStreamWithFailureBase.class.getSimpleName());
  protected static final int FLUSH_POS =
      9 * DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT + 1;

  public ECSchema getEcSchema() {
    return StripedFileTestUtil.getDefaultECPolicy().getSchema();
  }

  /*
   * Initialize erasure coding policy.
   */
  @Before
  public void init() {
    ecPolicy = new ErasureCodingPolicy(getEcSchema(), cellSize);
    dataBlocks = ecPolicy.getNumDataUnits();
    parityBlocks = ecPolicy.getNumParityUnits();
    blockSize = cellSize * stripesPerBlock;
    blockGroupSize = blockSize * dataBlocks;
    dnIndexSuite = getDnIndexSuite();
    lengths = newLengths();
  }

  List<Integer> newLengths() {
    final List<Integer> lens = new ArrayList<>();
    lens.add(FLUSH_POS + 2);
    for(int b = 0; b <= 2; b++) {
      for(int c = 0; c < stripesPerBlock * dataBlocks; c++) {
        for(int delta = -1; delta <= 1; delta++) {
          final int length = b * blockGroupSize + c * cellSize + delta;
          System.out.println(lens.size() + ": length=" + length
              + ", (b, c, d) = (" + b + ", " + c + ", " + delta + ")");
          lens.add(length);
        }
      }
    }
    return lens;
  }

  private int[][] getDnIndexSuite() {
    final int maxNumLevel = 2;
    final int maxPerLevel = 5;
    List<List<Integer>> allLists = new ArrayList<>();
    int numIndex = parityBlocks;
    for (int i = 0; i < maxNumLevel && numIndex > 1; i++) {
      List<List<Integer>> lists =
          combinations(dataBlocks + parityBlocks, numIndex);
      if (lists.size() > maxPerLevel) {
        Collections.shuffle(lists);
        lists = lists.subList(0, maxPerLevel);
      }
      allLists.addAll(lists);
      numIndex--;
    }
    int[][] dnIndexArray = new int[allLists.size()][];
    for (int i = 0; i < dnIndexArray.length; i++) {
      int[] list = new int[allLists.get(i).size()];
      for (int j = 0; j < list.length; j++) {
        list[j] = allLists.get(i).get(j);
      }
      dnIndexArray[i] = list;
    }
    return dnIndexArray;
  }

  // get all combinations of k integers from {0,...,n-1}
  private static List<List<Integer>> combinations(int n, int k) {
    List<List<Integer>> res = new LinkedList<List<Integer>>();
    if (k >= 1 && n >= k) {
      getComb(n, k, new Stack<Integer>(), res);
    }
    return res;
  }

  private static void getComb(int n, int k, Stack<Integer> stack,
      List<List<Integer>> res) {
    if (stack.size() == k) {
      List<Integer> list = new ArrayList<Integer>(stack);
      res.add(list);
    } else {
      int next = stack.empty() ? 0 : stack.peek() + 1;
      while (next < n) {
        stack.push(next);
        getComb(n, k, stack, res);
        next++;
      }
    }
    if (!stack.empty()) {
      stack.pop();
    }
  }

  int[] getKillPositions(int fileLen, int num) {
    int[] positions = new int[num];
    for (int i = 0; i < num; i++) {
      positions[i] = fileLen * (i + 1) / (num + 1);
    }
    return positions;
  }

  Integer getLength(int i) {
    return i >= 0 && i < lengths.size() ? lengths.get(i): null;
  }

  void setup(Configuration conf) throws IOException {
    System.out.println("NUM_DATA_BLOCKS  = " + dataBlocks);
    System.out.println("NUM_PARITY_BLOCKS= " + parityBlocks);
    System.out.println("CELL_SIZE        = " + cellSize + " (=" +
        StringUtils.TraditionalBinaryPrefix.long2String(cellSize, "B", 2)
        + ")");
    System.out.println("BLOCK_SIZE       = " + blockSize + " (=" +
        StringUtils.TraditionalBinaryPrefix.long2String(blockSize, "B", 2)
        + ")");
    System.out.println("BLOCK_GROUP_SIZE = " + blockGroupSize + " (=" +
        StringUtils.TraditionalBinaryPrefix.long2String(blockGroupSize, "B", 2)
        + ")");
    final int numDNs = dataBlocks + parityBlocks;
    if (ErasureCodeNative.isNativeCodeLoaded()) {
      conf.set(
          CodecUtil.IO_ERASURECODE_CODEC_RS_RAWCODERS_KEY,
          NativeRSRawErasureCoderFactory.CODER_NAME);
    }
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
    AddErasureCodingPolicyResponse[] res =
        dfs.addErasureCodingPolicies(new ErasureCodingPolicy[]{ecPolicy});
    ecPolicy = res[0].getPolicy();
    dfs.enableErasureCodingPolicy(ecPolicy.getName());
    DFSTestUtil.enableAllECPolicies(dfs);
    dfs.mkdirs(dir);
    dfs.setErasureCodingPolicy(dir, ecPolicy.getName());
  }

  void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  HdfsConfiguration newHdfsConfiguration() {
    final HdfsConfiguration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY,
        false);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 0);
    return conf;
  }

  void runTest(final int length) {
    final HdfsConfiguration conf = newHdfsConfiguration();
    for (int dn = 0; dn < dataBlocks + parityBlocks; dn++) {
      try {
        LOG.info("runTest: dn=" + dn + ", length=" + length);
        setup(conf);
        runTest(length, new int[]{length / 2}, new int[]{dn}, false);
      } catch (Throwable e) {
        final String err = "failed, dn=" + dn + ", length=" + length
            + StringUtils.stringifyException(e);
        LOG.error(err);
        Assert.fail(err);
      } finally {
        tearDown();
      }
    }
  }

  void runTestWithMultipleFailure(final int length) throws Exception {
    final HdfsConfiguration conf = newHdfsConfiguration();
    for (int[] dnIndex : dnIndexSuite) {
      int[] killPos = getKillPositions(length, dnIndex.length);
      try {
        LOG.info("runTestWithMultipleFailure: length==" + length + ", killPos="
            + Arrays.toString(killPos) + ", dnIndex="
            + Arrays.toString(dnIndex));
        setup(conf);
        runTest(length, killPos, dnIndex, false);
      } catch (Throwable e) {
        final String err = "failed, killPos=" + Arrays.toString(killPos)
            + ", dnIndex=" + Arrays.toString(dnIndex) + ", length=" + length;
        LOG.error(err);
        throw e;
      } finally {
        tearDown();
      }
    }
  }

  /**
   * runTest implementation.
   * @param length file length
   * @param killPos killing positions in ascending order
   * @param dnIndex DN index to kill when meets killing positions
   * @param tokenExpire wait token to expire when kill a DN
   * @throws Exception
   */
  void runTest(final int length, final int[] killPos,
      final int[] dnIndex, final boolean tokenExpire) throws Exception {
    if (killPos[0] <= FLUSH_POS) {
      LOG.warn("killPos=" + Arrays.toString(killPos) + " <= FLUSH_POS="
          + FLUSH_POS + ", length=" + length + ", dnIndex="
          + Arrays.toString(dnIndex));
      return; //skip test
    }
    Preconditions.checkArgument(length > killPos[0], "length=%s <= killPos=%s",
        length, killPos);
    Preconditions.checkArgument(killPos.length == dnIndex.length);

    final Path p = new Path(dir, "dn" + Arrays.toString(dnIndex)
        + "len" + length + "kill" +  Arrays.toString(killPos));
    final String fullPath = p.toString();
    LOG.info("fullPath=" + fullPath);

    if (tokenExpire) {
      final NameNode nn = cluster.getNameNode();
      final BlockManager bm = nn.getNamesystem().getBlockManager();
      final BlockTokenSecretManager sm = bm.getBlockTokenSecretManager();

      // set a short token lifetime (6 second)
      SecurityTestUtil.setBlockTokenLifetime(sm, 6000L);
    }

    final AtomicInteger pos = new AtomicInteger();
    final FSDataOutputStream out = dfs.create(p);
    final DFSStripedOutputStream stripedOut
        = (DFSStripedOutputStream)out.getWrappedStream();

    // first GS of this block group which never proceeds blockRecovery
    long firstGS = -1;
    long oldGS = -1; // the old GS before bumping
    List<Long> gsList = new ArrayList<>();
    final List<DatanodeInfo> killedDN = new ArrayList<>();
    int numKilled = 0;
    for(; pos.get() < length;) {
      final int i = pos.getAndIncrement();
      if (numKilled < killPos.length && i == killPos[numKilled]) {
        assertTrue(firstGS != -1);
        final long gs = getGenerationStamp(stripedOut);
        if (numKilled == 0) {
          assertEquals(firstGS, gs);
        } else {
          //TODO: implement hflush/hsync and verify gs strict greater than oldGS
          assertTrue(gs >= oldGS);
        }
        oldGS = gs;

        if (tokenExpire) {
          DFSTestUtil.flushInternal(stripedOut);
          waitTokenExpires(out);
        }

        killedDN.add(
            killDatanode(cluster, stripedOut, dnIndex[numKilled], pos));
        numKilled++;
      }

      write(out, i);

      if (i % blockGroupSize == FLUSH_POS) {
        firstGS = getGenerationStamp(stripedOut);
        oldGS = firstGS;
      }
      if (i > 0 && (i + 1) % blockGroupSize == 0) {
        gsList.add(oldGS);
      }
    }
    gsList.add(oldGS);
    out.close();
    assertEquals(dnIndex.length, numKilled);

    StripedFileTestUtil.waitBlockGroupsReported(dfs, fullPath, numKilled);

    cluster.triggerBlockReports();
    StripedFileTestUtil.checkData(dfs, p, length, killedDN, gsList,
        blockGroupSize);
  }

  static void write(FSDataOutputStream out, int i) throws IOException {
    try {
      out.write(StripedFileTestUtil.getByte(i));
    } catch(IOException ioe) {
      throw new IOException("Failed at i=" + i, ioe);
    }
  }

  static long getGenerationStamp(DFSStripedOutputStream out)
      throws IOException {
    final long gs = out.getBlock().getGenerationStamp();
    LOG.info("getGenerationStamp returns " + gs);
    return gs;
  }

  static DatanodeInfo getDatanodes(StripedDataStreamer streamer) {
    for(;;) {
      DatanodeInfo[] datanodes = streamer.getNodes();
      if (datanodes == null) {
        // try peeking following block.
        final LocatedBlock lb = streamer.peekFollowingBlock();
        if (lb != null) {
          datanodes = lb.getLocations();
        }
      }

      if (datanodes != null) {
        Assert.assertEquals(1, datanodes.length);
        Assert.assertNotNull(datanodes[0]);
        return datanodes[0];
      }

      try {
        Thread.sleep(100);
      } catch (InterruptedException ie) {
        Assert.fail(StringUtils.stringifyException(ie));
        return null;
      }
    }
  }

  static DatanodeInfo killDatanode(MiniDFSCluster cluster,
      DFSStripedOutputStream out, final int dnIndex, final AtomicInteger pos) {
    final StripedDataStreamer s = out.getStripedDataStreamer(dnIndex);
    final DatanodeInfo datanode = getDatanodes(s);
    LOG.info("killDatanode " + dnIndex + ": " + datanode + ", pos=" + pos);
    if (datanode != null) {
      cluster.stopDataNode(datanode.getXferAddr());
    }
    return datanode;
  }

  private void waitTokenExpires(FSDataOutputStream out) throws IOException {
    Token<BlockTokenIdentifier> token = DFSTestUtil.getBlockToken(out);
    while (!SecurityTestUtil.isBlockTokenExpired(token)) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException ignored) {
      }
    }
  }
}
