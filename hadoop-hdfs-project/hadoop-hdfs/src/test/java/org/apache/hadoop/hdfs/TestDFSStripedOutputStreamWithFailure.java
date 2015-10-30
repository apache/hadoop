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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.SecurityTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Preconditions;


public class TestDFSStripedOutputStreamWithFailure {
  public static final Log LOG = LogFactory.getLog(
      TestDFSStripedOutputStreamWithFailure.class);
  static {
    GenericTestUtils.setLogLevel(DFSOutputStream.LOG, Level.ALL);
    GenericTestUtils.setLogLevel(DataStreamer.LOG, Level.ALL);
    GenericTestUtils.setLogLevel(DFSClient.LOG, Level.ALL);
    ((Log4JLogger)LogFactory.getLog(BlockPlacementPolicy.class))
        .getLogger().setLevel(Level.ALL);
  }

  private static final int NUM_DATA_BLOCKS = StripedFileTestUtil.NUM_DATA_BLOCKS;
  private static final int NUM_PARITY_BLOCKS = StripedFileTestUtil.NUM_PARITY_BLOCKS;
  private static final int CELL_SIZE = StripedFileTestUtil.BLOCK_STRIPED_CELL_SIZE;
  private static final int STRIPES_PER_BLOCK = 4;
  private static final int BLOCK_SIZE = CELL_SIZE * STRIPES_PER_BLOCK;
  private static final int BLOCK_GROUP_SIZE = BLOCK_SIZE * NUM_DATA_BLOCKS;

  private static final int FLUSH_POS
      = 9*DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT + 1;

  static {
    System.out.println("NUM_DATA_BLOCKS  = " + NUM_DATA_BLOCKS);
    System.out.println("NUM_PARITY_BLOCKS= " + NUM_PARITY_BLOCKS);
    System.out.println("CELL_SIZE        = " + CELL_SIZE
        + " (=" + StringUtils.TraditionalBinaryPrefix.long2String(CELL_SIZE, "B", 2) + ")");
    System.out.println("BLOCK_SIZE       = " + BLOCK_SIZE
        + " (=" + StringUtils.TraditionalBinaryPrefix.long2String(BLOCK_SIZE, "B", 2) + ")");
    System.out.println("BLOCK_GROUP_SIZE = " + BLOCK_GROUP_SIZE
        + " (=" + StringUtils.TraditionalBinaryPrefix.long2String(BLOCK_GROUP_SIZE, "B", 2) + ")");
  }

  static List<Integer> newLengths() {
    final List<Integer> lengths = new ArrayList<>();
    lengths.add(FLUSH_POS + 2);
    for(int b = 0; b <= 2; b++) {
      for(int c = 0; c < STRIPES_PER_BLOCK*NUM_DATA_BLOCKS; c++) {
        for(int delta = -1; delta <= 1; delta++) {
          final int length = b*BLOCK_GROUP_SIZE + c*CELL_SIZE + delta;
          System.out.println(lengths.size() + ": length=" + length
              + ", (b, c, d) = (" + b + ", " + c + ", " + delta + ")");
          lengths.add(length);
        }
      }
    }
    return lengths;
  }

  private static final int[][] dnIndexSuite = {
      {0, 1},
      {0, 5},
      {0, 6},
      {0, 8},
      {1, 5},
      {1, 6},
      {6, 8},
      {0, 1, 2},
      {3, 4, 5},
      {0, 1, 6},
      {0, 5, 6},
      {0, 5, 8},
      {0, 6, 7},
      {5, 6, 7},
      {6, 7, 8},
  };

  private int[] getKillPositions(int fileLen, int num) {
    int[] positions = new int[num];
    for (int i = 0; i < num; i++) {
      positions[i] = fileLen * (i + 1) / (num + 1);
    }
    return positions;
  }

  private static final List<Integer> LENGTHS = newLengths();

  static Integer getLength(int i) {
    return i >= 0 && i < LENGTHS.size()? LENGTHS.get(i): null;
  }

  private static final Random RANDOM = new Random();

  private MiniDFSCluster cluster;
  private DistributedFileSystem dfs;
  private final Path dir = new Path("/"
      + TestDFSStripedOutputStreamWithFailure.class.getSimpleName());

  private void setup(Configuration conf) throws IOException {
    final int numDNs = NUM_DATA_BLOCKS + NUM_PARITY_BLOCKS;
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
    dfs.mkdirs(dir);
    dfs.setErasureCodingPolicy(dir, null);
  }

  private void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private HdfsConfiguration newHdfsConfiguration() {
    final HdfsConfiguration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY,
        false);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 0);
    return conf;
  }

  @Test(timeout=240000)
  public void testMultipleDatanodeFailure56() throws Exception {
    runTestWithMultipleFailure(getLength(56));
  }

  /**
   * Randomly pick a length and run tests with multiple data failures
   * TODO: enable this later
   */
  //@Test(timeout=240000)
  public void testMultipleDatanodeFailureRandomLength() throws Exception {
    int lenIndex = RANDOM.nextInt(LENGTHS.size());
    LOG.info("run testMultipleDatanodeFailureRandomLength with length index: "
        + lenIndex);
    runTestWithMultipleFailure(getLength(lenIndex));
  }

  @Test(timeout=240000)
  public void testBlockTokenExpired() throws Exception {
    final int length = NUM_DATA_BLOCKS * (BLOCK_SIZE - CELL_SIZE);
    final HdfsConfiguration conf = newHdfsConfiguration();

    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);
    // Set short retry timeouts so this test runs faster
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
    for (int dn = 0; dn < 9; dn += 2) {
      try {
        setup(conf);
        runTest(length, new int[]{length/2}, new int[]{dn}, true);
      } catch (Exception e) {
        LOG.error("failed, dn=" + dn + ", length=" + length);
        throw e;
      } finally {
        tearDown();
      }
    }
  }

  @Test(timeout = 90000)
  public void testAddBlockWhenNoSufficientDataBlockNumOfNodes()
      throws IOException {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    try {
      setup(conf);
      ArrayList<DataNode> dataNodes = cluster.getDataNodes();
      // shutdown few datanodes to avoid getting sufficient data blocks number
      // of datanodes
      int killDns = dataNodes.size() / 2;
      int numDatanodes = dataNodes.size() - killDns;
      for (int i = 0; i < killDns; i++) {
        cluster.stopDataNode(i);
      }
      cluster.restartNameNodes();
      cluster.triggerHeartbeats();
      DatanodeInfo[] info = dfs.getClient().datanodeReport(DatanodeReportType.LIVE);
      assertEquals("Mismatches number of live Dns ", numDatanodes, info.length);
      final Path dirFile = new Path(dir, "ecfile");
      FSDataOutputStream out;
      try {
        out = dfs.create(dirFile, true);
        out.write("something".getBytes());
        out.flush();
        out.close();
        Assert.fail("Failed to validate available dns against blkGroupSize");
      } catch (IOException ioe) {
        // expected
        GenericTestUtils.assertExceptionContains("Failed to get 6 nodes from" +
            " namenode: blockGroupSize= 9, blocks.length= 5", ioe);
      }
    } finally {
      tearDown();
    }
  }

  @Test(timeout = 90000)
  public void testAddBlockWhenNoSufficientParityNumOfNodes() throws IOException {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    try {
      setup(conf);
      ArrayList<DataNode> dataNodes = cluster.getDataNodes();
      // shutdown few data nodes to avoid writing parity blocks
      int killDns = (NUM_PARITY_BLOCKS - 1);
      int numDatanodes = dataNodes.size() - killDns;
      for (int i = 0; i < killDns; i++) {
        cluster.stopDataNode(i);
      }
      cluster.restartNameNodes();
      cluster.triggerHeartbeats();
      DatanodeInfo[] info = dfs.getClient().datanodeReport(
          DatanodeReportType.LIVE);
      assertEquals("Mismatches number of live Dns ", numDatanodes, info.length);
      Path srcPath = new Path(dir, "testAddBlockWhenNoSufficientParityNodes");
      int fileLength = StripedFileTestUtil.BLOCK_STRIPED_CELL_SIZE - 1000;
      final byte[] expected = StripedFileTestUtil.generateBytes(fileLength);
      DFSTestUtil.writeFile(dfs, srcPath, new String(expected));
      LOG.info("writing finished. Seek and read the file to verify.");
      StripedFileTestUtil.verifySeek(dfs, srcPath, fileLength);
    } finally {
      tearDown();
    }
  }

  void runTest(final int length) {
    final HdfsConfiguration conf = newHdfsConfiguration();
    for (int dn = 0; dn < 9; dn++) {
      try {
        LOG.info("runTest: dn=" + dn + ", length=" + length);
        setup(conf);
        runTest(length, new int[]{length/2}, new int[]{dn}, false);
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
            + Arrays.toString(killPos) + ", dnIndex=" + Arrays.toString(dnIndex));
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
   * runTest implementation
   * @param length file length
   * @param killPos killing positions in ascending order
   * @param dnIndex DN index to kill when meets killing positions
   * @param tokenExpire wait token to expire when kill a DN
   * @throws Exception
   */
  private void runTest(final int length, final int[] killPos,
      final int[] dnIndex, final boolean tokenExpire) throws Exception {
    if (killPos[0] <= FLUSH_POS) {
      LOG.warn("killPos=" + Arrays.toString(killPos) + " <= FLUSH_POS=" + FLUSH_POS
          + ", length=" + length + ", dnIndex=" + Arrays.toString(dnIndex));
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

      // set a short token lifetime (1 second)
      SecurityTestUtil.setBlockTokenLifetime(sm, 1000L);
    }

    final AtomicInteger pos = new AtomicInteger();
    final FSDataOutputStream out = dfs.create(p);
    final DFSStripedOutputStream stripedOut
        = (DFSStripedOutputStream)out.getWrappedStream();

    long firstGS = -1;  // first GS of this block group which never proceeds blockRecovery
    long oldGS = -1; // the old GS before bumping
    List<Long> gsList = new ArrayList<>();
    final List<DatanodeInfo> killedDN = new ArrayList<>();
    int numKilled=0;
    for(; pos.get() < length; ) {
      final int i = pos.getAndIncrement();
      if (numKilled < killPos.length &&  i == killPos[numKilled]) {
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

        killedDN.add(killDatanode(cluster, stripedOut, dnIndex[numKilled], pos));
        numKilled++;
      }

      write(out, i);

      if (i % BLOCK_GROUP_SIZE == FLUSH_POS) {
        firstGS = getGenerationStamp(stripedOut);
        oldGS = firstGS;
      }
      if (i > 0 && (i + 1) % BLOCK_GROUP_SIZE == 0) {
        gsList.add(oldGS);
      }
    }
    gsList.add(oldGS);
    out.close();
    assertEquals(dnIndex.length, numKilled);

    StripedFileTestUtil.waitBlockGroupsReported(dfs, fullPath, numKilled);

    cluster.triggerBlockReports();
    StripedFileTestUtil.checkData(dfs, p, length, killedDN, gsList);
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

  public static abstract class TestBase {
    static final long TIMEOUT = 240000;

    int getBase() {
      final String name = getClass().getSimpleName();
      int i = name.length() - 1;
      for(; i >= 0 && Character.isDigit(name.charAt(i)); i--);
      return Integer.parseInt(name.substring(i + 1));
    }

    private final TestDFSStripedOutputStreamWithFailure test
        = new TestDFSStripedOutputStreamWithFailure();
    private void run(int offset) {
      final int i = offset + getBase();
      final Integer length = getLength(i);
      if (length == null) {
        System.out.println("Skip test " + i + " since length=null.");
        return;
      }
      if (RANDOM.nextInt(16) != 0) {
        System.out.println("Test " + i + ", length=" + length
            + ", is not chosen to run.");
        return;
      }
      System.out.println("Run test " + i + ", length=" + length);
      test.runTest(length);
    }

    @Test(timeout=TIMEOUT) public void test0() {run(0);}
    @Test(timeout=TIMEOUT) public void test1() {run(1);}
    @Test(timeout=TIMEOUT) public void test2() {run(2);}
    @Test(timeout=TIMEOUT) public void test3() {run(3);}
    @Test(timeout=TIMEOUT) public void test4() {run(4);}
    @Test(timeout=TIMEOUT) public void test5() {run(5);}
    @Test(timeout=TIMEOUT) public void test6() {run(6);}
    @Test(timeout=TIMEOUT) public void test7() {run(7);}
    @Test(timeout=TIMEOUT) public void test8() {run(8);}
    @Test(timeout=TIMEOUT) public void test9() {run(9);}
  }
}
