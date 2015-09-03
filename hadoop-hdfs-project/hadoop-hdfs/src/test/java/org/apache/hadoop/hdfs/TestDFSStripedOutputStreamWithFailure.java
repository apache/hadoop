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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.SecurityTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
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
  }

  private static final int NUM_DATA_BLOCKS = HdfsConstants.NUM_DATA_BLOCKS;
  private static final int NUM_PARITY_BLOCKS = HdfsConstants.NUM_PARITY_BLOCKS;
  private static final int CELL_SIZE = HdfsConstants.BLOCK_STRIPED_CELL_SIZE;
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

  private static final List<Integer> LENGTHS = newLengths();

  static int getLength(int i) {
    return LENGTHS.get(i);
  }

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
    dfs.createErasureCodingZone(dir, null);
  }

  private void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private static byte getByte(long pos) {
    return (byte)pos;
  }

  private HdfsConfiguration newHdfsConfiguration() {
    final HdfsConfiguration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 6000L);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 0);
    return conf;
  }

  void runTest(final int length) {
    final HdfsConfiguration conf = newHdfsConfiguration();
    for (int dn = 0; dn < 9; dn++) {
      try {
        setup(conf);
        runTest(length, dn, false, conf);
      } catch (Exception e) {
        final String err = "failed, dn=" + dn + ", length=" + length
            + StringUtils.stringifyException(e);
        LOG.error(err);
        Assert.fail(err);
      } finally {
        tearDown();
      }
    }
  }

  @Test(timeout=240000)
  public void testDatanodeFailure56() throws Exception {
    runTest(getLength(56));
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
        runTest(length, dn, true, conf);
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
      DatanodeInfo[] info = dfs.getClient().datanodeReport(
          DatanodeReportType.LIVE);
      assertEquals("Mismatches number of live Dns ", numDatanodes, info.length);
      final Path dirFile = new Path(dir, "ecfile");
      FSDataOutputStream out = null;
      try {
        out = dfs.create(dirFile, true);
        out.write("something".getBytes());
        out.flush();
        out.close();
        Assert.fail("Failed to validate available dns against blkGroupSize");
      } catch (IOException ioe) {
        // expected
        GenericTestUtils.assertExceptionContains("Failed: the number of "
            + "remaining blocks = 5 < the number of data blocks = 6", ioe);
        DFSStripedOutputStream dfsout = (DFSStripedOutputStream) out
            .getWrappedStream();

        // get leading streamer and verify the last exception
        StripedDataStreamer datastreamer = dfsout.getStripedDataStreamer(0);
        try {
          datastreamer.getLastException().check(true);
          Assert.fail("Failed to validate available dns against blkGroupSize");
        } catch (IOException le) {
          GenericTestUtils.assertExceptionContains(
              "Failed to get datablocks number of nodes from"
                  + " namenode: blockGroupSize= 9, blocks.length= "
                  + numDatanodes, le);
        }
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
      int fileLength = HdfsConstants.BLOCK_STRIPED_CELL_SIZE - 1000;
      final byte[] expected = StripedFileTestUtil.generateBytes(fileLength);
      DFSTestUtil.writeFile(dfs, srcPath, new String(expected));
      StripedFileTestUtil.verifySeek(dfs, srcPath, fileLength);
    } finally {
      tearDown();
    }
  }

  private void runTest(final int length, final int dnIndex,
      final boolean tokenExpire, final HdfsConfiguration conf) {
    try {
      runTest(length, length/2, dnIndex, tokenExpire, conf);
    } catch(Exception e) {
      LOG.info("FAILED", e);
      Assert.fail(StringUtils.stringifyException(e));
    }
  }

  private void runTest(final int length, final int killPos,
      final int dnIndex, final boolean tokenExpire,
      final HdfsConfiguration conf) throws Exception {
    if (killPos <= FLUSH_POS) {
      LOG.warn("killPos=" + killPos + " <= FLUSH_POS=" + FLUSH_POS
          + ", length=" + length + ", dnIndex=" + dnIndex);
      return; //skip test
    }
    Preconditions.checkArgument(length > killPos,
        "length=%s <= killPos=%s", length, killPos);

    // start a datanode now, will kill one later
    cluster.startDataNodes(conf, 1, true, null, null);
    cluster.waitActive();

    final Path p = new Path(dir, "dn" + dnIndex + "len" + length + "kill" +  killPos);
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

    long oldGS = -1;
    boolean killed = false;
    for(; pos.get() < length; ) {
      final int i = pos.getAndIncrement();
      if (i == killPos) {
        final long gs = getGenerationStamp(stripedOut);
        Assert.assertTrue(oldGS != -1);
        Assert.assertEquals(oldGS, gs);

        if (tokenExpire) {
          DFSTestUtil.flushInternal(stripedOut);
          waitTokenExpires(out);
        }

        killDatanode(cluster, stripedOut, dnIndex, pos);
        killed = true;
      }

      write(out, i);

      if (i == FLUSH_POS) {
        oldGS = getGenerationStamp(stripedOut);
      }
    }
    out.close();

    short expectedReported = StripedFileTestUtil.getRealTotalBlockNum(length);
    if (length > dnIndex * CELL_SIZE || dnIndex >= NUM_DATA_BLOCKS) {
      expectedReported--;
    }
    DFSTestUtil.waitReplication(dfs, p, expectedReported);

    Assert.assertTrue(killed);

    // check file length
    final FileStatus status = dfs.getFileStatus(p);
    Assert.assertEquals(length, status.getLen());

    checkData(dfs, fullPath, length, dnIndex, oldGS);
  }

  static void write(FSDataOutputStream out, int i) throws IOException {
    try {
      out.write(getByte(i));
    } catch(IOException ioe) {
      throw new IOException("Failed at i=" + i, ioe);
    }
  }

  static long getGenerationStamp(DFSStripedOutputStream out)
      throws IOException {
    final long gs = DFSTestUtil.flushInternal(out).getGenerationStamp();
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

  static void killDatanode(MiniDFSCluster cluster, DFSStripedOutputStream out,
      final int dnIndex, final AtomicInteger pos) {
    final StripedDataStreamer s = out.getStripedDataStreamer(dnIndex);
    final DatanodeInfo datanode = getDatanodes(s);
    LOG.info("killDatanode " + dnIndex + ": " + datanode + ", pos=" + pos);
    cluster.stopDataNode(datanode.getXferAddr());
  }

  static void checkData(DistributedFileSystem dfs, String src, int length,
      int killedDnIndex, long oldGS) throws IOException {
    List<List<LocatedBlock>> blockGroupList = new ArrayList<>();
    LocatedBlocks lbs = dfs.getClient().getLocatedBlocks(src, 0L);
    final int expectedNumGroup = (length - 1)/BLOCK_GROUP_SIZE + 1;
    Assert.assertEquals(expectedNumGroup, lbs.getLocatedBlocks().size());

    for (LocatedBlock firstBlock : lbs.getLocatedBlocks()) {
      Assert.assertTrue(firstBlock instanceof LocatedStripedBlock);

      final long gs = firstBlock.getBlock().getGenerationStamp();
      final String s = "gs=" + gs + ", oldGS=" + oldGS;
      LOG.info(s);
      Assert.assertTrue(s, gs >= oldGS);

      LocatedBlock[] blocks = StripedBlockUtil.parseStripedBlockGroup(
          (LocatedStripedBlock) firstBlock,
          CELL_SIZE, NUM_DATA_BLOCKS, NUM_PARITY_BLOCKS);
      blockGroupList.add(Arrays.asList(blocks));
    }

    // test each block group
    for (int group = 0; group < blockGroupList.size(); group++) {
      final boolean isLastGroup = group == blockGroupList.size() - 1;
      final int groupSize = !isLastGroup? BLOCK_GROUP_SIZE
          : length - (blockGroupList.size() - 1)*BLOCK_GROUP_SIZE;
      final int numCellInGroup = (groupSize - 1)/CELL_SIZE + 1;
      final int lastCellIndex = (numCellInGroup - 1) % NUM_DATA_BLOCKS;
      final int lastCellSize = groupSize - (numCellInGroup - 1)*CELL_SIZE;

      //get the data of this block
      List<LocatedBlock> blockList = blockGroupList.get(group);
      byte[][] dataBlockBytes = new byte[NUM_DATA_BLOCKS][];
      byte[][] parityBlockBytes = new byte[NUM_PARITY_BLOCKS][];

      // for each block, use BlockReader to read data
      for (int i = 0; i < blockList.size(); i++) {
        final int j = i >= NUM_DATA_BLOCKS? 0: i;
        final int numCellInBlock = (numCellInGroup - 1)/NUM_DATA_BLOCKS
            + (j <= lastCellIndex? 1: 0);
        final int blockSize = numCellInBlock*CELL_SIZE
            + (isLastGroup && j == lastCellIndex? lastCellSize - CELL_SIZE: 0);

        final byte[] blockBytes = new byte[blockSize];
        if (i < NUM_DATA_BLOCKS) {
          dataBlockBytes[i] = blockBytes;
        } else {
          parityBlockBytes[i - NUM_DATA_BLOCKS] = blockBytes;
        }

        final LocatedBlock lb = blockList.get(i);
        LOG.info("i,j=" + i + ", " + j + ", numCellInBlock=" + numCellInBlock
            + ", blockSize=" + blockSize + ", lb=" + lb);
        if (lb == null) {
          continue;
        }
        final ExtendedBlock block = lb.getBlock();
        Assert.assertEquals(blockSize, block.getNumBytes());


        if (block.getNumBytes() == 0) {
          continue;
        }

        if (i != killedDnIndex) {
          final BlockReader blockReader = BlockReaderTestUtil.getBlockReader(
              dfs, lb, 0, block.getNumBytes());
          blockReader.readAll(blockBytes, 0, (int) block.getNumBytes());
          blockReader.close();
        }
      }

      // check data
      final int groupPosInFile = group*BLOCK_GROUP_SIZE;
      for (int i = 0; i < dataBlockBytes.length; i++) {
        final byte[] actual = dataBlockBytes[i];
        for (int posInBlk = 0; posInBlk < actual.length; posInBlk++) {
          final long posInFile = StripedBlockUtil.offsetInBlkToOffsetInBG(
              CELL_SIZE, NUM_DATA_BLOCKS, posInBlk, i) + groupPosInFile;
          Assert.assertTrue(posInFile < length);
          final byte expected = getByte(posInFile);

          if (i == killedDnIndex) {
            actual[posInBlk] = expected;
          } else {
            String s = "expected=" + expected + " but actual=" + actual[posInBlk]
                + ", posInFile=" + posInFile + ", posInBlk=" + posInBlk
                + ". group=" + group + ", i=" + i;
            Assert.assertEquals(s, expected, actual[posInBlk]);
          }
        }
      }

      // check parity
      TestDFSStripedOutputStream.verifyParity(dfs.getConf(),
          lbs.getLocatedBlocks().get(group).getBlockSize(),
          CELL_SIZE, dataBlockBytes, parityBlockBytes,
          killedDnIndex - dataBlockBytes.length);
    }
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
      final int length = getLength(i);
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
