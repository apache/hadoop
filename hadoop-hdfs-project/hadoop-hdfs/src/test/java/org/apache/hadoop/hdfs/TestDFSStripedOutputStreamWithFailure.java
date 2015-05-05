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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

  private final HdfsConfiguration conf = new HdfsConfiguration();
  private MiniDFSCluster cluster;
  private DistributedFileSystem dfs;
  private final Path dir = new Path("/"
      + TestDFSStripedOutputStreamWithFailure.class.getSimpleName());


  @Before
  public void setup() throws IOException {
    final int numDNs = NUM_DATA_BLOCKS + NUM_PARITY_BLOCKS;
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
    dfs.mkdirs(dir);
    dfs.createErasureCodingZone(dir, null);
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private static byte getByte(long pos) {
    return (byte)pos;
  }

  @Test(timeout=120000)
  public void testDatanodeFailure1() {
    final int length = NUM_DATA_BLOCKS*(BLOCK_SIZE - CELL_SIZE);
    final int dn = 1;
    runTest("file" + dn, length, dn);
  }

  @Test(timeout=120000)
  public void testDatanodeFailure2() {
    final int length = NUM_DATA_BLOCKS*(BLOCK_SIZE - CELL_SIZE);
    final int dn = 2;
    runTest("file" + dn, length, dn);
  }

  @Test(timeout=120000)
  public void testDatanodeFailure3() {
    final int length = NUM_DATA_BLOCKS*(BLOCK_SIZE - CELL_SIZE);
    final int dn = 3;
    runTest("file" + dn, length, dn);
  }

  @Test(timeout=120000)
  public void testDatanodeFailure4() {
    final int length = NUM_DATA_BLOCKS*(BLOCK_SIZE - CELL_SIZE);
    final int dn = 4;
    runTest("file" + dn, length, dn);
  }

  @Test(timeout=120000)
  public void testDatanodeFailure5() {
    final int length = NUM_DATA_BLOCKS*(BLOCK_SIZE - CELL_SIZE);
    final int dn = 5;
    runTest("file" + dn, length, dn);
  }

  @Test(timeout=120000)
  public void testDatanodeFailure6() {
    final int length = NUM_DATA_BLOCKS*(BLOCK_SIZE - CELL_SIZE);
    final int dn = 6;
    runTest("file" + dn, length, dn);
  }

  @Test(timeout=120000)
  public void testDatanodeFailure7() {
    final int length = NUM_DATA_BLOCKS*(BLOCK_SIZE - CELL_SIZE);
    final int dn = 7;
    runTest("file" + dn, length, dn);
  }

  @Test(timeout=120000)
  public void testDatanodeFailure8() {
    final int length = NUM_DATA_BLOCKS*(BLOCK_SIZE - CELL_SIZE);
    final int dn = 8;
    runTest("file" + dn, length, dn);
  }

  private void runTest(final String src, final int length, final int dnIndex) {
    try {
      cluster.startDataNodes(conf, 1, true, null, null);
      cluster.waitActive();

      runTest(new Path(dir, src), length, dnIndex);
    } catch(Exception e) {
      LOG.info("FAILED", e);
      Assert.fail(StringUtils.stringifyException(e));
    }
  }

  private void runTest(final Path p, final int length,
      final int dnIndex) throws Exception {
    LOG.info("p=" + p + ", length=" + length + ", dnIndex=" + dnIndex);
    final String fullPath = p.toString();

    final AtomicInteger pos = new AtomicInteger();
    final FSDataOutputStream out = dfs.create(p);
    final AtomicBoolean killed = new AtomicBoolean();
    final Thread killer = new Thread(new Runnable() {
      @Override
      public void run() {
        killDatanode(cluster, (DFSStripedOutputStream)out.getWrappedStream(),
            dnIndex, pos);
        killed.set(true);
      }
    });
    killer.start();

    final int mask = (1 << 16) - 1;
    for(; pos.get() < length; ) {
      final int i = pos.getAndIncrement();
      write(out, i);
      if ((i & mask) == 0) {
        final long ms = 100;
        LOG.info("i=" + i + " sleep " + ms);
        Thread.sleep(ms);
      }
    }
    killer.join(10000);
    Assert.assertTrue(killed.get());
    out.close();

    // check file length
    final FileStatus status = dfs.getFileStatus(p);
    Assert.assertEquals(length, status.getLen());

    checkData(dfs, fullPath, length, dnIndex);
  }

  static void write(FSDataOutputStream out, int i) throws IOException {
    try {
      out.write(getByte(i));
    } catch(IOException ioe) {
      throw new IOException("Failed at i=" + i, ioe);
    }
  }

  static DatanodeInfo getDatanodes(StripedDataStreamer streamer) {
    for(;;) {
      final DatanodeInfo[] datanodes = streamer.getNodes();
      if (datanodes != null) {
        Assert.assertEquals(1, datanodes.length);
        Assert.assertNotNull(datanodes[0]);
        return datanodes[0];
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException ignored) {
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
      int killedDnIndex) throws IOException {
    List<List<LocatedBlock>> blockGroupList = new ArrayList<>();
    LocatedBlocks lbs = dfs.getClient().getLocatedBlocks(src, 0L);
    final int expectedNumGroup = (length - 1)/BLOCK_GROUP_SIZE + 1;
    Assert.assertEquals(expectedNumGroup, lbs.getLocatedBlocks().size());

    for (LocatedBlock firstBlock : lbs.getLocatedBlocks()) {
      Assert.assertTrue(firstBlock instanceof LocatedStripedBlock);
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
      final int numCellInGroup = (int)((groupSize - 1)/CELL_SIZE + 1);
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
            + (isLastGroup && i == lastCellIndex? lastCellSize - CELL_SIZE: 0);

        final byte[] blockBytes = new byte[blockSize];
        if (i < NUM_DATA_BLOCKS) {
          dataBlockBytes[i] = blockBytes;
        } else {
          parityBlockBytes[i - NUM_DATA_BLOCKS] = blockBytes;
        }

        final LocatedBlock lb = blockList.get(i);
        LOG.info("XXX i=" + i + ", lb=" + lb);
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
      TestDFSStripedOutputStream.verifyParity(
          lbs.getLocatedBlocks().get(group).getBlockSize(),
          CELL_SIZE, dataBlockBytes, parityBlockBytes,
          killedDnIndex - dataBlockBytes.length);
    }
  }
}
