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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * This test simulates a variety of situations when blocks are being intentionally
 * corrupted, unexpectedly modified, and so on before a block report is happening
 */
public class TestBlockReport {
  public static final Log LOG = LogFactory.getLog(TestBlockReport.class);

  private static final short REPL_FACTOR = 1;
  private static final int RAND_LIMIT = 2000;
  private static final long DN_RESCAN_INTERVAL = 5000;
  private static final long DN_RESCAN_EXTRA_WAIT = 2 * DN_RESCAN_INTERVAL;
  private static final int DN_N0 = 0;
  private static final int FILE_START = 0;

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;

  Random rand = new Random(RAND_LIMIT);

  private static Configuration conf;

  static {
    conf = new Configuration();
    int customPerChecksumSize = 512;
    int customBlockSize = customPerChecksumSize * 3;
    conf.setInt("io.bytes.per.checksum", customPerChecksumSize);
    conf.setLong("dfs.block.size", customBlockSize);
    conf.setLong("dfs.datanode.directoryscan.interval", DN_RESCAN_INTERVAL);
  }

  @Before
  public void startUpCluster() throws IOException {
    cluster = new MiniDFSCluster(conf, REPL_FACTOR, true, null);
    fs = (DistributedFileSystem) cluster.getFileSystem();
  }

  @After
  public void shutDownCluster() throws IOException {
    fs.close();
    cluster.shutdownDataNodes();
    cluster.shutdown();
  }

  /**
   * Test write a file, verifies and closes it. Then the length of the blocks
   * are messed up and BlockReport is forced.
   * The length of blocks in NN's memory should be the same as set by the DN
   */
  @Test
  public void messWithBlocksLen() throws IOException {
    final String METHOD_NAME = "TestBlockReport";
    LOG.info("Running test " + METHOD_NAME);

    Path filePath = new Path("/" + METHOD_NAME + ".dat");
    DFSTestUtil.createFile(fs, filePath,
        (long)AppendTestUtil.FILE_SIZE, REPL_FACTOR, rand.nextLong());

    // mock with newly created blocks
    // I can't use DFSTestUtil.getAllBlocks(fs.open(filePath)) because it
    // will keep the file open which will prevent the effect of the test
    ArrayList<Block> blocks = 
      locatedToBlocks(cluster.getNameNode().getBlockLocations(
        filePath.toString(), FILE_START,
        AppendTestUtil.FILE_SIZE).getLocatedBlocks(), null);

    LOG.info("Number of blocks allocated " + blocks.size());
    int[] newLengths = new int[blocks.size()];
    int tempLen;
    for (int i = 0; i < blocks.size(); i++) {
      Block b = blocks.get(i);
      LOG.debug("Block " + b.getBlockName() + " before\t" + "Size " +
          b.getNumBytes());
      LOG.debug("Setting new length");
      tempLen = rand.nextInt(AppendTestUtil.BLOCK_SIZE);
      b.set(b.getBlockId(), tempLen, b.getGenerationStamp());
      LOG.debug("Block " + b.getBlockName() + " after\t " + "Size " +
          b.getNumBytes());
      newLengths[i] = tempLen;
    }
    cluster.getNameNode().blockReport(
        cluster.listDataNodes()[DN_N0].dnRegistration,
        new BlockListAsLongs(blocks, null).getBlockListAsLongs());

    List<LocatedBlock> blocksAfterReport =
        DFSTestUtil.getAllBlocks(fs.open(filePath));

    LOG.info("After mods: Number of blocks allocated " +
        blocksAfterReport.size());

    for (int i = 0; i < blocksAfterReport.size(); i++) {
      Block b = blocksAfterReport.get(i).getBlock();
      assertEquals("Length of " + i + "th block is incorrect",
          newLengths[i], b.getNumBytes());
    }
  }

  /**
   * Test write a file, verifies and closes it. Then a couple of random blocks
   * is removed and BlockReport is forced; the FSNamesystem is pushed to
   * recalculate required DN's activities such as replications and so on.
   * The number of missing and under-replicated blocks should be the same in
   * case of a single-DN cluster.
   */
  @Test
  public void messWithBlockReplication() throws IOException {
    final String METHOD_NAME = "messWithBlockReplication";
    LOG.info("Running test " + METHOD_NAME);

    Path filePath = new Path("/" + METHOD_NAME + ".dat");
    DFSTestUtil.createFile(fs, filePath,
        (long)AppendTestUtil.FILE_SIZE, REPL_FACTOR, rand.nextLong());

    // mock around with newly created blocks and delete some
    String testDataDirectory = cluster.getDataDirectory();

    File dataDir = new File(testDataDirectory);
    assertTrue(dataDir.isDirectory());

    List<Block> blocks2Remove = new ArrayList<Block>();
    List<Integer> removedIndex = new ArrayList<Integer>();
    List<LocatedBlock> lBlocks = cluster.getNameNode().getBlockLocations(
        filePath.toString(), FILE_START,
        AppendTestUtil.FILE_SIZE).getLocatedBlocks();

    while (removedIndex.size() != 2) {
      int newRemoveIndex = rand.nextInt(lBlocks.size());
      if (!removedIndex.contains(newRemoveIndex))
        removedIndex.add(newRemoveIndex);
    }

    for (Integer aRemovedIndex : removedIndex) {
      blocks2Remove.add(lBlocks.get(aRemovedIndex).getBlock());
    }
    ArrayList<Block> blocks = locatedToBlocks(lBlocks, removedIndex);

    LOG.debug("Number of blocks allocated " + lBlocks.size());

    for (Block b : blocks2Remove) {
      LOG.debug("Removing the block " + b.getBlockName());
      for (File f : findAllFiles(dataDir, new MyFileFilter(b.getBlockName()))) {
        cluster.listDataNodes()[DN_N0].getFSDataset().unfinalizeBlock(b);
        if (!f.delete())
          LOG.warn("Couldn't delete " + b.getBlockName());
      }
    }

    try { //Wait til next re-scan
      Thread.sleep(DN_RESCAN_EXTRA_WAIT);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    cluster.getNameNode().blockReport(
        cluster.listDataNodes()[DN_N0].dnRegistration,
        new BlockListAsLongs(blocks, null).getBlockListAsLongs());

    cluster.getNamesystem().computeDatanodeWork();

    // I suppose to see blocks2Remove.size() as under-replicated
    LOG.debug("Missing " + cluster.getNamesystem().getMissingBlocksCount());
    LOG.debug("Corrupted " + cluster.getNamesystem().getCorruptReplicaBlocks());
    LOG.debug("Under-replicated " + cluster.getNamesystem().
        getUnderReplicatedBlocks());
    LOG.debug("Pending delete " + cluster.getNamesystem().
        getPendingDeletionBlocks());
    LOG.debug("Pending replications " + cluster.getNamesystem().
        getPendingReplicationBlocks());
    LOG.debug("Excess " + cluster.getNamesystem().getExcessBlocks());
    LOG.debug("Total " + cluster.getNamesystem().getBlocksTotal());

    assertEquals("Wrong number of MissingBlocks is found",
        blocks2Remove.size(), cluster.getNamesystem().getMissingBlocksCount());
    assertEquals("Wrong number of UnderReplicatedBlocks is found",
        blocks2Remove.size(), cluster.getNamesystem().getUnderReplicatedBlocks());
  }

  private ArrayList<Block> locatedToBlocks(final List<LocatedBlock> locatedBlks,
                                           List<Integer> positionsToRemove) {
    int substructLen = 0;
    if (positionsToRemove != null) { // Need to allocated smaller array
      substructLen = positionsToRemove.size();
    }
    Block[] ret = new Block[substructLen];
    ArrayList<Block> newList = new ArrayList<Block>();
    for (int i = 0; i < locatedBlks.size(); i++) {
      if (positionsToRemove != null && positionsToRemove.contains(i)) {
        LOG.debug(i + " block to be omitted");
        continue;
      }
      newList.add(locatedBlks.get(i).getBlock());
    }
    return newList;
  }

  private List<File> findAllFiles(File top, FilenameFilter mask) {
    if (top == null) return null;
    ArrayList<File> ret = new ArrayList<File>();
    for (File f : top.listFiles()) {
      if (f.isDirectory())
        ret.addAll(findAllFiles(f, mask));
      else if (mask.accept(f, f.getName()))
        ret.add(f);
    }
    return ret;
  }

  private class MyFileFilter implements FilenameFilter {
    private String nameToAccept = "";

    public MyFileFilter(String nameToAccept) {
      if (nameToAccept == null)
        throw new IllegalArgumentException("Argument isn't suppose to be null");
      this.nameToAccept = nameToAccept;
    }

    public boolean accept(File file, String s) {
      return s != null && s.contains(nameToAccept);
    }
  }

  private static void initLoggers () {
    ((Log4JLogger) NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) DataNode.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) TestBlockReport.LOG).getLogger().setLevel(Level.ALL);
  }
}
