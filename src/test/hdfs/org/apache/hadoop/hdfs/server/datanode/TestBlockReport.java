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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * This test simulates a variety of situations when blocks are being
 * intentionally orrupted, unexpectedly modified, and so on before a block
 * report is happening
 */
public class TestBlockReport {
  public static final Log LOG = LogFactory.getLog(TestBlockReport.class);

  private static short REPL_FACTOR = 1;
  private static final int RAND_LIMIT = 2000;
  private static final long DN_RESCAN_INTERVAL = 5000;
  private static final long DN_RESCAN_EXTRA_WAIT = 2 * DN_RESCAN_INTERVAL;
  private static final int DN_N0 = 0;
  private static final int FILE_START = 0;

  static final int BLOCK_SIZE = 1024;
  static final int NUM_BLOCKS = 10;
  static final int FILE_SIZE = NUM_BLOCKS * BLOCK_SIZE + 1;

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;

  Random rand = new Random(RAND_LIMIT);

  private static Configuration conf;

  static {
    initLoggers();
    resetConfiguration();
  }

  @Before
  public void startUpCluster() throws IOException {
    REPL_FACTOR = 1; //Reset if case a test has modified the value
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
   * The modification of blocks' length has to be ignored
   *
   * @throws java.io.IOException on an error
   */
  @Test
  public void blockReport_01() throws IOException {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path filePath = new Path("/" + METHOD_NAME + ".dat");

    ArrayList<Block> blocks = prepareForRide(filePath, METHOD_NAME, FILE_SIZE);

    LOG.debug("Number of blocks allocated " + blocks.size());
    long[] oldLengths = new long[blocks.size()];
    int tempLen;
    for (int i = 0; i < blocks.size(); i++) {
      Block b = blocks.get(i);
      LOG.debug("Block " + b.getBlockName() + " before\t" + "Size " +
        b.getNumBytes());
      oldLengths[i] = b.getNumBytes();
      LOG.debug("Setting new length");
      tempLen = rand.nextInt(BLOCK_SIZE);
      b.set(b.getBlockId(), tempLen, b.getGenerationStamp());
      LOG.debug("Block " + b.getBlockName() + " after\t " + "Size " +
        b.getNumBytes());
    }
    cluster.getNameNode().blockReport(
      cluster.getDataNodes().get(DN_N0).dnRegistration,
      new BlockListAsLongs(blocks, null).getBlockListAsLongs());

    List<LocatedBlock> blocksAfterReport =
      DFSTestUtil.getAllBlocks(fs.open(filePath));

    LOG.debug("After mods: Number of blocks allocated " +
      blocksAfterReport.size());

    for (int i = 0; i < blocksAfterReport.size(); i++) {
      Block b = blocksAfterReport.get(i).getBlock();
      assertEquals("Length of " + i + "th block is incorrect",
        oldLengths[i], b.getNumBytes());
    }
  }

  /**
   * Test write a file, verifies and closes it. Then a couple of random blocks
   * is removed and BlockReport is forced; the FSNamesystem is pushed to
   * recalculate required DN's activities such as replications and so on.
   * The number of missing and under-replicated blocks should be the same in
   * case of a single-DN cluster.
   *
   * @throws IOException in case of errors
   */
  @Test
  public void blockReport_02() throws IOException {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    LOG.info("Running test " + METHOD_NAME);

    Path filePath = new Path("/" + METHOD_NAME + ".dat");
    DFSTestUtil.createFile(fs, filePath,
      (long) FILE_SIZE, REPL_FACTOR, rand.nextLong());

    // mock around with newly created blocks and delete some
    File dataDir = new File(cluster.getDataDirectory());
    assertTrue(dataDir.isDirectory());

    List<Block> blocks2Remove = new ArrayList<Block>();
    List<Integer> removedIndex = new ArrayList<Integer>();
    List<LocatedBlock> lBlocks = cluster.getNameNode().getBlockLocations(
      filePath.toString(), FILE_START,
      FILE_SIZE).getLocatedBlocks();

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
      for (File f : findAllFiles(dataDir,
        new MyFileFilter(b.getBlockName(), true))) {
        cluster.getDataNodes().get(DN_N0).getFSDataset().unfinalizeBlock(b);
        if (!f.delete())
          LOG.warn("Couldn't delete " + b.getBlockName());
      }
    }

    waitTil(DN_RESCAN_EXTRA_WAIT);

    cluster.getNameNode().blockReport(
      cluster.getDataNodes().get(DN_N0).dnRegistration,
      new BlockListAsLongs(blocks, null).getBlockListAsLongs());

    cluster.getNamesystem().computeDatanodeWork();

    printStats();

    assertEquals("Wrong number of MissingBlocks is found",
      blocks2Remove.size(), cluster.getNamesystem().getMissingBlocksCount());
    assertEquals("Wrong number of UnderReplicatedBlocks is found",
      blocks2Remove.size(), cluster.getNamesystem().getUnderReplicatedBlocks());
  }


  /**
   * Test writes a file and closes it. Then test finds a block
   * and changes its GS to be < of original one.
   * New empty block is added to the list of blocks.
   * Block report is forced and the check for # of corrupted blocks is performed.
   *
   * @throws IOException in case of an error
   */
  @Test
  public void blockReport_03() throws IOException {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path filePath = new Path("/" + METHOD_NAME + ".dat");

    ArrayList<Block> blocks =
      prepareForRide(filePath, METHOD_NAME, FILE_SIZE);

    // The block with modified GS won't be found. Has to be deleted
    blocks.get(0).setGenerationStamp(rand.nextLong());
    // This new block is unknown to NN and will be mark for deletion.
    blocks.add(new Block());
    DatanodeCommand dnCmd =
      cluster.getNameNode().blockReport(
        cluster.getDataNodes().get(DN_N0).dnRegistration,
        new BlockListAsLongs(blocks, null).getBlockListAsLongs());
    LOG.debug("Got the command: " + dnCmd);
    printStats();

    assertEquals("Wrong number of CorruptedReplica+PendingDeletion " +
      "blocks is found", 2,
        cluster.getNamesystem().getCorruptReplicaBlocks() +
        cluster.getNamesystem().getPendingDeletionBlocks());
  }

  /**
   * This test isn't a representative case for BlockReport
   * The empty method is going to be left here to keep the naming
   * of the test plan in synch with the actual implementation
   * @throws IOException in case of errors
   */
  public void blockReport_04() throws IOException {
  }

  // Client requests new block from NN. The test corrupts this very block
  // and forces new block report.
  // The test case isn't specific for BlockReport because it relies on
  // BlockScanner which is out of scope of this test
  // Keeping the name to be in synch with the test plan
  //
  public void blockReport_05() throws IOException {
  }

  /**
   * Test creates a file and closes it.
   * The second datanode is started in the cluster.
   * As soon as the replication process is completed test runs
   * Block report and checks that no underreplicated blocks are left
   *
   * @throws IOException in case of an error
   */
  @Test
  public void blockReport_06() throws IOException {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path filePath = new Path("/" + METHOD_NAME + ".dat");
    final int DN_N1 = DN_N0 + 1;

    ArrayList<Block> blocks = writeFile(METHOD_NAME, FILE_SIZE, filePath);
    startDNandWait(filePath, true);

    cluster.getNameNode().blockReport(
      cluster.getDataNodes().get(DN_N1).dnRegistration,
      new BlockListAsLongs(blocks, null).getBlockListAsLongs());
    printStats();
    assertEquals("Wrong number of PendingReplication Blocks",
      0, cluster.getNamesystem().getUnderReplicatedBlocks());
  }

  /**
   * Similar to BlockReport_03() but works with two DNs
   * Test writes a file and closes it.
   * The second datanode is started in the cluster.
   * As soon as the replication process is completed test finds a block from
   * the second DN and sets its GS to be < of original one.
   * Block report is forced and the check for # of currupted blocks is performed.
   * Another block is chosen and its length is set to a lesser than original.
   * A check for another corrupted block is performed after yet another
   * BlockReport
   *
   * @throws IOException in case of an error
   */
  @Test
  // Currently this test is failing as expected 'cause the correct behavior is
  // not yet implemented (9/15/09)
  public void blockReport_07() throws IOException {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path filePath = new Path("/" + METHOD_NAME + ".dat");
    final int DN_N1 = DN_N0 + 1;

    // write file and start second node to be "older" than the original
    ArrayList<Block> blocks = writeFile(METHOD_NAME, FILE_SIZE, filePath);
    startDNandWait(filePath, true);

    int randIndex = rand.nextInt(blocks.size());
    // Get a block and screw its GS
    Block corruptedBlock = blocks.get(randIndex);
    String secondNode = cluster.getDataNodes().get(DN_N1).
      getDatanodeRegistration().getStorageID();
    LOG.debug("Working with " + secondNode);
    LOG.debug("BlockGS before " + blocks.get(randIndex).getGenerationStamp());
    corruptBlockGS(corruptedBlock);
    LOG.debug("BlockGS after " + blocks.get(randIndex).getGenerationStamp());

    LOG.debug("Done corrupting GS of " + corruptedBlock.getBlockName());
    cluster.getNameNode().blockReport(
      cluster.getDataNodes().get(DN_N1).dnRegistration,
      new BlockListAsLongs(blocks, null).getBlockListAsLongs());
    printStats();
    assertEquals("Wrong number of Corrupted blocks",
      1, cluster.getNamesystem().getCorruptReplicaBlocks() +
// the following might have to be added into the equation if 
// the same block could be in two different states at the same time
// and then the expected number of has to be changed to '2'        
//        cluster.getNamesystem().getPendingReplicationBlocks() +
        cluster.getNamesystem().getPendingDeletionBlocks());

    // Get another block and screw its length to be less than original
    if (randIndex == 0)
      randIndex++;
    else
      randIndex--;
    corruptedBlock = blocks.get(randIndex);
    corruptBlockLen(corruptedBlock);
    LOG.debug("Done corrupting length of " + corruptedBlock.getBlockName());
    cluster.getNameNode().blockReport(
      cluster.getDataNodes().get(DN_N1).dnRegistration,
      new BlockListAsLongs(blocks, null).getBlockListAsLongs());
    printStats();

    assertEquals("Wrong number of Corrupted blocks",
      2, cluster.getNamesystem().getCorruptReplicaBlocks() +
        cluster.getNamesystem().getPendingReplicationBlocks() +
        cluster.getNamesystem().getPendingDeletionBlocks());

    printStats();

  }

  /**
   * The test set the configuration parameters for a large block size and
   * restarts initiated single-node cluster.
   * Then it writes a file > block_size and closes it.
   * The second datanode is started in the cluster.
   * As soon as the replication process is started and at least one TEMPORARY
   * replica is found test forces BlockReport process and checks
   * if the TEMPORARY replica isn't reported on it.
   * Eventually, the configuration is being restored into the original state.
   *
   * @throws IOException in case of an error
   */
  @Test
  public void blockReport_08() throws IOException {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path filePath = new Path("/" + METHOD_NAME + ".dat");
    final int DN_N1 = DN_N0 + 1;
    final int bytesChkSum = 1024 * 1000;

    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, bytesChkSum);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 6 * bytesChkSum);
    shutDownCluster();
    startUpCluster();

    try {
      ArrayList<Block> blocks =
        writeFile(METHOD_NAME, 12 * bytesChkSum, filePath);
      Block bl = findBlock(filePath, 12 * bytesChkSum);
      BlockChecker bc = new BlockChecker(filePath);
      bc.start();

      waitForTempReplica(bl, DN_N1);

      cluster.getNameNode().blockReport(
        cluster.getDataNodes().get(DN_N1).dnRegistration,
        new BlockListAsLongs(blocks, null).getBlockListAsLongs());
      printStats();
      assertEquals("Wrong number of PendingReplication blocks",
        blocks.size(), cluster.getNamesystem().getPendingReplicationBlocks());

      try {
        bc.join();
      } catch (InterruptedException e) { }
    } finally {
      resetConfiguration(); // return the initial state of the configuration
    }
  }

  // Similar to BlockReport_08 but corrupts GS and len of the TEMPORARY's
  // replica block. Expect the same behaviour: NN should simply ignore this
  // block
  @Test
  public void blockReport_09() throws IOException {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path filePath = new Path("/" + METHOD_NAME + ".dat");
    final int DN_N1 = DN_N0 + 1;
    final int bytesChkSum = 1024 * 1000;

    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, bytesChkSum);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 6 * bytesChkSum);
    shutDownCluster();
    startUpCluster();
    // write file and start second node to be "older" than the original

    try {
      ArrayList<Block> blocks =
        writeFile(METHOD_NAME, 12 * bytesChkSum, filePath);

      Block bl = findBlock(filePath, 12 * bytesChkSum);
      BlockChecker bc = new BlockChecker(filePath);
      bc.start();
      corruptBlockGS(bl);
      corruptBlockLen(bl);

      waitForTempReplica(bl, DN_N1);
                                                
      cluster.getNameNode().blockReport(
        cluster.getDataNodes().get(DN_N1).dnRegistration,
        new BlockListAsLongs(blocks, null).getBlockListAsLongs());
      printStats();
      assertEquals("Wrong number of PendingReplication blocks",
        2, cluster.getNamesystem().getPendingReplicationBlocks());
      
      try {
        bc.join();
      } catch (InterruptedException e) {}
    } finally {
      resetConfiguration(); // return the initial state of the configuration
    }
  }

  private void waitForTempReplica(Block bl, int DN_N1) {
    final boolean tooLongWait = false;
    final int TIMEOUT = 40000;
    
    LOG.debug("Wait for datanode " + DN_N1 + " to appear");
    while (cluster.getDataNodes().size() <= DN_N1) {
      waitTil(20);
    }
    LOG.debug("Total number of DNs " + cluster.getDataNodes().size());
    // Look about specified DN for the replica of the block from 1st DN
    Replica r;
    r = ((FSDataset) cluster.getDataNodes().get(DN_N1).getFSDataset()).
      fetchReplicaInfo(bl.getBlockId());
    long start = System.currentTimeMillis();
    int count = 0;
    while (r == null) {
      waitTil(50);
      r = ((FSDataset) cluster.getDataNodes().get(DN_N1).getFSDataset()).
        fetchReplicaInfo(bl.getBlockId());
      long waiting_period = System.currentTimeMillis() - start;
      if (count++ % 10 == 0)
        LOG.debug("Has been waiting for " + waiting_period + " ms.");
      if (waiting_period > TIMEOUT)
        assertTrue("Was waiting too long to get ReplicaInfo from a datanode",
          tooLongWait);
    }

    HdfsConstants.ReplicaState state = r.getState();
    LOG.debug("Replica state before the loop " + state.getValue());
    start = System.currentTimeMillis();
    while (state != HdfsConstants.ReplicaState.TEMPORARY) {
      waitTil(100);
      state = r.getState();
      LOG.debug("Keep waiting for " + bl.getBlockName() +
        " is in state " + state.getValue());
      if (System.currentTimeMillis() - start > TIMEOUT)
        assertTrue("Was waiting too long for a replica to become TEMPORARY",
          tooLongWait);
    }
    LOG.debug("Replica state after the loop " + state.getValue());
  }

  // Helper methods from here below...
  // Write file and start second data node.
  private ArrayList<Block> writeFile(final String METHOD_NAME,
                                               final long fileSize,
                                               Path filePath)
    throws IOException {
    ArrayList<Block> blocks = null;
    try {
      REPL_FACTOR = 2;
      blocks = prepareForRide(filePath, METHOD_NAME, fileSize);
    } catch (IOException e) {
      LOG.debug("Caught exception ", e);
    }
    return blocks;
  }

  private void startDNandWait(Path filePath, boolean waitReplicas) 
    throws IOException {
    LOG.debug("Before next DN start: " + cluster.getDataNodes().size());
    cluster.startDataNodes(conf, 1, true, null, null);
    ArrayList<DataNode> datanodes = cluster.getDataNodes();
    assertEquals(datanodes.size(), 2);

    LOG.debug("New datanode "
      + cluster.getDataNodes().get(datanodes.size() - 1)
      .getDatanodeRegistration() + " has been started");
    if (waitReplicas) DFSTestUtil.waitReplication(fs, filePath, REPL_FACTOR);
  }

  private ArrayList<Block> prepareForRide(final Path filePath,
                                          final String METHOD_NAME,
                                          long fileSize) throws IOException {
    LOG.info("Running test " + METHOD_NAME);

    DFSTestUtil.createFile(fs, filePath, fileSize,
      REPL_FACTOR, rand.nextLong());

    return locatedToBlocks(cluster.getNameNode()
      .getBlockLocations(filePath.toString(), FILE_START,
        fileSize).getLocatedBlocks(), null);
  }

  private void printStats() {
    NameNodeAdapter.refreshBlockCounts(cluster.getNameNode());
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
  }

  private ArrayList<Block> locatedToBlocks(final List<LocatedBlock> locatedBlks,
                                           List<Integer> positionsToRemove) {
    ArrayList<Block> newList = new ArrayList<Block>();
    for (int i = 0; i < locatedBlks.size(); i++) {
      if (positionsToRemove != null && positionsToRemove.contains(i)) {
        LOG.debug(i + " block to be omitted");
        continue;
      }
      newList.add(new Block(locatedBlks.get(i).getBlock()));
    }
    return newList;
  }

  private void waitTil(long waitPeriod) {
    try { //Wait til next re-scan
      Thread.sleep(waitPeriod);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
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
    private boolean all = false;

    public MyFileFilter(String nameToAccept, boolean all) {
      if (nameToAccept == null)
        throw new IllegalArgumentException("Argument isn't suppose to be null");
      this.nameToAccept = nameToAccept;
      this.all = all;
    }

    public boolean accept(File file, String s) {
      if (all)
        return s != null && s.startsWith(nameToAccept);
      else
        return s != null && s.equals(nameToAccept);
    }
  }

  private static void initLoggers() {
    ((Log4JLogger) NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) DataNode.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) TestBlockReport.LOG).getLogger().setLevel(Level.ALL);
  }

  private void corruptBlockLen(final Block block)
    throws IOException {
    if (block == null) {
      throw new IOException("Block isn't suppose to be null");
    }
    long oldLen = block.getNumBytes();
    long newLen = oldLen - rand.nextLong();
    assertTrue("Old and new length shouldn't be the same",
      block.getNumBytes() != newLen);
    block.setNumBytes(newLen);
    LOG.debug("Length of " + block.getBlockName() +
      " is changed to " + newLen + " from " + oldLen);
  }

  private void corruptBlockGS(final Block block)
    throws IOException {
    if (block == null) {
      throw new IOException("Block isn't suppose to be null");
    }
    long oldGS = block.getGenerationStamp();
    long newGS = oldGS - rand.nextLong();
    assertTrue("Old and new GS shouldn't be the same",
      block.getGenerationStamp() != newGS);
    block.setGenerationStamp(newGS);
    LOG.debug("Generation stamp of " + block.getBlockName() +
      " is changed to " + block.getGenerationStamp() + " from " + oldGS);
  }

  private Block findBlock(Path path, long size) throws IOException {
    Block ret;
      List<LocatedBlock> lbs =
        cluster.getNameNode().getBlockLocations(path.toString(),
          FILE_START, size).getLocatedBlocks();
      LocatedBlock lb = lbs.get(lbs.size() - 1);

      // Get block from the first DN
      ret = cluster.getDataNodes().get(DN_N0).
        data.getStoredBlock(lb.getBlock().getBlockId());
    return ret;
  }

  private class BlockChecker extends Thread {
    Path filePath;
    
    public BlockChecker(final Path filePath) {
      this.filePath = filePath;
    }
    
    public void run() {
      try {
        startDNandWait(filePath, true);
      } catch (IOException e) {
        LOG.warn("Shouldn't happen", e);
      }
    }
  }

  private static void resetConfiguration() {
    conf = new Configuration();
    int customPerChecksumSize = 512;
    int customBlockSize = customPerChecksumSize * 3;
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, customPerChecksumSize);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, customBlockSize);
    conf.setLong(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY, DN_RESCAN_INTERVAL);
  }
}
