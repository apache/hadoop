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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs.BlockReportReplica;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.DelayAnswer;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

/**
 * This is the base class for simulating a variety of situations
 * when blocks are being intentionally corrupted, unexpectedly modified,
 * and so on before a block report is happening.
 *
 * By overriding {@link #sendBlockReports}, derived classes can test
 * different variations of how block reports are split across storages
 * and messages.
 */
public abstract class BlockReportTestBase {
  public static final Logger LOG = LoggerFactory.getLogger(BlockReportTestBase.class);

  private static short REPL_FACTOR = 1;
  private static final int RAND_LIMIT = 2000;
  private static final long DN_RESCAN_INTERVAL = 1;
  private static final long DN_RESCAN_EXTRA_WAIT = 3 * DN_RESCAN_INTERVAL;
  private static final int DN_N0 = 0;
  private static final int FILE_START = 0;

  private static final int BLOCK_SIZE = 1024;
  private static final int NUM_BLOCKS = 10;
  private static final int FILE_SIZE = NUM_BLOCKS * BLOCK_SIZE + 1;

  protected MiniDFSCluster cluster;
  private DistributedFileSystem fs;

  private static final Random rand = new Random(RAND_LIMIT);

  private static Configuration conf;

  static {
    initLoggers();
    resetConfiguration();
  }

  @Before
  public void startUpCluster() throws IOException {
    REPL_FACTOR = 1; //Reset if case a test has modified the value
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPL_FACTOR).build();
    fs = cluster.getFileSystem();
  }

  @After
  public void shutDownCluster() throws IOException {
    if (fs != null) {
      fs.close();
      fs = null;
    }
    if (cluster != null) {
      cluster.shutdownDataNodes();
      cluster.shutdown();
      cluster = null;
    }
  }

  protected static void resetConfiguration() {
    conf = new Configuration();
    int customPerChecksumSize = 512;
    int customBlockSize = customPerChecksumSize * 3;
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, customPerChecksumSize);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, customBlockSize);
    conf.setLong(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY, DN_RESCAN_INTERVAL);
  }

  // Generate a block report, optionally corrupting the generation
  // stamp and/or length of one block.
  private static StorageBlockReport[] getBlockReports(
      DataNode dn, String bpid, boolean corruptOneBlockGs,
      boolean corruptOneBlockLen) {
    Map<DatanodeStorage, BlockListAsLongs> perVolumeBlockLists =
        dn.getFSDataset().getBlockReports(bpid);

    // Send block report
    StorageBlockReport[] reports =
        new StorageBlockReport[perVolumeBlockLists.size()];
    boolean corruptedGs = false;
    boolean corruptedLen = false;

    int reportIndex = 0;
    for(Map.Entry<DatanodeStorage, BlockListAsLongs> kvPair : perVolumeBlockLists.entrySet()) {
      DatanodeStorage dnStorage = kvPair.getKey();
      BlockListAsLongs blockList = kvPair.getValue();

      // Walk the list of blocks until we find one each to corrupt the
      // generation stamp and length, if so requested.
      BlockListAsLongs.Builder builder = BlockListAsLongs.builder();
      for (BlockReportReplica block : blockList) {
        if (corruptOneBlockGs && !corruptedGs) {
          long gsOld = block.getGenerationStamp();
          long gsNew;
          do {
            gsNew = rand.nextInt();
          } while (gsNew == gsOld);
          block.setGenerationStamp(gsNew);
          LOG.info("Corrupted the GS for block ID " + block);
          corruptedGs = true;
        } else if (corruptOneBlockLen && !corruptedLen) {
          long lenOld = block.getNumBytes();
          long lenNew;
          do {
            lenNew = rand.nextInt((int)lenOld - 1);
          } while (lenNew == lenOld);
          block.setNumBytes(lenNew);
          LOG.info("Corrupted the length for block ID " + block);
          corruptedLen = true;
        }
        builder.add(new BlockReportReplica(block));
      }

      reports[reportIndex++] =
          new StorageBlockReport(dnStorage, builder.build());
    }

    return reports;
  }

  /**
   * Utility routine to send block reports to the NN, either in a single call
   * or reporting one storage per call.
   *
   * @throws IOException
   */
  protected abstract void sendBlockReports(DatanodeRegistration dnR, String poolId,
      StorageBlockReport[] reports) throws IOException;

  /**
   * Test write a file, verifies and closes it. Then the length of the blocks
   * are messed up and BlockReport is forced.
   * The modification of blocks' length has to be ignored
   *
   * @throws java.io.IOException on an error
   */
  @Test(timeout=300000)
  public void blockReport_01() throws IOException {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path filePath = new Path("/" + METHOD_NAME + ".dat");

    ArrayList<Block> blocks = prepareForRide(filePath, METHOD_NAME, FILE_SIZE);

    if(LOG.isDebugEnabled()) {
      LOG.debug("Number of blocks allocated " + blocks.size());
    }
    long[] oldLengths = new long[blocks.size()];
    int tempLen;
    for (int i = 0; i < blocks.size(); i++) {
      Block b = blocks.get(i);
      if(LOG.isDebugEnabled()) {
        LOG.debug("Block " + b.getBlockName() + " before\t" + "Size " +
            b.getNumBytes());
      }
      oldLengths[i] = b.getNumBytes();
      if(LOG.isDebugEnabled()) {
        LOG.debug("Setting new length");
      }
      tempLen = rand.nextInt(BLOCK_SIZE);
      b.set(b.getBlockId(), tempLen, b.getGenerationStamp());
      if(LOG.isDebugEnabled()) {
        LOG.debug("Block " + b.getBlockName() + " after\t " + "Size " +
            b.getNumBytes());
      }
    }
    // all blocks belong to the same file, hence same BP
    DataNode dn = cluster.getDataNodes().get(DN_N0);
    String poolId = cluster.getNamesystem().getBlockPoolId();
    DatanodeRegistration dnR = dn.getDNRegistrationForBP(poolId);
    StorageBlockReport[] reports = getBlockReports(dn, poolId, false, false);
    sendBlockReports(dnR, poolId, reports);

    List<LocatedBlock> blocksAfterReport =
      DFSTestUtil.getAllBlocks(fs.open(filePath));

    if(LOG.isDebugEnabled()) {
      LOG.debug("After mods: Number of blocks allocated " +
          blocksAfterReport.size());
    }

    for (int i = 0; i < blocksAfterReport.size(); i++) {
      ExtendedBlock b = blocksAfterReport.get(i).getBlock();
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
  @Test(timeout=300000)
  public void blockReport_02() throws IOException {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    LOG.info("Running test " + METHOD_NAME);

    Path filePath = new Path("/" + METHOD_NAME + ".dat");
    DFSTestUtil.createFile(fs, filePath,
      FILE_SIZE, REPL_FACTOR, rand.nextLong());

    // mock around with newly created blocks and delete some
    File dataDir = new File(cluster.getDataDirectory());
    assertTrue(dataDir.isDirectory());

    List<ExtendedBlock> blocks2Remove = new ArrayList<ExtendedBlock>();
    List<Integer> removedIndex = new ArrayList<Integer>();
    List<LocatedBlock> lBlocks =
      cluster.getNameNodeRpc().getBlockLocations(
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

    if(LOG.isDebugEnabled()) {
      LOG.debug("Number of blocks allocated " + lBlocks.size());
    }

    final DataNode dn0 = cluster.getDataNodes().get(DN_N0);
    for (ExtendedBlock b : blocks2Remove) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Removing the block " + b.getBlockName());
      }
      for (File f : findAllFiles(dataDir,
        new MyFileFilter(b.getBlockName(), true))) {
        DataNodeTestUtils.getFSDataset(dn0).unfinalizeBlock(b);
        if (!f.delete()) {
          LOG.warn("Couldn't delete " + b.getBlockName());
        } else {
          LOG.debug("Deleted file " + f.toString());
        }
      }
    }

    waitTil(TimeUnit.SECONDS.toMillis(DN_RESCAN_EXTRA_WAIT));

    // all blocks belong to the same file, hence same BP
    String poolId = cluster.getNamesystem().getBlockPoolId();
    DatanodeRegistration dnR = dn0.getDNRegistrationForBP(poolId);
    StorageBlockReport[] reports = getBlockReports(dn0, poolId, false, false);
    sendBlockReports(dnR, poolId, reports);

    BlockManagerTestUtil.getComputedDatanodeWork(cluster.getNamesystem()
        .getBlockManager());

    printStats();

    assertEquals("Wrong number of MissingBlocks is found",
      blocks2Remove.size(), cluster.getNamesystem().getMissingBlocksCount());
    assertEquals("Wrong number of UnderReplicatedBlocks is found",
      blocks2Remove.size(), cluster.getNamesystem().getUnderReplicatedBlocks());
  }


  /**
   * Test writes a file and closes it.
   * Block reported is generated with a bad GS for a single block.
   * Block report is forced and the check for # of corrupted blocks is performed.
   *
   * @throws IOException in case of an error
   */
  @Test(timeout=300000)
  public void blockReport_03() throws IOException {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path filePath = new Path("/" + METHOD_NAME + ".dat");
    writeFile(METHOD_NAME, FILE_SIZE, filePath);

    // all blocks belong to the same file, hence same BP
    DataNode dn = cluster.getDataNodes().get(DN_N0);
    String poolId = cluster.getNamesystem().getBlockPoolId();
    DatanodeRegistration dnR = dn.getDNRegistrationForBP(poolId);
    StorageBlockReport[] reports = getBlockReports(dn, poolId, true, false);
    sendBlockReports(dnR, poolId, reports);
    printStats();

    assertThat("Wrong number of corrupt blocks",
               cluster.getNamesystem().getCorruptReplicaBlocks(), is(1L));
    assertThat("Wrong number of PendingDeletion blocks",
               cluster.getNamesystem().getPendingDeletionBlocks(), is(0L));
  }

  /**
   * Test writes a file and closes it.
   * Block reported is generated with an extra block.
   * Block report is forced and the check for # of pendingdeletion
   * blocks is performed.
   *
   * @throws IOException in case of an error
   */
  @Test(timeout=300000)
  public void blockReport_04() throws IOException {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path filePath = new Path("/" + METHOD_NAME + ".dat");
    DFSTestUtil.createFile(fs, filePath,
                           FILE_SIZE, REPL_FACTOR, rand.nextLong());


    DataNode dn = cluster.getDataNodes().get(DN_N0);
    // all blocks belong to the same file, hence same BP
    String poolId = cluster.getNamesystem().getBlockPoolId();

    // Create a bogus new block which will not be present on the namenode.
    ExtendedBlock b = new ExtendedBlock(
        poolId, rand.nextLong(), 1024L, rand.nextLong());
    dn.getFSDataset().createRbw(StorageType.DEFAULT, null, b, false);

    DatanodeRegistration dnR = dn.getDNRegistrationForBP(poolId);
    StorageBlockReport[] reports = getBlockReports(dn, poolId, false, false);
    sendBlockReports(dnR, poolId, reports);
    printStats();

    assertThat("Wrong number of corrupt blocks",
               cluster.getNamesystem().getCorruptReplicaBlocks(), is(0L));
    assertThat("Wrong number of PendingDeletion blocks",
               cluster.getNamesystem().getPendingDeletionBlocks(), is(1L));
  }

  /**
   * Test creates a file and closes it.
   * The second datanode is started in the cluster.
   * As soon as the replication process is completed test runs
   * Block report and checks that no underreplicated blocks are left
   *
   * @throws IOException in case of an error
   */
  @Test(timeout=300000)
  public void blockReport_06() throws Exception {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path filePath = new Path("/" + METHOD_NAME + ".dat");
    final int DN_N1 = DN_N0 + 1;

    writeFile(METHOD_NAME, FILE_SIZE, filePath);
    startDNandWait(filePath, true);

    // all blocks belong to the same file, hence same BP
    DataNode dn = cluster.getDataNodes().get(DN_N1);
    String poolId = cluster.getNamesystem().getBlockPoolId();
    DatanodeRegistration dnR = dn.getDNRegistrationForBP(poolId);
    StorageBlockReport[] reports = getBlockReports(dn, poolId, false, false);
    sendBlockReports(dnR, poolId, reports);
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
   * this is the markBlockAsCorrupt case 3 so we expect one pending deletion
   * Block report is forced and the check for # of currupted blocks is performed.
   * Another block is chosen and its length is set to a lesser than original.
   * A check for another corrupted block is performed after yet another
   * BlockReport
   *
   * @throws IOException in case of an error
   */
  @Test(timeout=300000)
  public void blockReport_07() throws Exception {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path filePath = new Path("/" + METHOD_NAME + ".dat");
    final int DN_N1 = DN_N0 + 1;

    // write file and start second node to be "older" than the original
    writeFile(METHOD_NAME, FILE_SIZE, filePath);
    startDNandWait(filePath, true);

    // all blocks belong to the same file, hence same BP
    DataNode dn = cluster.getDataNodes().get(DN_N1);
    String poolId = cluster.getNamesystem().getBlockPoolId();
    DatanodeRegistration dnR = dn.getDNRegistrationForBP(poolId);
    StorageBlockReport[] reports = getBlockReports(dn, poolId, true, false);
    sendBlockReports(dnR, poolId, reports);
    printStats();

    assertThat("Wrong number of corrupt blocks",
               cluster.getNamesystem().getCorruptReplicaBlocks(), is(0L));
    assertThat("Wrong number of PendingDeletion blocks",
               cluster.getNamesystem().getPendingDeletionBlocks(), is(1L));
    assertThat("Wrong number of PendingReplication blocks",
               cluster.getNamesystem().getPendingReplicationBlocks(), is(0L));

    reports = getBlockReports(dn, poolId, false, true);
    sendBlockReports(dnR, poolId, reports);
    printStats();

    assertThat("Wrong number of corrupt blocks",
               cluster.getNamesystem().getCorruptReplicaBlocks(), is(1L));
    assertThat("Wrong number of PendingDeletion blocks",
               cluster.getNamesystem().getPendingDeletionBlocks(), is(1L));
    assertThat("Wrong number of PendingReplication blocks",
               cluster.getNamesystem().getPendingReplicationBlocks(), is(0L));

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
  @Test(timeout=300000)
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

      // all blocks belong to the same file, hence same BP
      DataNode dn = cluster.getDataNodes().get(DN_N1);
      String poolId = cluster.getNamesystem().getBlockPoolId();
      DatanodeRegistration dnR = dn.getDNRegistrationForBP(poolId);
      StorageBlockReport[] reports = getBlockReports(dn, poolId, false, false);
      sendBlockReports(dnR, poolId, reports);
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
  @Test(timeout=300000)
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
      writeFile(METHOD_NAME, 12 * bytesChkSum, filePath);

      Block bl = findBlock(filePath, 12 * bytesChkSum);
      BlockChecker bc = new BlockChecker(filePath);
      bc.start();

      waitForTempReplica(bl, DN_N1);

      // all blocks belong to the same file, hence same BP
      DataNode dn = cluster.getDataNodes().get(DN_N1);
      String poolId = cluster.getNamesystem().getBlockPoolId();
      DatanodeRegistration dnR = dn.getDNRegistrationForBP(poolId);
      StorageBlockReport[] reports = getBlockReports(dn, poolId, true, true);
      sendBlockReports(dnR, poolId, reports);
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

  /**
   * Test for the case where one of the DNs in the pipeline is in the
   * process of doing a block report exactly when the block is closed.
   * In this case, the block report becomes delayed until after the
   * block is marked completed on the NN, and hence it reports an RBW
   * replica for a COMPLETE block. Such a report should not be marked
   * corrupt.
   * This is a regression test for HDFS-2791.
   */
  @Test(timeout=300000)
  public void testOneReplicaRbwReportArrivesAfterBlockCompleted() throws Exception {
    final CountDownLatch brFinished = new CountDownLatch(1);
    DelayAnswer delayer = new GenericTestUtils.DelayAnswer(LOG) {
      @Override
      protected Object passThrough(InvocationOnMock invocation)
          throws Throwable {
        try {
          return super.passThrough(invocation);
        } finally {
          // inform the test that our block report went through.
          brFinished.countDown();
        }
      }
    };

    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path filePath = new Path("/" + METHOD_NAME + ".dat");

    // Start a second DN for this test -- we're checking
    // what happens when one of the DNs is slowed for some reason.
    REPL_FACTOR = 2;
    startDNandWait(null, false);

    NameNode nn = cluster.getNameNode();

    FSDataOutputStream out = fs.create(filePath, REPL_FACTOR);
    try {
      AppendTestUtil.write(out, 0, 10);
      out.hflush();

      // Set up a spy so that we can delay the block report coming
      // from this node.
      DataNode dn = cluster.getDataNodes().get(0);
      DatanodeProtocolClientSideTranslatorPB spy =
        InternalDataNodeTestUtils.spyOnBposToNN(dn, nn);

      Mockito.doAnswer(delayer)
        .when(spy).blockReport(
            any(),
            anyString(),
            any(),
            any());

      // Force a block report to be generated. The block report will have
      // an RBW replica in it. Wait for the RPC to be sent, but block
      // it before it gets to the NN.
      dn.scheduleAllBlockReport(0);
      delayer.waitForCall();

    } finally {
      IOUtils.closeStream(out);
    }

    // Now that the stream is closed, the NN will have the block in COMPLETE
    // state.
    delayer.proceed();
    brFinished.await();

    // Verify that no replicas are marked corrupt, and that the
    // file is still readable.
    BlockManagerTestUtil.updateState(nn.getNamesystem().getBlockManager());
    assertEquals(0, nn.getNamesystem().getCorruptReplicaBlocks());
    DFSTestUtil.readFile(fs, filePath);

    // Ensure that the file is readable even from the DN that we futzed with.
    cluster.stopDataNode(1);
    DFSTestUtil.readFile(fs, filePath);
  }

  // See HDFS-10301
  @Test(timeout = 300000)
  public void testInterleavedBlockReports()
      throws IOException, ExecutionException, InterruptedException {
    int numConcurrentBlockReports = 3;
    DataNode dn = cluster.getDataNodes().get(DN_N0);
    final String poolId = cluster.getNamesystem().getBlockPoolId();
    LOG.info("Block pool id: " + poolId);
    final DatanodeRegistration dnR = dn.getDNRegistrationForBP(poolId);
    final StorageBlockReport[] reports =
        getBlockReports(dn, poolId, true, true);

    // Get the list of storage ids associated with the datanode
    // before the test
    BlockManager bm = cluster.getNameNode().getNamesystem().getBlockManager();
    final DatanodeDescriptor dnDescriptor =
        bm.getDatanodeManager().getDatanode(dn.getDatanodeId());
    DatanodeStorageInfo[] storageInfos = dnDescriptor.getStorageInfos();

    // Send the block report concurrently using
    // numThreads=numConcurrentBlockReports
    ExecutorService executorService =
        Executors.newFixedThreadPool(numConcurrentBlockReports);
    List<Future<Void>> futureList = new ArrayList<>(numConcurrentBlockReports);
    for (int i = 0; i < numConcurrentBlockReports; i++) {
      futureList.add(executorService.submit(new Callable<Void>() {
        @Override
        public Void call() throws IOException {
          sendBlockReports(dnR, poolId, reports);
          return null;
        }
      }));
    }
    for (Future<Void> future : futureList) {
      future.get();
    }
    executorService.shutdown();

    // Verify that the storages match before and after the test
    Assert.assertArrayEquals(storageInfos, dnDescriptor.getStorageInfos());
  }

  private void waitForTempReplica(Block bl, int DN_N1) throws IOException {
    final boolean tooLongWait = false;
    final int TIMEOUT = 40000;

    if(LOG.isDebugEnabled()) {
      LOG.debug("Wait for datanode " + DN_N1 + " to appear");
    }
    while (cluster.getDataNodes().size() <= DN_N1) {
      waitTil(20);
    }
    if(LOG.isDebugEnabled()) {
      LOG.debug("Total number of DNs " + cluster.getDataNodes().size());
    }
    cluster.waitActive();

    // Look about specified DN for the replica of the block from 1st DN
    final DataNode dn1 = cluster.getDataNodes().get(DN_N1);
    String bpid = cluster.getNamesystem().getBlockPoolId();
    Replica r = DataNodeTestUtils.fetchReplicaInfo(dn1, bpid, bl.getBlockId());
    long start = Time.monotonicNow();
    int count = 0;
    while (r == null) {
      waitTil(5);
      r = DataNodeTestUtils.fetchReplicaInfo(dn1, bpid, bl.getBlockId());
      long waiting_period = Time.monotonicNow() - start;
      if (count++ % 100 == 0)
        if(LOG.isDebugEnabled()) {
          LOG.debug("Has been waiting for " + waiting_period + " ms.");
        }
      if (waiting_period > TIMEOUT)
        assertTrue("Was waiting too long to get ReplicaInfo from a datanode",
          tooLongWait);
    }

    HdfsServerConstants.ReplicaState state = r.getState();
    if(LOG.isDebugEnabled()) {
      LOG.debug("Replica state before the loop " + state.getValue());
    }
    start = Time.monotonicNow();
    while (state != HdfsServerConstants.ReplicaState.TEMPORARY) {
      waitTil(5);
      state = r.getState();
      if(LOG.isDebugEnabled()) {
        LOG.debug("Keep waiting for " + bl.getBlockName() +
            " is in state " + state.getValue());
      }
      if (Time.monotonicNow() - start > TIMEOUT)
        assertTrue("Was waiting too long for a replica to become TEMPORARY",
          tooLongWait);
    }
    if(LOG.isDebugEnabled()) {
      LOG.debug("Replica state after the loop " + state.getValue());
    }
  }

  // Helper methods from here below...
  // Write file and start second data node.
  private ArrayList<Block> writeFile(final String METHOD_NAME,
                                               final long fileSize,
                                               Path filePath) {
    ArrayList<Block> blocks = null;
    try {
      REPL_FACTOR = 2;
      blocks = prepareForRide(filePath, METHOD_NAME, fileSize);
    } catch (IOException e) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Caught exception ", e);
      }
    }
    return blocks;
  }

  private void startDNandWait(Path filePath, boolean waitReplicas)
      throws IOException, InterruptedException, TimeoutException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Before next DN start: " + cluster.getDataNodes().size());
    }
    cluster.startDataNodes(conf, 1, true, null, null);
    cluster.waitClusterUp();
    ArrayList<DataNode> datanodes = cluster.getDataNodes();
    assertEquals(datanodes.size(), 2);

    if (LOG.isDebugEnabled()) {
      int lastDn = datanodes.size() - 1;
      LOG.debug("New datanode "
          + cluster.getDataNodes().get(lastDn).getDisplayName()
          + " has been started");
    }
    if (waitReplicas) {
      DFSTestUtil.waitReplication(fs, filePath, REPL_FACTOR);
    }
  }

  private ArrayList<Block> prepareForRide(final Path filePath,
                                          final String METHOD_NAME,
                                          long fileSize) throws IOException {
    LOG.info("Running test " + METHOD_NAME);

    DFSTestUtil.createFile(fs, filePath, fileSize,
      REPL_FACTOR, rand.nextLong());

    return locatedToBlocks(cluster.getNameNodeRpc()
      .getBlockLocations(filePath.toString(), FILE_START,
        fileSize).getLocatedBlocks(), null);
  }

  private void printStats() {
    BlockManagerTestUtil.updateState(cluster.getNamesystem().getBlockManager());
    if(LOG.isDebugEnabled()) {
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
  }

  private ArrayList<Block> locatedToBlocks(final List<LocatedBlock> locatedBlks,
                                           List<Integer> positionsToRemove) {
    ArrayList<Block> newList = new ArrayList<Block>();
    for (int i = 0; i < locatedBlks.size(); i++) {
      if (positionsToRemove != null && positionsToRemove.contains(i)) {
        if(LOG.isDebugEnabled()) {
          LOG.debug(i + " block to be omitted");
        }
        continue;
      }
      newList.add(new Block(locatedBlks.get(i).getBlock().getLocalBlock()));
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

    @Override
    public boolean accept(File file, String s) {
      if (all)
        return s != null && s.startsWith(nameToAccept);
      else
        return s != null && s.equals(nameToAccept);
    }
  }

  private static void initLoggers() {
    DFSTestUtil.setNameNodeLogLevel(Level.ALL);
    GenericTestUtils.setLogLevel(DataNode.LOG, Level.ALL);
    GenericTestUtils.setLogLevel(BlockReportTestBase.LOG, org.slf4j.event.Level.DEBUG);
  }

  private Block findBlock(Path path, long size) throws IOException {
    Block ret;
      List<LocatedBlock> lbs =
        cluster.getNameNodeRpc()
        .getBlockLocations(path.toString(),
          FILE_START, size).getLocatedBlocks();
      LocatedBlock lb = lbs.get(lbs.size() - 1);

      // Get block from the first DN
      ret = cluster.getDataNodes().get(DN_N0).
        data.getStoredBlock(lb.getBlock()
        .getBlockPoolId(), lb.getBlock().getBlockId());
    return ret;
  }

  private class BlockChecker extends Thread {
    final Path filePath;

    public BlockChecker(final Path filePath) {
      this.filePath = filePath;
    }

    @Override
    public void run() {
      try {
        startDNandWait(filePath, true);
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail("Failed to start BlockChecker: " + e);
      }
    }
  }
}
