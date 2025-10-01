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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.mockito.invocation.InvocationOnMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;

import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

/**
 * Verify that incremental block reports are generated in response to
 * block additions/deletions.
 */
public class TestIncrementalBlockReports {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestIncrementalBlockReports.class);

  private static final short DN_COUNT = 1;
  private static final long DUMMY_BLOCK_ID = 5678;
  private static final long DUMMY_BLOCK_LENGTH = 1024 * 1024;
  private static final long DUMMY_BLOCK_GENSTAMP = 1000;
  private static final String TEST_FILE_DATA = "hello world";
  private static final String TEST_FILE = "/TestStandbyBlockManagement";
  private static final Path TEST_FILE_PATH = new Path(TEST_FILE);

  private MiniDFSCluster cluster = null;
  private Configuration conf;
  private NameNode singletonNn;
  private DataNode singletonDn;
  private BPOfferService bpos;    // BPOS to use for block injection.
  private BPServiceActor actor;   // BPSA to use for block injection.
  private String storageUuid;     // DatanodeStorage to use for block injection.

  @BeforeEach
  public void startCluster() throws IOException {
    conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(DN_COUNT).build();
    singletonNn = cluster.getNameNode();
    singletonDn = cluster.getDataNodes().get(0);
    bpos = singletonDn.getAllBpOs().get(0);
    actor = bpos.getBPServiceActors().get(0);
    try (FsDatasetSpi.FsVolumeReferences volumes =
        singletonDn.getFSDataset().getFsVolumeReferences()) {
      storageUuid = volumes.get(0).getStorageID();
    }
  }

  private static Block getDummyBlock() {
    return new Block(DUMMY_BLOCK_ID, DUMMY_BLOCK_LENGTH, DUMMY_BLOCK_GENSTAMP);
  }

  /**
   * Inject a fake 'received' block into the BPServiceActor state.
   */
  private void injectBlockReceived() {
    ReceivedDeletedBlockInfo rdbi = new ReceivedDeletedBlockInfo(
        getDummyBlock(), BlockStatus.RECEIVED_BLOCK, null);
    DatanodeStorage s = singletonDn.getFSDataset().getStorage(storageUuid);
    actor.getIbrManager().notifyNamenodeBlock(rdbi, s, false);
  }

  /**
   * Inject a fake 'deleted' block into the BPServiceActor state.
   */
  private void injectBlockDeleted() {
    ReceivedDeletedBlockInfo rdbi = new ReceivedDeletedBlockInfo(
        getDummyBlock(), BlockStatus.DELETED_BLOCK, null);
    actor.getIbrManager().addRDBI(rdbi,
        singletonDn.getFSDataset().getStorage(storageUuid));
  }

  /**
   * Spy on calls from the DN to the NN.
   * @return spy object that can be used for Mockito verification.
   */
  DatanodeProtocolClientSideTranslatorPB spyOnDnCallsToNn() {
    return InternalDataNodeTestUtils.spyOnBposToNN(singletonDn, singletonNn);
  }

  /**
   * Ensure that an IBR is generated immediately for a block received by
   * the DN.
   *
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  @Timeout(value = 60)
  public void testReportBlockReceived() throws InterruptedException, IOException {
    try {
      DatanodeProtocolClientSideTranslatorPB nnSpy = spyOnDnCallsToNn();
      injectBlockReceived();

      // Sleep for a very short time, this is necessary since the IBR is
      // generated asynchronously.
      Thread.sleep(2000);

      // Ensure that the received block was reported immediately.
      Mockito.verify(nnSpy, times(1)).blockReceivedAndDeleted(
          any(DatanodeRegistration.class),
          anyString(),
          any(StorageReceivedDeletedBlocks[].class));
    } finally {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Ensure that a delayed IBR is generated for a block deleted on the DN.
   *
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  @Timeout(value = 60)
  public void testReportBlockDeleted() throws InterruptedException, IOException {
    try {
      // Trigger a block report to reset the IBR timer.
      DataNodeTestUtils.triggerBlockReport(singletonDn);

      // Spy on calls from the DN to the NN
      DatanodeProtocolClientSideTranslatorPB nnSpy = spyOnDnCallsToNn();
      injectBlockDeleted();

      // Sleep for a very short time since IBR is generated
      // asynchronously.
      Thread.sleep(2000);

      // Ensure that no block report was generated immediately.
      // Deleted blocks are reported when the IBR timer elapses.
      Mockito.verify(nnSpy, times(0)).blockReceivedAndDeleted(
          any(DatanodeRegistration.class),
          anyString(),
          any(StorageReceivedDeletedBlocks[].class));

      // Trigger a heartbeat, this also triggers an IBR.
      DataNodeTestUtils.triggerHeartbeat(singletonDn);
      Thread.sleep(2000);

      // Ensure that the deleted block is reported.
      Mockito.verify(nnSpy, times(1)).blockReceivedAndDeleted(
          any(DatanodeRegistration.class),
          anyString(),
          any(StorageReceivedDeletedBlocks[].class));

    } finally {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Add a received block entry and then replace it. Ensure that a single
   * IBR is generated and that pending receive request state is cleared.
   * This test case verifies the failure in HDFS-5922.
   *
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  @Timeout(value = 60)
  public void testReplaceReceivedBlock() throws InterruptedException, IOException {
    try {
      // Spy on calls from the DN to the NN
      DatanodeProtocolClientSideTranslatorPB nnSpy = spyOnDnCallsToNn();
      injectBlockReceived();
      injectBlockReceived();    // Overwrite the existing entry.

      // Sleep for a very short time since IBR is generated
      // asynchronously.
      Thread.sleep(2000);

      // Ensure that the received block is reported.
      Mockito.verify(nnSpy, atLeastOnce()).blockReceivedAndDeleted(
          any(DatanodeRegistration.class),
          anyString(),
          any(StorageReceivedDeletedBlocks[].class));

      // Ensure that no more IBRs are pending.
      assertFalse(actor.getIbrManager().sendImmediately());

    } finally {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testIBRRaceCondition() throws Exception {
    cluster.shutdown();
    conf = new Configuration();
    HAUtil.setAllowStandbyReads(conf, true);
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(3)
        .build();
    try {
      cluster.waitActive();
      cluster.transitionToActive(0);

      NameNode nn1 = cluster.getNameNode(0);
      NameNode nn2 = cluster.getNameNode(1);
      BlockManager bm2 = nn2.getNamesystem().getBlockManager();
      FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
      List<InvocationOnMock> ibrsToStandby = new ArrayList<>();
      List<DatanodeProtocolClientSideTranslatorPB> spies = new ArrayList<>();
      Phaser ibrPhaser = new Phaser(1);
      for (DataNode dn : cluster.getDataNodes()) {
        DatanodeProtocolClientSideTranslatorPB nnSpy =
            InternalDataNodeTestUtils.spyOnBposToNN(dn, nn2);
        doAnswer((inv) -> {
          for (StorageReceivedDeletedBlocks srdb :
              inv.getArgument(2, StorageReceivedDeletedBlocks[].class)) {
            for (ReceivedDeletedBlockInfo block : srdb.getBlocks()) {
              if (block.getStatus().equals(BlockStatus.RECEIVED_BLOCK)) {
                ibrPhaser.arriveAndDeregister();
              }
            }
          }
          return null;
        }).when(nnSpy).blockReceivedAndDeleted(
            any(DatanodeRegistration.class),
            anyString(),
            any(StorageReceivedDeletedBlocks[].class));
        spies.add(nnSpy);
      }

      LOG.info("==================================");
      // Force the DNs to delay report to the SNN
      ibrPhaser.bulkRegister(9);
      DFSTestUtil.writeFile(fs, TEST_FILE_PATH, TEST_FILE_DATA);
      DFSTestUtil.appendFile(fs, TEST_FILE_PATH, TEST_FILE_DATA);
      DFSTestUtil.appendFile(fs, TEST_FILE_PATH, TEST_FILE_DATA);
      HATestUtil.waitForStandbyToCatchUp(nn1, nn2);
      // SNN has caught up to the latest edit log so we send the IBRs to SNN
      int phase = ibrPhaser.arrive();
      ibrPhaser.awaitAdvanceInterruptibly(phase, 60, TimeUnit.SECONDS);
      for (InvocationOnMock sendIBRs : ibrsToStandby) {
        try {
          sendIBRs.callRealMethod();
        } catch (Throwable t) {
          LOG.error("Exception thrown while calling sendIBRs: ", t);
        }
      }

      GenericTestUtils.waitFor(() -> bm2.getPendingDataNodeMessageCount() == 0,
          1000, 30000,
          "There should be 0 pending DN messages");
      ibrsToStandby.clear();
      // We need to trigger another edit log roll so that the pendingDNMessages
      // are processed.
      ibrPhaser.bulkRegister(6);
      DFSTestUtil.appendFile(fs, TEST_FILE_PATH, TEST_FILE_DATA);
      DFSTestUtil.appendFile(fs, TEST_FILE_PATH, TEST_FILE_DATA);
      phase = ibrPhaser.arrive();
      ibrPhaser.awaitAdvanceInterruptibly(phase, 60, TimeUnit.SECONDS);
      for (InvocationOnMock sendIBRs : ibrsToStandby) {
        try {
          sendIBRs.callRealMethod();
        } catch (Throwable t) {
          LOG.error("Exception thrown while calling sendIBRs: ", t);
        }
      }
      ibrsToStandby.clear();
      ibrPhaser.arriveAndDeregister();
      GenericTestUtils.waitFor(() -> bm2.getPendingDataNodeMessageCount() == 0,
          1000, 30000,
          "There should be 0 pending DN messages");
      ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, TEST_FILE_PATH);
      HATestUtil.waitForStandbyToCatchUp(nn1, nn2);
      LOG.info("==================================");

      // Trigger an active switch to force SNN to mark blocks as corrupt if they
      // have a bad genstamp in the pendingDNMessages queue.
      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);
      cluster.waitActive(1);

      assertEquals(0,
          nn2.getNamesystem().getBlockManager().numCorruptReplicas(block.getLocalBlock()),
          "There should not be any corrupt replicas");
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testIBRRaceCondition2() throws Exception {
    cluster.shutdown();
    Configuration conf = new Configuration();
    HAUtil.setAllowStandbyReads(conf, true);
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(3)
        .build();
    try {
      cluster.waitActive();
      cluster.transitionToActive(0);

      NameNode nn1 = cluster.getNameNode(0);
      NameNode nn2 = cluster.getNameNode(1);
      BlockManager bm2 = nn2.getNamesystem().getBlockManager();
      FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
      List<InvocationOnMock> ibrsToStandby = new ArrayList<>();
      List<DatanodeProtocolClientSideTranslatorPB> spies = new ArrayList<>();
      Phaser ibrPhaser = new Phaser(1);
      for (DataNode dn : cluster.getDataNodes()) {
        DatanodeProtocolClientSideTranslatorPB nnSpy =
            InternalDataNodeTestUtils.spyOnBposToNN(dn, nn2);
        doAnswer((inv) -> {
          for (StorageReceivedDeletedBlocks srdb :
              inv.getArgument(2, StorageReceivedDeletedBlocks[].class)) {
            for (ReceivedDeletedBlockInfo block : srdb.getBlocks()) {
              if (block.getStatus().equals(BlockStatus.RECEIVED_BLOCK)) {
                ibrsToStandby.add(inv);
                ibrPhaser.arriveAndDeregister();
              }
            }
          }
          return null;
        }).when(nnSpy).blockReceivedAndDeleted(
            any(DatanodeRegistration.class),
            anyString(),
            any(StorageReceivedDeletedBlocks[].class));
        spies.add(nnSpy);
      }

      LOG.info("==================================");
      // Force the DNs to delay report to the SNN
      ibrPhaser.bulkRegister(9);
      DFSTestUtil.writeFile(fs, TEST_FILE_PATH, TEST_FILE_DATA);
      DFSTestUtil.appendFile(fs, TEST_FILE_PATH, TEST_FILE_DATA);
      DFSTestUtil.appendFile(fs, TEST_FILE_PATH, TEST_FILE_DATA);
      HATestUtil.waitForStandbyToCatchUp(nn1, nn2);
      // SNN has caught up to the latest edit log so we send the IBRs to SNN
      int phase = ibrPhaser.arrive();
      ibrPhaser.awaitAdvanceInterruptibly(phase, 60, TimeUnit.SECONDS);
      for (InvocationOnMock sendIBRs : ibrsToStandby) {
        try {
          sendIBRs.callRealMethod();
        } catch (Throwable t) {
          LOG.error("Exception thrown while calling sendIBRs: ", t);
        }
      }

      GenericTestUtils.waitFor(() -> bm2.getPendingDataNodeMessageCount() == 0,
          1000, 30000,
          "There should be 0 pending DN messages");
      ibrsToStandby.clear();
      ibrPhaser.arriveAndDeregister();
      ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, TEST_FILE_PATH);
      HATestUtil.waitForStandbyToCatchUp(nn1, nn2);
      LOG.info("==================================");

      // Trigger an active switch to force SNN to mark blocks as corrupt if they
      // have a bad genstamp in the pendingDNMessages queue.
      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);
      cluster.waitActive(1);

      assertEquals(0,
          nn2.getNamesystem().getBlockManager().numCorruptReplicas(block.getLocalBlock()),
          "There should not be any corrupt replicas");
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testIBRRaceCondition3() throws Exception {
    cluster.shutdown();
    Configuration conf = new Configuration();
    HAUtil.setAllowStandbyReads(conf, true);
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(3)
        .build();
    try {
      cluster.waitActive();
      cluster.transitionToActive(0);

      NameNode nn1 = cluster.getNameNode(0);
      NameNode nn2 = cluster.getNameNode(1);
      BlockManager bm2 = nn2.getNamesystem().getBlockManager();
      FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
      LinkedHashMap<Long, List<InvocationOnMock>> ibrsToStandby =
          new LinkedHashMap<>();
      AtomicLong lowestGenStamp = new AtomicLong(Long.MAX_VALUE);
      List<DatanodeProtocolClientSideTranslatorPB> spies = new ArrayList<>();
      Phaser ibrPhaser = new Phaser(1);
      for (DataNode dn : cluster.getDataNodes()) {
        DatanodeProtocolClientSideTranslatorPB nnSpy =
            InternalDataNodeTestUtils.spyOnBposToNN(dn, nn2);
        doAnswer((inv) -> {
          for (StorageReceivedDeletedBlocks srdb :
              inv.getArgument(2, StorageReceivedDeletedBlocks[].class)) {
            for (ReceivedDeletedBlockInfo block : srdb.getBlocks()) {
              if (block.getStatus().equals(BlockStatus.RECEIVED_BLOCK)) {
                long genStamp = block.getBlock().getGenerationStamp();
                ibrsToStandby.putIfAbsent(genStamp, new ArrayList<>());
                ibrsToStandby.get(genStamp).add(inv);
                lowestGenStamp.getAndUpdate((prev) -> Math.min(prev, genStamp));
                ibrPhaser.arriveAndDeregister();
              }
            }
          }
          return null;
        }).when(nnSpy).blockReceivedAndDeleted(
            any(DatanodeRegistration.class),
            anyString(),
            any(StorageReceivedDeletedBlocks[].class));
        spies.add(nnSpy);
      }

      LOG.info("==================================");
      // Force the DNs to delay report to the SNN
      ibrPhaser.bulkRegister(9);
      DFSTestUtil.writeFile(fs, TEST_FILE_PATH, TEST_FILE_DATA);
      DFSTestUtil.appendFile(fs, TEST_FILE_PATH, TEST_FILE_DATA);
      DFSTestUtil.appendFile(fs, TEST_FILE_PATH, TEST_FILE_DATA);
      HATestUtil.waitForStandbyToCatchUp(nn1, nn2);
      // SNN has caught up to the latest edit log so we send the IBRs to SNN
      int phase = ibrPhaser.arrive();
      ibrPhaser.awaitAdvanceInterruptibly(phase, 60, TimeUnit.SECONDS);
      ibrsToStandby.forEach((genStamp, ibrs) -> {
        if (lowestGenStamp.get() != genStamp) {
          ibrs.removeIf(inv -> {
            try {
              inv.callRealMethod();
            } catch (Throwable t) {
              LOG.error("Exception thrown while calling sendIBRs: ", t);
            }
            return true;
          });
        }
      });

      GenericTestUtils.waitFor(() -> bm2.getPendingDataNodeMessageCount() == 0,
          1000, 30000,
          "There should be 0 pending DN messages");
      ibrPhaser.arriveAndDeregister();
      ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, TEST_FILE_PATH);
      HATestUtil.waitForStandbyToCatchUp(nn1, nn2);

      // Send old ibrs to simulate actual stale or corrupt DNs
      for (InvocationOnMock sendIBR : ibrsToStandby.get(lowestGenStamp.get())) {
        try {
          sendIBR.callRealMethod();
        } catch (Throwable t) {
          LOG.error("Exception thrown while calling sendIBRs: ", t);
        }
      }

      GenericTestUtils.waitFor(() -> bm2.getPendingDataNodeMessageCount() == 3,
          1000, 30000,
          "There should be 0 pending DN messages");
      LOG.info("==================================");

      // Trigger an active switch to force SNN to mark blocks as corrupt if they
      // have a bad genstamp in the pendingDNMessages queue.
      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);
      cluster.waitActive(1);

      assertEquals(1,
          nn2.getNamesystem().getBlockManager().numCorruptReplicas(block.getLocalBlock()),
          "There should be 1 corrupt replica");
    } finally {
      cluster.shutdown();
    }
  }
}
