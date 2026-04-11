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

package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.FinalizeCommand;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.InvalidBlockReportLeaseException;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.server.protocol.RegisterCommand;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports;
import org.apache.hadoop.hdfs.server.protocol.SlowPeerReports;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.test.GenericTestUtils.DelayAnswer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * Tests that BlockReportLease in BlockManager.
 */
public class TestBlockReportLease {

  /**
   * Test check lease about one BlockReport with many StorageBlockReport.
   * Before HDFS-12914, when batch storage report to NameNode, it will check
   * less for one storage by one, So it could part storage report can
   * be process normally, however, the rest storage report can not be process
   * since check lease failed.
   * After HDFS-12914, NameNode check lease once for every blockreport request,
   * So this issue will not exist anymore.
   */
  @Test
  public void testCheckBlockReportLease() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    Random rand = new Random();

    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1).build()) {
      cluster.waitActive();

      FSNamesystem fsn = cluster.getNamesystem();
      BlockManager blockManager = fsn.getBlockManager();
      BlockManager spyBlockManager = spy(blockManager);
      fsn.setBlockManagerForTesting(spyBlockManager);
      String poolId = cluster.getNamesystem().getBlockPoolId();

      NamenodeProtocols rpcServer = cluster.getNameNodeRpc();

      // Test based on one DataNode report to Namenode
      DataNode dn = cluster.getDataNodes().get(0);
      DatanodeDescriptor datanodeDescriptor = spyBlockManager
          .getDatanodeManager().getDatanode(dn.getDatanodeId());

      DatanodeRegistration dnRegistration = dn.getDNRegistrationForBP(poolId);
      StorageReport[] storages = dn.getFSDataset().getStorageReports(poolId);

      // Send heartbeat and request full block report lease
      HeartbeatResponse hbResponse = rpcServer.sendHeartbeat(
          dnRegistration, storages, 0, 0, 0, 0, 0, null, true,
              SlowPeerReports.EMPTY_REPORT, SlowDiskReports.EMPTY_REPORT);

      DelayAnswer delayer = new DelayAnswer(BlockManager.LOG);
      doAnswer(delayer).when(spyBlockManager).processReport(
          any(DatanodeStorageInfo.class),
          any(BlockListAsLongs.class));

      ExecutorService pool = Executors.newFixedThreadPool(1);

      // Trigger sendBlockReport
      BlockReportContext brContext = new BlockReportContext(1, 0,
          rand.nextLong(), hbResponse.getFullBlockReportLeaseId());
      Future<DatanodeCommand> sendBRfuturea = pool.submit(() -> {
        // Build every storage with 100 blocks for sending report
        DatanodeStorage[] datanodeStorages
            = new DatanodeStorage[storages.length];
        for (int i = 0; i < storages.length; i++) {
          datanodeStorages[i] = storages[i].getStorage();
        }
        StorageBlockReport[] reports = createReports(datanodeStorages, 100);

        // Send blockReport
        return rpcServer.blockReport(dnRegistration, poolId, reports,
            brContext);
      });

      // Wait until BlockManager calls processReport
      delayer.waitForCall();

      // Remove full block report lease about dn
      spyBlockManager.getBlockReportLeaseManager()
          .removeLease(datanodeDescriptor);

      // Allow blockreport to proceed
      delayer.proceed();

      // Get result, it will not null if process successfully
      DatanodeCommand datanodeCommand = sendBRfuturea.get();
      assertTrue(datanodeCommand instanceof FinalizeCommand);
      assertEquals(poolId, ((FinalizeCommand) datanodeCommand).getBlockPoolId());
    }
  }

  @Test
  public void testExceptionThrownWhenFBRLeaseExpired() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    Random rand = new Random();

    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1).build()) {
      cluster.waitActive();

      FSNamesystem fsn = cluster.getNamesystem();
      BlockManager blockManager = fsn.getBlockManager();
      BlockManager spyBlockManager = spy(blockManager);
      fsn.setBlockManagerForTesting(spyBlockManager);
      String poolId = cluster.getNamesystem().getBlockPoolId();

      NamenodeProtocols rpcServer = cluster.getNameNodeRpc();

      // Test based on one DataNode report to Namenode
      DataNode dn = cluster.getDataNodes().get(0);
      DatanodeDescriptor datanodeDescriptor = spyBlockManager
          .getDatanodeManager().getDatanode(dn.getDatanodeId());

      DatanodeRegistration dnRegistration = dn.getDNRegistrationForBP(poolId);
      StorageReport[] storages = dn.getFSDataset().getStorageReports(poolId);

      // Send heartbeat and request full block report lease
      HeartbeatResponse hbResponse = rpcServer.sendHeartbeat(
          dnRegistration, storages, 0, 0, 0, 0, 0, null, true,
          SlowPeerReports.EMPTY_REPORT, SlowDiskReports.EMPTY_REPORT);

      // Remove full block report lease about dn
      spyBlockManager.getBlockReportLeaseManager()
          .removeLease(datanodeDescriptor);

      ExecutorService pool = Executors.newFixedThreadPool(1);

      // Trigger sendBlockReport
      BlockReportContext brContext = new BlockReportContext(1, 0,
          rand.nextLong(), hbResponse.getFullBlockReportLeaseId());
      Future<DatanodeCommand> sendBRfuturea = pool.submit(() -> {
        // Build every storage with 100 blocks for sending report
        DatanodeStorage[] datanodeStorages
            = new DatanodeStorage[storages.length];
        for (int i = 0; i < storages.length; i++) {
          datanodeStorages[i] = storages[i].getStorage();
        }
        StorageBlockReport[] reports = createReports(datanodeStorages, 100);

        // Send blockReport
        return rpcServer.blockReport(dnRegistration, poolId, reports,
            brContext);
      });

      // Get result, it will not null if process successfully
      ExecutionException exception = null;
      try {
        sendBRfuturea.get();
      } catch (ExecutionException e) {
        exception = e;
      }
      assertNotNull(exception);
      assertEquals(InvalidBlockReportLeaseException.class, exception.getCause().getClass());
    }
  }

  /**
   * Test that when dfs.blockreport.reject.invalid.lease is set to false,
   * the NameNode does not throw InvalidBlockReportLeaseException for an
   * expired lease. This is needed for rolling upgrade compatibility where
   * old DataNodes cannot handle InvalidBlockReportLeaseException and would
   * get stuck in an infinite loop of rejected block reports.
   */
  @Test
  public void testNoExceptionWhenRejectInvalidLeaseDisabled() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setBoolean(
        DFSConfigKeys.DFS_BLOCKREPORT_REJECT_INVALID_LEASE_KEY, false);
    Random rand = new Random();

    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1).build()) {
      cluster.waitActive();

      FSNamesystem fsn = cluster.getNamesystem();
      BlockManager blockManager = fsn.getBlockManager();
      BlockManager spyBlockManager = spy(blockManager);
      fsn.setBlockManagerForTesting(spyBlockManager);
      String poolId = cluster.getNamesystem().getBlockPoolId();

      NamenodeProtocols rpcServer = cluster.getNameNodeRpc();

      DataNode dn = cluster.getDataNodes().get(0);
      DatanodeDescriptor datanodeDescriptor = spyBlockManager
          .getDatanodeManager().getDatanode(dn.getDatanodeId());

      DatanodeRegistration dnRegistration = dn.getDNRegistrationForBP(poolId);
      StorageReport[] storages = dn.getFSDataset().getStorageReports(poolId);

      // Send heartbeat and request full block report lease
      HeartbeatResponse hbResponse = rpcServer.sendHeartbeat(
          dnRegistration, storages, 0, 0, 0, 0, 0, null, true,
          SlowPeerReports.EMPTY_REPORT, SlowDiskReports.EMPTY_REPORT);
      assertTrue(hbResponse.getFullBlockReportLeaseId() != 0,
          "Expected heartbeat to grant a non-zero full block report lease");
      // Remove the lease to simulate expiration
      spyBlockManager.getBlockReportLeaseManager()
          .removeLease(datanodeDescriptor);

      // Trigger sendBlockReport with the now-invalid lease
      BlockReportContext brContext = new BlockReportContext(1, 0,
          rand.nextLong(), hbResponse.getFullBlockReportLeaseId());
      DatanodeStorage[] datanodeStorages
          = new DatanodeStorage[storages.length];
      for (int i = 0; i < storages.length; i++) {
        datanodeStorages[i] = storages[i].getStorage();
      }
      StorageBlockReport[] reports = createReports(datanodeStorages, 100);

      // Should NOT throw InvalidBlockReportLeaseException
      DatanodeCommand cmd = rpcServer.blockReport(
          dnRegistration, poolId, reports, brContext);
      assertNull(cmd);
    }
  }

  @Test
  public void testCheckBlockReportLeaseWhenDnUnregister() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    Random rand = new Random();

    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build()) {
      FSNamesystem fsn = cluster.getNamesystem();
      BlockManager blockManager = fsn.getBlockManager();
      String poolId = cluster.getNamesystem().getBlockPoolId();
      NamenodeProtocols rpcServer = cluster.getNameNodeRpc();

      // Remove the unique DataNode to simulate the unregistered situation.
      // This is similar to starting NameNode, and DataNodes are not registered yet.
      DataNode dn = cluster.getDataNodes().get(0);
      blockManager.getDatanodeManager().getDatanodeMap().remove(dn.getDatanodeUuid());

      // Trigger BlockReport.
      DatanodeRegistration dnRegistration = dn.getDNRegistrationForBP(poolId);
      StorageReport[] storages = dn.getFSDataset().getStorageReports(poolId);
      ExecutorService pool = Executors.newFixedThreadPool(1);
      BlockReportContext brContext = new BlockReportContext(1, 0,
          rand.nextLong(), 1);
      Future<DatanodeCommand> sendBRFuture = pool.submit(() -> {
        // Build every storage with 100 blocks for sending report.
        DatanodeStorage[] datanodeStorages
            = new DatanodeStorage[storages.length];
        for (int i = 0; i < storages.length; i++) {
          datanodeStorages[i] = storages[i].getStorage();
        }
        StorageBlockReport[] reports = createReports(datanodeStorages, 100);

        // Send blockReport.
        return rpcServer.blockReport(dnRegistration, poolId, reports,
            brContext);
      });

      // When unregistered DataNode triggering the block report, will throw an
      // UnregisteredNodeException. After NameNode processing, RegisterCommand
      // is returned to the DataNode.
      DatanodeCommand datanodeCommand = sendBRFuture.get();
      assertTrue(datanodeCommand instanceof RegisterCommand);
    }
  }

  private StorageBlockReport[] createReports(DatanodeStorage[] dnStorages,
      int numBlocks) {
    int longsPerBlock = 3;
    int blockListSize = 2 + numBlocks * longsPerBlock;
    int numStorages = dnStorages.length;
    StorageBlockReport[] storageBlockReports
        = new StorageBlockReport[numStorages];
    for (int i = 0; i < numStorages; i++) {
      List<Long> longs = new ArrayList<Long>(blockListSize);
      longs.add(Long.valueOf(numBlocks));
      longs.add(0L);
      for (int j = 0; j < blockListSize; ++j) {
        longs.add(Long.valueOf(j));
      }
      BlockListAsLongs blockList = BlockListAsLongs.decodeLongs(longs);
      storageBlockReports[i] = new StorageBlockReport(dnStorages[i], blockList);
    }
    return storageBlockReports;
  }

  @Test
  @Timeout(value = 360)
  public void testFirstIncompleteBlockReport() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    Random rand = new Random();

    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
            .numDataNodes(1).build()) {
      cluster.waitActive();

      FSNamesystem fsn = cluster.getNamesystem();

      NameNode nameNode = cluster.getNameNode();
      // Pretend to be in safemode.
      NameNodeAdapter.enterSafeMode(nameNode, false);

      BlockManager blockManager = fsn.getBlockManager();
      BlockManager spyBlockManager = spy(blockManager);
      fsn.setBlockManagerForTesting(spyBlockManager);
      String poolId = cluster.getNamesystem().getBlockPoolId();

      NamenodeProtocols rpcServer = cluster.getNameNodeRpc();

      // Test based on one DataNode report to Namenode.
      DataNode dn = cluster.getDataNodes().get(0);
      DatanodeDescriptor datanodeDescriptor = spyBlockManager
          .getDatanodeManager().getDatanode(dn.getDatanodeId());

      DatanodeRegistration dnRegistration = dn.getDNRegistrationForBP(poolId);
      StorageReport[] storages = dn.getFSDataset().getStorageReports(poolId);

      // Send heartbeat and request full block report lease.
      HeartbeatResponse hbResponse = rpcServer.sendHeartbeat(
          dnRegistration, storages, 0, 0, 0, 0, 0, null, true,
          SlowPeerReports.EMPTY_REPORT, SlowDiskReports.EMPTY_REPORT);

      DelayAnswer delayer = new DelayAnswer(BlockManager.LOG);
      doAnswer(delayer).when(spyBlockManager).processReport(
          any(DatanodeStorageInfo.class),
          any(BlockListAsLongs.class));

      ExecutorService pool = Executors.newFixedThreadPool(1);
      // Trigger sendBlockReport.
      BlockReportContext brContext = new BlockReportContext(1, 0,
          rand.nextLong(), hbResponse.getFullBlockReportLeaseId());
      // Build every storage with 100 blocks for sending report.
      for (int i = 0; i < storages.length; i++) {
        DatanodeStorage s = storages[i].getStorage();
        StorageBlockReport[] reports = createReports(new DatanodeStorage[]{s}, 100);
        DatanodeStorageInfo target = Arrays.stream(datanodeDescriptor.getStorageInfos())
            .filter(info -> info.getStorageID().equals(s.getStorageID()))
            .findFirst()
            .get();
        int before = target.getBlockReportCount();

        Future<DatanodeCommand> f1 = null;
        // The first multiple send once, simulating the failure of the first report,
        // only send successfully once.
        if (i == 0) {
          f1 = pool.submit(() ->
              rpcServer.blockReport(dnRegistration, poolId, reports, brContext));
          delayer.waitForCall();
          delayer.proceed();
          f1.get();
        }

        HeartbeatResponse hbResponse2 = rpcServer.sendHeartbeat(
            dnRegistration, storages, 0, 0, 0, 0, 0, null, true,
            SlowPeerReports.EMPTY_REPORT, SlowDiskReports.EMPTY_REPORT);

        BlockReportContext brContext2 = new BlockReportContext(
            1, 0, rand.nextLong(), hbResponse2.getFullBlockReportLeaseId());
        // Send blockReport.
        Future<DatanodeCommand> f2 = pool.submit(() ->
            rpcServer.blockReport(dnRegistration, poolId, reports, brContext2));

        // Wait until BlockManager calls processReport.
        delayer.waitForCall();

        // Allow blockreport to proceed.
        delayer.proceed();

        DatanodeCommand datanodeCommand = f2.get();
        // Get result, it will not null if process successfully.
        assertTrue(datanodeCommand instanceof FinalizeCommand);
        assertEquals(poolId, ((FinalizeCommand) datanodeCommand)
            .getBlockPoolId());
        if(i == 0){
          assertEquals(2,
              target.getBlockReportCount() - before);
        } else {
          assertEquals(1,
              target.getBlockReportCount() - before);
        }
      }
    }
  }
}
