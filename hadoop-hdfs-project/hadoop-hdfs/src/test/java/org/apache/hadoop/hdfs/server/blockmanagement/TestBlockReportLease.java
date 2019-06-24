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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.FinalizeCommand;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports;
import org.apache.hadoop.hdfs.server.protocol.SlowPeerReports;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.test.GenericTestUtils.DelayAnswer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * Tests that BlockReportLease in BlockManager.
 */
public class TestBlockReportLease {
  private static final Log LOG = LogFactory.getLog(TestBlockReportLease.class);
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

      DelayAnswer delayer = new DelayAnswer(LOG);
      doAnswer(delayer).when(spyBlockManager).processReport(
          any(DatanodeStorageInfo.class),
          any(BlockListAsLongs.class),
          any(BlockReportContext.class));

      ExecutorService pool = Executors.newFixedThreadPool(1);

      // Trigger sendBlockReport
      BlockReportContext brContext = new BlockReportContext(1, 0,
          rand.nextLong(), hbResponse.getFullBlockReportLeaseId(), true);
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
      assertEquals(poolId, ((FinalizeCommand)datanodeCommand)
          .getBlockPoolId());
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
}