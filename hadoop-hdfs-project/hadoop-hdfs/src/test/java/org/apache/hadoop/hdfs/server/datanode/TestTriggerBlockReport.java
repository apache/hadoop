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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.timeout;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.client.BlockReportOptions;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.InetSocketAddress;

/**
 * Test manually requesting that the DataNode send a block report.
 */
public final class TestTriggerBlockReport {
  private void testTriggerBlockReport(boolean incremental, boolean withSpecificNN) throws Exception {
    Configuration conf = new HdfsConfiguration();

    // Set a really long value for dfs.blockreport.intervalMsec and
    // dfs.heartbeat.interval, so that incremental block reports and heartbeats
    // won't be sent during this test unless they're triggered
    // manually.
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 10800000L);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1080L);

    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).nnTopology(MiniDFSNNTopology.simpleHATopology()).numDataNodes(1).build();
    cluster.waitActive();
    cluster.transitionToActive(0);
    FileSystem fs = cluster.getFileSystem(0);
    DatanodeProtocolClientSideTranslatorPB spyOnNn0 =
        InternalDataNodeTestUtils.spyOnBposToNN(
            cluster.getDataNodes().get(0), cluster.getNameNode(0));
    DatanodeProtocolClientSideTranslatorPB spyOnNn1 =
        InternalDataNodeTestUtils.spyOnBposToNN(
            cluster.getDataNodes().get(0), cluster.getNameNode(1));
    DFSTestUtil.createFile(fs, new Path("/abc"), 16, (short) 1, 1L);

    // We should get 1 incremental block report on both NNs.
    Mockito.verify(spyOnNn0, timeout(60000).times(1)).blockReceivedAndDeleted(
        any(DatanodeRegistration.class),
        anyString(),
        any(StorageReceivedDeletedBlocks[].class));
    Mockito.verify(spyOnNn1, timeout(60000).times(1)).blockReceivedAndDeleted(
        any(DatanodeRegistration.class),
        anyString(),
        any(StorageReceivedDeletedBlocks[].class));

    // We should not receive any more incremental or incremental block reports,
    // since the interval we configured is so long.
    for (int i = 0; i < 3; i++) {
      Thread.sleep(10);
      Mockito.verify(spyOnNn0, times(0)).blockReport(
          any(DatanodeRegistration.class),
          anyString(),
          any(StorageBlockReport[].class),
          any());
      Mockito.verify(spyOnNn0, times(1)).blockReceivedAndDeleted(
          any(DatanodeRegistration.class),
          anyString(),
          any(StorageReceivedDeletedBlocks[].class));
      Mockito.verify(spyOnNn1, times(0)).blockReport(
          any(DatanodeRegistration.class),
          anyString(),
          any(StorageBlockReport[].class),
          any());
      Mockito.verify(spyOnNn1, times(1)).blockReceivedAndDeleted(
          any(DatanodeRegistration.class),
          anyString(),
          any(StorageReceivedDeletedBlocks[].class));
    }

    // Create a fake block deletion notification on the DataNode.
    // This will be sent with the next incremental block report.
    ReceivedDeletedBlockInfo rdbi = new ReceivedDeletedBlockInfo(
        new Block(5678, 512, 1000),  BlockStatus.DELETED_BLOCK, null);
    DataNode datanode = cluster.getDataNodes().get(0);
    for (BPServiceActor actor : datanode.getAllBpOs().get(0).getBPServiceActors()) {
      final FsDatasetSpi<?> dataset = datanode.getFSDataset();
      final DatanodeStorage storage;
      try (FsDatasetSpi.FsVolumeReferences volumes = dataset.getFsVolumeReferences()) {
        storage = dataset.getStorage(volumes.get(0).getStorageID());
      }
      actor.getIbrManager().addRDBI(rdbi, storage);
    }

    // Manually trigger a block report.
    // Only trigger block report to NN1 when testing triggering block report on specific namenode.
    InetSocketAddress nnAddr = withSpecificNN ? cluster.getNameNode(1).getServiceRpcAddress() : null;
    datanode.triggerBlockReport(
        new BlockReportOptions.Factory().
            setNamenodeAddr(nnAddr).
            setIncremental(incremental).
            build()
    );

    // triggerBlockReport returns before the block report is
    // actually sent.  Wait for it to be sent here.
    if (incremental) {
      Mockito.verify(spyOnNn1, timeout(60000).times(2)).
          blockReceivedAndDeleted(
              any(DatanodeRegistration.class),
              anyString(),
              any(StorageReceivedDeletedBlocks[].class));
      int nn0IncrBlockReport = withSpecificNN ? 1 : 2;
      Mockito.verify(spyOnNn0, timeout(60000).times(nn0IncrBlockReport)).
          blockReceivedAndDeleted(
              any(DatanodeRegistration.class),
              anyString(),
              any(StorageReceivedDeletedBlocks[].class));
    } else {
      Mockito.verify(spyOnNn1, timeout(60000).times(1)).blockReport(
          any(DatanodeRegistration.class),
          anyString(),
          any(StorageBlockReport[].class),
          any());
      int nn0BlockReport = withSpecificNN ? 0 : 1;
      Mockito.verify(spyOnNn0, timeout(60000).times(nn0BlockReport)).blockReport(
          any(DatanodeRegistration.class),
          anyString(),
          any(StorageBlockReport[].class),
          any());
    }

    cluster.shutdown();
  }

  @Test
  public void testTriggerFullBlockReport() throws Exception {
    testTriggerBlockReport(false, false);
    testTriggerBlockReport(false, true);
  }

  @Test
  public void testTriggerIncrementalBlockReport() throws Exception {
    testTriggerBlockReport(true, false);
    testTriggerBlockReport(true, true);
  }
}
