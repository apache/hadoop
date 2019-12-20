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
import static org.mockito.Mockito.timeout;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.BlockReportOptions;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.test.MetricsAsserts;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test counters for number of blocks in pending IBR.
 */
public class TestBlockCountersInPendingIBR {

  @Test
  public void testBlockCounters() throws Exception {
    final Configuration conf = new HdfsConfiguration();

    /*
     * Set a really long value for dfs.blockreport.intervalMsec and
     * dfs.heartbeat.interval, so that incremental block reports and heartbeats
     * won't be sent during this test unless they're triggered manually.
     */
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 10800000L);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1080L);

    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    final DatanodeProtocolClientSideTranslatorPB spy =
        InternalDataNodeTestUtils.spyOnBposToNN(
            cluster.getDataNodes().get(0), cluster.getNameNode());
    final DataNode datanode = cluster.getDataNodes().get(0);

    /* We should get 0 incremental block report. */
    Mockito.verify(spy, timeout(60000).times(0)).blockReceivedAndDeleted(
        any(DatanodeRegistration.class),
        anyString(),
        any(StorageReceivedDeletedBlocks[].class));

    /*
     * Create fake blocks notification on the DataNode. This will be sent with
     * the next incremental block report.
     */
    final BPServiceActor actor =
        datanode.getAllBpOs().get(0).getBPServiceActors().get(0);
    final FsDatasetSpi<?> dataset = datanode.getFSDataset();
    final DatanodeStorage storage;
    try (FsDatasetSpi.FsVolumeReferences volumes =
        dataset.getFsVolumeReferences()) {
      storage = dataset.getStorage(volumes.get(0).getStorageID());
    }

    ReceivedDeletedBlockInfo rdbi = null;
    /* block at status of RECEIVING_BLOCK */
    rdbi = new ReceivedDeletedBlockInfo(
        new Block(5678, 512, 1000),  BlockStatus.RECEIVING_BLOCK, null);
    actor.getIbrManager().addRDBI(rdbi, storage);

    /* block at status of RECEIVED_BLOCK */
    rdbi = new ReceivedDeletedBlockInfo(
        new Block(5679, 512, 1000),  BlockStatus.RECEIVED_BLOCK, null);
    actor.getIbrManager().addRDBI(rdbi, storage);

    /* block at status of DELETED_BLOCK */
    rdbi = new ReceivedDeletedBlockInfo(
        new Block(5680, 512, 1000),  BlockStatus.DELETED_BLOCK, null);
    actor.getIbrManager().addRDBI(rdbi, storage);

    /* verify counters before sending IBR */
    verifyBlockCounters(datanode, 3, 1, 1, 1);

    /* Manually trigger a block report. */
    datanode.triggerBlockReport(
        new BlockReportOptions.Factory().
            setIncremental(true).
            build()
    );

    /*
     * triggerBlockReport returns before the block report is actually sent. Wait
     * for it to be sent here.
     */
    Mockito.verify(spy, timeout(60000).times(1)).
        blockReceivedAndDeleted(
            any(DatanodeRegistration.class),
            anyString(),
            any(StorageReceivedDeletedBlocks[].class));

    /* verify counters after sending IBR */
    verifyBlockCounters(datanode, 0, 0, 0, 0);

    cluster.shutdown();
  }


  private void verifyBlockCounters(final DataNode datanode,
      final long blocksInPendingIBR, final long blocksReceivingInPendingIBR,
      final long blocksReceivedInPendingIBR,
      final long blocksDeletedInPendingIBR) {

    final MetricsRecordBuilder m = MetricsAsserts
        .getMetrics(datanode.getMetrics().name());

    MetricsAsserts.assertGauge("BlocksInPendingIBR",
        blocksInPendingIBR, m);
    MetricsAsserts.assertGauge("BlocksReceivingInPendingIBR",
        blocksReceivingInPendingIBR, m);
    MetricsAsserts.assertGauge("BlocksReceivedInPendingIBR",
        blocksReceivedInPendingIBR, m);
    MetricsAsserts.assertGauge("BlocksDeletedInPendingIBR",
        blocksDeletedInPendingIBR, m);
  }
}
