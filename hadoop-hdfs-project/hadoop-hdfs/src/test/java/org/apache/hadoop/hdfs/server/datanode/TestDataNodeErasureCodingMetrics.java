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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounterWithoutCheck;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;

/**
 * This file tests the erasure coding metrics in DataNode.
 */
public class TestDataNodeErasureCodingMetrics {
  public static final Log LOG = LogFactory.
      getLog(TestDataNodeErasureCodingMetrics.class);
  private final ErasureCodingPolicy ecPolicy =
      StripedFileTestUtil.getDefaultECPolicy();
  private final int dataBlocks = ecPolicy.getNumDataUnits();
  private final int parityBlocks = ecPolicy.getNumParityUnits();
  private final int cellSize = ecPolicy.getCellSize();
  private final int blockSize = cellSize * 2;
  private final int groupSize = dataBlocks + parityBlocks;
  private final int blockGroupSize = blockSize * dataBlocks;
  private final int numDNs = groupSize + 1;

  private MiniDFSCluster cluster;
  private Configuration conf;
  private DistributedFileSystem fs;

  @Before
  public void setup() throws IOException {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 1);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    cluster.waitActive();
    cluster.getFileSystem().getClient().setErasureCodingPolicy("/",
        StripedFileTestUtil.getDefaultECPolicy().getName());
    fs = cluster.getFileSystem();
    fs.enableErasureCodingPolicy(
        StripedFileTestUtil.getDefaultECPolicy().getName());
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test(timeout = 120000)
  public void testFullBlock() throws Exception {
    doTest("/testEcMetrics", blockGroupSize, 0);

    Assert.assertEquals("EcReconstructionTasks should be ",
        1, getLongMetric("EcReconstructionTasks"));
    Assert.assertEquals("EcFailedReconstructionTasks should be ",
        0, getLongMetric("EcFailedReconstructionTasks"));
    Assert.assertTrue(getLongMetric("EcDecodingTimeNanos") > 0);
    Assert.assertEquals("EcReconstructionBytesRead should be ",
        blockGroupSize, getLongMetric("EcReconstructionBytesRead"));
    Assert.assertEquals("EcReconstructionBytesWritten should be ",
        blockSize, getLongMetric("EcReconstructionBytesWritten"));
    Assert.assertEquals("EcReconstructionRemoteBytesRead should be ",
        0, getLongMetricWithoutCheck("EcReconstructionRemoteBytesRead"));
  }

  // A partial block, reconstruct the partial block
  @Test(timeout = 120000)
  public void testReconstructionBytesPartialGroup1() throws Exception {
    final int fileLen = blockSize / 10;
    doTest("/testEcBytes", fileLen, 0);

    Assert.assertEquals("EcReconstructionBytesRead should be ",
        fileLen,  getLongMetric("EcReconstructionBytesRead"));
    Assert.assertEquals("EcReconstructionBytesWritten should be ",
        fileLen, getLongMetric("EcReconstructionBytesWritten"));
    Assert.assertEquals("EcReconstructionRemoteBytesRead should be ",
        0, getLongMetricWithoutCheck("EcReconstructionRemoteBytesRead"));
  }

  // 1 full block + 5 partial block, reconstruct the full block
  @Test(timeout = 120000)
  public void testReconstructionBytesPartialGroup2() throws Exception {
    final int fileLen = cellSize * dataBlocks + cellSize + cellSize / 10;
    doTest("/testEcBytes", fileLen, 0);

    Assert.assertEquals("ecReconstructionBytesRead should be ",
        cellSize * dataBlocks + cellSize + cellSize / 10,
        getLongMetric("EcReconstructionBytesRead"));
    Assert.assertEquals("EcReconstructionBytesWritten should be ",
        blockSize, getLongMetric("EcReconstructionBytesWritten"));
    Assert.assertEquals("EcReconstructionRemoteBytesRead should be ",
        0, getLongMetricWithoutCheck("EcReconstructionRemoteBytesRead"));
  }

  // 1 full block + 5 partial block, reconstruct the partial block
  @Test(timeout = 120000)
  public void testReconstructionBytesPartialGroup3() throws Exception {
    final int fileLen = cellSize * dataBlocks + cellSize + cellSize / 10;
    doTest("/testEcBytes", fileLen, 1);

    Assert.assertEquals("ecReconstructionBytesRead should be ",
        cellSize * dataBlocks + (cellSize / 10) * 2 ,
        getLongMetric("EcReconstructionBytesRead"));
    Assert.assertEquals("ecReconstructionBytesWritten should be ",
        cellSize + cellSize / 10,
        getLongMetric("EcReconstructionBytesWritten"));
    Assert.assertEquals("EcReconstructionRemoteBytesRead should be ",
        0, getLongMetricWithoutCheck("EcReconstructionRemoteBytesRead"));
  }

  private long getLongMetric(String metricName) {
    long metricValue = 0;
    // Add all reconstruction metric value from all data nodes
    for (DataNode dn : cluster.getDataNodes()) {
      MetricsRecordBuilder rb = getMetrics(dn.getMetrics().name());
      metricValue += getLongCounter(metricName, rb);
    }
    return metricValue;
  }

  private long getLongMetricWithoutCheck(String metricName) {
    long metricValue = 0;
    // Add all reconstruction metric value from all data nodes
    for (DataNode dn : cluster.getDataNodes()) {
      MetricsRecordBuilder rb = getMetrics(dn.getMetrics().name());
      metricValue += getLongCounterWithoutCheck(metricName, rb);
    }
    return metricValue;
  }

  private void doTest(String fileName, int fileLen,
      int deadNodeIndex) throws Exception {
    assertTrue(fileLen > 0);
    assertTrue(deadNodeIndex >= 0 && deadNodeIndex < numDNs);
    Path file = new Path(fileName);
    final byte[] data = StripedFileTestUtil.generateBytes(fileLen);
    DFSTestUtil.writeFile(fs, file, data);
    StripedFileTestUtil.waitBlockGroupsReported(fs, fileName);

    final LocatedBlocks locatedBlocks =
        StripedFileTestUtil.getLocatedBlocks(file, fs);
    final LocatedStripedBlock lastBlock =
        (LocatedStripedBlock)locatedBlocks.getLastLocatedBlock();
    assertTrue(lastBlock.getLocations().length > deadNodeIndex);

    final DataNode toCorruptDn = cluster.getDataNode(
        lastBlock.getLocations()[deadNodeIndex].getIpcPort());
    LOG.info("Datanode to be corrupted: " + toCorruptDn);
    assertNotNull("Failed to find a datanode to be corrupted", toCorruptDn);
    toCorruptDn.shutdown();
    setDataNodeDead(toCorruptDn.getDatanodeId());
    DFSTestUtil.waitForDatanodeState(cluster, toCorruptDn.getDatanodeUuid(),
        false, 10000);

    final int workCount = getComputedDatanodeWork();
    assertTrue("Wrongly computed block reconstruction work", workCount > 0);
    cluster.triggerHeartbeats();
    int totalBlocks =  (fileLen / blockGroupSize) * groupSize;
    final int remainder = fileLen % blockGroupSize;
    totalBlocks += (remainder == 0) ? 0 :
        (remainder % blockSize == 0) ? remainder / blockSize + parityBlocks :
            remainder / blockSize + 1 + parityBlocks;
    StripedFileTestUtil.waitForAllReconstructionFinished(file, fs, totalBlocks);
  }

  private int getComputedDatanodeWork()
      throws IOException, InterruptedException {
    final BlockManager bm = cluster.getNamesystem().getBlockManager();
    // Giving a grace period to compute datanode work.
    int workCount = 0;
    int retries = 20;
    while (retries > 0) {
      workCount = BlockManagerTestUtil.getComputedDatanodeWork(bm);
      if (workCount > 0) {
        break;
      }
      retries--;
      Thread.sleep(500);
    }
    LOG.info("Computed datanode work: " + workCount + ", retries: " + retries);
    return workCount;
  }

  private void setDataNodeDead(DatanodeID dnID) throws IOException {
    DatanodeDescriptor dnd =
        NameNodeAdapter.getDatanode(cluster.getNamesystem(), dnID);
    DFSTestUtil.setDatanodeDead(dnd);
    BlockManagerTestUtil.checkHeartbeat(
        cluster.getNamesystem().getBlockManager());
  }
}
