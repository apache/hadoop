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

import com.google.common.base.Supplier;
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
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * This file tests the erasure coding metrics in DataNode.
 */
public class TestDataNodeErasureCodingMetrics {
  public static final Log LOG = LogFactory.
      getLog(TestDataNodeErasureCodingMetrics.class);

  private static final int DATA_BLK_NUM = StripedFileTestUtil.NUM_DATA_BLOCKS;
  private static final int PARITY_BLK_NUM =
      StripedFileTestUtil.NUM_PARITY_BLOCKS;
  private static final int CELLSIZE =
      StripedFileTestUtil.BLOCK_STRIPED_CELL_SIZE;
  private static final int BLOCKSIZE = CELLSIZE;
  private static final int GROUPSIZE = DATA_BLK_NUM + PARITY_BLK_NUM;
  private static final int DN_NUM = GROUPSIZE + 1;

  private MiniDFSCluster cluster;
  private Configuration conf;
  private DistributedFileSystem fs;

  @Before
  public void setup() throws IOException {
    conf = new Configuration();

    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(DN_NUM).build();
    cluster.waitActive();
    cluster.getFileSystem().getClient().setErasureCodingPolicy("/", null);
    fs = cluster.getFileSystem();
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test(timeout = 120000)
  public void testEcTasks() throws Exception {
    DataNode workerDn = doTest("/testEcTasks");
    MetricsRecordBuilder rb = getMetrics(workerDn.getMetrics().name());

    // Ensure that reconstruction task is finished
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        long taskMetricValue = getLongCounter("EcReconstructionTasks", rb);
        return (taskMetricValue > 0);
      }
    }, 500, 10000);

    assertCounter("EcReconstructionTasks", (long) 1, rb);
    assertCounter("EcFailedReconstructionTasks", (long) 0, rb);
  }

  @Test(timeout = 120000)
  public void testEcCodingTime() throws Exception {
    DataNode workerDn = doTest("/testEcCodingTime");
    MetricsRecordBuilder rb = getMetrics(workerDn.getMetrics().name());

    // Ensure that reconstruction task is finished
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        long taskMetricValue = getLongCounter("EcReconstructionTasks", rb);
        return (taskMetricValue > 0);
      }
    }, 500, 10000);

    long decodeTime = getLongCounter("ecDecodingTimeNanos", rb);
    Assert.assertTrue(decodeTime > 0);
  }

  private DataNode doTest(String fileName) throws Exception {

    Path file = new Path(fileName);
    long fileLen = DATA_BLK_NUM * BLOCKSIZE;
    final byte[] data = StripedFileTestUtil.generateBytes((int) fileLen);
    DFSTestUtil.writeFile(fs, file, data);
    StripedFileTestUtil.waitBlockGroupsReported(fs, fileName);

    LocatedBlocks locatedBlocks =
        StripedFileTestUtil.getLocatedBlocks(file, fs);
    //only one block group
    LocatedStripedBlock lastBlock =
        (LocatedStripedBlock)locatedBlocks.getLastLocatedBlock();
    DataNode workerDn = null;
    DatanodeInfo[] locations = lastBlock.getLocations();
    assertEquals(locations.length, GROUPSIZE);

    // we have ONE extra datanode in addition to the GROUPSIZE datanodes, here
    // is to find the extra datanode that the reconstruction task will run on,
    // according to the current block placement logic for striped files.
    // This can be improved later to be flexible regardless wherever the task
    // runs.
    for (DataNode dn : cluster.getDataNodes()) {
      boolean appear = false;
      for (DatanodeInfo info : locations) {
        if (dn.getDatanodeUuid().equals(info.getDatanodeUuid())) {
          appear = true;
          break;
        }
      }
      if (!appear) {
        workerDn = dn;
        break;
      }
    }
    // Get a datanode from the block locations.
    LOG.info("Block locations: " + Arrays.asList(locations));
    LOG.info("Erasure coding worker datanode: " + workerDn);
    assertNotNull("Failed to find a worker datanode", workerDn);

    DataNode toCorruptDn = cluster.getDataNode(locations[0].getIpcPort());
    LOG.info("Datanode to be corrupted: " + toCorruptDn);
    assertNotNull("Failed to find a datanode to be corrupted", toCorruptDn);
    toCorruptDn.shutdown();
    setDataNodeDead(toCorruptDn.getDatanodeId());
    DFSTestUtil.waitForDatanodeState(cluster, toCorruptDn.getDatanodeUuid(),
        false, 10000);

    int workCount = getComputedDatanodeWork();
    assertTrue("Wrongly computed block reconstruction work", workCount > 0);
    cluster.triggerHeartbeats();
    StripedFileTestUtil.waitForReconstructionFinished(file, fs, GROUPSIZE);

    return workerDn;
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
