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
package org.apache.hadoop.hdfs.server.namenode;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.FsDatasetTestUtils;
import org.junit.Test;



/**
 * This tests InterDataNodeProtocol for block handling. 
 */
public class TestNamenodeCapacityReport {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestNamenodeCapacityReport.class);

  /**
   * The following test first creates a file.
   * It verifies the block information from a datanode.
   * Then, it updates the block with new information and verifies again. 
   */
  @Test
  public void testVolumeSize() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;

    // Set aside fifth of the total capacity as reserved
    long reserved = 10000;
    conf.setLong(DFSConfigKeys.DFS_DATANODE_DU_RESERVED_KEY, reserved);
    
    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      final FSNamesystem namesystem = cluster.getNamesystem();
      final DatanodeManager dm = cluster.getNamesystem().getBlockManager(
          ).getDatanodeManager();
      
      // Ensure the data reported for each data node is right
      final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
      final List<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
      dm.fetchDatanodes(live, dead, false);
      
      assertTrue(live.size() == 1);
      
      long used, remaining, configCapacity, nonDFSUsed, bpUsed;
      float percentUsed, percentRemaining, percentBpUsed;
      
      for (final DatanodeDescriptor datanode : live) {
        used = datanode.getDfsUsed();
        remaining = datanode.getRemaining();
        nonDFSUsed = datanode.getNonDfsUsed();
        configCapacity = datanode.getCapacity();
        percentUsed = datanode.getDfsUsedPercent();
        percentRemaining = datanode.getRemainingPercent();
        bpUsed = datanode.getBlockPoolUsed();
        percentBpUsed = datanode.getBlockPoolUsedPercent();
        
        LOG.info("Datanode configCapacity " + configCapacity
            + " used " + used + " non DFS used " + nonDFSUsed 
            + " remaining " + remaining + " perentUsed " + percentUsed
            + " percentRemaining " + percentRemaining);
        // There will be 5% space reserved in ext filesystem which is not
        // considered.
        assertTrue(configCapacity >= (used + remaining + nonDFSUsed));
        assertTrue(percentUsed == DFSUtilClient.getPercentUsed(used,
                                                               configCapacity));
        assertTrue(percentRemaining == DFSUtilClient.getPercentRemaining(
            remaining, configCapacity));
        assertTrue(percentBpUsed == DFSUtilClient.getPercentUsed(bpUsed,
                                                                 configCapacity));
      }   

      //
      // Currently two data directories are created by the data node
      // in the MiniDFSCluster. This results in each data directory having
      // capacity equals to the disk capacity of the data directory.
      // Hence the capacity reported by the data node is twice the disk space
      // the disk capacity
      //
      // So multiply the disk capacity and reserved space by two 
      // for accommodating it
      //
      final FsDatasetTestUtils utils = cluster.getFsDatasetTestUtils(0);
      int numOfDataDirs = utils.getDefaultNumOfDataDirs();

      long diskCapacity = numOfDataDirs * utils.getRawCapacity();
      reserved *= numOfDataDirs;
      
      configCapacity = namesystem.getCapacityTotal();
      used = namesystem.getCapacityUsed();
      nonDFSUsed = namesystem.getNonDfsUsedSpace();
      remaining = namesystem.getCapacityRemaining();
      percentUsed = namesystem.getPercentUsed();
      percentRemaining = namesystem.getPercentRemaining();
      bpUsed = namesystem.getBlockPoolUsedSpace();
      percentBpUsed = namesystem.getPercentBlockPoolUsed();
      
      LOG.info("Data node directory " + cluster.getDataDirectory());
           
      LOG.info("Name node diskCapacity " + diskCapacity + " configCapacity "
          + configCapacity + " reserved " + reserved + " used " + used 
          + " remaining " + remaining + " nonDFSUsed " + nonDFSUsed 
          + " remaining " + remaining + " percentUsed " + percentUsed 
          + " percentRemaining " + percentRemaining + " bpUsed " + bpUsed
          + " percentBpUsed " + percentBpUsed);
      
      // Ensure new total capacity reported excludes the reserved space
      assertTrue(configCapacity == diskCapacity - reserved);
      
      // Ensure new total capacity reported excludes the reserved space
      // There will be 5% space reserved in ext filesystem which is not
      // considered.
      assertTrue(configCapacity >= (used + remaining + nonDFSUsed));

      // Ensure percent used is calculated based on used and present capacity
      assertTrue(percentUsed == DFSUtilClient.getPercentUsed(used,
                                                             configCapacity));

      // Ensure percent used is calculated based on used and present capacity
      assertTrue(percentBpUsed == DFSUtilClient.getPercentUsed(bpUsed,
                                                               configCapacity));

      // Ensure percent used is calculated based on used and present capacity
      assertTrue(percentRemaining == ((float)remaining * 100.0f)/(float)configCapacity);

      //Adding testcase for non-dfs used where we need to consider
      // reserved replica also.
      final int fileCount = 5;
      final DistributedFileSystem fs = cluster.getFileSystem();
      // create streams and hsync to force datastreamers to start
      DFSOutputStream[] streams = new DFSOutputStream[fileCount];
      for (int i=0; i < fileCount; i++) {
        streams[i] = (DFSOutputStream)fs.create(new Path("/f"+i))
            .getWrappedStream();
        streams[i].write("1".getBytes());
        streams[i].hsync();
      }
      triggerHeartbeats(cluster.getDataNodes());
      assertTrue(configCapacity > (namesystem.getCapacityUsed() + namesystem
          .getCapacityRemaining() + namesystem.getNonDfsUsedSpace()));
      // There is a chance that nonDFS usage might have slightly due to
      // testlogs, So assume 1MB other files used within this gap
      assertTrue(
          (namesystem.getCapacityUsed() + namesystem.getCapacityRemaining()
              + namesystem.getNonDfsUsedSpace() + fileCount * fs
              .getDefaultBlockSize()) - configCapacity < 1 * 1024);
    }
    finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  private static final float EPSILON = 0.0001f;
  @Test
  public void testXceiverCount() throws Exception {
    testXceiverCountInternal(0);
    testXceiverCountInternal(1);
  }

  public void testXceiverCountInternal(int minMaintenanceR) throws Exception {
    Configuration conf = new HdfsConfiguration();
    // retry one time, if close fails
    conf.setInt(
        HdfsClientConfigKeys.BlockWrite.LOCATEFOLLOWINGBLOCK_RETRIES_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAINTENANCE_REPLICATION_MIN_KEY,
        minMaintenanceR);
    MiniDFSCluster cluster = null;

    final int nodes = 8;
    final int fileCount = 5;
    final short fileRepl = 3;

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(nodes).build();
      cluster.waitActive();

      final FSNamesystem namesystem = cluster.getNamesystem();
      final DatanodeManager dnm = namesystem.getBlockManager().getDatanodeManager();
      List<DataNode> datanodes = cluster.getDataNodes();
      final DistributedFileSystem fs = cluster.getFileSystem();

      // trigger heartbeats in case not already sent
      triggerHeartbeats(datanodes);
      
      // check that all nodes are live and in service
      int expectedTotalLoad = 0;
      int expectedInServiceNodes = nodes;
      int expectedInServiceLoad = 0;
      checkClusterHealth(nodes, namesystem, expectedTotalLoad,
          expectedInServiceNodes, expectedInServiceLoad);

      // Shutdown half the nodes followed by admin operations on those nodes.
      // Ensure counts are accurate.
      for (int i=0; i < nodes/2; i++) {
        DataNode dn = datanodes.get(i);
        DatanodeDescriptor dnd = dnm.getDatanode(dn.getDatanodeId());
        dn.shutdown();
        DFSTestUtil.setDatanodeDead(dnd);
        BlockManagerTestUtil.checkHeartbeat(namesystem.getBlockManager());
        //Admin operations on dead nodes won't impact nodesInService metrics.
        startDecommissionOrMaintenance(dnm, dnd, (i % 2 == 0));
        expectedInServiceNodes--;
        assertEquals(expectedInServiceNodes, namesystem.getNumLiveDataNodes());
        assertEquals(expectedInServiceNodes, getNumDNInService(namesystem));
        stopDecommissionOrMaintenance(dnm, dnd, (i % 2 == 0));
        assertEquals(expectedInServiceNodes, getNumDNInService(namesystem));
      }

      // restart the nodes to verify that counts are correct after
      // node re-registration 
      cluster.restartDataNodes();
      cluster.waitActive();
      datanodes = cluster.getDataNodes();
      expectedInServiceNodes = nodes;
      assertEquals(nodes, datanodes.size());
      checkClusterHealth(nodes, namesystem, expectedTotalLoad,
          expectedInServiceNodes, expectedInServiceLoad);

      // create streams and hsync to force datastreamers to start
      DFSOutputStream[] streams = new DFSOutputStream[fileCount];
      for (int i=0; i < fileCount; i++) {
        streams[i] = (DFSOutputStream)fs.create(new Path("/f"+i), fileRepl)
            .getWrappedStream();
        streams[i].write("1".getBytes());
        streams[i].hsync();
        // the load for writers is 2 because both the write xceiver & packet
        // responder threads are counted in the load
        expectedTotalLoad += 2*fileRepl;
        expectedInServiceLoad += 2*fileRepl;
      }
      // force nodes to send load update
      triggerHeartbeats(datanodes);
      checkClusterHealth(nodes, namesystem, expectedTotalLoad,
          expectedInServiceNodes, expectedInServiceLoad);

      // admin operations on a few nodes, substract their load from the
      // expected load, trigger heartbeat to force load update.
      for (int i=0; i < fileRepl; i++) {
        expectedInServiceNodes--;
        DatanodeDescriptor dnd =
            dnm.getDatanode(datanodes.get(i).getDatanodeId());
        expectedInServiceLoad -= dnd.getXceiverCount();
        startDecommissionOrMaintenance(dnm, dnd, (i % 2 == 0));
        DataNodeTestUtils.triggerHeartbeat(datanodes.get(i));
        Thread.sleep(100);
        checkClusterHealth(nodes, namesystem, expectedTotalLoad,
            expectedInServiceNodes, expectedInServiceLoad);
      }

      // check expected load while closing each stream.  recalc expected
      // load based on whether the nodes in the pipeline are decomm
      for (int i=0; i < fileCount; i++) {
        int adminOps = 0;
        for (DatanodeInfo dni : streams[i].getPipeline()) {
          DatanodeDescriptor dnd = dnm.getDatanode(dni);
          expectedTotalLoad -= 2;
          if (!dnd.isInService()) {
            adminOps++;
          } else {
            expectedInServiceLoad -= 2;
          }
        }
        try {
          streams[i].close();
        } catch (IOException ioe) {
          // nodes will go decommissioned even if there's a UC block whose
          // other locations are decommissioned too.  we'll ignore that
          // bug for now
          if (adminOps < fileRepl) {
            throw ioe;
          }
        }
        triggerHeartbeats(datanodes);
        // verify node count and loads 
        checkClusterHealth(nodes, namesystem, expectedTotalLoad,
            expectedInServiceNodes, expectedInServiceLoad);
      }

      // shutdown each node, verify node counts based on admin state
      for (int i=0; i < nodes; i++) {
        DataNode dn = datanodes.get(i);
        dn.shutdown();
        // force it to appear dead so live count decreases
        DatanodeDescriptor dnDesc = dnm.getDatanode(dn.getDatanodeId());
        DFSTestUtil.setDatanodeDead(dnDesc);
        BlockManagerTestUtil.checkHeartbeat(namesystem.getBlockManager());
        assertEquals(nodes-1-i, namesystem.getNumLiveDataNodes());
        // first few nodes are already out of service
        if (i >= fileRepl) {
          expectedInServiceNodes--;
        }
        assertEquals(expectedInServiceNodes, getNumDNInService(namesystem));
        assertEquals(0, getInServiceXceiverAverage(namesystem), EPSILON);
      }
      // final sanity check
      checkClusterHealth(0, namesystem, 0.0, 0, 0.0);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private void startDecommissionOrMaintenance(DatanodeManager dnm,
      DatanodeDescriptor dnd, boolean decomm) {
    if (decomm) {
      dnm.getDatanodeAdminManager().startDecommission(dnd);
    } else {
      dnm.getDatanodeAdminManager().startMaintenance(dnd, Long.MAX_VALUE);
    }
  }

  private void stopDecommissionOrMaintenance(DatanodeManager dnm,
      DatanodeDescriptor dnd, boolean decomm) {
    if (decomm) {
      dnm.getDatanodeAdminManager().stopDecommission(dnd);
    } else {
      dnm.getDatanodeAdminManager().stopMaintenance(dnd);
    }
  }

  private static void checkClusterHealth(
    int numOfLiveNodes,
    FSNamesystem namesystem, double expectedTotalLoad,
    int expectedInServiceNodes, double expectedInServiceLoad) {

    assertEquals(numOfLiveNodes, namesystem.getNumLiveDataNodes());
    assertEquals(expectedInServiceNodes, getNumDNInService(namesystem));
    assertEquals(expectedTotalLoad, namesystem.getTotalLoad(), EPSILON);
    if (expectedInServiceNodes != 0) {
      assertEquals(expectedInServiceLoad / expectedInServiceNodes,
        getInServiceXceiverAverage(namesystem), EPSILON);
    } else {
      assertEquals(0.0, getInServiceXceiverAverage(namesystem), EPSILON);
    }
  }

  private static int getNumDNInService(FSNamesystem fsn) {
    return fsn.getBlockManager().getDatanodeManager().getFSClusterStats()
      .getNumDatanodesInService();
  }

  private static double getInServiceXceiverAverage(FSNamesystem fsn) {
    return fsn.getBlockManager().getDatanodeManager().getFSClusterStats()
      .getInServiceXceiverAverage();
  }

  private void triggerHeartbeats(List<DataNode> datanodes)
      throws IOException, InterruptedException {
    for (DataNode dn : datanodes) {
      DataNodeTestUtils.triggerHeartbeat(dn);
    }
    Thread.sleep(100);
  }
}
