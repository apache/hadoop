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


import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.junit.Test;



/**
 * This tests InterDataNodeProtocol for block handling. 
 */
public class TestNamenodeCapacityReport {
  private static final Log LOG = LogFactory.getLog(TestNamenodeCapacityReport.class);

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
        
        assertTrue(configCapacity == (used + remaining + nonDFSUsed));
        assertTrue(percentUsed == DFSUtil.getPercentUsed(used, configCapacity));
        assertTrue(percentRemaining == DFSUtil.getPercentRemaining(remaining,
            configCapacity));
        assertTrue(percentBpUsed == DFSUtil.getPercentUsed(bpUsed,
            configCapacity));
      }   
      
      DF df = new DF(new File(cluster.getDataDirectory()), conf);
     
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
      int numOfDataDirs = 2;
      
      long diskCapacity = numOfDataDirs * df.getCapacity();
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
      assertTrue(configCapacity == (used + remaining + nonDFSUsed));

      // Ensure percent used is calculated based on used and present capacity
      assertTrue(percentUsed == DFSUtil.getPercentUsed(used, configCapacity));

      // Ensure percent used is calculated based on used and present capacity
      assertTrue(percentBpUsed == DFSUtil.getPercentUsed(bpUsed, configCapacity));

      // Ensure percent used is calculated based on used and present capacity
      assertTrue(percentRemaining == ((float)remaining * 100.0f)/(float)configCapacity);
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
}
