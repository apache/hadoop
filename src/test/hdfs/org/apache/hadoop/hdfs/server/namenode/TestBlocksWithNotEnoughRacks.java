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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.NumberReplicas;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Level;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import junit.framework.TestCase;

public class TestBlocksWithNotEnoughRacks extends TestCase {

  static {
    ((Log4JLogger)NameNode.stateChangeLog).getLogger().setLevel(Level.ALL) ;
  }

  private static final Log LOG =
    LogFactory.getLog(TestBlocksWithNotEnoughRacks.class.getName());
  //Creates a block with all datanodes on same rack
  //Adds additional datanode on a different rack
  //The block should be replicated to the new rack
  public void testSufficientlyReplicatedBlocksWithNotEnoughRacks() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong("dfs.heartbeat.interval", 1L);
    conf.setInt("dfs.replication.interval", 1);
    conf.set("topology.script.file.name", "xyz");
    final short REPLICATION_FACTOR = 3;
    final String FILE_NAME = "/testFile";
    final Path FILE_PATH = new Path(FILE_NAME);
    //All datanodes are on the same rack
    String racks[] = {"/rack1","/rack1","/rack1",} ;
    MiniDFSCluster cluster = new MiniDFSCluster(conf, REPLICATION_FACTOR, true, racks);
    try {
      // create a file with one block with a replication factor of 3
      final FileSystem fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, FILE_PATH, 1L, REPLICATION_FACTOR, 1L);
      DFSTestUtil.waitReplication(fs, FILE_PATH, REPLICATION_FACTOR);
      
      Block b = DFSTestUtil.getFirstBlock(fs, FILE_PATH);
      final FSNamesystem namesystem = cluster.getNamesystem();
      int numRacks = namesystem.blockManager.getNumberOfRacks(b);
      NumberReplicas number = namesystem.blockManager.countNodes(b);
      int curReplicas = number.liveReplicas();
      int neededReplicationSize = 
                           namesystem.blockManager.neededReplications.size();
      
      //Add a new datanode on a different rack
      String newRacks[] = {"/rack2"} ;
      cluster.startDataNodes(conf, 1, true, null, newRacks);

      while ( (numRacks < 2) || (curReplicas < REPLICATION_FACTOR) ||
              (neededReplicationSize > 0) ) {
        LOG.info("Waiting for replication");
        Thread.sleep(600);
        numRacks = namesystem.blockManager.getNumberOfRacks(b);
        number = namesystem.blockManager.countNodes(b);
        curReplicas = number.liveReplicas();
        neededReplicationSize = 
                           namesystem.blockManager.neededReplications.size();
      }

      LOG.info("curReplicas = " + curReplicas);
      LOG.info("numRacks = " + numRacks);
      LOG.info("Size = " + namesystem.blockManager.neededReplications.size());

      assertEquals(2,numRacks);
      assertTrue(curReplicas == REPLICATION_FACTOR);
      assertEquals(0,namesystem.blockManager.neededReplications.size());
    } finally {
      cluster.shutdown();
    }
    
  }

  public void testUnderReplicatedNotEnoughRacks() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong("dfs.heartbeat.interval", 1L);
    conf.setInt("dfs.replication.interval", 1);
    conf.setInt("dfs.replication.pending.timeout.sec", 1);
    conf.set("topology.script.file.name", "xyz");
    short REPLICATION_FACTOR = 3;
    final String FILE_NAME = "/testFile";
    final Path FILE_PATH = new Path(FILE_NAME);
    //All datanodes are on the same rack
    String racks[] = {"/rack1","/rack1","/rack1",} ;
    MiniDFSCluster cluster = new MiniDFSCluster(conf, REPLICATION_FACTOR, true, racks);
    try {
      // create a file with one block with a replication factor of 3
      final FileSystem fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, FILE_PATH, 1L, REPLICATION_FACTOR, 1L);
      DFSTestUtil.waitReplication(fs, FILE_PATH, REPLICATION_FACTOR);
      
      Block b = DFSTestUtil.getFirstBlock(fs, FILE_PATH);
      final FSNamesystem namesystem = cluster.getNamesystem();
      int numRacks = namesystem.blockManager.getNumberOfRacks(b);
      NumberReplicas number = namesystem.blockManager.countNodes(b);
      int curReplicas = number.liveReplicas();
      int neededReplicationSize = 
                           namesystem.blockManager.neededReplications.size();
      
      //Add a new datanode on a different rack
      String newRacks[] = {"/rack2","/rack2","/rack2"} ;
      cluster.startDataNodes(conf, 3, true, null, newRacks);
      REPLICATION_FACTOR = 5;
      namesystem.setReplication(FILE_NAME, REPLICATION_FACTOR); 

      while ( (numRacks < 2) || (curReplicas < REPLICATION_FACTOR) ||
              (neededReplicationSize > 0) ) {
        LOG.info("Waiting for replication");
        Thread.sleep(600);
        numRacks = namesystem.blockManager.getNumberOfRacks(b);
        number = namesystem.blockManager.countNodes(b);
        curReplicas = number.liveReplicas();
        neededReplicationSize = 
                           namesystem.blockManager.neededReplications.size();
      }

      LOG.info("curReplicas = " + curReplicas);
      LOG.info("numRacks = " + numRacks);
      LOG.info("Size = " + namesystem.blockManager.neededReplications.size());

      assertEquals(2,numRacks);
      assertTrue(curReplicas == REPLICATION_FACTOR);
      assertEquals(0,namesystem.blockManager.neededReplications.size());
    } finally {
      cluster.shutdown();
    }
    
  }
}
