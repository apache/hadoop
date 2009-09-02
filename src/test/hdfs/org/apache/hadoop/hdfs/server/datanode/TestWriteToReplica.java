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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;

import org.junit.Assert;
import org.junit.Test;

/** Test if FSDataset#append, writeToRbw, and writeToTmp */
public class TestWriteToReplica {
  final private static Block[] blocks = new Block[] {
    new Block(1, 1, 2001), new Block(2, 1, 2002), 
    new Block(3, 1, 2003), new Block(4, 1, 2004),
    new Block(5, 1, 2005), new Block(6, 1, 2006)
  };
  final private static int FINALIZED = 0;
  final private static int TEMPORARY = 1;
  final private static int RBW = 2;
  final private static int RWR = 3;
  final private static int RUR = 4;
  final private static int NON_EXISTENT = 5;
  
  // test append
  @Test
  public void testAppend() throws Exception {
    MiniDFSCluster cluster = new MiniDFSCluster(new Configuration(), 1, true, null);
    try {
      cluster.waitActive();
      DataNode dn = cluster.getDataNodes().get(0);
      FSDataset dataSet = (FSDataset)dn.data;

      // set up replicasMap
      setup(dataSet);

      // test append
      testAppend(dataSet);
    } finally {
      cluster.shutdown();
    }
  }

  // test writeToRbw
  @Test
  public void testWriteToRbw() throws Exception {
    MiniDFSCluster cluster = new MiniDFSCluster(new Configuration(), 1, true, null);
    try {
      cluster.waitActive();
      DataNode dn = cluster.getDataNodes().get(0);
      FSDataset dataSet = (FSDataset)dn.data;

      // set up replicasMap
      setup(dataSet);

      // test writeToRbw
      testWriteToRbw(dataSet);
    } finally {
      cluster.shutdown();
    }
  }
  
  // test writeToRbw
  @Test
  public void testWriteToTempoary() throws Exception {
    MiniDFSCluster cluster = new MiniDFSCluster(new Configuration(), 1, true, null);
    try {
      cluster.waitActive();
      DataNode dn = cluster.getDataNodes().get(0);
      FSDataset dataSet = (FSDataset)dn.data;

      // set up replicasMap
      setup(dataSet);

      // test writeToTemporary
      testWriteToTemporary(dataSet);
    } finally {
      cluster.shutdown();
    }
  }
  
  private void setup(FSDataset dataSet) throws IOException {
    // setup replicas map
    ReplicasMap replicasMap = dataSet.volumeMap;
    FSVolume vol = dataSet.volumes.getNextVolume(0);
    ReplicaInfo replicaInfo = new FinalizedReplica(
        blocks[FINALIZED], vol, vol.getDir());
    replicasMap.add(replicaInfo);
    replicaInfo.getBlockFile().createNewFile();
    replicaInfo.getMetaFile().createNewFile();
    replicasMap.add(new ReplicaInPipeline(blocks[TEMPORARY].getBlockId(),
        blocks[TEMPORARY].getGenerationStamp(), vol, 
        vol.createTmpFile(blocks[TEMPORARY]).getParentFile()));
    replicasMap.add(new ReplicaBeingWritten(blocks[RBW].getBlockId(),
        blocks[RBW].getGenerationStamp(), vol, 
        vol.createRbwFile(blocks[RBW]).getParentFile()));
    replicasMap.add(new ReplicaWaitingToBeRecovered(blocks[RWR], vol, 
        vol.createRbwFile(blocks[RWR]).getParentFile()));
    replicasMap.add(new ReplicaUnderRecovery(
        new FinalizedReplica(blocks[RUR], vol, vol.getDir()), 2007));    
  }
  
  private void testAppend(FSDataset dataSet) throws IOException {
    dataSet.append(blocks[FINALIZED]);  // successful
    
    try {
      dataSet.append(blocks[TEMPORARY]);
      Assert.fail("Should not have appended to a temporary replica " 
          + blocks[TEMPORARY]);
    } catch (BlockNotFoundException e) {
      Assert.assertEquals(BlockNotFoundException.UNFINALIZED_REPLICA +
          blocks[TEMPORARY], e.getMessage());
    }

    try {
      dataSet.append(blocks[RBW]);
      Assert.fail("Should not have appended to an RBW replica" + blocks[RBW]);
    } catch (BlockNotFoundException e) {
      Assert.assertEquals(BlockNotFoundException.UNFINALIZED_REPLICA +
          blocks[RBW], e.getMessage());
    }

    try {
      dataSet.append(blocks[RWR]);
      Assert.fail("Should not have appended to an RWR replica" + blocks[RWR]);
    } catch (BlockNotFoundException e) {
      Assert.assertEquals(BlockNotFoundException.UNFINALIZED_REPLICA +
          blocks[RWR], e.getMessage());
    }

    try {
      dataSet.append(blocks[RUR]);
      Assert.fail("Should not have appended to an RUR replica" + blocks[RUR]);
    } catch (BlockNotFoundException e) {
      Assert.assertEquals(BlockNotFoundException.UNFINALIZED_REPLICA +
          blocks[RUR], e.getMessage());
    }

    try {
      dataSet.append(blocks[NON_EXISTENT]);
      Assert.fail("Should not have appended to a non-existent replica " + 
          blocks[NON_EXISTENT]);
    } catch (BlockNotFoundException e) {
      Assert.assertEquals(BlockNotFoundException.NON_EXISTENT_REPLICA + 
          blocks[NON_EXISTENT], e.getMessage());
    }
  }

  private void testWriteToRbw(FSDataset dataSet) throws IOException {
    try {
      dataSet.writeToRbw(blocks[FINALIZED], true);
      Assert.fail("Should not have recovered a finalized replica " +
          blocks[FINALIZED]);
    } catch (BlockNotFoundException e) {
      Assert.assertEquals(BlockNotFoundException.NON_RBW_REPLICA + 
          blocks[FINALIZED], e.getMessage());
    }
 
    try {
      dataSet.writeToRbw(blocks[FINALIZED], false);
      Assert.fail("Should not have created a replica that's already " +
      		"finalized " + blocks[FINALIZED]);
    } catch (BlockAlreadyExistsException e) {
    }
 
    try {
      dataSet.writeToRbw(blocks[TEMPORARY], true);
      Assert.fail("Should not have recovered a temporary replica " +
          blocks[TEMPORARY]);
    } catch (BlockNotFoundException e) {
      Assert.assertEquals(BlockNotFoundException.NON_RBW_REPLICA + 
          blocks[TEMPORARY], e.getMessage());
    }

    try {
      dataSet.writeToRbw(blocks[TEMPORARY], false);
      Assert.fail("Should not have created a replica that had created as " +
      		"temporary " + blocks[TEMPORARY]);
    } catch (BlockAlreadyExistsException e) {
    }
        
    dataSet.writeToRbw(blocks[RBW], true);  // expect to be successful
    
    try {
      dataSet.writeToRbw(blocks[RBW], false);
      Assert.fail("Should not have created a replica that had created as RBW " +
          blocks[RBW]);
    } catch (BlockAlreadyExistsException e) {
    }
    
    try {
      dataSet.writeToRbw(blocks[RWR], true);
      Assert.fail("Should not have recovered a RWR replica " + blocks[RWR]);
    } catch (BlockNotFoundException e) {
      Assert.assertEquals(BlockNotFoundException.NON_RBW_REPLICA + 
          blocks[RWR], e.getMessage());
    }

    try {
      dataSet.writeToRbw(blocks[RWR], false);
      Assert.fail("Should not have created a replica that was waiting to be " +
      		"recovered " + blocks[RWR]);
    } catch (BlockAlreadyExistsException e) {
    }
    
    try {
      dataSet.writeToRbw(blocks[RUR], true);
      Assert.fail("Should not have recovered a RUR replica " + blocks[RUR]);
    } catch (BlockNotFoundException e) {
      Assert.assertEquals(BlockNotFoundException.NON_RBW_REPLICA + 
          blocks[RUR], e.getMessage());
    }

    try {
      dataSet.writeToRbw(blocks[RUR], false);
      Assert.fail("Should not have created a replica that was under recovery " +
          blocks[RUR]);
    } catch (BlockAlreadyExistsException e) {
    }
    
    dataSet.writeToRbw(blocks[NON_EXISTENT], true);
    
    // remove this replica
    ReplicaInfo removedReplica = dataSet.volumeMap.remove(blocks[NON_EXISTENT]);
    removedReplica.getBlockFile().delete();
    removedReplica.getMetaFile().delete();
    
    dataSet.writeToRbw(blocks[NON_EXISTENT], false);
  }
  
  private void testWriteToTemporary(FSDataset dataSet) throws IOException {
    try {
      dataSet.writeToTemporary(blocks[FINALIZED]);
      Assert.fail("Should not have created a temporary replica that was " +
      		"finalized " + blocks[FINALIZED]);
    } catch (BlockAlreadyExistsException e) {
    }
 
    try {
      dataSet.writeToTemporary(blocks[TEMPORARY]);
      Assert.fail("Should not have created a replica that had created as" +
      		"temporary " + blocks[TEMPORARY]);
    } catch (BlockAlreadyExistsException e) {
    }
    
    try {
      dataSet.writeToTemporary(blocks[RBW]);
      Assert.fail("Should not have created a replica that had created as RBW " +
          blocks[RBW]);
    } catch (BlockAlreadyExistsException e) {
    }
    
    try {
      dataSet.writeToTemporary(blocks[RWR]);
      Assert.fail("Should not have created a replica that was waiting to be " +
      		"recovered " + blocks[RWR]);
    } catch (BlockAlreadyExistsException e) {
    }
    
    try {
      dataSet.writeToTemporary(blocks[RUR]);
      Assert.fail("Should not have created a replica that was under recovery " +
          blocks[RUR]);
    } catch (BlockAlreadyExistsException e) {
    }
    
    dataSet.writeToTemporary(blocks[NON_EXISTENT]);
  }
}
