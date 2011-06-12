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
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;

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
  
  // test close
  @Test
  public void testClose() throws Exception {
    MiniDFSCluster cluster = new MiniDFSCluster(new HdfsConfiguration(), 1, true, null);
    try {
      cluster.waitActive();
      DataNode dn = cluster.getDataNodes().get(0);
      FSDataset dataSet = (FSDataset)dn.data;

      // set up replicasMap
      setup(dataSet);

      // test close
      testClose(dataSet);
    } finally {
      cluster.shutdown();
    }
  }

  // test append
  @Test
  public void testAppend() throws Exception {
    MiniDFSCluster cluster = new MiniDFSCluster(new HdfsConfiguration(), 1, true, null);
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
    MiniDFSCluster cluster = new MiniDFSCluster(new HdfsConfiguration(), 1, true, null);
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
  
  // test writeToTemporary
  @Test
  public void testWriteToTempoary() throws Exception {
    MiniDFSCluster cluster = new MiniDFSCluster(new HdfsConfiguration(), 1, true, null);
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
    
    replicasMap.add(new ReplicaInPipeline(
        blocks[TEMPORARY].getBlockId(),
        blocks[TEMPORARY].getGenerationStamp(), vol, 
        vol.createTmpFile(blocks[TEMPORARY]).getParentFile()));
    
    replicaInfo = new ReplicaBeingWritten(blocks[RBW], vol, 
        vol.createRbwFile(blocks[RBW]).getParentFile(), null);
    replicasMap.add(replicaInfo);
    replicaInfo.getBlockFile().createNewFile();
    replicaInfo.getMetaFile().createNewFile();
    
    replicasMap.add(new ReplicaWaitingToBeRecovered(blocks[RWR], vol, 
        vol.createRbwFile(blocks[RWR]).getParentFile()));
    replicasMap.add(new ReplicaUnderRecovery(
        new FinalizedReplica(blocks[RUR], vol, vol.getDir()), 2007));    
  }
  
  private void testAppend(FSDataset dataSet) throws IOException {
    long newGS = blocks[FINALIZED].getGenerationStamp()+1;
    FSVolume v = dataSet.volumeMap.get(blocks[FINALIZED]).getVolume();
    long available = v.getCapacity()-v.getDfsUsed();
    long expectedLen = blocks[FINALIZED].getNumBytes();
    try {
      v.decDfsUsed(-available);
      blocks[FINALIZED].setNumBytes(expectedLen+100);
      dataSet.append(blocks[FINALIZED], newGS, expectedLen);
      Assert.fail("Should not have space to append to an RWR replica" + blocks[RWR]);
    } catch (DiskOutOfSpaceException e) {
      Assert.assertTrue(e.getMessage().startsWith(
          "Insufficient space for appending to "));
    }
    v.decDfsUsed(available);
    blocks[FINALIZED].setNumBytes(expectedLen);

    newGS = blocks[RBW].getGenerationStamp()+1;
    dataSet.append(blocks[FINALIZED], newGS, 
        blocks[FINALIZED].getNumBytes());  // successful
    blocks[FINALIZED].setGenerationStamp(newGS);
        
    try {
      dataSet.append(blocks[TEMPORARY], blocks[TEMPORARY].getGenerationStamp()+1, 
          blocks[TEMPORARY].getNumBytes());
      Assert.fail("Should not have appended to a temporary replica " 
          + blocks[TEMPORARY]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertEquals(ReplicaNotFoundException.UNFINALIZED_REPLICA +
          blocks[TEMPORARY], e.getMessage());
    }

    try {
      dataSet.append(blocks[RBW], blocks[RBW].getGenerationStamp()+1,
          blocks[RBW].getNumBytes());
      Assert.fail("Should not have appended to an RBW replica" + blocks[RBW]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertEquals(ReplicaNotFoundException.UNFINALIZED_REPLICA +
          blocks[RBW], e.getMessage());
    }

    try {
      dataSet.append(blocks[RWR], blocks[RWR].getGenerationStamp()+1,
          blocks[RBW].getNumBytes());
      Assert.fail("Should not have appended to an RWR replica" + blocks[RWR]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertEquals(ReplicaNotFoundException.UNFINALIZED_REPLICA +
          blocks[RWR], e.getMessage());
    }

    try {
      dataSet.append(blocks[RUR], blocks[RUR].getGenerationStamp()+1,
          blocks[RUR].getNumBytes());
      Assert.fail("Should not have appended to an RUR replica" + blocks[RUR]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertEquals(ReplicaNotFoundException.UNFINALIZED_REPLICA +
          blocks[RUR], e.getMessage());
    }

    try {
      dataSet.append(blocks[NON_EXISTENT], 
          blocks[NON_EXISTENT].getGenerationStamp(), 
          blocks[NON_EXISTENT].getNumBytes());
      Assert.fail("Should not have appended to a non-existent replica " + 
          blocks[NON_EXISTENT]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertEquals(ReplicaNotFoundException.NON_EXISTENT_REPLICA + 
          blocks[NON_EXISTENT], e.getMessage());
    }
    
    newGS = blocks[FINALIZED].getGenerationStamp()+1;
    dataSet.recoverAppend(blocks[FINALIZED], newGS, 
        blocks[FINALIZED].getNumBytes());  // successful
    blocks[FINALIZED].setGenerationStamp(newGS);
    
    try {
      dataSet.recoverAppend(blocks[TEMPORARY], blocks[TEMPORARY].getGenerationStamp()+1, 
          blocks[TEMPORARY].getNumBytes());
      Assert.fail("Should not have appended to a temporary replica " 
          + blocks[TEMPORARY]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertTrue(e.getMessage().startsWith(
          ReplicaNotFoundException.UNFINALIZED_AND_NONRBW_REPLICA));
    }

    newGS = blocks[RBW].getGenerationStamp()+1;
    dataSet.recoverAppend(blocks[RBW], newGS, blocks[RBW].getNumBytes());
    blocks[RBW].setGenerationStamp(newGS);

    try {
      dataSet.recoverAppend(blocks[RWR], blocks[RWR].getGenerationStamp()+1,
          blocks[RBW].getNumBytes());
      Assert.fail("Should not have appended to an RWR replica" + blocks[RWR]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertTrue(e.getMessage().startsWith(
          ReplicaNotFoundException.UNFINALIZED_AND_NONRBW_REPLICA));
    }

    try {
      dataSet.recoverAppend(blocks[RUR], blocks[RUR].getGenerationStamp()+1,
          blocks[RUR].getNumBytes());
      Assert.fail("Should not have appended to an RUR replica" + blocks[RUR]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertTrue(e.getMessage().startsWith(
          ReplicaNotFoundException.UNFINALIZED_AND_NONRBW_REPLICA));
    }

    try {
      dataSet.recoverAppend(blocks[NON_EXISTENT], 
          blocks[NON_EXISTENT].getGenerationStamp(), 
          blocks[NON_EXISTENT].getNumBytes());
      Assert.fail("Should not have appended to a non-existent replica " + 
          blocks[NON_EXISTENT]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertTrue(e.getMessage().startsWith(
          ReplicaNotFoundException.NON_EXISTENT_REPLICA));
    }
  }

  private void testClose(FSDataset dataSet) throws IOException {
    long newGS = blocks[FINALIZED].getGenerationStamp()+1;
    dataSet.recoverClose(blocks[FINALIZED], newGS, 
        blocks[FINALIZED].getNumBytes());  // successful
    blocks[FINALIZED].setGenerationStamp(newGS);
    
    try {
      dataSet.recoverClose(blocks[TEMPORARY], blocks[TEMPORARY].getGenerationStamp()+1, 
          blocks[TEMPORARY].getNumBytes());
      Assert.fail("Should not have recovered close a temporary replica " 
          + blocks[TEMPORARY]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertTrue(e.getMessage().startsWith(
          ReplicaNotFoundException.UNFINALIZED_AND_NONRBW_REPLICA));
    }

    newGS = blocks[RBW].getGenerationStamp()+1;
    dataSet.recoverClose(blocks[RBW], newGS, blocks[RBW].getNumBytes());
    blocks[RBW].setGenerationStamp(newGS);

    try {
      dataSet.recoverClose(blocks[RWR], blocks[RWR].getGenerationStamp()+1,
          blocks[RBW].getNumBytes());
      Assert.fail("Should not have recovered close an RWR replica" + blocks[RWR]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertTrue(e.getMessage().startsWith(
          ReplicaNotFoundException.UNFINALIZED_AND_NONRBW_REPLICA));
    }

    try {
      dataSet.recoverClose(blocks[RUR], blocks[RUR].getGenerationStamp()+1,
          blocks[RUR].getNumBytes());
      Assert.fail("Should not have recovered close an RUR replica" + blocks[RUR]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertTrue(e.getMessage().startsWith(
          ReplicaNotFoundException.UNFINALIZED_AND_NONRBW_REPLICA));
    }

    try {
      dataSet.recoverClose(blocks[NON_EXISTENT], 
          blocks[NON_EXISTENT].getGenerationStamp(), 
          blocks[NON_EXISTENT].getNumBytes());
      Assert.fail("Should not have recovered close a non-existent replica " + 
          blocks[NON_EXISTENT]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertTrue(e.getMessage().startsWith(
          ReplicaNotFoundException.NON_EXISTENT_REPLICA));
    }
  }
  
  private void testWriteToRbw(FSDataset dataSet) throws IOException {
    try {
      dataSet.recoverRbw(blocks[FINALIZED],
          blocks[FINALIZED].getGenerationStamp()+1,
          0L, blocks[FINALIZED].getNumBytes());
      Assert.fail("Should not have recovered a finalized replica " +
          blocks[FINALIZED]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertTrue(e.getMessage().startsWith(
          ReplicaNotFoundException.NON_RBW_REPLICA));
    }
 
    try {
      dataSet.createRbw(blocks[FINALIZED]);
      Assert.fail("Should not have created a replica that's already " +
      		"finalized " + blocks[FINALIZED]);
    } catch (ReplicaAlreadyExistsException e) {
    }
 
    try {
      dataSet.recoverRbw(blocks[TEMPORARY], 
          blocks[TEMPORARY].getGenerationStamp()+1, 
          0L, blocks[TEMPORARY].getNumBytes());
      Assert.fail("Should not have recovered a temporary replica " +
          blocks[TEMPORARY]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertTrue(e.getMessage().startsWith(
          ReplicaNotFoundException.NON_RBW_REPLICA));
    }

    try {
      dataSet.createRbw(blocks[TEMPORARY]);
      Assert.fail("Should not have created a replica that had created as " +
      		"temporary " + blocks[TEMPORARY]);
    } catch (ReplicaAlreadyExistsException e) {
    }
        
    dataSet.recoverRbw(blocks[RBW], blocks[RBW].getGenerationStamp()+1, 
        0L, blocks[RBW].getNumBytes());  // expect to be successful
    
    try {
      dataSet.createRbw(blocks[RBW]);
      Assert.fail("Should not have created a replica that had created as RBW " +
          blocks[RBW]);
    } catch (ReplicaAlreadyExistsException e) {
    }
    
    try {
      dataSet.recoverRbw(blocks[RWR], blocks[RWR].getGenerationStamp()+1,
          0L, blocks[RWR].getNumBytes());
      Assert.fail("Should not have recovered a RWR replica " + blocks[RWR]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertTrue(e.getMessage().startsWith(
          ReplicaNotFoundException.NON_RBW_REPLICA));
    }

    try {
      dataSet.createRbw(blocks[RWR]);
      Assert.fail("Should not have created a replica that was waiting to be " +
      		"recovered " + blocks[RWR]);
    } catch (ReplicaAlreadyExistsException e) {
    }
    
    try {
      dataSet.recoverRbw(blocks[RUR], blocks[RUR].getGenerationStamp()+1,
          0L, blocks[RUR].getNumBytes());
      Assert.fail("Should not have recovered a RUR replica " + blocks[RUR]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertTrue(e.getMessage().startsWith(
          ReplicaNotFoundException.NON_RBW_REPLICA));
    }

    try {
      dataSet.createRbw(blocks[RUR]);
      Assert.fail("Should not have created a replica that was under recovery " +
          blocks[RUR]);
    } catch (ReplicaAlreadyExistsException e) {
    }
    
    try {
      dataSet.recoverRbw(blocks[NON_EXISTENT],
          blocks[NON_EXISTENT].getGenerationStamp()+1,
          0L, blocks[NON_EXISTENT].getNumBytes());
      Assert.fail("Cannot recover a non-existent replica " +
          blocks[NON_EXISTENT]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertTrue(
          e.getMessage().contains(ReplicaNotFoundException.NON_EXISTENT_REPLICA));
    }
    
    dataSet.createRbw(blocks[NON_EXISTENT]);
  }
  
  private void testWriteToTemporary(FSDataset dataSet) throws IOException {
    try {
      dataSet.createTemporary(blocks[FINALIZED]);
      Assert.fail("Should not have created a temporary replica that was " +
      		"finalized " + blocks[FINALIZED]);
    } catch (ReplicaAlreadyExistsException e) {
    }
 
    try {
      dataSet.createTemporary(blocks[TEMPORARY]);
      Assert.fail("Should not have created a replica that had created as" +
      		"temporary " + blocks[TEMPORARY]);
    } catch (ReplicaAlreadyExistsException e) {
    }
    
    try {
      dataSet.createTemporary(blocks[RBW]);
      Assert.fail("Should not have created a replica that had created as RBW " +
          blocks[RBW]);
    } catch (ReplicaAlreadyExistsException e) {
    }
    
    try {
      dataSet.createTemporary(blocks[RWR]);
      Assert.fail("Should not have created a replica that was waiting to be " +
      		"recovered " + blocks[RWR]);
    } catch (ReplicaAlreadyExistsException e) {
    }
    
    try {
      dataSet.createTemporary(blocks[RUR]);
      Assert.fail("Should not have created a replica that was under recovery " +
          blocks[RUR]);
    } catch (ReplicaAlreadyExistsException e) {
    }
    
    dataSet.createTemporary(blocks[NON_EXISTENT]);
  }
}
