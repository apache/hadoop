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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.FsDatasetTestUtils;
import org.apache.hadoop.hdfs.server.datanode.ReplicaAlreadyExistsException;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipeline;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.junit.Assert;
import org.junit.Test;

/** Test if FSDataset#append, writeToRbw, and writeToTmp */
public class TestWriteToReplica {

  final private static int FINALIZED = 0;
  final private static int TEMPORARY = 1;
  final private static int RBW = 2;
  final private static int RWR = 3;
  final private static int RUR = 4;
  final private static int NON_EXISTENT = 5;
  
  // test close
  @Test
  public void testClose() throws Exception {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(new HdfsConfiguration()).build();
    
    try {
      cluster.waitActive();
      DataNode dn = cluster.getDataNodes().get(0);
      FsDatasetSpi<?> dataSet = DataNodeTestUtils.getFSDataset(dn);

      // set up replicasMap
      String bpid = cluster.getNamesystem().getBlockPoolId();
      
      ExtendedBlock[] blocks = setup(bpid, cluster.getFsDatasetTestUtils(dn));

      // test close
      testClose(dataSet, blocks);
    } finally {
      cluster.shutdown();
    }
  }

  // test append
  @Test
  public void testAppend() throws Exception {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(new HdfsConfiguration()).build();
    try {
      cluster.waitActive();
      DataNode dn = cluster.getDataNodes().get(0);
      FsDatasetSpi<?> dataSet = DataNodeTestUtils.getFSDataset(dn);

      // set up replicasMap
      String bpid = cluster.getNamesystem().getBlockPoolId();
      ExtendedBlock[] blocks = setup(bpid, cluster.getFsDatasetTestUtils(dn));

      // test append
      testAppend(bpid, dataSet, blocks);
    } finally {
      cluster.shutdown();
    }
  }

  // test writeToRbw
  @Test
  public void testWriteToRbw() throws Exception {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(new HdfsConfiguration()).build();
    try {
      cluster.waitActive();
      DataNode dn = cluster.getDataNodes().get(0);
      FsDatasetImpl dataSet = (FsDatasetImpl)DataNodeTestUtils.getFSDataset(dn);

      // set up replicasMap
      String bpid = cluster.getNamesystem().getBlockPoolId();
      ExtendedBlock[] blocks = setup(bpid, cluster.getFsDatasetTestUtils(dn));

      // test writeToRbw
      testWriteToRbw(dataSet, blocks);
    } finally {
      cluster.shutdown();
    }
  }
  
  // test writeToTemporary
  @Test
  public void testWriteToTemporary() throws Exception {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(new HdfsConfiguration()).build();
    try {
      cluster.waitActive();
      DataNode dn = cluster.getDataNodes().get(0);
      FsDatasetImpl dataSet = (FsDatasetImpl)DataNodeTestUtils.getFSDataset(dn);

      // set up replicasMap
      String bpid = cluster.getNamesystem().getBlockPoolId();
      ExtendedBlock[] blocks = setup(bpid, cluster.getFsDatasetTestUtils(dn));

      // test writeToTemporary
      testWriteToTemporary(dataSet, blocks);
    } finally {
      cluster.shutdown();
    }
  }
  
  /**
   * Generate testing environment and return a collection of blocks
   * on which to run the tests.
   * 
   * @param bpid Block pool ID to generate blocks for
   * @param testUtils FsDatasetTestUtils provides white box access to FsDataset.
   * @return Contrived blocks for further testing.
   * @throws IOException
   */
  private ExtendedBlock[] setup(String bpid, FsDatasetTestUtils testUtils)
      throws IOException {
    // setup replicas map
    
    ExtendedBlock[] blocks = new ExtendedBlock[] {
        new ExtendedBlock(bpid, 1, 1, 2001), new ExtendedBlock(bpid, 2, 1, 2002), 
        new ExtendedBlock(bpid, 3, 1, 2003), new ExtendedBlock(bpid, 4, 1, 2004),
        new ExtendedBlock(bpid, 5, 1, 2005), new ExtendedBlock(bpid, 6, 1, 2006)
    };

    testUtils.createFinalizedReplica(blocks[FINALIZED]);
    testUtils.createReplicaInPipeline(blocks[TEMPORARY]);
    testUtils.createRBW(blocks[RBW]);
    testUtils.createReplicaWaitingToBeRecovered(blocks[RWR]);
    testUtils.createReplicaUnderRecovery(blocks[RUR], 2007);

    return blocks;
  }
  
  private void testAppend(String bpid, FsDatasetSpi<?> dataSet,
                          ExtendedBlock[] blocks) throws IOException {
    long newGS = blocks[FINALIZED].getGenerationStamp()+1;
    final FsVolumeSpi v = dataSet.getVolume(blocks[FINALIZED]);
    if (v instanceof FsVolumeImpl) {
      FsVolumeImpl fvi = (FsVolumeImpl) v;
      long available = fvi.getCapacity() - fvi.getDfsUsed();
      long expectedLen = blocks[FINALIZED].getNumBytes();
      try {
        fvi.onBlockFileDeletion(bpid, -available);
        blocks[FINALIZED].setNumBytes(expectedLen + 100);
        dataSet.append(blocks[FINALIZED], newGS, expectedLen);
        Assert.fail("Should not have space to append to an RWR replica" + blocks[RWR]);
      } catch (DiskOutOfSpaceException e) {
        Assert.assertTrue(e.getMessage().startsWith(
            "Insufficient space for appending to "));
      }
      fvi.onBlockFileDeletion(bpid, available);
      blocks[FINALIZED].setNumBytes(expectedLen);
    }

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

  private void testClose(FsDatasetSpi<?> dataSet, ExtendedBlock [] blocks) throws IOException {
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
  
  private void testWriteToRbw(FsDatasetImpl dataSet, ExtendedBlock[] blocks) throws IOException {
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
      dataSet.createRbw(StorageType.DEFAULT, blocks[FINALIZED], false);
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
      dataSet.createRbw(StorageType.DEFAULT, blocks[TEMPORARY], false);
      Assert.fail("Should not have created a replica that had created as " +
      		"temporary " + blocks[TEMPORARY]);
    } catch (ReplicaAlreadyExistsException e) {
    }
        
    dataSet.recoverRbw(blocks[RBW], blocks[RBW].getGenerationStamp()+1, 
        0L, blocks[RBW].getNumBytes());  // expect to be successful
    
    try {
      dataSet.createRbw(StorageType.DEFAULT, blocks[RBW], false);
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
      dataSet.createRbw(StorageType.DEFAULT, blocks[RWR], false);
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
      dataSet.createRbw(StorageType.DEFAULT, blocks[RUR], false);
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
    
    dataSet.createRbw(StorageType.DEFAULT, blocks[NON_EXISTENT], false);
  }
  
  private void testWriteToTemporary(FsDatasetImpl dataSet, ExtendedBlock[] blocks) throws IOException {
    try {
      dataSet.createTemporary(StorageType.DEFAULT, blocks[FINALIZED]);
      Assert.fail("Should not have created a temporary replica that was " +
      		"finalized " + blocks[FINALIZED]);
    } catch (ReplicaAlreadyExistsException e) {
    }
 
    try {
      dataSet.createTemporary(StorageType.DEFAULT, blocks[TEMPORARY]);
      Assert.fail("Should not have created a replica that had created as" +
      		"temporary " + blocks[TEMPORARY]);
    } catch (ReplicaAlreadyExistsException e) {
    }
    
    try {
      dataSet.createTemporary(StorageType.DEFAULT, blocks[RBW]);
      Assert.fail("Should not have created a replica that had created as RBW " +
          blocks[RBW]);
    } catch (ReplicaAlreadyExistsException e) {
    }
    
    try {
      dataSet.createTemporary(StorageType.DEFAULT, blocks[RWR]);
      Assert.fail("Should not have created a replica that was waiting to be " +
      		"recovered " + blocks[RWR]);
    } catch (ReplicaAlreadyExistsException e) {
    }
    
    try {
      dataSet.createTemporary(StorageType.DEFAULT, blocks[RUR]);
      Assert.fail("Should not have created a replica that was under recovery " +
          blocks[RUR]);
    } catch (ReplicaAlreadyExistsException e) {
    }
    
    dataSet.createTemporary(StorageType.DEFAULT, blocks[NON_EXISTENT]);

    try {
      dataSet.createTemporary(StorageType.DEFAULT, blocks[NON_EXISTENT]);
      Assert.fail("Should not have created a replica that had already been "
          + "created " + blocks[NON_EXISTENT]);
    } catch (Exception e) {
      Assert.assertTrue(
          e.getMessage().contains(blocks[NON_EXISTENT].getBlockName()));
      Assert.assertTrue(e instanceof ReplicaAlreadyExistsException);
    }

    long newGenStamp = blocks[NON_EXISTENT].getGenerationStamp() * 10;
    blocks[NON_EXISTENT].setGenerationStamp(newGenStamp);
    try {
      ReplicaInPipeline replicaInfo =
          dataSet.createTemporary(StorageType.DEFAULT, blocks[NON_EXISTENT]).getReplica();
      Assert.assertTrue(replicaInfo.getGenerationStamp() == newGenStamp);
      Assert.assertTrue(
          replicaInfo.getBlockId() == blocks[NON_EXISTENT].getBlockId());
    } catch (ReplicaAlreadyExistsException e) {
      Assert.fail("createTemporary should have allowed the block with newer "
          + " generation stamp to be created " + blocks[NON_EXISTENT]);
    }
  }
  
  /**
   * This is a test to check the replica map before and after the datanode 
   * quick restart (less than 5 minutes)
   * @throws Exception
   */
  @Test
  public  void testReplicaMapAfterDatanodeRestart() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(2))
        .build();
    try {
      cluster.waitActive();
      NameNode nn1 = cluster.getNameNode(0);
      NameNode nn2 = cluster.getNameNode(1);
      assertNotNull("cannot create nn1", nn1);
      assertNotNull("cannot create nn2", nn2);
      
      // check number of volumes in fsdataset
      DataNode dn = cluster.getDataNodes().get(0);
      FsDatasetImpl dataSet = (FsDatasetImpl)DataNodeTestUtils.
          getFSDataset(dn);
      
      List<FsVolumeSpi> volumes = null;
      try (FsDatasetSpi.FsVolumeReferences referredVols = dataSet.getFsVolumeReferences()) {
        // number of volumes should be 2 - [data1, data2]
        assertEquals("number of volumes is wrong", 2, referredVols.size());
        volumes = new ArrayList<>(referredVols.size());
        for (FsVolumeSpi vol : referredVols) {
          volumes.add(vol);
        }
      }
      ArrayList<String> bpList = new ArrayList<>(Arrays.asList(
          cluster.getNamesystem(0).getBlockPoolId(),
          cluster.getNamesystem(1).getBlockPoolId()));
      
      Assert.assertTrue("Cluster should have 2 block pools", 
          bpList.size() == 2);
      
      createReplicas(bpList, volumes, cluster.getFsDatasetTestUtils(dn));
      ReplicaMap oldReplicaMap = new ReplicaMap(new AutoCloseableLock());
      oldReplicaMap.addAll(dataSet.volumeMap);

      cluster.restartDataNode(0);
      cluster.waitActive();
      dn = cluster.getDataNodes().get(0);
      dataSet = (FsDatasetImpl) dn.getFSDataset();
      testEqualityOfReplicaMap(oldReplicaMap, dataSet.volumeMap, bpList);
    } finally {
      cluster.shutdown();
    }
  }
  
  /**
   * Compare the replica map before and after the restart
   **/
  private void testEqualityOfReplicaMap(ReplicaMap oldReplicaMap, ReplicaMap 
      newReplicaMap, List<String> bpidList) {
    // Traversing through newReplica map and remove the corresponding 
    // replicaInfo from oldReplicaMap.
    for (String bpid: bpidList) {
      for (ReplicaInfo info: newReplicaMap.replicas(bpid)) {
        assertNotNull("Volume map before restart didn't contain the "
            + "blockpool: " + bpid, oldReplicaMap.replicas(bpid));
        
        ReplicaInfo oldReplicaInfo = oldReplicaMap.get(bpid, 
            info.getBlockId());
        // Volume map after restart contains a blockpool id which 
        assertNotNull("Old Replica Map didnt't contain block with blockId: " +
            info.getBlockId(), oldReplicaInfo);
        
        ReplicaState oldState = oldReplicaInfo.getState();
        // Since after restart, all the RWR, RBW and RUR blocks gets 
        // converted to RWR
        if (info.getState() == ReplicaState.RWR) {
           if (oldState == ReplicaState.RWR || oldState == ReplicaState.RBW 
               || oldState == ReplicaState.RUR) {
             oldReplicaMap.remove(bpid, oldReplicaInfo);
           }
        } else if (info.getState() == ReplicaState.FINALIZED && 
            oldState == ReplicaState.FINALIZED) {
          oldReplicaMap.remove(bpid, oldReplicaInfo);
        }
      }
    }
    
    // We don't persist the ReplicaInPipeline replica
    // and if the old replica map contains any replica except ReplicaInPipeline
    // then we didn't persist that replica
    for (String bpid: bpidList) {
      for (ReplicaInfo replicaInfo: oldReplicaMap.replicas(bpid)) {
        if (replicaInfo.getState() != ReplicaState.TEMPORARY) {
          Assert.fail("After datanode restart we lost the block with blockId: "
              +  replicaInfo.getBlockId());
        }
      }
    }
  }

  private void createReplicas(List<String> bpList, List<FsVolumeSpi> volumes,
                              FsDatasetTestUtils testUtils) throws IOException {
    // Here we create all different type of replicas and add it
    // to volume map. 
    // Created all type of ReplicaInfo, each under Blkpool corresponding volume
    long id = 1; // This variable is used as both blockId and genStamp
    for (String bpId: bpList) {
      for (FsVolumeSpi volume: volumes) {
        ExtendedBlock eb = new ExtendedBlock(bpId, id, 1, id);
        testUtils.createFinalizedReplica(volume, eb);
        id++;

        eb = new ExtendedBlock(bpId, id, 1, id);
        testUtils.createRBW(volume, eb);
        id++;

        eb = new ExtendedBlock(bpId, id, 1, id);
        testUtils.createReplicaWaitingToBeRecovered(volume, eb);
        id++;

        eb = new ExtendedBlock(bpId, id, 1, id);
        testUtils.createReplicaInPipeline(volume, eb);
        id++;
      }
    }
  }
}
