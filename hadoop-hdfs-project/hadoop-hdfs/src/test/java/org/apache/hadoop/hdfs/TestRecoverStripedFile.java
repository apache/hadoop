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
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.ErasureCodingPolicyManager;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.BlockECRecoveryCommand.BlockECRecoveryInfo;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestRecoverStripedFile {
  public static final Log LOG = LogFactory.getLog(TestRecoverStripedFile.class);
  
  private static final int dataBlkNum = StripedFileTestUtil.NUM_DATA_BLOCKS;
  private static final int parityBlkNum = StripedFileTestUtil.NUM_PARITY_BLOCKS;
  private static final int cellSize = StripedFileTestUtil.BLOCK_STRIPED_CELL_SIZE;
  private static final int blockSize = cellSize * 3;
  private static final int groupSize = dataBlkNum + parityBlkNum;
  private static final int dnNum = groupSize + parityBlkNum;

  static {
    GenericTestUtils.setLogLevel(DFSClient.LOG, Level.ALL);
  }

  private MiniDFSCluster cluster;
  private Configuration conf;
  private DistributedFileSystem fs;
  // Map: DatanodeID -> datanode index in cluster
  private Map<DatanodeID, Integer> dnMap = new HashMap<DatanodeID, Integer>();

  @Before
  public void setup() throws IOException {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_STRIPED_READ_BUFFER_SIZE_KEY,
        cellSize - 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY,
        false);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(dnNum).build();
    cluster.waitActive();
    
    fs = cluster.getFileSystem();
    fs.getClient().setErasureCodingPolicy("/", null);

    List<DataNode> datanodes = cluster.getDataNodes();
    for (int i = 0; i < dnNum; i++) {
      dnMap.put(datanodes.get(i).getDatanodeId(), i);
    }
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  @Test(timeout = 120000)
  public void testRecoverOneParityBlock() throws Exception {
    int fileLen = 10 * blockSize + blockSize/10;
    assertFileBlocksRecovery("/testRecoverOneParityBlock", fileLen, 0, 1);
  }
  
  @Test(timeout = 120000)
  public void testRecoverOneParityBlock1() throws Exception {
    int fileLen = cellSize + cellSize/10;
    assertFileBlocksRecovery("/testRecoverOneParityBlock1", fileLen, 0, 1);
  }
  
  @Test(timeout = 120000)
  public void testRecoverOneParityBlock2() throws Exception {
    int fileLen = 1;
    assertFileBlocksRecovery("/testRecoverOneParityBlock2", fileLen, 0, 1);
  }
  
  @Test(timeout = 120000)
  public void testRecoverOneParityBlock3() throws Exception {
    int fileLen = 3 * blockSize + blockSize/10;
    assertFileBlocksRecovery("/testRecoverOneParityBlock3", fileLen, 0, 1);
  }
  
  @Test(timeout = 120000)
  public void testRecoverThreeParityBlocks() throws Exception {
    int fileLen = 10 * blockSize + blockSize/10;
    assertFileBlocksRecovery("/testRecoverThreeParityBlocks", fileLen, 0, 3);
  }
  
  @Test(timeout = 120000)
  public void testRecoverThreeDataBlocks() throws Exception {
    int fileLen = 10 * blockSize + blockSize/10;
    assertFileBlocksRecovery("/testRecoverThreeDataBlocks", fileLen, 1, 3);
  }
  
  @Test(timeout = 120000)
  public void testRecoverThreeDataBlocks1() throws Exception {
    int fileLen = 3 * blockSize + blockSize/10;
    assertFileBlocksRecovery("/testRecoverThreeDataBlocks1", fileLen, 1, 3);
  }
  
  @Test(timeout = 120000)
  public void testRecoverOneDataBlock() throws Exception {
    int fileLen = 10 * blockSize + blockSize/10;
    assertFileBlocksRecovery("/testRecoverOneDataBlock", fileLen, 1, 1);
  }
  
  @Test(timeout = 120000)
  public void testRecoverOneDataBlock1() throws Exception {
    int fileLen = cellSize + cellSize/10;
    assertFileBlocksRecovery("/testRecoverOneDataBlock1", fileLen, 1, 1);
  }
  
  @Test(timeout = 120000)
  public void testRecoverOneDataBlock2() throws Exception {
    int fileLen = 1;
    assertFileBlocksRecovery("/testRecoverOneDataBlock2", fileLen, 1, 1);
  }
  
  @Test(timeout = 120000)
  public void testRecoverAnyBlocks() throws Exception {
    int fileLen = 3 * blockSize + blockSize/10;
    assertFileBlocksRecovery("/testRecoverAnyBlocks", fileLen, 2, 2);
  }
  
  @Test(timeout = 120000)
  public void testRecoverAnyBlocks1() throws Exception {
    int fileLen = 10 * blockSize + blockSize/10;
    assertFileBlocksRecovery("/testRecoverAnyBlocks1", fileLen, 2, 3);
  }
  
  /**
   * Test the file blocks recovery.
   * 1. Check the replica is recovered in the target datanode, 
   *    and verify the block replica length, generationStamp and content.
   * 2. Read the file and verify content. 
   */
  private void assertFileBlocksRecovery(String fileName, int fileLen,
      int recovery, int toRecoverBlockNum) throws Exception {
    if (recovery != 0 && recovery != 1 && recovery != 2) {
      Assert.fail("Invalid recovery: 0 is to recovery parity blocks,"
          + "1 is to recovery data blocks, 2 is any.");
    }
    if (toRecoverBlockNum < 1 || toRecoverBlockNum > parityBlkNum) {
      Assert.fail("toRecoverBlockNum should be between 1 ~ " + parityBlkNum);
    }
    
    Path file = new Path(fileName);

    final byte[] data = new byte[fileLen];
    ThreadLocalRandom.current().nextBytes(data);
    DFSTestUtil.writeFile(fs, file, data);
    StripedFileTestUtil.waitBlockGroupsReported(fs, fileName);

    LocatedBlocks locatedBlocks = getLocatedBlocks(file);
    assertEquals(locatedBlocks.getFileLength(), fileLen);
    
    LocatedStripedBlock lastBlock = 
        (LocatedStripedBlock)locatedBlocks.getLastLocatedBlock();
    
    DatanodeInfo[] storageInfos = lastBlock.getLocations();
    int[] indices = lastBlock.getBlockIndices();
    
    BitSet bitset = new BitSet(dnNum);
    for (DatanodeInfo storageInfo : storageInfos) {
      bitset.set(dnMap.get(storageInfo));
    }
    
    int[] toDead = new int[toRecoverBlockNum];
    int n = 0;
    for (int i = 0; i < indices.length; i++) {
      if (n < toRecoverBlockNum) {
        if (recovery == 0) {
          if (indices[i] >= dataBlkNum) {
            toDead[n++] = i;
          }
        } else if (recovery == 1) {
          if (indices[i] < dataBlkNum) {
            toDead[n++] = i;
          }
        } else {
          toDead[n++] = i;
        }
      } else {
        break;
      }
    }
    
    DatanodeInfo[] dataDNs = new DatanodeInfo[toRecoverBlockNum];
    int[] deadDnIndices = new int[toRecoverBlockNum];
    ExtendedBlock[] blocks = new ExtendedBlock[toRecoverBlockNum];
    File[] replicas = new File[toRecoverBlockNum];
    File[] metadatas = new File[toRecoverBlockNum];
    byte[][] replicaContents = new byte[toRecoverBlockNum][];
    for (int i = 0; i < toRecoverBlockNum; i++) {
      dataDNs[i] = storageInfos[toDead[i]];
      deadDnIndices[i] = dnMap.get(dataDNs[i]);
      
      // Check the block replica file on deadDn before it dead.
      blocks[i] = StripedBlockUtil.constructInternalBlock(
          lastBlock.getBlock(), cellSize, dataBlkNum, indices[toDead[i]]);
      replicas[i] = cluster.getBlockFile(deadDnIndices[i], blocks[i]);
      metadatas[i] = cluster.getBlockMetadataFile(deadDnIndices[i], blocks[i]);
      // the block replica on the datanode should be the same as expected
      assertEquals(replicas[i].length(), 
          StripedBlockUtil.getInternalBlockLength(
          lastBlock.getBlockSize(), cellSize, dataBlkNum, indices[toDead[i]]));
      assertTrue(metadatas[i].getName().
          endsWith(blocks[i].getGenerationStamp() + ".meta"));
      replicaContents[i] = DFSTestUtil.readFileAsBytes(replicas[i]);
    }
    
    int cellsNum = (fileLen - 1) / cellSize + 1;
    int groupSize = Math.min(cellsNum, dataBlkNum) + parityBlkNum;

    for (int i = 0; i < toRecoverBlockNum; i++) {
      /*
       * Kill the datanode which contains one replica
       * We need to make sure it dead in namenode: clear its update time and
       * trigger NN to check heartbeat.
       */
      DataNode dn = cluster.getDataNodes().get(deadDnIndices[i]);
      dn.shutdown();
      cluster.setDataNodeDead(dn.getDatanodeId());
    }

    // Check the locatedBlocks of the file again
    locatedBlocks = getLocatedBlocks(file);
    lastBlock = (LocatedStripedBlock)locatedBlocks.getLastLocatedBlock();
    storageInfos = lastBlock.getLocations();
    assertEquals(storageInfos.length, groupSize - toRecoverBlockNum);

    int[] targetDNs = new int[dnNum - groupSize];
    n = 0;
    for (int i = 0; i < dnNum; i++) {
      if (!bitset.get(i)) { // not contain replica of the block.
        targetDNs[n++] = i;
      }
    }
    
    waitForRecoveryFinished(file, groupSize);

    targetDNs = sortTargetsByReplicas(blocks, targetDNs);

    // Check the replica on the new target node.
    for (int i = 0; i < toRecoverBlockNum; i++) {
      File replicaAfterRecovery = cluster.getBlockFile(targetDNs[i], blocks[i]);
      File metadataAfterRecovery =
          cluster.getBlockMetadataFile(targetDNs[i], blocks[i]);
      assertEquals(replicaAfterRecovery.length(), replicas[i].length());
      assertTrue(metadataAfterRecovery.getName().
          endsWith(blocks[i].getGenerationStamp() + ".meta"));
      byte[] replicaContentAfterRecovery =
          DFSTestUtil.readFileAsBytes(replicaAfterRecovery);

      Assert.assertArrayEquals(replicaContents[i], replicaContentAfterRecovery);
    }
  }

  private int[] sortTargetsByReplicas(ExtendedBlock[] blocks, int[] targetDNs) {
    int[] result = new int[blocks.length];
    for (int i = 0; i < blocks.length; i++) {
      result[i] = -1;
      for (int j = 0; j < targetDNs.length; j++) {
        if (targetDNs[j] != -1) {
          File replica = cluster.getBlockFile(targetDNs[j], blocks[i]);
          if (replica != null) {
            result[i] = targetDNs[j];
            targetDNs[j] = -1;
            break;
          }
        }
      }
      if (result[i] == -1) {
        Assert.fail("Failed to recover striped block: " + blocks[i].getBlockId());
      }
    }
    return result;
  }

  private LocatedBlocks waitForRecoveryFinished(Path file, int groupSize) 
      throws Exception {
    final int ATTEMPTS = 60;
    for (int i = 0; i < ATTEMPTS; i++) {
      LocatedBlocks locatedBlocks = getLocatedBlocks(file);
      LocatedStripedBlock lastBlock = 
          (LocatedStripedBlock)locatedBlocks.getLastLocatedBlock();
      DatanodeInfo[] storageInfos = lastBlock.getLocations();
      if (storageInfos.length >= groupSize) {
        return locatedBlocks;
      }
      Thread.sleep(1000);
    }
    throw new IOException ("Time out waiting for EC block recovery.");
  }
  
  private LocatedBlocks getLocatedBlocks(Path file) throws IOException {
    return fs.getClient().getLocatedBlocks(file.toString(), 0, Long.MAX_VALUE);
  }

  /*
   * Tests that processErasureCodingTasks should not throw exceptions out due to
   * invalid ECTask submission.
   */
  @Test
  public void testProcessErasureCodingTasksSubmitionShouldSucceed()
      throws Exception {
    DataNode dataNode = cluster.dataNodes.get(0).datanode;

    // Pack invalid(dummy) parameters in ecTasks. Irrespective of parameters, each task
    // thread pool submission should succeed, so that it will not prevent
    // processing other tasks in the list if any exceptions.
    int size = cluster.dataNodes.size();
    short[] liveIndices = new short[size];
    DatanodeInfo[] dataDNs = new DatanodeInfo[size + 1];
    DatanodeStorageInfo targetDnInfos_1 = BlockManagerTestUtil
        .newDatanodeStorageInfo(DFSTestUtil.getLocalDatanodeDescriptor(),
            new DatanodeStorage("s01"));
    DatanodeStorageInfo[] dnStorageInfo = new DatanodeStorageInfo[] {
        targetDnInfos_1 };

    BlockECRecoveryInfo invalidECInfo = new BlockECRecoveryInfo(
        new ExtendedBlock("bp-id", 123456), dataDNs, dnStorageInfo, liveIndices,
        ErasureCodingPolicyManager.getSystemDefaultPolicy());
    List<BlockECRecoveryInfo> ecTasks = new ArrayList<BlockECRecoveryInfo>();
    ecTasks.add(invalidECInfo);
    dataNode.getErasureCodingWorker().processErasureCodingTasks(ecTasks);
  }
}
