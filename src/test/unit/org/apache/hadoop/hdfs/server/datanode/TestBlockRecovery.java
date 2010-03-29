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
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNode.BlockRecord;
import org.apache.hadoop.hdfs.server.datanode.FSDatasetInterface.BlockWriteStreams;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * This tests if sync all replicas in block recovery works correctly
 */
public class TestBlockRecovery {
  private static final Log LOG = LogFactory.getLog(TestBlockRecovery.class);
  private static final String DATA_DIR =
    MiniDFSCluster.getBaseDirectory() + "data";
  private DataNode dn;
  private Configuration conf;
  private final static long RECOVERY_ID = 3000L;
  private final static long BLOCK_ID = 1000L;
  private final static long GEN_STAMP = 2000L;
  private final static long BLOCK_LEN = 3000L;
  private final static long REPLICA_LEN1 = 6000L;
  private final static long REPLICA_LEN2 = 5000L;
  private final static Block block = new Block(BLOCK_ID, BLOCK_LEN, GEN_STAMP);

  static {
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LOG).getLogger().setLevel(Level.ALL);
  }

  /**
   * Starts an instance of DataNode
   * @throws IOException
   */
  @Before
  public void startUp() throws IOException {
    conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, DATA_DIR);
    FileSystem.setDefaultUri(conf, "hdfs://localhost:5020");
    ArrayList<File> dirs = new ArrayList<File>();
    File dataDir = new File(DATA_DIR);
    FileUtil.fullyDelete(dataDir);
    dataDir.mkdirs();
    dirs.add(dataDir);
    DatanodeProtocol namenode = mock(DatanodeProtocol.class);
    when(namenode.versionRequest()).thenReturn(new NamespaceInfo(1, 1L, 1));
    when(namenode.sendHeartbeat(any(DatanodeRegistration.class), anyLong(), 
        anyLong(), anyLong(), anyInt(), anyInt())).thenReturn(
            new DatanodeCommand[0]);
    dn = new DataNode(conf, dirs, namenode);
  }

  /**
   * Cleans the resources and closes the instance of datanode
   * @throws IOException if an error occurred
   */
  @After
  public void tearDown() throws IOException {
    if (dn != null) {
      try {
        dn.shutdown();
      } catch(Exception e) {
        LOG.error("Cannot close: ", e);
      } finally {
        File dir = new File(DATA_DIR);
        if (dir.exists())
          Assert.assertTrue(
              "Cannot delete data-node dirs", FileUtil.fullyDelete(dir));
      }
    }
  }

  /** Sync two replicas */
  private void testSyncReplicas(ReplicaRecoveryInfo replica1, 
      ReplicaRecoveryInfo replica2,
      InterDatanodeProtocol dn1,
      InterDatanodeProtocol dn2) throws IOException {
    
    DatanodeInfo[] locs = new DatanodeInfo[]{
        mock(DatanodeInfo.class), mock(DatanodeInfo.class)};
    RecoveringBlock rBlock = new RecoveringBlock(block, 
        locs, RECOVERY_ID);
    ArrayList<BlockRecord> syncList = new ArrayList<BlockRecord>(2);
    BlockRecord record1 = new BlockRecord(
        new DatanodeID("xx", "yy", 44, 55), dn1, replica1);
    BlockRecord record2 = new BlockRecord(
        new DatanodeID("aa", "bb", 11, 22), dn2, replica2);
    syncList.add(record1);
    syncList.add(record2);
    dn.syncBlock(rBlock, syncList);
  }
  
  /**
   * BlockRecovery_02.8.
   * Two replicas are in Finalized state
   * @throws IOException in case of an error
   */
  @Test
  public void testFinalizedReplicas () throws IOException {
    LOG.debug("Running " + GenericTestUtils.getMethodName());
    ReplicaRecoveryInfo replica1 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN1, GEN_STAMP-1, ReplicaState.FINALIZED);
    ReplicaRecoveryInfo replica2 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN1, GEN_STAMP-2, ReplicaState.FINALIZED);

    InterDatanodeProtocol dn1 = mock(InterDatanodeProtocol.class);
    InterDatanodeProtocol dn2 = mock(InterDatanodeProtocol.class);

    testSyncReplicas(replica1, replica2, dn1, dn2);
    verify(dn1).updateReplicaUnderRecovery(block, RECOVERY_ID, REPLICA_LEN1);
    verify(dn2).updateReplicaUnderRecovery(block, RECOVERY_ID, REPLICA_LEN1);    

    // two finalized replicas have different length
    replica1 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN1, GEN_STAMP-1, ReplicaState.FINALIZED);
    replica2 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN2, GEN_STAMP-2, ReplicaState.FINALIZED);

    try {
      testSyncReplicas(replica1, replica2, dn1, dn2);
      Assert.fail("Two finalized replicas should not have different lengthes!");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().startsWith(
          "Inconsistent size of finalized replicas. "));
    }
  }
  
  /**
   * BlockRecovery_02.9.
   * One replica is Finalized and another is RBW. 
   * @throws IOException in case of an error
   */
  @Test
  public void testFinalizedRbwReplicas() throws IOException {
    LOG.debug("Running " + GenericTestUtils.getMethodName());
    
    // rbw and finalized replicas have the same length
    ReplicaRecoveryInfo replica1 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN1, GEN_STAMP-1, ReplicaState.FINALIZED);
    ReplicaRecoveryInfo replica2 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN1, GEN_STAMP-2, ReplicaState.RBW);

    InterDatanodeProtocol dn1 = mock(InterDatanodeProtocol.class);
    InterDatanodeProtocol dn2 = mock(InterDatanodeProtocol.class);

    testSyncReplicas(replica1, replica2, dn1, dn2);
    verify(dn1).updateReplicaUnderRecovery(block, RECOVERY_ID, REPLICA_LEN1);
    verify(dn2).updateReplicaUnderRecovery(block, RECOVERY_ID, REPLICA_LEN1);
    
    // rbw replica has a different length from the finalized one
    replica1 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN1, GEN_STAMP-1, ReplicaState.FINALIZED);
    replica2 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN2, GEN_STAMP-2, ReplicaState.RBW);

    dn1 = mock(InterDatanodeProtocol.class);
    dn2 = mock(InterDatanodeProtocol.class);

    testSyncReplicas(replica1, replica2, dn1, dn2);
    verify(dn1).updateReplicaUnderRecovery(block, RECOVERY_ID, REPLICA_LEN1);
    verify(dn2, never()).updateReplicaUnderRecovery(
        block, RECOVERY_ID, REPLICA_LEN1);
  }
  
  /**
   * BlockRecovery_02.10.
   * One replica is Finalized and another is RWR. 
   * @throws IOException in case of an error
   */
  @Test
  public void testFinalizedRwrReplicas() throws IOException {
    LOG.debug("Running " + GenericTestUtils.getMethodName());
    
    // rbw and finalized replicas have the same length
    ReplicaRecoveryInfo replica1 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN1, GEN_STAMP-1, ReplicaState.FINALIZED);
    ReplicaRecoveryInfo replica2 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN1, GEN_STAMP-2, ReplicaState.RWR);

    InterDatanodeProtocol dn1 = mock(InterDatanodeProtocol.class);
    InterDatanodeProtocol dn2 = mock(InterDatanodeProtocol.class);

    testSyncReplicas(replica1, replica2, dn1, dn2);
    verify(dn1).updateReplicaUnderRecovery(block, RECOVERY_ID, REPLICA_LEN1);
    verify(dn2, never()).updateReplicaUnderRecovery(
        block, RECOVERY_ID, REPLICA_LEN1);
    
    // rbw replica has a different length from the finalized one
    replica1 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN1, GEN_STAMP-1, ReplicaState.FINALIZED);
    replica2 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN2, GEN_STAMP-2, ReplicaState.RBW);

    dn1 = mock(InterDatanodeProtocol.class);
    dn2 = mock(InterDatanodeProtocol.class);

    testSyncReplicas(replica1, replica2, dn1, dn2);
    verify(dn1).updateReplicaUnderRecovery(block, RECOVERY_ID, REPLICA_LEN1);
    verify(dn2, never()).updateReplicaUnderRecovery(
        block, RECOVERY_ID, REPLICA_LEN1);
  }
  
  /**
   * BlockRecovery_02.11.
   * Two replicas are RBW.
   * @throws IOException in case of an error
   */
  @Test
  public void testRBWReplicas() throws IOException {
    LOG.debug("Running " + GenericTestUtils.getMethodName());
    ReplicaRecoveryInfo replica1 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN1, GEN_STAMP-1, ReplicaState.RBW);
    ReplicaRecoveryInfo replica2 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN2, GEN_STAMP-2, ReplicaState.RBW);

    InterDatanodeProtocol dn1 = mock(InterDatanodeProtocol.class);
    InterDatanodeProtocol dn2 = mock(InterDatanodeProtocol.class);

    testSyncReplicas(replica1, replica2, dn1, dn2);
    long minLen = Math.min(REPLICA_LEN1, REPLICA_LEN2);
    verify(dn1).updateReplicaUnderRecovery(block, RECOVERY_ID, minLen);
    verify(dn2).updateReplicaUnderRecovery(block, RECOVERY_ID, minLen);    
  }
  
  /**
   * BlockRecovery_02.12.
   * One replica is RBW and another is RWR. 
   * @throws IOException in case of an error
   */
  @Test
  public void testRBW_RWRReplicas() throws IOException {
    LOG.debug("Running " + GenericTestUtils.getMethodName());
    ReplicaRecoveryInfo replica1 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN1, GEN_STAMP-1, ReplicaState.RBW);
    ReplicaRecoveryInfo replica2 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN1, GEN_STAMP-2, ReplicaState.RWR);

    InterDatanodeProtocol dn1 = mock(InterDatanodeProtocol.class);
    InterDatanodeProtocol dn2 = mock(InterDatanodeProtocol.class);

    testSyncReplicas(replica1, replica2, dn1, dn2);
    verify(dn1).updateReplicaUnderRecovery(block, RECOVERY_ID, REPLICA_LEN1);
    verify(dn2, never()).updateReplicaUnderRecovery(
        block, RECOVERY_ID, REPLICA_LEN1);    
  }
  
  /**
   * BlockRecovery_02.13. 
   * Two replicas are RWR.
   * @throws IOException in case of an error
   */
  @Test
  public void testRWRReplicas() throws IOException {
    LOG.debug("Running " + GenericTestUtils.getMethodName());
    ReplicaRecoveryInfo replica1 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN1, GEN_STAMP-1, ReplicaState.RWR);
    ReplicaRecoveryInfo replica2 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN2, GEN_STAMP-2, ReplicaState.RWR);

    InterDatanodeProtocol dn1 = mock(InterDatanodeProtocol.class);
    InterDatanodeProtocol dn2 = mock(InterDatanodeProtocol.class);

    testSyncReplicas(replica1, replica2, dn1, dn2);
    
    long minLen = Math.min(REPLICA_LEN1, REPLICA_LEN2);
    verify(dn1).updateReplicaUnderRecovery(block, RECOVERY_ID, minLen);
    verify(dn2).updateReplicaUnderRecovery(block, RECOVERY_ID, minLen);    
  }  

  private Collection<RecoveringBlock> initRecoveringBlocks() {
    Collection<RecoveringBlock> blocks = new ArrayList<RecoveringBlock>(1);
    DatanodeInfo[] locs = new DatanodeInfo[] {
        new DatanodeInfo(dn.dnRegistration),
        mock(DatanodeInfo.class) };
    RecoveringBlock rBlock = new RecoveringBlock(block, locs, RECOVERY_ID);
    blocks.add(rBlock);
    return blocks;
  }
  /**
   * BlockRecoveryFI_05. One DN throws RecoveryInProgressException.
   *
   * @throws IOException
   *           in case of an error
   */
  @Test
  public void testRecoveryInProgressException()
    throws IOException, InterruptedException {
    LOG.debug("Running " + GenericTestUtils.getMethodName());
    DataNode spyDN = spy(dn);
    doThrow(new RecoveryInProgressException("Replica recovery is in progress")).
       when(spyDN).initReplicaRecovery(any(RecoveringBlock.class));
    Daemon d = spyDN.recoverBlocks(initRecoveringBlocks());
    d.join();
    verify(spyDN, never()).syncBlock(
        any(RecoveringBlock.class), anyListOf(BlockRecord.class));
  }

  /**
   * BlockRecoveryFI_06. all datanodes throws an exception.
   *
   * @throws IOException
   *           in case of an error
   */
  @Test
  public void testErrorReplicas() throws IOException, InterruptedException {
    LOG.debug("Running " + GenericTestUtils.getMethodName());
    DataNode spyDN = spy(dn);
    doThrow(new IOException()).
       when(spyDN).initReplicaRecovery(any(RecoveringBlock.class));
    Daemon d = spyDN.recoverBlocks(initRecoveringBlocks());
    d.join();
    verify(spyDN, never()).syncBlock(
        any(RecoveringBlock.class), anyListOf(BlockRecord.class));
  }

  /**
   * BlockRecoveryFI_07. max replica length from all DNs is zero.
   *
   * @throws IOException in case of an error
   */
  @Test
  public void testZeroLenReplicas() throws IOException, InterruptedException {
    LOG.debug("Running " + GenericTestUtils.getMethodName());
    DataNode spyDN = spy(dn);
    doReturn(new ReplicaRecoveryInfo(block.getBlockId(), 0,
        block.getGenerationStamp(), ReplicaState.FINALIZED)).when(spyDN).
        initReplicaRecovery(any(RecoveringBlock.class));
    Daemon d = spyDN.recoverBlocks(initRecoveringBlocks());
    d.join();
    verify(dn.namenode).commitBlockSynchronization(
        block, RECOVERY_ID, 0, true, true, DatanodeID.EMPTY_ARRAY);
  }

  private List<BlockRecord> initBlockRecords(DataNode spyDN) {
    List<BlockRecord> blocks = new ArrayList<BlockRecord>(1);
    BlockRecord blockRecord = new BlockRecord(
        new DatanodeID(dn.dnRegistration), spyDN,
        new ReplicaRecoveryInfo(block.getBlockId(), block.getNumBytes(),
            block.getGenerationStamp(), ReplicaState.FINALIZED));
    blocks.add(blockRecord);
    return blocks;
  }

  private final static RecoveringBlock rBlock =
    new RecoveringBlock(block, null, RECOVERY_ID);

  /**
   * BlockRecoveryFI_09. some/all DNs failed to update replicas.
   *
   * @throws IOException in case of an error
   */
  @Test
  public void testFailedReplicaUpdate() throws IOException {
    LOG.debug("Running " + GenericTestUtils.getMethodName());
    DataNode spyDN = spy(dn);
    doThrow(new IOException()).when(spyDN).updateReplicaUnderRecovery(
        block, RECOVERY_ID, block.getNumBytes());
    try {
      spyDN.syncBlock(rBlock, initBlockRecords(spyDN));
      fail("Sync should fail");
    } catch (IOException e) {
      e.getMessage().startsWith("Cannot recover ");
    }
  }

  /**
   * BlockRecoveryFI_10. DN has no ReplicaUnderRecovery.
   *
   * @throws IOException in case of an error
   */
  @Test
  public void testNoReplicaUnderRecovery() throws IOException {
    LOG.debug("Running " + GenericTestUtils.getMethodName());
    dn.data.createRbw(block);
    try {
      dn.syncBlock(rBlock, initBlockRecords(dn));
      fail("Sync should fail");
    } catch (IOException e) {
      e.getMessage().startsWith("Cannot recover ");
    }
    verify(dn.namenode, never()).commitBlockSynchronization(
        any(Block.class), anyLong(), anyLong(), anyBoolean(),
        anyBoolean(), any(DatanodeID[].class));
  }

  /**
   * BlockRecoveryFI_11. a replica's recovery id does not match new GS.
   *
   * @throws IOException in case of an error
   */
  @Test
  public void testNotMatchedReplicaID() throws IOException {
    LOG.debug("Running " + GenericTestUtils.getMethodName());
    ReplicaInPipelineInterface replicaInfo = dn.data.createRbw(block);
    BlockWriteStreams streams = null;
    try {
      streams = replicaInfo.createStreams(true, 0, 0);
      streams.checksumOut.write('a');
      dn.data.initReplicaRecovery(new RecoveringBlock(block, null, RECOVERY_ID+1));
      try {
        dn.syncBlock(rBlock, initBlockRecords(dn));
        fail("Sync should fail");
      } catch (IOException e) {
        e.getMessage().startsWith("Cannot recover ");
      }
      verify(dn.namenode, never()).commitBlockSynchronization(
          any(Block.class), anyLong(), anyLong(), anyBoolean(),
          anyBoolean(), any(DatanodeID[].class));
    } finally {
      streams.close();
    }
  }
}
