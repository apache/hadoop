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

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.DataNode.BlockRecord;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NNHAStatusHeartbeat;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * This tests if sync all replicas in block recovery works correctly
 */
public class TestBlockRecovery {
  private static final Log LOG = LogFactory.getLog(TestBlockRecovery.class);
  private static final String DATA_DIR =
    MiniDFSCluster.getBaseDirectory() + "data";
  private DataNode dn;
  private Configuration conf;
  private boolean tearDownDone;
  private final static long RECOVERY_ID = 3000L;
  private final static String CLUSTER_ID = "testClusterID";
  private final static String POOL_ID = "BP-TEST";
  private final static InetSocketAddress NN_ADDR = new InetSocketAddress(
      "localhost", 5020);
  private final static long BLOCK_ID = 1000L;
  private final static long GEN_STAMP = 2000L;
  private final static long BLOCK_LEN = 3000L;
  private final static long REPLICA_LEN1 = 6000L;
  private final static long REPLICA_LEN2 = 5000L;
  private final static ExtendedBlock block = new ExtendedBlock(POOL_ID,
      BLOCK_ID, BLOCK_LEN, GEN_STAMP);
  
  static {
    ((Log4JLogger)LogFactory.getLog(FSNamesystem.class)).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LOG).getLogger().setLevel(Level.ALL);
  }

  /**
   * Starts an instance of DataNode
   * @throws IOException
   */
  @Before
  public void startUp() throws IOException, URISyntaxException {
    tearDownDone = false;
    conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, DATA_DIR);
    conf.set(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY, "0.0.0.0:0");
    conf.set(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    conf.set(DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY, "0.0.0.0:0");
    conf.setInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);
    FileSystem.setDefaultUri(conf,
        "hdfs://" + NN_ADDR.getHostName() + ":" + NN_ADDR.getPort());
    ArrayList<StorageLocation> locations = new ArrayList<StorageLocation>();
    File dataDir = new File(DATA_DIR);
    FileUtil.fullyDelete(dataDir);
    dataDir.mkdirs();
    StorageLocation location = StorageLocation.parse(dataDir.getPath());
    locations.add(location);
    final DatanodeProtocolClientSideTranslatorPB namenode =
      mock(DatanodeProtocolClientSideTranslatorPB.class);

    Mockito.doAnswer(new Answer<DatanodeRegistration>() {
      @Override
      public DatanodeRegistration answer(InvocationOnMock invocation)
          throws Throwable {
        return (DatanodeRegistration) invocation.getArguments()[0];
      }
    }).when(namenode).registerDatanode(
        Mockito.any(DatanodeRegistration.class));

    when(namenode.versionRequest()).thenReturn(new NamespaceInfo
        (1, CLUSTER_ID, POOL_ID, 1L));

    when(namenode.sendHeartbeat(
            Mockito.any(DatanodeRegistration.class),
            Mockito.any(StorageReport[].class),
            Mockito.anyLong(),
            Mockito.anyLong(),
            Mockito.anyInt(),
            Mockito.anyInt(),
            Mockito.anyInt()))
        .thenReturn(new HeartbeatResponse(
            new DatanodeCommand[0],
            new NNHAStatusHeartbeat(HAServiceState.ACTIVE, 1),
            null));

    dn = new DataNode(conf, locations, null) {
      @Override
      DatanodeProtocolClientSideTranslatorPB connectToNN(
          InetSocketAddress nnAddr) throws IOException {
        Assert.assertEquals(NN_ADDR, nnAddr);
        return namenode;
      }
    };
    // Trigger a heartbeat so that it acknowledges the NN as active.
    dn.getAllBpOs()[0].triggerHeartbeatForTests();
  }

  /**
   * Cleans the resources and closes the instance of datanode
   * @throws IOException if an error occurred
   */
  @After
  public void tearDown() throws IOException {
    if (!tearDownDone && dn != null) {
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
      tearDownDone = true;
    }
  }

  /** Sync two replicas */
  private void testSyncReplicas(ReplicaRecoveryInfo replica1, 
      ReplicaRecoveryInfo replica2,
      InterDatanodeProtocol dn1,
      InterDatanodeProtocol dn2,
      long expectLen) throws IOException {
    
    DatanodeInfo[] locs = new DatanodeInfo[]{
        mock(DatanodeInfo.class), mock(DatanodeInfo.class)};
    RecoveringBlock rBlock = new RecoveringBlock(block, 
        locs, RECOVERY_ID);
    ArrayList<BlockRecord> syncList = new ArrayList<BlockRecord>(2);
    BlockRecord record1 = new BlockRecord(
        DFSTestUtil.getDatanodeInfo("1.2.3.4", "bogus", 1234), dn1, replica1);
    BlockRecord record2 = new BlockRecord(
        DFSTestUtil.getDatanodeInfo("1.2.3.4", "bogus", 1234), dn2, replica2);
    syncList.add(record1);
    syncList.add(record2);
    
    when(dn1.updateReplicaUnderRecovery((ExtendedBlock)anyObject(), anyLong(), 
        anyLong())).thenReturn("storage1");
    when(dn2.updateReplicaUnderRecovery((ExtendedBlock)anyObject(), anyLong(), 
        anyLong())).thenReturn("storage2");
    dn.syncBlock(rBlock, syncList);
  }
  
  /**
   * BlockRecovery_02.8.
   * Two replicas are in Finalized state
   * @throws IOException in case of an error
   */
  @Test
  public void testFinalizedReplicas () throws IOException {
    if(LOG.isDebugEnabled()) {
      LOG.debug("Running " + GenericTestUtils.getMethodName());
    }
    ReplicaRecoveryInfo replica1 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN1, GEN_STAMP-1, ReplicaState.FINALIZED);
    ReplicaRecoveryInfo replica2 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN1, GEN_STAMP-2, ReplicaState.FINALIZED);

    InterDatanodeProtocol dn1 = mock(InterDatanodeProtocol.class);
    InterDatanodeProtocol dn2 = mock(InterDatanodeProtocol.class);

    testSyncReplicas(replica1, replica2, dn1, dn2, REPLICA_LEN1);
    verify(dn1).updateReplicaUnderRecovery(block, RECOVERY_ID, REPLICA_LEN1);
    verify(dn2).updateReplicaUnderRecovery(block, RECOVERY_ID, REPLICA_LEN1);    

    // two finalized replicas have different length
    replica1 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN1, GEN_STAMP-1, ReplicaState.FINALIZED);
    replica2 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN2, GEN_STAMP-2, ReplicaState.FINALIZED);

    try {
      testSyncReplicas(replica1, replica2, dn1, dn2, REPLICA_LEN1);
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
    if(LOG.isDebugEnabled()) {
      LOG.debug("Running " + GenericTestUtils.getMethodName());
    }
    
    // rbw and finalized replicas have the same length
    ReplicaRecoveryInfo replica1 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN1, GEN_STAMP-1, ReplicaState.FINALIZED);
    ReplicaRecoveryInfo replica2 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN1, GEN_STAMP-2, ReplicaState.RBW);

    InterDatanodeProtocol dn1 = mock(InterDatanodeProtocol.class);
    InterDatanodeProtocol dn2 = mock(InterDatanodeProtocol.class);

    testSyncReplicas(replica1, replica2, dn1, dn2, REPLICA_LEN1);
    verify(dn1).updateReplicaUnderRecovery(block, RECOVERY_ID, REPLICA_LEN1);
    verify(dn2).updateReplicaUnderRecovery(block, RECOVERY_ID, REPLICA_LEN1);
    
    // rbw replica has a different length from the finalized one
    replica1 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN1, GEN_STAMP-1, ReplicaState.FINALIZED);
    replica2 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN2, GEN_STAMP-2, ReplicaState.RBW);

    dn1 = mock(InterDatanodeProtocol.class);
    dn2 = mock(InterDatanodeProtocol.class);

    testSyncReplicas(replica1, replica2, dn1, dn2, REPLICA_LEN1);
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
    if(LOG.isDebugEnabled()) {
      LOG.debug("Running " + GenericTestUtils.getMethodName());
    }
    
    // rbw and finalized replicas have the same length
    ReplicaRecoveryInfo replica1 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN1, GEN_STAMP-1, ReplicaState.FINALIZED);
    ReplicaRecoveryInfo replica2 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN1, GEN_STAMP-2, ReplicaState.RWR);

    InterDatanodeProtocol dn1 = mock(InterDatanodeProtocol.class);
    InterDatanodeProtocol dn2 = mock(InterDatanodeProtocol.class);

    testSyncReplicas(replica1, replica2, dn1, dn2, REPLICA_LEN1);
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

    testSyncReplicas(replica1, replica2, dn1, dn2, REPLICA_LEN1);
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
    if(LOG.isDebugEnabled()) {
      LOG.debug("Running " + GenericTestUtils.getMethodName());
    }
    ReplicaRecoveryInfo replica1 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN1, GEN_STAMP-1, ReplicaState.RBW);
    ReplicaRecoveryInfo replica2 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN2, GEN_STAMP-2, ReplicaState.RBW);

    InterDatanodeProtocol dn1 = mock(InterDatanodeProtocol.class);
    InterDatanodeProtocol dn2 = mock(InterDatanodeProtocol.class);

    long minLen = Math.min(REPLICA_LEN1, REPLICA_LEN2);
    testSyncReplicas(replica1, replica2, dn1, dn2, minLen);
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
    if(LOG.isDebugEnabled()) {
      LOG.debug("Running " + GenericTestUtils.getMethodName());
    }
    ReplicaRecoveryInfo replica1 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN1, GEN_STAMP-1, ReplicaState.RBW);
    ReplicaRecoveryInfo replica2 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN1, GEN_STAMP-2, ReplicaState.RWR);

    InterDatanodeProtocol dn1 = mock(InterDatanodeProtocol.class);
    InterDatanodeProtocol dn2 = mock(InterDatanodeProtocol.class);

    testSyncReplicas(replica1, replica2, dn1, dn2, REPLICA_LEN1);
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
    if(LOG.isDebugEnabled()) {
      LOG.debug("Running " + GenericTestUtils.getMethodName());
    }
    ReplicaRecoveryInfo replica1 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN1, GEN_STAMP-1, ReplicaState.RWR);
    ReplicaRecoveryInfo replica2 = new ReplicaRecoveryInfo(BLOCK_ID, 
        REPLICA_LEN2, GEN_STAMP-2, ReplicaState.RWR);

    InterDatanodeProtocol dn1 = mock(InterDatanodeProtocol.class);
    InterDatanodeProtocol dn2 = mock(InterDatanodeProtocol.class);

    long minLen = Math.min(REPLICA_LEN1, REPLICA_LEN2);
    testSyncReplicas(replica1, replica2, dn1, dn2, minLen);
    
    verify(dn1).updateReplicaUnderRecovery(block, RECOVERY_ID, minLen);
    verify(dn2).updateReplicaUnderRecovery(block, RECOVERY_ID, minLen);    
  }  

  private Collection<RecoveringBlock> initRecoveringBlocks() throws IOException {
    Collection<RecoveringBlock> blocks = new ArrayList<RecoveringBlock>(1);
    DatanodeInfo mockOtherDN = DFSTestUtil.getLocalDatanodeInfo();
    DatanodeInfo[] locs = new DatanodeInfo[] {
        new DatanodeInfo(dn.getDNRegistrationForBP(block.getBlockPoolId())),
        mockOtherDN };
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
    if(LOG.isDebugEnabled()) {
      LOG.debug("Running " + GenericTestUtils.getMethodName());
    }
    DataNode spyDN = spy(dn);
    doThrow(new RecoveryInProgressException("Replica recovery is in progress")).
       when(spyDN).initReplicaRecovery(any(RecoveringBlock.class));
    Daemon d = spyDN.recoverBlocks("fake NN", initRecoveringBlocks());
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
    if(LOG.isDebugEnabled()) {
      LOG.debug("Running " + GenericTestUtils.getMethodName());
    }
    DataNode spyDN = spy(dn);
    doThrow(new IOException()).
       when(spyDN).initReplicaRecovery(any(RecoveringBlock.class));
    Daemon d = spyDN.recoverBlocks("fake NN", initRecoveringBlocks());
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
    if(LOG.isDebugEnabled()) {
      LOG.debug("Running " + GenericTestUtils.getMethodName());
    }
    DataNode spyDN = spy(dn);
    doReturn(new ReplicaRecoveryInfo(block.getBlockId(), 0,
        block.getGenerationStamp(), ReplicaState.FINALIZED)).when(spyDN).
        initReplicaRecovery(any(RecoveringBlock.class));
    Daemon d = spyDN.recoverBlocks("fake NN", initRecoveringBlocks());
    d.join();
    DatanodeProtocol dnP = dn.getActiveNamenodeForBP(POOL_ID);
    verify(dnP).commitBlockSynchronization(
        block, RECOVERY_ID, 0, true, true, DatanodeID.EMPTY_ARRAY, null);
  }

  private List<BlockRecord> initBlockRecords(DataNode spyDN) throws IOException {
    List<BlockRecord> blocks = new ArrayList<BlockRecord>(1);
    DatanodeRegistration dnR = dn.getDNRegistrationForBP(block.getBlockPoolId());
    BlockRecord blockRecord = new BlockRecord(
        new DatanodeID(dnR), spyDN,
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
    if(LOG.isDebugEnabled()) {
      LOG.debug("Running " + GenericTestUtils.getMethodName());
    }
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
    if(LOG.isDebugEnabled()) {
      LOG.debug("Running " + GenericTestUtils.getMethodName());
    }
    dn.data.createRbw(StorageType.DEFAULT, block);
    try {
      dn.syncBlock(rBlock, initBlockRecords(dn));
      fail("Sync should fail");
    } catch (IOException e) {
      e.getMessage().startsWith("Cannot recover ");
    }
    DatanodeProtocol namenode = dn.getActiveNamenodeForBP(POOL_ID);
    verify(namenode, never()).commitBlockSynchronization(
        any(ExtendedBlock.class), anyLong(), anyLong(), anyBoolean(),
        anyBoolean(), any(DatanodeID[].class), any(String[].class));
  }

  /**
   * BlockRecoveryFI_11. a replica's recovery id does not match new GS.
   *
   * @throws IOException in case of an error
   */
  @Test
  public void testNotMatchedReplicaID() throws IOException {
    if(LOG.isDebugEnabled()) {
      LOG.debug("Running " + GenericTestUtils.getMethodName());
    }
    ReplicaInPipelineInterface replicaInfo = dn.data.createRbw(
        StorageType.DEFAULT, block);
    ReplicaOutputStreams streams = null;
    try {
      streams = replicaInfo.createStreams(true,
          DataChecksum.newDataChecksum(DataChecksum.Type.CRC32, 512));
      streams.getChecksumOut().write('a');
      dn.data.initReplicaRecovery(new RecoveringBlock(block, null, RECOVERY_ID+1));
      try {
        dn.syncBlock(rBlock, initBlockRecords(dn));
        fail("Sync should fail");
      } catch (IOException e) {
        e.getMessage().startsWith("Cannot recover ");
      }
      DatanodeProtocol namenode = dn.getActiveNamenodeForBP(POOL_ID);
      verify(namenode, never()).commitBlockSynchronization(
          any(ExtendedBlock.class), anyLong(), anyLong(), anyBoolean(),
          anyBoolean(), any(DatanodeID[].class), any(String[].class));
    } finally {
      streams.close();
    }
  }
  
  /**
   * Test to verify the race between finalizeBlock and Lease recovery
   * 
   * @throws Exception
   */
  @Test(timeout = 20000)
  public void testRaceBetweenReplicaRecoveryAndFinalizeBlock() throws Exception {
    tearDown();// Stop the Mocked DN started in startup()

    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_DATANODE_XCEIVER_STOP_TIMEOUT_MILLIS_KEY, "1000");
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleSingleNN(8020, 50070))
        .numDataNodes(1).build();
    try {
      cluster.waitClusterUp();
      DistributedFileSystem fs = cluster.getFileSystem();
      Path path = new Path("/test");
      FSDataOutputStream out = fs.create(path);
      out.writeBytes("data");
      out.hsync();
      
      List<LocatedBlock> blocks = DFSTestUtil.getAllBlocks(fs.open(path));
      final LocatedBlock block = blocks.get(0);
      final DataNode dataNode = cluster.getDataNodes().get(0);
      
      final AtomicBoolean recoveryInitResult = new AtomicBoolean(true);
      Thread recoveryThread = new Thread() {
        @Override
        public void run() {
          try {
            DatanodeInfo[] locations = block.getLocations();
            final RecoveringBlock recoveringBlock = new RecoveringBlock(
                block.getBlock(), locations, block.getBlock()
                    .getGenerationStamp() + 1);
            synchronized (dataNode.data) {
              Thread.sleep(2000);
              dataNode.initReplicaRecovery(recoveringBlock);
            }
          } catch (Exception e) {
            recoveryInitResult.set(false);
          }
        }
      };
      recoveryThread.start();
      try {
        out.close();
      } catch (IOException e) {
        Assert.assertTrue("Writing should fail",
            e.getMessage().contains("are bad. Aborting..."));
      } finally {
        recoveryThread.join();
      }
      Assert.assertTrue("Recovery should be initiated successfully",
          recoveryInitResult.get());
      
      dataNode.updateReplicaUnderRecovery(block.getBlock(), block.getBlock()
          .getGenerationStamp() + 1, block.getBlockSize());
    } finally {
      if (null != cluster) {
        cluster.shutdown();
        cluster = null;
      }
    }
  }
}
