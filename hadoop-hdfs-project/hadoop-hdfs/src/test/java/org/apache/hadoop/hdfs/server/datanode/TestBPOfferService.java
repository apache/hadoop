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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.NNHAStatusHeartbeat;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.RegisterCommand;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TestBPOfferService {

  private static final String FAKE_BPID = "fake bpid";
  private static final String FAKE_CLUSTERID = "fake cluster";
  protected static final Log LOG = LogFactory.getLog(
      TestBPOfferService.class);
  private static final ExtendedBlock FAKE_BLOCK =
    new ExtendedBlock(FAKE_BPID, 12345L);
  private static final File TEST_BUILD_DATA = PathUtils.getTestDir(TestBPOfferService.class);
  private long firstCallTime = 0; 
  private long secondCallTime = 0;

  static {
    GenericTestUtils.setLogLevel(DataNode.LOG, Level.ALL);
  }

  private DatanodeProtocolClientSideTranslatorPB mockNN1;
  private DatanodeProtocolClientSideTranslatorPB mockNN2;
  private final NNHAStatusHeartbeat[] mockHaStatuses =
      new NNHAStatusHeartbeat[2];
  private final DatanodeCommand[][] datanodeCommands =
      new DatanodeCommand[2][0];
  private final int[] heartbeatCounts = new int[2];
  private DataNode mockDn;
  private FsDatasetSpi<?> mockFSDataset;
  
  @Before
  public void setupMocks() throws Exception {
    mockNN1 = setupNNMock(0);
    mockNN2 = setupNNMock(1);

    // Set up a mock DN with the bare-bones configuration
    // objects, etc.
    mockDn = Mockito.mock(DataNode.class);
    Mockito.doReturn(true).when(mockDn).shouldRun();
    Configuration conf = new Configuration();
    File dnDataDir = new File(new File(TEST_BUILD_DATA, "dfs"), "data");
    conf.set(DFS_DATANODE_DATA_DIR_KEY, dnDataDir.toURI().toString());
    Mockito.doReturn(conf).when(mockDn).getConf();
    Mockito.doReturn(new DNConf(conf)).when(mockDn).getDnConf();
    Mockito.doReturn(DataNodeMetrics.create(conf, "fake dn"))
    .when(mockDn).getMetrics();

    // Set up a simulated dataset with our fake BP
    mockFSDataset = Mockito.spy(new SimulatedFSDataset(null, conf));
    mockFSDataset.addBlockPool(FAKE_BPID, conf);

    // Wire the dataset to the DN.
    Mockito.doReturn(mockFSDataset).when(mockDn).getFSDataset();
  }

  /**
   * Set up a mock NN with the bare minimum for a DN to register to it.
   */
  private DatanodeProtocolClientSideTranslatorPB setupNNMock(int nnIdx)
      throws Exception {
    DatanodeProtocolClientSideTranslatorPB mock =
        Mockito.mock(DatanodeProtocolClientSideTranslatorPB.class);
    Mockito.doReturn(new NamespaceInfo(1, FAKE_CLUSTERID, FAKE_BPID, 0))
        .when(mock).versionRequest();
    
    Mockito.doReturn(DFSTestUtil.getLocalDatanodeRegistration())
      .when(mock).registerDatanode(Mockito.any(DatanodeRegistration.class));
    
    Mockito.doAnswer(new HeartbeatAnswer(nnIdx))
      .when(mock).sendHeartbeat(
          Mockito.any(DatanodeRegistration.class),
          Mockito.any(StorageReport[].class),
          Mockito.anyLong(),
          Mockito.anyLong(),
          Mockito.anyInt(),
          Mockito.anyInt(),
          Mockito.anyInt(),
          Mockito.any(VolumeFailureSummary.class),
          Mockito.anyBoolean());
    mockHaStatuses[nnIdx] = new NNHAStatusHeartbeat(HAServiceState.STANDBY, 0);
    datanodeCommands[nnIdx] = new DatanodeCommand[0];
    return mock;
  }
  
  /**
   * Mock answer for heartbeats which returns an empty set of commands
   * and the HA status for the chosen NN from the
   * {@link TestBPOfferService#mockHaStatuses} array.
   */
  private class HeartbeatAnswer implements Answer<HeartbeatResponse> {
    private final int nnIdx;

    public HeartbeatAnswer(int nnIdx) {
      this.nnIdx = nnIdx;
    }

    @Override
    public HeartbeatResponse answer(InvocationOnMock invocation) throws Throwable {
      heartbeatCounts[nnIdx]++;
      HeartbeatResponse heartbeatResponse = new HeartbeatResponse(
          datanodeCommands[nnIdx], mockHaStatuses[nnIdx], null,
          ThreadLocalRandom.current().nextLong() | 1L);
      //reset the command
      datanodeCommands[nnIdx] = new DatanodeCommand[0];
      return heartbeatResponse;
    }
  }


  /**
   * Test that the BPOS can register to talk to two different NNs,
   * sends block reports to both, etc.
   */
  @Test
  public void testBasicFunctionality() throws Exception {
    BPOfferService bpos = setupBPOSForNNs(mockNN1, mockNN2);
    bpos.start();
    try {
      waitForBothActors(bpos);
      
      // The DN should have register to both NNs.
      Mockito.verify(mockNN1).registerDatanode(
          Mockito.any(DatanodeRegistration.class));
      Mockito.verify(mockNN2).registerDatanode(
          Mockito.any(DatanodeRegistration.class));
      
      // Should get block reports from both NNs
      waitForBlockReport(mockNN1);
      waitForBlockReport(mockNN2);

      // When we receive a block, it should report it to both NNs
      bpos.notifyNamenodeReceivedBlock(FAKE_BLOCK, null, "", false);

      ReceivedDeletedBlockInfo[] ret = waitForBlockReceived(FAKE_BLOCK, mockNN1);
      assertEquals(1, ret.length);
      assertEquals(FAKE_BLOCK.getLocalBlock(), ret[0].getBlock());
      
      ret = waitForBlockReceived(FAKE_BLOCK, mockNN2);
      assertEquals(1, ret.length);
      assertEquals(FAKE_BLOCK.getLocalBlock(), ret[0].getBlock());

    } finally {
      bpos.stop();
      bpos.join();
    }
  }

  /**
   * Test that DNA_INVALIDATE commands from the standby are ignored.
   */
  @Test
  public void testIgnoreDeletionsFromNonActive() throws Exception {
    BPOfferService bpos = setupBPOSForNNs(mockNN1, mockNN2);

    // Ask to invalidate FAKE_BLOCK when block report hits the
    // standby
    Mockito.doReturn(new BlockCommand(DatanodeProtocol.DNA_INVALIDATE,
        FAKE_BPID, new Block[] { FAKE_BLOCK.getLocalBlock() }))
        .when(mockNN2).blockReport(
            Mockito.<DatanodeRegistration>anyObject(),  
            Mockito.eq(FAKE_BPID),
            Mockito.<StorageBlockReport[]>anyObject(),
            Mockito.<BlockReportContext>anyObject());

    bpos.start();
    try {
      waitForInitialization(bpos);
      
      // Should get block reports from both NNs
      waitForBlockReport(mockNN1);
      waitForBlockReport(mockNN2);

    } finally {
      bpos.stop();
      bpos.join();
    }
    
    // Should ignore the delete command from the standby
    Mockito.verify(mockFSDataset, Mockito.never())
      .invalidate(Mockito.eq(FAKE_BPID),
          (Block[]) Mockito.anyObject());
  }

  /**
   * Ensure that, if the two NNs configured for a block pool
   * have different block pool IDs, they will refuse to both
   * register.
   */
  @Test
  public void testNNsFromDifferentClusters() throws Exception {
    Mockito
        .doReturn(new NamespaceInfo(1, "fake foreign cluster", FAKE_BPID, 0))
        .when(mockNN1).versionRequest();
        
    BPOfferService bpos = setupBPOSForNNs(mockNN1, mockNN2);
    bpos.start();
    try {
      waitForOneToFail(bpos);
    } finally {
      bpos.stop();
      bpos.join();
    }
  }
  
  /**
   * Test that the DataNode determines the active NameNode correctly
   * based on the HA-related information in heartbeat responses.
   * See HDFS-2627.
   */
  @Test
  public void testPickActiveNameNode() throws Exception {
    BPOfferService bpos = setupBPOSForNNs(mockNN1, mockNN2);
    bpos.start();
    try {
      waitForInitialization(bpos);
      
      // Should start with neither NN as active.
      assertNull(bpos.getActiveNN());

      // Have NN1 claim active at txid 1
      mockHaStatuses[0] = new NNHAStatusHeartbeat(HAServiceState.ACTIVE, 1);
      bpos.triggerHeartbeatForTests();
      assertSame(mockNN1, bpos.getActiveNN());

      // NN2 claims active at a higher txid
      mockHaStatuses[1] = new NNHAStatusHeartbeat(HAServiceState.ACTIVE, 2);
      bpos.triggerHeartbeatForTests();
      assertSame(mockNN2, bpos.getActiveNN());
      
      // Even after another heartbeat from the first NN, it should
      // think NN2 is active, since it claimed a higher txid
      bpos.triggerHeartbeatForTests();
      assertSame(mockNN2, bpos.getActiveNN());
      
      // Even if NN2 goes to standby, DN shouldn't reset to talking to NN1,
      // because NN1's txid is lower than the last active txid. Instead,
      // it should consider neither active.
      mockHaStatuses[1] = new NNHAStatusHeartbeat(HAServiceState.STANDBY, 2);
      bpos.triggerHeartbeatForTests();
      assertNull(bpos.getActiveNN());
      
      // Now if NN1 goes back to a higher txid, it should be considered active
      mockHaStatuses[0] = new NNHAStatusHeartbeat(HAServiceState.ACTIVE, 3);
      bpos.triggerHeartbeatForTests();
      assertSame(mockNN1, bpos.getActiveNN());

    } finally {
      bpos.stop();
      bpos.join();
    }
  }

  /**
   * Test datanode block pool initialization error handling.
   * Failure in initializing a block pool should not cause NPE.
   */
  @Test
  public void testBPInitErrorHandling() throws Exception {
    final DataNode mockDn = Mockito.mock(DataNode.class);
    Mockito.doReturn(true).when(mockDn).shouldRun();
    Configuration conf = new Configuration();
    File dnDataDir = new File(
      new File(TEST_BUILD_DATA, "testBPInitErrorHandling"), "data");
    conf.set(DFS_DATANODE_DATA_DIR_KEY, dnDataDir.toURI().toString());
    Mockito.doReturn(conf).when(mockDn).getConf();
    Mockito.doReturn(new DNConf(conf)).when(mockDn).getDnConf();
    Mockito.doReturn(DataNodeMetrics.create(conf, "fake dn")).
      when(mockDn).getMetrics();
    final AtomicInteger count = new AtomicInteger();
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        if (count.getAndIncrement() == 0) {
          throw new IOException("faked initBlockPool exception");
        }
        // The initBlockPool is called again. Now mock init is done.
        Mockito.doReturn(mockFSDataset).when(mockDn).getFSDataset();
        return null;
      }
    }).when(mockDn).initBlockPool(Mockito.any(BPOfferService.class));
    BPOfferService bpos = setupBPOSForNNs(mockDn, mockNN1, mockNN2);
    List<BPServiceActor> actors = bpos.getBPServiceActors();
    assertEquals(2, actors.size());
    bpos.start();
    try {
      waitForInitialization(bpos);
      // even if one of the actor initialization fails, the other one will be
      // finish block report.
      waitForBlockReport(mockNN1, mockNN2);
    } finally {
      bpos.stop();
      bpos.join();
    }
  }

  private void waitForOneToFail(final BPOfferService bpos)
      throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        List<BPServiceActor> actors = bpos.getBPServiceActors();
        int failedcount = 0;
        for (BPServiceActor actor : actors) {
          if (!actor.isAlive()) {
            failedcount++;
          }
        }
        return failedcount == 1;
      }
    }, 100, 10000);
  }

  /**
   * Create a BPOfferService which registers with and heartbeats with the
   * specified namenode proxy objects.
   * @throws IOException 
   */
  private BPOfferService setupBPOSForNNs(
      DatanodeProtocolClientSideTranslatorPB ... nns) throws IOException {
    return setupBPOSForNNs(mockDn, nns);
  }

  private BPOfferService setupBPOSForNNs(DataNode mockDn,
      DatanodeProtocolClientSideTranslatorPB ... nns) throws IOException {
    // Set up some fake InetAddresses, then override the connectToNN
    // function to return the corresponding proxies.

    final Map<InetSocketAddress, DatanodeProtocolClientSideTranslatorPB> nnMap = Maps.newLinkedHashMap();
    for (int port = 0; port < nns.length; port++) {
      nnMap.put(new InetSocketAddress(port), nns[port]);
      Mockito.doReturn(nns[port]).when(mockDn).connectToNN(
          Mockito.eq(new InetSocketAddress(port)));
    }

    return new BPOfferService(Lists.newArrayList(nnMap.keySet()),
        Collections.<InetSocketAddress>nCopies(nnMap.size(), null), mockDn);
  }

  private void waitForInitialization(final BPOfferService bpos)
      throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return bpos.isAlive() && bpos.isInitialized();
      }
    }, 100, 10000);
  }

  private void waitForBothActors(final BPOfferService bpos)
      throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        List<BPServiceActor> actors = bpos.getBPServiceActors();

        return bpos.isAlive() && getRegisteredActors(actors) == 2;
      }
      private int getRegisteredActors(List<BPServiceActor> actors) {
        int regActors = 0;
        for (BPServiceActor actor : actors) {
          if (actor.getBpRegistration() != null) {
            regActors++;
          }
        }
        return regActors;
      }
    }, 100, 10000);
  }
  
  private void waitForBlockReport(final DatanodeProtocolClientSideTranslatorPB mockNN)
      throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          Mockito.verify(mockNN).blockReport(
              Mockito.<DatanodeRegistration>anyObject(),  
              Mockito.eq(FAKE_BPID),
              Mockito.<StorageBlockReport[]>anyObject(),
              Mockito.<BlockReportContext>anyObject());
          return true;
        } catch (Throwable t) {
          LOG.info("waiting on block report: " + t.getMessage());
          return false;
        }
      }
    }, 500, 10000);
  }

  private void waitForBlockReport(
      final DatanodeProtocolClientSideTranslatorPB mockNN1,
      final DatanodeProtocolClientSideTranslatorPB mockNN2)
          throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return get(mockNN1) || get(mockNN2);
      }

      private Boolean get(DatanodeProtocolClientSideTranslatorPB mockNN) {
        try {
          Mockito.verify(mockNN).blockReport(
                  Mockito.<DatanodeRegistration>anyObject(),
                  Mockito.eq(FAKE_BPID),
                  Mockito.<StorageBlockReport[]>anyObject(),
                  Mockito.<BlockReportContext>anyObject());
          return true;
        } catch (Throwable t) {
          LOG.info("waiting on block report: " + t.getMessage());
          return false;
        }
      }
    }, 500, 10000);
  }

  private ReceivedDeletedBlockInfo[] waitForBlockReceived(
      final ExtendedBlock fakeBlock,
      final DatanodeProtocolClientSideTranslatorPB mockNN) throws Exception {
    final String fakeBlockPoolId = fakeBlock.getBlockPoolId();
    final ArgumentCaptor<StorageReceivedDeletedBlocks[]> captor =
      ArgumentCaptor.forClass(StorageReceivedDeletedBlocks[].class);
    GenericTestUtils.waitFor(new Supplier<Boolean>() {

      @Override
      public Boolean get() {
        try {
          Mockito.verify(mockNN).blockReceivedAndDeleted(
            Mockito.<DatanodeRegistration>anyObject(),
            Mockito.eq(fakeBlockPoolId),
            captor.capture());
          return true;
        } catch (Throwable t) {
          return false;
        }
      }
    }, 100, 10000);
    return captor.getValue()[0].getBlocks();
  }

  private void setTimeForSynchronousBPOSCalls() {
    if (firstCallTime == 0) {
      firstCallTime = Time.now();
    } else {
      secondCallTime = Time.now();
    }
  }
  
  private class BPOfferServiceSynchronousCallAnswer implements Answer<Void> {
    private final int nnIdx;

    public BPOfferServiceSynchronousCallAnswer(int nnIdx) {
      this.nnIdx = nnIdx;
    }

    // For active namenode we will record the processTime and for standby
    // namenode we will sleep for 5 seconds (This will simulate the situation
    // where the standby namenode is down ) .
    @Override
    public Void answer(InvocationOnMock invocation) throws Throwable {
      if (nnIdx == 0) {
        setTimeForSynchronousBPOSCalls();
      } else {
        Thread.sleep(5000);
      }
      return null;
    }
   }

  /**
   * This test case test the {@link BPOfferService#reportBadBlocks} method
   * such that if call to standby namenode times out then that should not 
   * affect the active namenode heartbeat processing since this function 
   * are in writeLock.
   * @throws Exception
   */
  @Test
  public void testReportBadBlockWhenStandbyNNTimesOut() throws Exception {
    BPOfferService bpos = setupBPOSForNNs(mockNN1, mockNN2);
    bpos.start();
    try {
      waitForInitialization(bpos);
      // Should start with neither NN as active.
      assertNull(bpos.getActiveNN());
      // Have NN1 claim active at txid 1
      mockHaStatuses[0] = new NNHAStatusHeartbeat(HAServiceState.ACTIVE, 1);
      bpos.triggerHeartbeatForTests();
      // Now mockNN1 is acting like active namenode and mockNN2 as Standby
      assertSame(mockNN1, bpos.getActiveNN());
      Mockito.doAnswer(new BPOfferServiceSynchronousCallAnswer(0))
         .when(mockNN1).reportBadBlocks(Mockito.any(LocatedBlock[].class));
      Mockito.doAnswer(new BPOfferServiceSynchronousCallAnswer(1))
         .when(mockNN2).reportBadBlocks(Mockito.any(LocatedBlock[].class));
      bpos.reportBadBlocks(FAKE_BLOCK, mockFSDataset.getVolume(FAKE_BLOCK)
          .getStorageID(), mockFSDataset.getVolume(FAKE_BLOCK)
          .getStorageType());
      bpos.reportBadBlocks(FAKE_BLOCK, mockFSDataset.getVolume(FAKE_BLOCK)
          .getStorageID(), mockFSDataset.getVolume(FAKE_BLOCK)
          .getStorageType());
      Thread.sleep(10000);
      long difference = secondCallTime - firstCallTime;
      assertTrue("Active namenode reportBadBlock processing should be "
          + "independent of standby namenode reportBadBlock processing ",
          difference < 5000);
    } finally {
      bpos.stop();
      bpos.join();
    }
  }

  /**
   * This test case test the {@link BPOfferService#trySendErrorReport} method
   * such that if call to standby namenode times out then that should not 
   * affect the active namenode heartbeat processing since this function 
   * are in writeLock.
   * @throws Exception
   */
  @Test
  public void testTrySendErrorReportWhenStandbyNNTimesOut() throws Exception {
    BPOfferService bpos = setupBPOSForNNs(mockNN1, mockNN2);
    bpos.start();
    try {
      waitForInitialization(bpos);
      // Should start with neither NN as active.
      assertNull(bpos.getActiveNN());
      // Have NN1 claim active at txid 1
      mockHaStatuses[0] = new NNHAStatusHeartbeat(HAServiceState.ACTIVE, 1);
      bpos.triggerHeartbeatForTests();
      // Now mockNN1 is acting like active namenode and mockNN2 as Standby
      assertSame(mockNN1, bpos.getActiveNN());
      Mockito.doAnswer(new BPOfferServiceSynchronousCallAnswer(0))
          .when(mockNN1).errorReport(Mockito.any(DatanodeRegistration.class),
          Mockito.anyInt(), Mockito.anyString());
      Mockito.doAnswer(new BPOfferServiceSynchronousCallAnswer(1))
          .when(mockNN2).errorReport(Mockito.any(DatanodeRegistration.class),
          Mockito.anyInt(), Mockito.anyString());
      String errorString = "Can't send invalid block " + FAKE_BLOCK;
      bpos.trySendErrorReport(DatanodeProtocol.INVALID_BLOCK, errorString);
      bpos.trySendErrorReport(DatanodeProtocol.INVALID_BLOCK, errorString);
      Thread.sleep(10000);
      long difference = secondCallTime - firstCallTime;
      assertTrue("Active namenode trySendErrorReport processing "
          + "should be independent of standby namenode trySendErrorReport"
          + " processing ", difference < 5000);
    } finally {
      bpos.stop();
      bpos.join();
    }
  }
  /**
   * This test case tests whether the {@BPServiceActor#processQueueMessages}
   * adds back the error report back to the queue when 
   * {BPServiceActorAction#reportTo} throws an IOException
   * @throws Exception
   */
  @Test
  public void testTrySendErrorReportWhenNNThrowsIOException() 
      throws Exception {
    BPOfferService bpos = setupBPOSForNNs(mockNN1, mockNN2);
    bpos.start();
    try {
      waitForInitialization(bpos);
      // Should start with neither NN as active.
      assertNull(bpos.getActiveNN());
      // Have NN1 claim active at txid 1
      mockHaStatuses[0] = new NNHAStatusHeartbeat(HAServiceState.ACTIVE, 1);
      bpos.triggerHeartbeatForTests();
      // Now mockNN1 is acting like active namenode and mockNN2 as Standby
      assertSame(mockNN1, bpos.getActiveNN());
      Mockito.doAnswer(new Answer<Void>() {
        // Throw an IOException when this function is first called which will
        // in turn add that errorReport back to the bpThreadQueue and let it 
        // process the next time. 
        @Override
        public Void answer(InvocationOnMock invocation) throws Throwable {
          if (firstCallTime == 0) {
            firstCallTime = Time.now();
            throw new IOException();
          } else {
            secondCallTime = Time.now();
            return null;
          }
        }
      }).when(mockNN1).errorReport(Mockito.any(DatanodeRegistration.class),
          Mockito.anyInt(), Mockito.anyString());
      String errorString = "Can't send invalid block " + FAKE_BLOCK;
      bpos.trySendErrorReport(DatanodeProtocol.INVALID_BLOCK, errorString);
      Thread.sleep(10000);
      assertTrue("Active namenode didn't add the report back to the queue "
          + "when errorReport threw IOException", secondCallTime != 0);
    } finally {
      bpos.stop();
      bpos.join();
    }
  } 

  /**
   * This test case doesn't add the reportBadBlock request to
   * {@link BPServiceActor#bpThreadEnqueue} when the Standby namenode throws
   * {@link StandbyException}
   * @throws Exception
   */
 @Test
  public void testReportBadBlocksWhenNNThrowsStandbyException()
      throws Exception {
    BPOfferService bpos = setupBPOSForNNs(mockNN1, mockNN2);
    bpos.start();
    try {
      waitForInitialization(bpos);
      // Should start with neither NN as active.
      assertNull(bpos.getActiveNN());
      // Have NN1 claim active at txid 1
      mockHaStatuses[0] = new NNHAStatusHeartbeat(HAServiceState.ACTIVE, 1);
      bpos.triggerHeartbeatForTests();
      // Now mockNN1 is acting like active namenode and mockNN2 as Standby
      assertSame(mockNN1, bpos.getActiveNN());
      // Return nothing when active Active Namenode calls reportBadBlocks
      Mockito.doNothing().when(mockNN1).reportBadBlocks
          (Mockito.any(LocatedBlock[].class));

      RemoteException re = new RemoteException(StandbyException.class.
          getName(), "Operation category WRITE is not supported in state "
          + "standby", RpcErrorCodeProto.ERROR_APPLICATION);
      // Return StandbyException wrapped in RemoteException when Standby NN
      // calls reportBadBlocks
      Mockito.doThrow(re).when(mockNN2).reportBadBlocks
          (Mockito.any(LocatedBlock[].class));

      bpos.reportBadBlocks(FAKE_BLOCK, mockFSDataset.getVolume(FAKE_BLOCK)
          .getStorageID(), mockFSDataset.getVolume(FAKE_BLOCK)
          .getStorageType());
      // Send heartbeat so that the BpServiceActor can report bad block to
      // namenode
      bpos.triggerHeartbeatForTests();
      Mockito.verify(mockNN2, Mockito.times(1))
      .reportBadBlocks(Mockito.any(LocatedBlock[].class));

      // Trigger another heartbeat, this will send reportBadBlock again if it
      // is present in the queue.
      bpos.triggerHeartbeatForTests();
      Mockito.verify(mockNN2, Mockito.times(1))
          .reportBadBlocks(Mockito.any(LocatedBlock[].class));
    } finally {
      bpos.stop();
      bpos.join();
    }
  }

  /*
   * HDFS-9917 : Standby IBR accumulation when Standby was down.
   */
  @Test
  public void testIBRClearanceForStandbyOnReRegister() throws Exception {
    final BPOfferService bpos = setupBPOSForNNs(mockNN1, mockNN2);
    bpos.start();
    try {
      waitForInitialization(bpos);
      // Should start with neither NN as active.
      assertNull(bpos.getActiveNN());
      // Have NN1 claim active at txid 1
      mockHaStatuses[0] = new NNHAStatusHeartbeat(HAServiceState.ACTIVE, 1);
      bpos.triggerHeartbeatForTests();
      // Now mockNN1 is acting like active namenode and mockNN2 as Standby
      assertSame(mockNN1, bpos.getActiveNN());
      // Return nothing when active Active Namenode gets IBRs
      Mockito.doNothing().when(mockNN1).blockReceivedAndDeleted(
          Mockito.any(DatanodeRegistration.class), Mockito.anyString(), Mockito
              .any(StorageReceivedDeletedBlocks[].class));

      final IOException re = new IOException(
          "Standby NN is currently not able to process IBR");

      final AtomicBoolean ibrReported = new AtomicBoolean(false);
      // throw exception for standby when first IBR is receieved
      Mockito.doAnswer(new Answer<Void>() {
        @Override
        public Void answer(InvocationOnMock invocation) throws Throwable {
          ibrReported.set(true);
          throw re;
        }
      }).when(mockNN2).blockReceivedAndDeleted(
          Mockito.any(DatanodeRegistration.class), Mockito.anyString(), Mockito
              .any(StorageReceivedDeletedBlocks[].class));

      DatanodeStorage storage = Mockito.mock(DatanodeStorage.class);
      Mockito.doReturn(storage).when(mockFSDataset).getStorage("storage0");
      // Add IBRs
      bpos.notifyNamenodeReceivedBlock(FAKE_BLOCK, null, "storage0", false);
      // Send heartbeat so that the BpServiceActor can send IBR to
      // namenode
      bpos.triggerHeartbeatForTests();
      // Wait till first IBR is received at standbyNN. Just for confirmation.
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          return ibrReported.get();
        }
      }, 100, 1000);

      // Send register command back to Datanode to reRegister().
      // After reRegister IBRs should be cleared.
      datanodeCommands[1] = new DatanodeCommand[] { new RegisterCommand() };
      assertEquals(
          "IBR size before reRegister should be non-0", 1, getStandbyIBRSize(
              bpos));
      bpos.triggerHeartbeatForTests();
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          return getStandbyIBRSize(bpos) == 0;
        }
      }, 100, 1000);
    } finally {
      bpos.stop();
      bpos.join();
    }
  }

  private int getStandbyIBRSize(BPOfferService bpos) {
    List<BPServiceActor> bpServiceActors = bpos.getBPServiceActors();
    for (BPServiceActor bpServiceActor : bpServiceActors) {
      if (bpServiceActor.state == HAServiceState.STANDBY) {
        return bpServiceActor.getIbrManager().getPendingIBRSize();
      }
    }
    return -1;
  }
}
