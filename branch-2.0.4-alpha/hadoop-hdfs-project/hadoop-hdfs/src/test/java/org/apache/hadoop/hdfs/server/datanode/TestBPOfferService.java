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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.NNHAStatusHeartbeat;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.test.GenericTestUtils;
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

  static {
    ((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
  }

  private DatanodeProtocolClientSideTranslatorPB mockNN1;
  private DatanodeProtocolClientSideTranslatorPB mockNN2;
  private NNHAStatusHeartbeat[] mockHaStatuses = new NNHAStatusHeartbeat[2];
  private int heartbeatCounts[] = new int[2];
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
    Mockito.doReturn(conf).when(mockDn).getConf();
    Mockito.doReturn(new DNConf(conf)).when(mockDn).getDnConf();
    Mockito.doReturn(DataNodeMetrics.create(conf, "fake dn"))
    .when(mockDn).getMetrics();

    // Set up a simulated dataset with our fake BP
    mockFSDataset = Mockito.spy(new SimulatedFSDataset(null, null, conf));
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
          Mockito.anyInt(),
          Mockito.anyInt(),
          Mockito.anyInt());
    mockHaStatuses[nnIdx] = new NNHAStatusHeartbeat(HAServiceState.STANDBY, 0);
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
      return new HeartbeatResponse(new DatanodeCommand[0],
          mockHaStatuses[nnIdx]);
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
      waitForInitialization(bpos);
      
      // The DN should have register to both NNs.
      Mockito.verify(mockNN1).registerDatanode(
          Mockito.any(DatanodeRegistration.class));
      Mockito.verify(mockNN2).registerDatanode(
          Mockito.any(DatanodeRegistration.class));
      
      // Should get block reports from both NNs
      waitForBlockReport(mockNN1);
      waitForBlockReport(mockNN2);

      // When we receive a block, it should report it to both NNs
      bpos.notifyNamenodeReceivedBlock(FAKE_BLOCK, "");

      ReceivedDeletedBlockInfo[] ret = waitForBlockReceived(FAKE_BLOCK, mockNN1);
      assertEquals(1, ret.length);
      assertEquals(FAKE_BLOCK.getLocalBlock(), ret[0].getBlock());
      
      ret = waitForBlockReceived(FAKE_BLOCK, mockNN2);
      assertEquals(1, ret.length);
      assertEquals(FAKE_BLOCK.getLocalBlock(), ret[0].getBlock());

    } finally {
      bpos.stop();
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
            Mockito.<StorageBlockReport[]>anyObject());

    bpos.start();
    try {
      waitForInitialization(bpos);
      
      // Should get block reports from both NNs
      waitForBlockReport(mockNN1);
      waitForBlockReport(mockNN2);

    } finally {
      bpos.stop();
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
    }
  }

  private void waitForOneToFail(final BPOfferService bpos)
      throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return bpos.countNameNodes() == 1;
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
    // Set up some fake InetAddresses, then override the connectToNN
    // function to return the corresponding proxies.

    final Map<InetSocketAddress, DatanodeProtocolClientSideTranslatorPB> nnMap = Maps.newLinkedHashMap();
    for (int port = 0; port < nns.length; port++) {
      nnMap.put(new InetSocketAddress(port), nns[port]);
      Mockito.doReturn(nns[port]).when(mockDn).connectToNN(
          Mockito.eq(new InetSocketAddress(port)));
    }

    return new BPOfferService(Lists.newArrayList(nnMap.keySet()), mockDn);
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
  
  private void waitForBlockReport(final DatanodeProtocolClientSideTranslatorPB mockNN)
      throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          Mockito.verify(mockNN).blockReport(
              Mockito.<DatanodeRegistration>anyObject(),  
              Mockito.eq(FAKE_BPID),
              Mockito.<StorageBlockReport[]>anyObject());
          return true;
        } catch (Throwable t) {
          LOG.info("waiting on block report: " + t.getMessage());
          return false;
        }
      }
    }, 500, 10000);
  }
  
  private ReceivedDeletedBlockInfo[] waitForBlockReceived(
      ExtendedBlock fakeBlock,
      DatanodeProtocolClientSideTranslatorPB mockNN) throws Exception {
    final ArgumentCaptor<StorageReceivedDeletedBlocks[]> captor =
      ArgumentCaptor.forClass(StorageReceivedDeletedBlocks[].class);
    GenericTestUtils.waitFor(new Supplier<Boolean>() {

      @Override
      public Boolean get() {
        try {
          Mockito.verify(mockNN1).blockReceivedAndDeleted(
            Mockito.<DatanodeRegistration>anyObject(),
            Mockito.eq(FAKE_BPID),
            captor.capture());
          return true;
        } catch (Throwable t) {
          return false;
        }
      }
    }, 100, 10000);
    return captor.getValue()[0].getBlocks();
  }

}
