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

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

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

  private DatanodeProtocol mockNN1;
  private DatanodeProtocol mockNN2;
  private DataNode mockDn;
  private FSDatasetInterface mockFSDataset;
  
  @Before
  public void setupMocks() throws Exception {
    mockNN1 = setupNNMock();
    mockNN2 = setupNNMock();

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
    mockFSDataset = Mockito.spy(new SimulatedFSDataset(conf));
    mockFSDataset.addBlockPool(FAKE_BPID, conf);

    // Wire the dataset to the DN.
    Mockito.doReturn(mockFSDataset).when(mockDn).getFSDataset();
  }

  /**
   * Set up a mock NN with the bare minimum for a DN to register to it.
   */
  private DatanodeProtocol setupNNMock() throws Exception {
    DatanodeProtocol mock = Mockito.mock(DatanodeProtocol.class);
    Mockito.doReturn(
        new NamespaceInfo(1, FAKE_CLUSTERID, FAKE_BPID,
            0, HdfsConstants.LAYOUT_VERSION))
      .when(mock).versionRequest();
    return mock;
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
          (DatanodeRegistration) Mockito.anyObject());
      Mockito.verify(mockNN2).registerDatanode(
          (DatanodeRegistration) Mockito.anyObject());
      
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
            Mockito.<long[]>anyObject());

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
    Mockito.doReturn(
        new NamespaceInfo(1, "fake foreign cluster", FAKE_BPID,
            0, HdfsConstants.LAYOUT_VERSION))
      .when(mockNN1).versionRequest();
        
    BPOfferService bpos = setupBPOSForNNs(mockNN1, mockNN2);
    bpos.start();
    try {
      waitForOneToFail(bpos);
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
   */
  private BPOfferService setupBPOSForNNs(DatanodeProtocol ... nns) {
    // Set up some fake InetAddresses, then override the connectToNN
    // function to return the corresponding proxies.

    final Map<InetSocketAddress, DatanodeProtocol> nnMap = Maps.newLinkedHashMap();
    for (int port = 0; port < nns.length; port++) {
      nnMap.put(new InetSocketAddress(port), nns[port]);
    }

    return new BPOfferService(Lists.newArrayList(nnMap.keySet()), mockDn) {
      @Override
      DatanodeProtocol connectToNN(InetSocketAddress nnAddr) throws IOException {
        DatanodeProtocol nn = nnMap.get(nnAddr);
        if (nn == null) {
          throw new AssertionError("bad NN addr: " + nnAddr);
        }
        return nn;
      }
    };
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
  
  private void waitForBlockReport(final DatanodeProtocol mockNN)
      throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          Mockito.verify(mockNN).blockReport(
              Mockito.<DatanodeRegistration>anyObject(),  
              Mockito.eq(FAKE_BPID),
              Mockito.<long[]>anyObject());
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
      DatanodeProtocol mockNN) throws Exception {
    final ArgumentCaptor<ReceivedDeletedBlockInfo[]> captor =
      ArgumentCaptor.forClass(ReceivedDeletedBlockInfo[].class);
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
    return captor.getValue();
  }

}
