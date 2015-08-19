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

package org.apache.hadoop.hdfs.protocol;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs.BlockReportReplica;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReportResponseProto;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolPB;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.FinalizedReplica;
import org.apache.hadoop.hdfs.server.datanode.Replica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBeingWritten;
import org.apache.hadoop.hdfs.server.datanode.ReplicaWaitingToBeRecovered;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo.Capability;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class TestBlockListAsLongs {
  static Block b1 = new Block(1, 11, 111);
  static Block b2 = new Block(2, 22, 222);
  static Block b3 = new Block(3, 33, 333);
  static Block b4 = new Block(4, 44, 444);

  @Test
  public void testEmptyReport() {
    BlockListAsLongs blocks = checkReport();
    assertArrayEquals(
        new long[] {
            0, 0,
            -1, -1, -1 }, 
        blocks.getBlockListAsLongs());
  }

  @Test
  public void testFinalized() {
    BlockListAsLongs blocks = checkReport(
        new FinalizedReplica(b1, null, null));
    assertArrayEquals(
        new long[] {
            1, 0,
            1, 11, 111,
            -1, -1, -1 }, 
        blocks.getBlockListAsLongs());
  }

  @Test
  public void testUc() {
    BlockListAsLongs blocks = checkReport(
      new ReplicaBeingWritten(b1, null, null, null));
    assertArrayEquals(
        new long[] {
            0, 1,
            -1, -1, -1,
            1, 11, 111, ReplicaState.RBW.getValue() }, 
        blocks.getBlockListAsLongs());
  }
  
  @Test
  public void testMix() {
    BlockListAsLongs blocks = checkReport(
        new FinalizedReplica(b1, null, null),
        new FinalizedReplica(b2, null, null),
        new ReplicaBeingWritten(b3, null, null, null),
        new ReplicaWaitingToBeRecovered(b4, null, null));
    assertArrayEquals(
        new long[] {
            2, 2,
            1, 11, 111,
            2, 22, 222,
            -1, -1, -1,
            3, 33, 333, ReplicaState.RBW.getValue(),
            4, 44, 444, ReplicaState.RWR.getValue() },
        blocks.getBlockListAsLongs());
  }

  @Test
  public void testFuzz() throws InterruptedException {
    Replica[] replicas = new Replica[100000];
    Random rand = new Random(0);
    for (int i=0; i<replicas.length; i++) {
      Block b = new Block(rand.nextLong(), i, i<<4);
      switch (rand.nextInt(2)) {
        case 0:
          replicas[i] = new FinalizedReplica(b, null, null);
          break;
        case 1:
          replicas[i] = new ReplicaBeingWritten(b, null, null, null);
          break;
        case 2:
          replicas[i] = new ReplicaWaitingToBeRecovered(b, null, null);
          break;
      }
    }
    checkReport(replicas);
  }

  private BlockListAsLongs checkReport(Replica...replicas) {
    Map<Long, Replica> expectedReplicas = new HashMap<>();
    for (Replica replica : replicas) {
      expectedReplicas.put(replica.getBlockId(), replica);
    }
    expectedReplicas = Collections.unmodifiableMap(expectedReplicas);
    
    // encode the blocks and extract the buffers
    BlockListAsLongs blocks =
        BlockListAsLongs.encode(expectedReplicas.values());
    List<ByteString> buffers = blocks.getBlocksBuffers();
    
    // convert to old-style list of longs
    List<Long> longs = new ArrayList<Long>();
    for (long value : blocks.getBlockListAsLongs()) {
      longs.add(value);
    }

    // decode the buffers and verify its contents
    BlockListAsLongs decodedBlocks =
        BlockListAsLongs.decodeBuffers(expectedReplicas.size(), buffers);
    checkReplicas(expectedReplicas, decodedBlocks);

    // decode the long and verify its contents
    BlockListAsLongs decodedList = BlockListAsLongs.decodeLongs(longs);
    checkReplicas(expectedReplicas, decodedList);
    return blocks;
  }
  
  private void checkReplicas(Map<Long,Replica> expectedReplicas,
                             BlockListAsLongs decodedBlocks) {
    assertEquals(expectedReplicas.size(), decodedBlocks.getNumberOfBlocks());

    Map<Long, Replica> reportReplicas = new HashMap<>(expectedReplicas);
    for (BlockReportReplica replica : decodedBlocks) {
      assertNotNull(replica);
      Replica expected = reportReplicas.remove(replica.getBlockId());
      assertNotNull(expected);
      assertEquals("wrong bytes",
          expected.getNumBytes(), replica.getNumBytes());
      assertEquals("wrong genstamp",
          expected.getGenerationStamp(), replica.getGenerationStamp());
      assertEquals("wrong replica state",
          expected.getState(), replica.getState());
    }
    assertTrue(reportReplicas.isEmpty());
  }

  @Test
  public void testCapabilitiesInited() {
    NamespaceInfo nsInfo = new NamespaceInfo();
    assertTrue(
        nsInfo.isCapabilitySupported(Capability.STORAGE_BLOCK_REPORT_BUFFERS));
  }

  @Test
  public void testDatanodeDetect() throws ServiceException, IOException {
    final AtomicReference<BlockReportRequestProto> request =
        new AtomicReference<>();

    // just capture the outgoing PB
    DatanodeProtocolPB mockProxy = mock(DatanodeProtocolPB.class);
    doAnswer(new Answer<BlockReportResponseProto>() {
      public BlockReportResponseProto answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        request.set((BlockReportRequestProto) args[1]);
        return BlockReportResponseProto.newBuilder().build();
      }
    }).when(mockProxy).blockReport(any(RpcController.class),
                                   any(BlockReportRequestProto.class));
    
    @SuppressWarnings("resource")
    DatanodeProtocolClientSideTranslatorPB nn =
        new DatanodeProtocolClientSideTranslatorPB(mockProxy);

    DatanodeRegistration reg = DFSTestUtil.getLocalDatanodeRegistration();
    NamespaceInfo nsInfo = new NamespaceInfo(1, "cluster", "bp", 1);
    reg.setNamespaceInfo(nsInfo);

    Replica r = new FinalizedReplica(new Block(1, 2, 3), null, null);
    BlockListAsLongs bbl = BlockListAsLongs.encode(Collections.singleton(r));
    DatanodeStorage storage = new DatanodeStorage("s1");
    StorageBlockReport[] sbr = { new StorageBlockReport(storage, bbl) };    

    // check DN sends new-style BR
    request.set(null);
    nsInfo.setCapabilities(Capability.STORAGE_BLOCK_REPORT_BUFFERS.getMask());
    nn.blockReport(reg, "pool", sbr,
        new BlockReportContext(1, 0, System.nanoTime()));
    BlockReportRequestProto proto = request.get();
    assertNotNull(proto);
    assertTrue(proto.getReports(0).getBlocksList().isEmpty());
    assertFalse(proto.getReports(0).getBlocksBuffersList().isEmpty());
    
    // back up to prior version and check DN sends old-style BR
    request.set(null);
    nsInfo.setCapabilities(Capability.UNKNOWN.getMask());
    nn.blockReport(reg, "pool", sbr,
        new BlockReportContext(1, 0, System.nanoTime()));
    proto = request.get();
    assertNotNull(proto);
    assertFalse(proto.getReports(0).getBlocksList().isEmpty());
    assertTrue(proto.getReports(0).getBlocksBuffersList().isEmpty());
  }
}
