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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.ipc.AlignmentContext;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class is used to test server sending state alignment information to clients
 * via RPC and likewise clients receiving and updating their last known
 * state alignment info.
 * These tests check that after a single RPC call a client will have caught up
 * to the most recent alignment state of the server.
 */
public class TestStateAlignmentContext {

  static final long BLOCK_SIZE = 64 * 1024;
  private static final int NUMDATANODES = 3;
  private static final Configuration CONF = new HdfsConfiguration();

  private static MiniDFSCluster cluster;
  private static DistributedFileSystem dfs;

  @BeforeClass
  public static void startUpCluster() throws IOException {
    // disable block scanner
    CONF.setInt(DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, -1);
    // Set short retry timeouts so this test runs faster
    CONF.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
    CONF.setBoolean("fs.hdfs.impl.disable.cache", true);
    cluster = new MiniDFSCluster.Builder(CONF).numDataNodes(NUMDATANODES)
        .build();
    cluster.waitActive();
  }

  @Before
  public void before() throws IOException {
    dfs = cluster.getFileSystem();
  }

  @AfterClass
  public static void shutDownCluster() throws IOException {
    if (dfs != null) {
      dfs.close();
      dfs = null;
    }
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @After
  public void after() throws IOException {
    dfs.close();
  }

  /**
   * This test checks if after a client writes we can see the state id in
   * updated via the response.
   */
  @Test
  public void testStateTransferOnWrite() throws Exception {
    long preWriteState = cluster.getNamesystem().getLastWrittenTransactionId();
    DFSTestUtil.writeFile(dfs, new Path("/testFile1"), "abc");
    long clientState = dfs.dfs.getAlignmentContext().getLastSeenStateId();
    long postWriteState = cluster.getNamesystem().getLastWrittenTransactionId();
    // Write(s) should have increased state. Check for greater than.
    assertThat(clientState > preWriteState, is(true));
    // Client and server state should be equal.
    assertThat(clientState, is(postWriteState));
  }

  /**
   * This test checks if after a client reads we can see the state id in
   * updated via the response.
   */
  @Test
  public void testStateTransferOnRead() throws Exception {
    DFSTestUtil.writeFile(dfs, new Path("/testFile2"), "123");
    long lastWrittenId = cluster.getNamesystem().getLastWrittenTransactionId();
    DFSTestUtil.readFile(dfs, new Path("/testFile2"));
    // Read should catch client up to last written state.
    long clientState = dfs.dfs.getAlignmentContext().getLastSeenStateId();
    assertThat(clientState, is(lastWrittenId));
  }

  /**
   * This test checks that a fresh client starts with no state and becomes
   * updated of state from RPC call.
   */
  @Test
  public void testStateTransferOnFreshClient() throws Exception {
    DFSTestUtil.writeFile(dfs, new Path("/testFile3"), "ezpz");
    long lastWrittenId = cluster.getNamesystem().getLastWrittenTransactionId();
    try (DistributedFileSystem clearDfs =
             (DistributedFileSystem) FileSystem.get(CONF)) {
      ClientGCIContext clientState = clearDfs.dfs.getAlignmentContext();
      assertThat(clientState.getLastSeenStateId(), is(Long.MIN_VALUE));
      DFSTestUtil.readFile(clearDfs, new Path("/testFile3"));
      assertThat(clientState.getLastSeenStateId(), is(lastWrittenId));
    }
  }

  /**
   * This test mocks an AlignmentContext and ensures that DFSClient
   * writes its lastSeenStateId into RPC requests.
   */
  @Test
  public void testClientSendsState() throws Exception {
    AlignmentContext alignmentContext = dfs.dfs.getAlignmentContext();
    AlignmentContext spiedAlignContext = Mockito.spy(alignmentContext);
    Client.setAlignmentContext(spiedAlignContext);

    // Collect RpcRequestHeaders for verification later.
    final List<RpcHeaderProtos.RpcRequestHeaderProto.Builder> collectedHeaders =
        new ArrayList<>();
    Mockito.doAnswer(a -> {
      Object[] arguments = a.getArguments();
      RpcHeaderProtos.RpcRequestHeaderProto.Builder header =
          (RpcHeaderProtos.RpcRequestHeaderProto.Builder) arguments[0];
      collectedHeaders.add(header);
      return a.callRealMethod();
    }).when(spiedAlignContext).updateRequestState(Mockito.any());

    DFSTestUtil.writeFile(dfs, new Path("/testFile4"), "shv");

    // Ensure first header and last header have different state.
    assertThat(collectedHeaders.size() > 1, is(true));
    assertThat(collectedHeaders.get(0).getStateId(),
        is(not(collectedHeaders.get(collectedHeaders.size() - 1))));

    // Ensure collected RpcRequestHeaders are in increasing order.
    long lastHeader = collectedHeaders.get(0).getStateId();
    for(RpcHeaderProtos.RpcRequestHeaderProto.Builder header :
        collectedHeaders.subList(1, collectedHeaders.size())) {
      long currentHeader = header.getStateId();
      assertThat(currentHeader >= lastHeader, is(true));
      lastHeader = header.getStateId();
    }
  }

  /**
   * This test mocks an AlignmentContext to send stateIds greater than
   * server's stateId in RPC requests.
   */
  @Test
  public void testClientSendsGreaterState() throws Exception {
    AlignmentContext alignmentContext = dfs.dfs.getAlignmentContext();
    AlignmentContext spiedAlignContext = Mockito.spy(alignmentContext);
    Client.setAlignmentContext(spiedAlignContext);

    // Make every client call have a stateId > server's stateId.
    Mockito.doAnswer(a -> {
      Object[] arguments = a.getArguments();
      RpcHeaderProtos.RpcRequestHeaderProto.Builder header =
          (RpcHeaderProtos.RpcRequestHeaderProto.Builder) arguments[0];
      try {
        return a.callRealMethod();
      } finally {
        header.setStateId(Long.MAX_VALUE);
      }
    }).when(spiedAlignContext).updateRequestState(Mockito.any());

    GenericTestUtils.LogCapturer logCapturer =
        GenericTestUtils.LogCapturer.captureLogs(FSNamesystem.LOG);
    DFSTestUtil.writeFile(dfs, new Path("/testFile4"), "shv");
    logCapturer.stopCapturing();

    String output = logCapturer.getOutput();
    assertThat(output, containsString("A client sent stateId: "));
  }

}
