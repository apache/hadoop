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
import org.apache.hadoop.hdfs.server.namenode.ha.ClientHAProxyFactory;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.hdfs.server.namenode.ha.HAProxyFactory;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Class is used to test server sending state alignment information to clients
 * via RPC and likewise clients receiving and updating their last known
 * state alignment info.
 * These tests check that after a single RPC call a client will have caught up
 * to the most recent alignment state of the server.
 */
public class TestStateAlignmentContextWithHA {

  private static final int NUMDATANODES = 1;
  private static final int NUMCLIENTS = 10;
  private static final int NUMFILES = 300;
  private static final Configuration CONF = new HdfsConfiguration();
  private static final String NAMESERVICE = "nameservice";
  private static final List<ClientGSIContext> AC_LIST = new ArrayList<>();

  private static MiniDFSCluster cluster;
  private static List<Worker> clients;
  private static ClientGSIContext spy;

  private DistributedFileSystem dfs;
  private int active = 0;
  private int standby = 1;

  static class AlignmentContextProxyProvider<T>
      extends ConfiguredFailoverProxyProvider<T> {

    private ClientGSIContext alignmentContext;

    public AlignmentContextProxyProvider(
        Configuration conf, URI uri, Class<T> xface,
        HAProxyFactory<T> factory) throws IOException {
      super(conf, uri, xface, factory);

      // Create and set AlignmentContext in HAProxyFactory.
      // All proxies by factory will now have AlignmentContext assigned.
      this.alignmentContext = (spy != null ? spy : new ClientGSIContext());
      ((ClientHAProxyFactory) factory).setAlignmentContext(alignmentContext);

      AC_LIST.add(alignmentContext);
    }

    @Override // AbstractNNFailoverProxyProvider
    public synchronized ClientGSIContext getAlignmentContext() {
      return this.alignmentContext;
    }
  }

  static class SpyConfiguredContextProxyProvider<T>
      extends ConfiguredFailoverProxyProvider<T> {

    private ClientGSIContext alignmentContext;

    public SpyConfiguredContextProxyProvider(
        Configuration conf, URI uri, Class<T> xface,
        HAProxyFactory<T> factory) throws IOException {
      super(conf, uri, xface, factory);

      // Create but DON'T set in HAProxyFactory.
      this.alignmentContext = (spy != null ? spy : new ClientGSIContext());

      AC_LIST.add(alignmentContext);
    }
  }

  @BeforeClass
  public static void startUpCluster() throws IOException {
    // disable block scanner
    CONF.setInt(DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, -1);
    // Set short retry timeouts so this test runs faster
    CONF.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
    CONF.setBoolean("fs.hdfs.impl.disable.cache", true);

    MiniDFSNNTopology.NSConf nsConf = new MiniDFSNNTopology.NSConf(NAMESERVICE);
    nsConf.addNN(new MiniDFSNNTopology.NNConf("nn1"));
    nsConf.addNN(new MiniDFSNNTopology.NNConf("nn2"));

    cluster = new MiniDFSCluster.Builder(CONF).numDataNodes(NUMDATANODES)
        .nnTopology(MiniDFSNNTopology.simpleHATopology().addNameservice(nsConf))
        .build();
    cluster.waitActive();
    cluster.transitionToActive(0);
  }

  @Before
  public void before() throws IOException, URISyntaxException {
    killWorkers();
    HATestUtil.setFailoverConfigurations(cluster, CONF, NAMESERVICE, 0);
    CONF.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX +
        "." + NAMESERVICE, AlignmentContextProxyProvider.class.getName());
    dfs = (DistributedFileSystem) FileSystem.get(CONF);
  }

  @AfterClass
  public static void shutDownCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @After
  public void after() throws IOException {
    cluster.transitionToStandby(1);
    cluster.transitionToActive(0);
    active = 0;
    standby = 1;
    if (dfs != null) {
      dfs.close();
      dfs = null;
    }
    AC_LIST.clear();
    spy = null;
  }

  /**
   * This test checks if after a client writes we can see the state id in
   * updated via the response.
   */
  @Test
  public void testNoStateOnConfiguredProxyProvider() throws Exception {
    Configuration confCopy = new Configuration(CONF);
    confCopy.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX +
        "." + NAMESERVICE, SpyConfiguredContextProxyProvider.class.getName());

    try (DistributedFileSystem clearDfs =
             (DistributedFileSystem) FileSystem.get(confCopy)) {
      ClientGSIContext clientState = getContext(1);
      assertThat(clientState.getLastSeenStateId(), is(Long.MIN_VALUE));
      DFSTestUtil.writeFile(clearDfs, new Path("/testFileNoState"), "no_state");
      assertThat(clientState.getLastSeenStateId(), is(Long.MIN_VALUE));
    }
  }

  /**
   * This test checks if after a client writes we can see the state id in
   * updated via the response.
   */
  @Test
  public void testStateTransferOnWrite() throws Exception {
    long preWriteState =
        cluster.getNamesystem(active).getLastWrittenTransactionId();
    DFSTestUtil.writeFile(dfs, new Path("/testFile1"), "abc");
    long clientState = getContext(0).getLastSeenStateId();
    long postWriteState =
        cluster.getNamesystem(active).getLastWrittenTransactionId();
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
    long lastWrittenId =
        cluster.getNamesystem(active).getLastWrittenTransactionId();
    DFSTestUtil.readFile(dfs, new Path("/testFile2"));
    // Read should catch client up to last written state.
    long clientState = getContext(0).getLastSeenStateId();
    assertThat(clientState, is(lastWrittenId));
  }

  /**
   * This test checks that a fresh client starts with no state and becomes
   * updated of state from RPC call.
   */
  @Test
  public void testStateTransferOnFreshClient() throws Exception {
    DFSTestUtil.writeFile(dfs, new Path("/testFile3"), "ezpz");
    long lastWrittenId =
        cluster.getNamesystem(active).getLastWrittenTransactionId();
    try (DistributedFileSystem clearDfs =
             (DistributedFileSystem) FileSystem.get(CONF)) {
      ClientGSIContext clientState = getContext(1);
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
    ClientGSIContext alignmentContext = new ClientGSIContext();
    ClientGSIContext spiedAlignContext = Mockito.spy(alignmentContext);
    spy = spiedAlignContext;

    try (DistributedFileSystem clearDfs =
             (DistributedFileSystem) FileSystem.get(CONF)) {

      // Collect RpcRequestHeaders for verification later.
      final List<RpcHeaderProtos.RpcRequestHeaderProto.Builder> headers =
          new ArrayList<>();
      Mockito.doAnswer(a -> {
        Object[] arguments = a.getArguments();
        RpcHeaderProtos.RpcRequestHeaderProto.Builder header =
            (RpcHeaderProtos.RpcRequestHeaderProto.Builder) arguments[0];
        headers.add(header);
        return a.callRealMethod();
      }).when(spiedAlignContext).updateRequestState(Mockito.any());

      DFSTestUtil.writeFile(clearDfs, new Path("/testFile4"), "shv");

      // Ensure first header and last header have different state.
      assertThat(headers.size() > 1, is(true));
      assertThat(headers.get(0).getStateId(),
          is(not(headers.get(headers.size() - 1))));

      // Ensure collected RpcRequestHeaders are in increasing order.
      long lastHeader = headers.get(0).getStateId();
      for (RpcHeaderProtos.RpcRequestHeaderProto.Builder header :
          headers.subList(1, headers.size())) {
        long currentHeader = header.getStateId();
        assertThat(currentHeader >= lastHeader, is(true));
        lastHeader = header.getStateId();
      }
    }
  }

  /**
   * This test mocks an AlignmentContext to send stateIds greater than
   * server's stateId in RPC requests.
   */
  @Test
  public void testClientSendsGreaterState() throws Exception {
    ClientGSIContext alignmentContext = new ClientGSIContext();
    ClientGSIContext spiedAlignContext = Mockito.spy(alignmentContext);
    spy = spiedAlignContext;

    try (DistributedFileSystem clearDfs =
             (DistributedFileSystem) FileSystem.get(CONF)) {

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

      DFSTestUtil.writeFile(clearDfs, new Path("/testFile4"), "shv");
      logCapturer.stopCapturing();

      String output = logCapturer.getOutput();
      assertThat(output, containsString("A client sent stateId: "));
    }
  }

  /**
   * This test checks if after a client writes we can see the state id in
   * updated via the response.
   */
  @Test
  public void testStateTransferOnWriteWithFailover() throws Exception {
    long preWriteState =
        cluster.getNamesystem(active).getLastWrittenTransactionId();
    // Write using HA client.
    DFSTestUtil.writeFile(dfs, new Path("/testFile1FO"), "123");
    long clientState = getContext(0).getLastSeenStateId();
    long postWriteState =
        cluster.getNamesystem(active).getLastWrittenTransactionId();
    // Write(s) should have increased state. Check for greater than.
    assertThat(clientState > preWriteState, is(true));
    // Client and server state should be equal.
    assertThat(clientState, is(postWriteState));

    // Failover NameNode.
    failOver();

    // Write using HA client.
    DFSTestUtil.writeFile(dfs, new Path("/testFile2FO"), "456");
    long clientStateFO = getContext(0).getLastSeenStateId();
    long writeStateFO =
        cluster.getNamesystem(active).getLastWrittenTransactionId();

    // Write(s) should have increased state. Check for greater than.
    assertThat(clientStateFO > postWriteState, is(true));
    // Client and server state should be equal.
    assertThat(clientStateFO, is(writeStateFO));
  }

  @Test(timeout=300000)
  public void testMultiClientStatesWithRandomFailovers() throws Exception {
    // We want threads to run during failovers; assuming at minimum 4 cores,
    // would like to see 2 clients competing against 2 NameNodes.
    ExecutorService execService = Executors.newFixedThreadPool(2);
    clients = new ArrayList<>(NUMCLIENTS);
    for (int i = 1; i <= NUMCLIENTS; i++) {
      DistributedFileSystem haClient =
          (DistributedFileSystem) FileSystem.get(CONF);
      clients.add(new Worker(haClient, NUMFILES, "/testFile3FO_", i));
    }

    // Execute workers in threadpool with random failovers.
    List<Future<STATE>> futures = submitAll(execService, clients);
    execService.shutdown();

    boolean finished = false;
    while (!finished) {
      failOver();
      finished = execService.awaitTermination(1L, TimeUnit.SECONDS);
    }

    // Validation.
    for (Future<STATE> future : futures) {
      assertThat(future.get(), is(STATE.SUCCESS));
    }
  }

  private ClientGSIContext getContext(int clientCreationIndex) {
    return AC_LIST.get(clientCreationIndex);
  }

  private void failOver() throws IOException {
    cluster.transitionToStandby(active);
    cluster.transitionToActive(standby);
    int tempActive = active;
    active = standby;
    standby = tempActive;
  }

  /* Executor.invokeAll() is blocking so utilizing submit instead. */
  private static List<Future<STATE>> submitAll(ExecutorService executor,
                                              Collection<Worker> calls) {
    List<Future<STATE>> futures = new ArrayList<>(calls.size());
    for (Worker call : calls) {
      Future<STATE> future = executor.submit(call);
      futures.add(future);
    }
    return futures;
  }

  private void killWorkers() throws IOException {
    if (clients != null) {
      for(Worker worker : clients) {
        worker.kill();
      }
      clients = null;
    }
  }

  private enum STATE { SUCCESS, FAIL, ERROR }

  private class Worker implements Callable<STATE> {
    private final DistributedFileSystem client;
    private final int filesToMake;
    private String filePath;
    private final int nonce;

    Worker(DistributedFileSystem client,
           int filesToMake,
           String filePath,
           int nonce) {
      this.client = client;
      this.filesToMake = filesToMake;
      this.filePath = filePath;
      this.nonce = nonce;
    }

    @Override
    public STATE call() {
      try {
        for (int i = 0; i < filesToMake; i++) {
          long preClientStateFO =
              getContext(nonce).getLastSeenStateId();

          // Write using HA client.
          Path path = new Path(filePath + nonce + i);
          DFSTestUtil.writeFile(client, path, "erk");

          long postClientStateFO =
              getContext(nonce).getLastSeenStateId();

          // Write(s) should have increased state. Check for greater than.
          if (postClientStateFO <= preClientStateFO) {
            System.out.println("FAIL: Worker started with: " +
                preClientStateFO + ", but finished with: " + postClientStateFO);
            return STATE.FAIL;
          }
        }
        client.close();
        return STATE.SUCCESS;
      } catch (IOException e) {
        System.out.println("ERROR: Worker failed with: " + e);
        return STATE.ERROR;
      }
    }

    public void kill() throws IOException {
      client.dfs.closeAllFilesBeingWritten(true);
      client.dfs.closeOutputStreams(true);
      client.dfs.closeConnectionToNamenode();
      client.dfs.close();
      client.close();
    }
  }
}
