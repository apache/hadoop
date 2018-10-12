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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.server.namenode.ha.HAProxyFactory;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.namenode.ha.ObserverReadProxyProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  public static final Logger LOG =
      LoggerFactory.getLogger(TestStateAlignmentContextWithHA.class.getName());

  private static final int NUMDATANODES = 1;
  private static final int NUMCLIENTS = 10;
  private static final int NUMFILES = 120;
  private static final Configuration CONF = new HdfsConfiguration();
  private static final List<ClientGSIContext> AC_LIST = new ArrayList<>();

  private static MiniQJMHACluster qjmhaCluster;
  private static MiniDFSCluster cluster;
  private static List<Worker> clients;

  private DistributedFileSystem dfs;
  private int active = 0;
  private int standby = 1;

  static class ORPPwithAlignmentContexts<T extends ClientProtocol>
      extends ObserverReadProxyProvider<T> {

    public ORPPwithAlignmentContexts(
        Configuration conf, URI uri, Class<T> xface,
        HAProxyFactory<T> factory) throws IOException {
      super(conf, uri, xface, factory);

      AC_LIST.add((ClientGSIContext) getAlignmentContext());
    }
  }

  @BeforeClass
  public static void startUpCluster() throws IOException {
    // Set short retry timeouts so this test runs faster
    CONF.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
    CONF.setBoolean(String.format(
        "fs.%s.impl.disable.cache", HdfsConstants.HDFS_URI_SCHEME), true);
    CONF.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, NUMDATANODES);

    qjmhaCluster = HATestUtil.setUpObserverCluster(CONF, 1, NUMDATANODES, true);
    cluster = qjmhaCluster.getDfsCluster();
  }

  @Before
  public void before() throws IOException, URISyntaxException {
    dfs = HATestUtil.configureObserverReadFs(
        cluster, CONF, ORPPwithAlignmentContexts.class, true);
  }

  @AfterClass
  public static void shutDownCluster() throws IOException {
    if (qjmhaCluster != null) {
      qjmhaCluster.shutdown();
    }
  }

  @After
  public void after() throws IOException {
    killWorkers();
    cluster.transitionToStandby(1);
    cluster.transitionToActive(0);
    active = 0;
    standby = 1;
    if (dfs != null) {
      dfs.close();
      dfs = null;
    }
    AC_LIST.clear();
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
    assertTrue(clientState > preWriteState);
    // Client and server state should be equal.
    assertEquals(clientState, postWriteState);
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
    assertEquals(clientState, lastWrittenId);
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
    try (DistributedFileSystem clearDfs = HATestUtil.configureObserverReadFs(
            cluster, CONF, ORPPwithAlignmentContexts.class, true);) {
      ClientGSIContext clientState = getContext(1);
      assertEquals(clientState.getLastSeenStateId(), Long.MIN_VALUE);
      DFSTestUtil.readFile(clearDfs, new Path("/testFile3"));
      assertEquals(clientState.getLastSeenStateId(), lastWrittenId);
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
    assertTrue(clientState > preWriteState);
    // Client and server state should be equal.
    assertEquals(clientState, postWriteState);

    // Failover NameNode.
    failOver();

    // Write using HA client.
    DFSTestUtil.writeFile(dfs, new Path("/testFile2FO"), "456");
    long clientStateFO = getContext(0).getLastSeenStateId();
    long writeStateFO =
        cluster.getNamesystem(active).getLastWrittenTransactionId();

    // Write(s) should have increased state. Check for greater than.
    assertTrue(clientStateFO > postWriteState);
    // Client and server state should be equal.
    assertEquals(clientStateFO, writeStateFO);
  }

  @Test(timeout=300000)
  public void testMultiClientStatesWithRandomFailovers() throws Exception {
    // First run, half the load, with one failover.
    runClientsWithFailover(1, NUMCLIENTS/2, NUMFILES/2);
    // Second half, with fail back.
    runClientsWithFailover(NUMCLIENTS/2 + 1, NUMCLIENTS, NUMFILES/2);
  }

  private void runClientsWithFailover(int clientStartId,
                                      int numClients,
                                      int numFiles)
      throws Exception {
    ExecutorService execService = Executors.newFixedThreadPool(2);
    clients = new ArrayList<>(numClients);
    for (int i = clientStartId; i <= numClients; i++) {
      DistributedFileSystem haClient = HATestUtil.configureObserverReadFs(
          cluster, CONF, ORPPwithAlignmentContexts.class, true);
      clients.add(new Worker(haClient, numFiles, "/testFile3FO_", i));
    }

    // Execute workers in threadpool with random failovers.
    List<Future<STATE>> futures = submitAll(execService, clients);
    execService.shutdown();

    boolean finished = false;
    failOver();

    while (!finished) {
      finished = execService.awaitTermination(20L, TimeUnit.SECONDS);
    }

    // Validation.
    for (Future<STATE> future : futures) {
      assertEquals(future.get(), STATE.SUCCESS);
    }

    clients.clear();
  }

  private ClientGSIContext getContext(int clientCreationIndex) {
    return AC_LIST.get(clientCreationIndex);
  }

  private void failOver() throws IOException {
    LOG.info("Transitioning Active to Standby");
    cluster.transitionToStandby(active);
    LOG.info("Transitioning Standby to Active");
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
      int i = -1;
      try {
        for (i = 0; i < filesToMake; i++) {
          ClientGSIContext gsiContext = getContext(nonce);
          long preClientStateFO = gsiContext.getLastSeenStateId();

          // Write using HA client.
          Path path = new Path(filePath + nonce + "_" + i);
          DFSTestUtil.writeFile(client, path, "erk");

          long postClientStateFO = gsiContext.getLastSeenStateId();

          // Write(s) should have increased state. Check for greater than.
          if (postClientStateFO < 0 || postClientStateFO <= preClientStateFO) {
            LOG.error("FAIL: Worker started with: {} , but finished with: {}",
                preClientStateFO, postClientStateFO);
            return STATE.FAIL;
          }

          if(i % (NUMFILES/10) == 0) {
            LOG.info("Worker {} created {} files", nonce, i);
            LOG.info("LastSeenStateId = {}", postClientStateFO);
          }
        }
        return STATE.SUCCESS;
      } catch (Exception e) {
        LOG.error("ERROR: Worker failed with: ", e);
        return STATE.ERROR;
      } finally {
        LOG.info("Worker {} created {} files", nonce, i);
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
