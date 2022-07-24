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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.qjournal.client.AsyncLogger;
import org.apache.hadoop.hdfs.qjournal.client.DirectExecutorService;
import org.apache.hadoop.hdfs.qjournal.client.IPCLoggerChannel;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournaledEditsResponseProto;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerFaultInjector;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Futures;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter.getFileInfo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;

public class TestHAWithInProgressTail {
  private MiniQJMHACluster qjmhaCluster;
  private MiniDFSCluster cluster;
  private MiniJournalCluster jnCluster;
  private NameNode nn0;
  private NameNode nn1;

  @Before
  public void startUp() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_QJOURNAL_SELECT_INPUT_STREAMS_TIMEOUT_KEY, 500);
    HAUtil.setAllowStandbyReads(conf, true);
    qjmhaCluster = new MiniQJMHACluster.Builder(conf).build();
    cluster = qjmhaCluster.getDfsCluster();
    jnCluster = qjmhaCluster.getJournalCluster();

    // Get NameNode from cluster to future manual control
    nn0 = cluster.getNameNode(0);
    nn1 = cluster.getNameNode(1);
  }

  @After
  public void tearDown() throws IOException {
    if (qjmhaCluster != null) {
      qjmhaCluster.shutdown();
    }
  }


  /**
   * Test that Standby Node tails multiple segments while catching up
   * during the transition to Active.
   */
  @Test
  public void testFailoverWithAbnormalJN() throws Exception {
    cluster.transitionToActive(0);
    cluster.waitActive(0);

    BlockManagerFaultInjector.instance = new BlockManagerFaultInjector() {
      @Override
      public void mockJNStreams() throws IOException {
        spyOnJASjournal();
      }
    };

    // Stop EditlogTailer in Standby NameNode.
    cluster.getNameNode(1).getNamesystem().getEditLogTailer().stop();

    String p = "/testFailoverWhileTailingWithoutCache/";
    mkdirs(nn0, p + 0, p + 1, p + 2, p + 3, p + 4);
    mkdirs(nn0, p + 5, p + 6, p + 7, p + 8, p + 9);
    mkdirs(nn0, p + 10, p + 11, p + 12, p + 13, p + 14);

    cluster.transitionToStandby(0);

    cluster.transitionToActive(1);

    // we should read them in nn1.
    waitForFileInfo(nn1, p + 0, p + 1, p + 14);
  }

  private QuorumJournalManager createSpyingQJM(Configuration conf)
      throws IOException {
    AsyncLogger.Factory spyFactory = (conf1, nsInfo, journalId, nameServiceId, addr) -> {
      AsyncLogger logger = new IPCLoggerChannel(conf1, nsInfo, journalId,
          nameServiceId, addr) {
        protected ExecutorService createSingleThreadExecutor() {
          return new DirectExecutorService();
        }
      };
      return Mockito.spy(logger);
    };
    return new QuorumJournalManager(conf, jnCluster.getQuorumJournalURI("ns1"),
        nn1.getNamesystem().getNamespaceInfo(), "ns1", spyFactory);
  }

  private void spyOnJASjournal() throws IOException {
    JournalSet.JournalAndStream jas = nn1.getNamesystem().getEditLogTailer()
        .getEditLog().getJournalSet().getAllJournalStreams().get(0);

    JournalManager oldManager = jas.getManager();
    oldManager.close();

    // Create a SpyingQJM
    QuorumJournalManager manager = createSpyingQJM(nn1.getConf());
    manager.recoverUnfinalizedSegments();
    jas.setJournalForTests(manager);

    List<AsyncLogger> spies = manager.getLoggerSetForTests().getLoggersForTests();

    // Mock JN0 return an empty response.
    GetJournaledEditsResponseProto responseProto = GetJournaledEditsResponseProto
        .newBuilder().setTxnCount(0).build();
    ListenableFuture<GetJournaledEditsResponseProto> ret = Futures.immediateFuture(responseProto);
    Mockito.doReturn(ret).when(spies.get(0))
        .getJournaledEdits(eq(1L), eq(QuorumJournalManager.QJM_RPC_MAX_TXNS_DEFAULT));

    // Mock JN1 return a slow correct response.
    ListeningExecutorService service = MoreExecutors.listeningDecorator(
        Executors.newSingleThreadExecutor());
    Mockito.doAnswer(invocation -> service.submit(() -> {
      Thread.sleep(3000);
      ListenableFuture<?> future = null;
      try {
        future = (ListenableFuture<?>) invocation.callRealMethod();
      } catch (Throwable e) {
        fail("getJournaledEdits failed " + e.getMessage());
      }
      return future.get();
    })).when(spies.get(1))
        .getJournaledEdits(1, QuorumJournalManager.QJM_RPC_MAX_TXNS_DEFAULT);
  }

  /**
   * Create the given directories on the provided NameNode.
   */
  private static void mkdirs(NameNode nameNode, String... dirNames)
      throws Exception {
    for (String dirName : dirNames) {
      nameNode.getRpcServer().mkdirs(dirName,
          FsPermission.createImmutable((short) 0755), true);
    }
  }

  /**
   * Wait up to 1 second until the given NameNode is aware of the existing of
   * all of the provided fileNames.
   */
  private static void waitForFileInfo(NameNode nn, String... fileNames)
      throws Exception {
    for (String fileName : fileNames){
      assertNotNull(getFileInfo(nn, fileName, true, false, false));
    }
  }
}
