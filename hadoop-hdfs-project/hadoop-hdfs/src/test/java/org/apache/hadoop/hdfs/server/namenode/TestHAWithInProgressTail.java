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
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.qjournal.client.SpyQJournalUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter.getFileInfo;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.spy;

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

    // Stop EditlogTailer in Standby NameNode.
    cluster.getNameNode(1).getNamesystem().getEditLogTailer().stop();

    String p = "/testFailoverWhileTailingWithoutCache/";
    nn0.getRpcServer().mkdirs(p + 0, FsPermission.getCachePoolDefault(), true);

    cluster.transitionToStandby(0);
    spyFSEditLog();
    cluster.transitionToActive(1);

    // we should read them in nn1.
    assertNotNull(getFileInfo(nn1, p + 0, true, false, false));
  }

  private void spyFSEditLog() throws IOException {
    FSEditLog spyEditLog = spy(nn1.getNamesystem().getFSImage().getEditLog());
    Mockito.doAnswer(invocation -> {
      invocation.callRealMethod();
      spyOnJASjournal(spyEditLog.getJournalSet());
      return null;
    }).when(spyEditLog).recoverUnclosedStreams(anyBoolean());

    DFSTestUtil.setEditLogForTesting(nn1.getNamesystem(), spyEditLog);
    nn1.getNamesystem().getEditLogTailer().setEditLog(spyEditLog);
  }

  private void spyOnJASjournal(JournalSet journalSet) throws IOException {
    JournalSet.JournalAndStream jas = journalSet.getAllJournalStreams().get(0);
    JournalManager oldManager = jas.getManager();
    oldManager.close();

    // Create a SpyingQJM
    QuorumJournalManager manager = SpyQJournalUtil.createSpyingQJM(nn1.getConf(),
        jnCluster.getQuorumJournalURI("ns1"),
        nn1.getNamesystem().getNamespaceInfo(), "ns1");
    manager.recoverUnfinalizedSegments();
    jas.setJournalForTests(manager);

    SpyQJournalUtil.mockJNWithEmptyOrSlowResponse(manager, 1);
  }
}
