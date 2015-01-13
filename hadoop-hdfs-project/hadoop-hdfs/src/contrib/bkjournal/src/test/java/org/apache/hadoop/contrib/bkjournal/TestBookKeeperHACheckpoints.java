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
package org.apache.hadoop.contrib.bkjournal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.namenode.ha.TestStandbyCheckpoints;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Runs the same tests as TestStandbyCheckpoints, but
 * using a bookkeeper journal manager as the shared directory
 */
public class TestBookKeeperHACheckpoints extends TestStandbyCheckpoints {
  private static BKJMUtil bkutil = null;
  static int numBookies = 3;
  static int journalCount = 0;

  @SuppressWarnings("rawtypes")
  @Override
  @Before
  public void setupCluster() throws Exception {
    Configuration conf = setupCommonConfig();
    conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY,
             BKJMUtil.createJournalURI("/checkpointing" + journalCount++)
             .toString());
    BKJMUtil.addJournalManagerDefinition(conf);
    MiniDFSNNTopology topology = new MiniDFSNNTopology()
      .addNameservice(new MiniDFSNNTopology.NSConf("ns1")
        .addNN(new MiniDFSNNTopology.NNConf("nn1").setHttpPort(10001))
        .addNN(new MiniDFSNNTopology.NNConf("nn2").setHttpPort(10002)));

    cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(topology)
      .numDataNodes(1)
      .manageNameDfsSharedDirs(false)
      .build();
    cluster.waitActive();

    nn0 = cluster.getNameNode(0);
    nn1 = cluster.getNameNode(1);
    fs = HATestUtil.configureFailoverFs(cluster, conf);

    cluster.transitionToActive(0);
  }

  @BeforeClass
  public static void startBK() throws Exception {
    journalCount = 0;
    bkutil = new BKJMUtil(numBookies);
    bkutil.start();
  }

  @AfterClass
  public static void shutdownBK() throws Exception {
    if (bkutil != null) {
      bkutil.teardown();
    }
  }

  @Override
  public void testCheckpointCancellation() throws Exception {
    // Overriden as the implementation in the superclass assumes that writes
    // are to a file. This should be fixed at some point
  }
}
