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

import java.net.BindException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_CHECK_INTERVAL_MS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.NameNodeEditLogRoller;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.base.Supplier;

@RunWith(Parameterized.class)
public class TestEditLogAutoroll {
  static {
    GenericTestUtils.setLogLevel(FSEditLog.LOG, Level.DEBUG);
  }

  @Parameters
  public static Collection<Object[]> data() {
    Collection<Object[]> params = new ArrayList<Object[]>();
    params.add(new Object[]{ Boolean.FALSE });
    params.add(new Object[]{ Boolean.TRUE });
    return params;
  }

  private static boolean useAsyncEditLog;
  public TestEditLogAutoroll(Boolean async) {
    useAsyncEditLog = async;
  }

  private Configuration conf;
  private MiniDFSCluster cluster;
  private NameNode nn0;
  private FileSystem fs;
  private FSEditLog editLog;
  private final Random random = new Random();

  public static final Logger LOG = LoggerFactory.getLogger(FSEditLog.class);

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    // Stall the standby checkpointer in two ways
    conf.setLong(DFS_NAMENODE_CHECKPOINT_PERIOD_KEY, Long.MAX_VALUE);
    conf.setLong(DFS_NAMENODE_CHECKPOINT_TXNS_KEY, 20);
    // Make it autoroll after 10 edits
    conf.setFloat(DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD, 0.5f);
    conf.setInt(DFS_NAMENODE_EDIT_LOG_AUTOROLL_CHECK_INTERVAL_MS, 100);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_EDITS_ASYNC_LOGGING,
        useAsyncEditLog);

    int retryCount = 0;
    while (true) {
      try {
        int basePort = 10060 + random.nextInt(100) * 2;
        MiniDFSNNTopology topology = new MiniDFSNNTopology()
            .addNameservice(new MiniDFSNNTopology.NSConf("ns1")
                .addNN(new MiniDFSNNTopology.NNConf("nn1").setHttpPort(basePort))
                .addNN(new MiniDFSNNTopology.NNConf("nn2").setHttpPort(basePort + 1)));

        cluster = new MiniDFSCluster.Builder(conf)
            .nnTopology(topology)
            .numDataNodes(0)
            .build();
        cluster.waitActive();

        nn0 = cluster.getNameNode(0);
        fs = HATestUtil.configureFailoverFs(cluster, conf);

        cluster.transitionToActive(0);

        fs = cluster.getFileSystem(0);
        editLog = nn0.getNamesystem().getEditLog();
        ++retryCount;
        break;
      } catch (BindException e) {
        LOG.info("Set up MiniDFSCluster failed due to port conflicts, retry "
            + retryCount + " times");
      }
    }
  }

  @After
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.close();
      fs = null;
    }
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test(timeout=60000)
  public void testEditLogAutoroll() throws Exception {
    // Make some edits
    final long startTxId = editLog.getCurSegmentTxId();
    for (int i=0; i<11; i++) {
      fs.mkdirs(new Path("testEditLogAutoroll-" + i));
    }
    // Wait for the NN to autoroll
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return editLog.getCurSegmentTxId() > startTxId;
      }
    }, 1000, 5000);
    // Transition to standby and make sure the roller stopped
    nn0.transitionToStandby();
    GenericTestUtils.assertNoThreadsMatching(
        ".*" + NameNodeEditLogRoller.class.getSimpleName() + ".*");
  }
}
