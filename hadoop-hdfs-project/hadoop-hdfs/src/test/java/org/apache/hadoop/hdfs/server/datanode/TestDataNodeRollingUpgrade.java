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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.log4j.Level;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Ensure that the DataNode correctly handles rolling upgrade
 * finalize and rollback.
 */
public class TestDataNodeRollingUpgrade {
  private static final Log LOG = LogFactory.getLog(TestDataNodeRollingUpgrade.class);

  private static final short REPL_FACTOR = 1;
  private static final int BLOCK_SIZE = 1024 * 1024;
  private static final long FILE_SIZE = BLOCK_SIZE * 4;
  private static final long SEED = 0x1BADF00DL;

  Configuration conf;
  MiniDFSCluster cluster = null;
  DistributedFileSystem fs;

  private void runCmd(DFSAdmin dfsadmin, String... args) throws Exception {
    assertThat(dfsadmin.run(args), is(0));
  }

  private void startRollingUpgrade() throws Exception {
    LOG.info("Starting rolling upgrade");
    final DFSAdmin dfsadmin = new DFSAdmin(conf);
    runCmd(dfsadmin, "-rollingUpgrade", "start");
  }

  private void finalizeRollingUpgrade() throws Exception {
    LOG.info("Finalizing rolling upgrade");
    final DFSAdmin dfsadmin = new DFSAdmin(conf);
    runCmd(dfsadmin, "-rollingUpgrade", "finalize");
  }

  private void rollbackRollingUpgrade() throws Exception {
    LOG.info("Starting rollback of the rolling upgrade");

    // Shutdown the DN and the NN in preparation for rollback.
    DataNodeProperties dnprop = cluster.stopDataNode(0);
    cluster.shutdownNameNodes();

    // Restart the daemons with rollback flags.
    cluster.restartNameNode("-rollingupgrade", "rollback");
    dnprop.setDnArgs("-rollingupgrade", "rollback");
    cluster.restartDataNode(dnprop);
    cluster.waitActive();
  }

  @Test (timeout=600000)
  public void testDatanodeRollingUpgradeWithFinalize() throws Exception {
    // start a cluster
    try {
      // Start a cluster.
      conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPL_FACTOR).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();
      Path testFile1 = new Path("/TestDataNodeRollingUpgrade1.dat");
      Path testFile2 = new Path("/TestDataNodeRollingUpgrade2.dat");

      // Create files in DFS.
      DFSTestUtil.createFile(fs, testFile1, FILE_SIZE, REPL_FACTOR, SEED);
      DFSTestUtil.createFile(fs, testFile2, FILE_SIZE, REPL_FACTOR, SEED);

      startRollingUpgrade();

      // Sleep briefly so that DN learns of the rolling upgrade
      // from heartbeats.
      cluster.triggerHeartbeats();
      Thread.sleep(5000);

      fs.delete(testFile2, false);

      // Sleep briefly so that block files can be moved to trash
      // (this is scheduled for asynchronous execution).
      cluster.triggerBlockReports();
      Thread.sleep(5000);

      finalizeRollingUpgrade();

      // Ensure that testFile2 stays deleted.
      assert(!fs.exists(testFile2));
      assert(fs.exists(testFile1));

    } finally {
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
    }
  }

  @Test (timeout=600000)
  public void testDatanodeRollingUpgradeWithRollback() throws Exception {
    // start a cluster
    try {
      // Start a cluster.
      conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPL_FACTOR).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();
      Path testFile1 = new Path("/TestDataNodeRollingUpgrade1.dat");

      // Create files in DFS.
      DFSTestUtil.createFile(fs, testFile1, BLOCK_SIZE, BLOCK_SIZE, FILE_SIZE, REPL_FACTOR, SEED);
      String fileContents1 = DFSTestUtil.readFile(fs, testFile1);

      startRollingUpgrade();

      // Sleep briefly so that DN learns of the rolling upgrade
      // from heartbeats.
      cluster.triggerHeartbeats();
      Thread.sleep(5000);

      LOG.info("Deleting file during rolling upgrade");
      fs.delete(testFile1, false);

      // Sleep briefly so that block files can be moved to trash
      // (this is scheduled for asynchronous execution).
      cluster.triggerBlockReports();
      Thread.sleep(5000);
      assert(!fs.exists(testFile1));

      // Now perform a rollback to restore DFS to the pre-rollback state.
      rollbackRollingUpgrade();

      // Ensure that testFile1 was restored after the rollback.
      assert(fs.exists(testFile1));
      String fileContents2 = DFSTestUtil.readFile(fs, testFile1);

      // Ensure that file contents are the same.
      assertThat(fileContents1, is(fileContents2));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
    }
  }
}
