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
package org.apache.hadoop.hdfs.server.namenode.ha;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.log4j.Level;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Makes sure that standby doesn't do the unnecessary block management such as
 * invalidate block, etc.
 */
public class TestStandbyBlockManagement {
  protected static final Log LOG = LogFactory.getLog(
      TestStandbyBlockManagement.class);
  private static final String TEST_FILE_DATA = "hello world";
  private static final String TEST_FILE = "/TestStandbyBlockManagement";
  private static final Path TEST_FILE_PATH = new Path(TEST_FILE);

  static {
    DFSTestUtil.setNameNodeLogLevel(Level.ALL);
  }

  @Test(timeout=60000)
  public void testInvalidateBlock() throws Exception {
    Configuration conf = new Configuration();
    HAUtil.setAllowStandbyReads(conf, true);
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(3)
        .build();
    try {
      cluster.waitActive();
      cluster.transitionToActive(0);

      NameNode nn1 = cluster.getNameNode(0);
      NameNode nn2 = cluster.getNameNode(1);

      FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);

      Thread.sleep(1000);
      LOG.info("==================================");
      DFSTestUtil.writeFile(fs, TEST_FILE_PATH, TEST_FILE_DATA);
      // Have to force an edit log roll so that the standby catches up
      nn1.getRpcServer().rollEditLog();
      LOG.info("==================================");

      // delete the file
      fs.delete(TEST_FILE_PATH, false);
      BlockManagerTestUtil.computeAllPendingWork(
          nn1.getNamesystem().getBlockManager());

      nn1.getRpcServer().rollEditLog();

      // standby nn doesn't need to invalidate blocks.
      assertEquals(0,
          nn2.getNamesystem().getBlockManager().getPendingDeletionBlocksCount());

      cluster.triggerHeartbeats();
      cluster.triggerBlockReports();

      // standby nn doesn't need to invalidate blocks.
      assertEquals(0,
          nn2.getNamesystem().getBlockManager().getPendingDeletionBlocksCount());

    } finally {
      cluster.shutdown();
    }
  }

}
