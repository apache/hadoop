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

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeAdapter;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Supplier;

/**
 * The hotornot.com of unit tests: makes sure that the standby not only
 * has namespace information, but also has the correct block reports, etc.
 */
public class TestStandbyIsHot {
  protected static final Log LOG = LogFactory.getLog(
      TestStandbyIsHot.class);
  private static final String TEST_FILE_DATA = "hello highly available world";
  private static final String TEST_FILE = "/testStandbyIsHot";
  private static final Path TEST_FILE_PATH = new Path(TEST_FILE);

  static {
    ((Log4JLogger)LogFactory.getLog(FSNamesystem.class)).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LogFactory.getLog(BlockManager.class)).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
  }

  @Test
  public void testStandbyIsHot() throws Exception {
    Configuration conf = new Configuration();
    // We read from the standby to watch block locations
    HAUtil.setAllowStandbyReads(conf, true);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .numDataNodes(3)
      .build();
    Runtime mockRuntime = mock(Runtime.class);
    try {
      cluster.waitActive();
      cluster.transitionToActive(0);
      
      NameNode nn1 = cluster.getNameNode(0);
      NameNode nn2 = cluster.getNameNode(1);
      
      nn2.getNamesystem().getEditLogTailer().setRuntime(mockRuntime);
      nn2.getNamesystem().getEditLogTailer().setSleepTime(250);
      nn2.getNamesystem().getEditLogTailer().interrupt();
      
      FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
      
      Thread.sleep(1000);
      System.err.println("==================================");
      DFSTestUtil.writeFile(fs, TEST_FILE_PATH, TEST_FILE_DATA);
      // Have to force an edit log roll so that the standby catches up
      nn1.getRpcServer().rollEditLog();
      System.err.println("==================================");

      // Block locations should show up on standby.
      LOG.info("Waiting for block locations to appear on standby node");
      waitForBlockLocations(cluster, nn2, TEST_FILE, 3);

      // Trigger immediate heartbeats and block reports so
      // that the active "trusts" all of the DNs
      cluster.triggerHeartbeats();
      cluster.triggerBlockReports();

      // Change replication
      LOG.info("Changing replication to 1");
      fs.setReplication(TEST_FILE_PATH, (short)1);
      waitForBlockLocations(cluster, nn1, TEST_FILE, 1);

      nn1.getRpcServer().rollEditLog();
      
      LOG.info("Waiting for lowered replication to show up on standby");
      waitForBlockLocations(cluster, nn2, TEST_FILE, 1);
      
      // Change back to 3
      LOG.info("Changing replication to 3");
      fs.setReplication(TEST_FILE_PATH, (short)3);
      nn1.getRpcServer().rollEditLog();
      
      LOG.info("Waiting for higher replication to show up on standby");
      waitForBlockLocations(cluster, nn2, TEST_FILE, 3);
      
    } finally {
      verify(mockRuntime, times(0)).exit(anyInt());
      cluster.shutdown();
    }
  }

  static void waitForBlockLocations(final MiniDFSCluster cluster,
      final NameNode nn,
      final String path, final int expectedReplicas)
      throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      
      @Override
      public Boolean get() {
        try {
          LocatedBlocks locs = NameNodeAdapter.getBlockLocations(nn, path, 0, 1000);
          DatanodeInfo[] dnis = locs.getLastLocatedBlock().getLocations();
          for (DatanodeInfo dni : dnis) {
            Assert.assertNotNull(dni);
          }
          int numReplicas = dnis.length;
          
          LOG.info("Got " + numReplicas + " locs: " + locs);
          if (numReplicas > expectedReplicas) {
            for (DataNode dn : cluster.getDataNodes()) {
              DataNodeAdapter.triggerDeletionReport(dn);
            }
          }
          return numReplicas == expectedReplicas;
        } catch (IOException e) {
          LOG.warn("No block locations yet: " + e.getMessage());
          return false;
        }
      }
    }, 500, 10000);
    
  }
}
