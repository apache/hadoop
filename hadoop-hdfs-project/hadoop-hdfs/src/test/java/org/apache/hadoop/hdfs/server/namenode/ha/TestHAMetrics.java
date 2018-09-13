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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Make sure HA-related metrics are updated and reported appropriately.
 */
public class TestHAMetrics {
  
  private static final Logger LOG =
      LoggerFactory.getLogger(TestHAMetrics.class);

  @Test(timeout = 300000)
  public void testHAMetrics() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, Integer.MAX_VALUE);

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology()).numDataNodes(1)
        .build();
    FileSystem fs = null;
    try {
      cluster.waitActive();
      
      FSNamesystem nn0 = cluster.getNamesystem(0);
      FSNamesystem nn1 = cluster.getNamesystem(1);
      
      assertEquals(nn0.getHAState(), "standby");
      assertTrue(0 < nn0.getMillisSinceLastLoadedEdits());
      assertEquals(nn1.getHAState(), "standby");
      assertTrue(0 < nn1.getMillisSinceLastLoadedEdits());

      cluster.transitionToActive(0);
      final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      final ObjectName mxbeanName =
          new ObjectName("Hadoop:service=NameNode,name=NameNodeStatus");
      final Long ltt1 =
          (Long) mbs.getAttribute(mxbeanName, "LastHATransitionTime");
      assertTrue("lastHATransitionTime should be > 0", ltt1 > 0);
      
      assertEquals("active", nn0.getHAState());
      assertEquals(0, nn0.getMillisSinceLastLoadedEdits());
      assertEquals("standby", nn1.getHAState());
      assertTrue(0 < nn1.getMillisSinceLastLoadedEdits());
      
      cluster.transitionToStandby(0);
      final Long ltt2 =
          (Long) mbs.getAttribute(mxbeanName, "LastHATransitionTime");
      assertTrue("lastHATransitionTime should be > " + ltt1, ltt2 > ltt1);
      cluster.transitionToActive(1);
      
      assertEquals("standby", nn0.getHAState());
      assertTrue(0 < nn0.getMillisSinceLastLoadedEdits());
      assertEquals("active", nn1.getHAState());
      assertEquals(0, nn1.getMillisSinceLastLoadedEdits());
      
      Thread.sleep(2000); // make sure standby gets a little out-of-date
      assertTrue(2000 <= nn0.getMillisSinceLastLoadedEdits());
      
      assertEquals(0, nn0.getPendingDataNodeMessageCount());
      assertEquals(0, nn1.getPendingDataNodeMessageCount());
      
      fs = HATestUtil.configureFailoverFs(cluster, conf);
      DFSTestUtil.createFile(fs, new Path("/foo"),
          10, (short)1, 1L);
      
      assertTrue(0 < nn0.getPendingDataNodeMessageCount());
      assertEquals(0, nn1.getPendingDataNodeMessageCount());
      long millisSinceLastLoadedEdits = nn0.getMillisSinceLastLoadedEdits();
      
      HATestUtil.waitForStandbyToCatchUp(cluster.getNameNode(1),
          cluster.getNameNode(0));
      
      assertEquals(0, nn0.getPendingDataNodeMessageCount());
      assertEquals(0, nn1.getPendingDataNodeMessageCount());
      long newMillisSinceLastLoadedEdits = nn0.getMillisSinceLastLoadedEdits();
      // Since we just waited for the standby to catch up, the time since we
      // last loaded edits should be very low.
      assertTrue("expected " + millisSinceLastLoadedEdits + " > " +
          newMillisSinceLastLoadedEdits,
          millisSinceLastLoadedEdits > newMillisSinceLastLoadedEdits);
    } finally {
      IOUtils.cleanupWithLogger(LOG, fs);
      cluster.shutdown();
    }
  }

  @Test
  public void testHAInodeCount() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, Integer.MAX_VALUE);

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology()).numDataNodes(1)
        .build();
    FileSystem fs = null;
    try {
      cluster.waitActive();

      FSNamesystem nn0 = cluster.getNamesystem(0);
      FSNamesystem nn1 = cluster.getNamesystem(1);

      cluster.transitionToActive(0);
      fs = HATestUtil.configureFailoverFs(cluster, conf);
      DFSTestUtil.createFile(fs, new Path("/testHAInodeCount1"),
          10, (short)1, 1L);
      DFSTestUtil.createFile(fs, new Path("/testHAInodeCount2"),
          10, (short)1, 1L);
      DFSTestUtil.createFile(fs, new Path("/testHAInodeCount3"),
          10, (short)1, 1L);
      DFSTestUtil.createFile(fs, new Path("/testHAInodeCount4"),
          10, (short)1, 1L);

      // 1 dir and 4 files
      assertEquals(5, nn0.getFilesTotal());
      // The SBN still has one dir, which is "/".
      assertEquals(1, nn1.getFilesTotal());

      // Save fsimage so that nn does not build up namesystem by replaying
      // edits, but load from the image.
      ((DistributedFileSystem)fs).setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      ((DistributedFileSystem)fs).saveNamespace();

      // Flip the two namenodes and restart the standby, which will load
      // the fsimage.
      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);
      cluster.restartNameNode(0);
      assertEquals(nn0.getHAState(), "standby");

      // The restarted standby should report the correct count
      nn0 = cluster.getNamesystem(0);
      assertEquals(5, nn0.getFilesTotal());
    } finally {
      IOUtils.cleanupWithLogger(LOG, fs);
      cluster.shutdown();
    }

  }
}
