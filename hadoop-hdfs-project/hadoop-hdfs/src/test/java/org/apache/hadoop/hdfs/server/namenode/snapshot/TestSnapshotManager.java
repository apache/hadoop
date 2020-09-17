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

package org.apache.hadoop.hdfs.server.namenode.snapshot;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodesInPath;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;


/**
 * Testing snapshot manager functionality.
 */
public class TestSnapshotManager {
  private static final int testMaxSnapshotIDLimit = 7;

  /**
   * Test that the global limit on snapshot Ids is honored.
   */
  @Test (timeout=10000)
  public void testSnapshotIDLimits() throws Exception {
    testMaxSnapshotLimit(testMaxSnapshotIDLimit, "rollover",
        new Configuration(), testMaxSnapshotIDLimit);
  }

  /**
   * Tests that the global limit on snapshots is honored.
   */
  @Test (timeout=10000)
  public void testMaxSnapshotLimit() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_FILESYSTEM_LIMIT,
        testMaxSnapshotIDLimit);
    conf.setInt(DFSConfigKeys.
            DFS_NAMENODE_SNAPSHOT_MAX_LIMIT,
        testMaxSnapshotIDLimit);
    testMaxSnapshotLimit(testMaxSnapshotIDLimit,"file system snapshot limit" ,
        conf, testMaxSnapshotIDLimit * 2);
  }

  private void testMaxSnapshotLimit(int maxSnapshotLimit, String errMsg,
                                    Configuration conf, int maxSnapID)
      throws IOException {
    LeaseManager leaseManager = mock(LeaseManager.class);
    INodeDirectory ids = mock(INodeDirectory.class);
    FSDirectory fsdir = mock(FSDirectory.class);
    INodesInPath iip = mock(INodesInPath.class);

    SnapshotManager sm = spy(new SnapshotManager(conf, fsdir));
    doReturn(ids).when(sm).getSnapshottableRoot(any());
    doReturn(maxSnapID).when(sm).getMaxSnapshotID();
    doReturn(true).when(fsdir).isImageLoaded();

    // Create testMaxSnapshotLimit snapshots. These should all succeed.
    //
    for (Integer i = 0; i < maxSnapshotLimit; ++i) {
      sm.createSnapshot(leaseManager, iip, "dummy", i.toString(), Time.now());
    }

    // Attempt to create one more snapshot. This should fail due to snapshot
    // ID rollover.
    //
    try {
      sm.createSnapshot(leaseManager, iip, "dummy", "shouldFailSnapshot",
          Time.now());
      Assert.fail("Expected SnapshotException not thrown");
    } catch (SnapshotException se) {
      Assert.assertTrue(
          StringUtils.toLowerCase(se.getMessage()).contains(errMsg));
    }

    // Delete a snapshot to free up a slot.
    //
    sm.deleteSnapshot(iip, "", mock(INode.ReclaimContext.class), Time.now());

    // Attempt to create a snapshot again. It should still fail due
    // to snapshot ID rollover.
    //

    try {
      sm.createSnapshot(leaseManager, iip, "dummy", "shouldFailSnapshot2",
          Time.now());
      // in case the snapshot ID limit is hit, further creation of snapshots
      // even post deletions of snapshots won't succeed
      if (maxSnapID < maxSnapshotLimit) {
        Assert.fail("CreateSnapshot should succeed");
      }
    } catch (SnapshotException se) {
      Assert.assertTrue(
          StringUtils.toLowerCase(se.getMessage()).contains(errMsg));
    }
  }
  /**
   *  Snapshot is identified by INODE CURRENT_STATE_ID.
   *  So maximum allowable snapshotID should be less than CURRENT_STATE_ID
   */
  @Test
  public void testValidateSnapshotIDWidth() throws Exception {
    FSDirectory fsdir = mock(FSDirectory.class);
    SnapshotManager snapshotManager = new SnapshotManager(new Configuration(),
        fsdir);
    Assert.assertTrue(snapshotManager.
        getMaxSnapshotID() < Snapshot.CURRENT_STATE_ID);
  }

  @Test
  public void testSnapshotLimitOnRestart() throws Exception {
    final Configuration conf = new Configuration();
    final Path snapshottableDir
        = new Path("/" + getClass().getSimpleName());
    int numSnapshots = 5;
    conf.setInt(DFSConfigKeys.
            DFS_NAMENODE_SNAPSHOT_MAX_LIMIT, numSnapshots);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_FILESYSTEM_LIMIT,
        numSnapshots * 2);
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).
          numDataNodes(0).build();
      cluster.waitActive();
      DistributedFileSystem hdfs = cluster.getFileSystem();
      hdfs.mkdirs(snapshottableDir);
      hdfs.allowSnapshot(snapshottableDir);
      for (int i = 0; i < numSnapshots; i++) {
        hdfs.createSnapshot(snapshottableDir, "s" + i);
      }
      LambdaTestUtils.intercept(SnapshotException.class,
          "snapshot limit",
          () -> hdfs.createSnapshot(snapshottableDir, "s5"));

      // now change max snapshot directory limit to 2 and restart namenode
      cluster.getNameNode().getConf().setInt(DFSConfigKeys.
          DFS_NAMENODE_SNAPSHOT_MAX_LIMIT, 2);
      cluster.restartNameNodes();
      SnapshotManager snapshotManager = cluster.getNamesystem().
          getSnapshotManager();

      // make sure edits of all previous 5 create snapshots are replayed
      Assert.assertEquals(numSnapshots, snapshotManager.getNumSnapshots());

      // make sure namenode has the new snapshot limit configured as 2
      Assert.assertEquals(2, snapshotManager.getMaxSnapshotLimit());

      // Any new snapshot creation should still fail
      LambdaTestUtils.intercept(SnapshotException.class,
          "snapshot limit", () -> hdfs.createSnapshot(snapshottableDir, "s5"));
      // now change max snapshot FS limit to 2 and restart namenode
      cluster.getNameNode().getConf().setInt(DFSConfigKeys.
          DFS_NAMENODE_SNAPSHOT_FILESYSTEM_LIMIT, 2);
      cluster.restartNameNodes();
      snapshotManager = cluster.getNamesystem().
          getSnapshotManager();
      // make sure edits of all previous 5 create snapshots are replayed
      Assert.assertEquals(numSnapshots, snapshotManager.getNumSnapshots());

      // make sure namenode has the new snapshot limit configured as 2
      Assert.assertEquals(2, snapshotManager.getMaxSnapshotLimit());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

}
