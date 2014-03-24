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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode;
import org.junit.Before;
import org.junit.Test;

public class TestCheckpointsWithSnapshots {
  
  private static final Path TEST_PATH = new Path("/foo");
  private static final Configuration conf = new HdfsConfiguration();
  static {
    conf.set(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY, "0.0.0.0:0");
  }
  
  @Before
  public void setUp() {
    FileUtil.fullyDeleteContents(new File(MiniDFSCluster.getBaseDirectory()));
  }

  /**
   * Regression test for HDFS-5433 - "When reloading fsimage during
   * checkpointing, we should clear existing snapshottable directories"
   */
  @Test
  public void testCheckpoint() throws IOException {
    MiniDFSCluster cluster = null;
    SecondaryNameNode secondary = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      secondary = new SecondaryNameNode(conf);
      SnapshotManager nnSnapshotManager = cluster.getNamesystem().getSnapshotManager();
      SnapshotManager secondarySnapshotManager = secondary.getFSNamesystem().getSnapshotManager();
      
      FileSystem fs = cluster.getFileSystem();
      HdfsAdmin admin =  new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
      
      assertEquals(0, nnSnapshotManager.getNumSnapshots());
      assertEquals(0, nnSnapshotManager.getNumSnapshottableDirs());
      assertEquals(0, secondarySnapshotManager.getNumSnapshots());
      assertEquals(0, secondarySnapshotManager.getNumSnapshottableDirs());
      
      // 1. Create a snapshottable directory foo on the NN.
      fs.mkdirs(TEST_PATH);
      admin.allowSnapshot(TEST_PATH);
      assertEquals(0, nnSnapshotManager.getNumSnapshots());
      assertEquals(1, nnSnapshotManager.getNumSnapshottableDirs());
      
      // 2. Create a snapshot of the dir foo. This will be referenced both in
      // the SnapshotManager as well as in the file system tree. The snapshot
      // count will go up to 1.
      Path snapshotPath = fs.createSnapshot(TEST_PATH);
      assertEquals(1, nnSnapshotManager.getNumSnapshots());
      assertEquals(1, nnSnapshotManager.getNumSnapshottableDirs());
      
      // 3. Start up a 2NN and have it do a checkpoint. It will have foo and its
      // snapshot in its list of snapshottable dirs referenced from the
      // SnapshotManager, as well as in the file system tree.
      secondary.doCheckpoint();
      assertEquals(1, secondarySnapshotManager.getNumSnapshots());
      assertEquals(1, secondarySnapshotManager.getNumSnapshottableDirs());
      
      // 4. Disallow snapshots on and delete foo on the NN. The snapshot count
      // will go down to 0 and the snapshottable dir will be removed from the fs
      // tree.
      fs.deleteSnapshot(TEST_PATH, snapshotPath.getName());
      admin.disallowSnapshot(TEST_PATH);
      assertEquals(0, nnSnapshotManager.getNumSnapshots());
      assertEquals(0, nnSnapshotManager.getNumSnapshottableDirs());
      
      // 5. Have the NN do a saveNamespace, writing out a new fsimage with
      // snapshot count 0.
      NameNodeAdapter.enterSafeMode(cluster.getNameNode(), false);
      NameNodeAdapter.saveNamespace(cluster.getNameNode());
      NameNodeAdapter.leaveSafeMode(cluster.getNameNode());
      
      // 6. Have the still-running 2NN do a checkpoint. It will notice that the
      // fsimage has changed on the NN and redownload/reload from that image.
      // This will replace all INodes in the file system tree as well as reset
      // the snapshot counter to 0 in the SnapshotManager. However, it will not
      // clear the list of snapshottable dirs referenced from the
      // SnapshotManager. When it writes out an fsimage, the 2NN will write out
      // 0 for the snapshot count, but still serialize the snapshottable dir
      // referenced in the SnapshotManager even though it no longer appears in
      // the file system tree. The NN will not be able to start up with this.
      secondary.doCheckpoint();
      assertEquals(0, secondarySnapshotManager.getNumSnapshots());
      assertEquals(0, secondarySnapshotManager.getNumSnapshottableDirs());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      if (secondary != null) {
        secondary.shutdown();
      }
    }

  }

}
