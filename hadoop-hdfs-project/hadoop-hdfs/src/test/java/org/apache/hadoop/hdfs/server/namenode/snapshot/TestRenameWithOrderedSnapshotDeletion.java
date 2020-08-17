/*
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


import java.io.IOException;

import static org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotManager.DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED;
import static org.apache.hadoop.hdfs.server.namenode.FSNamesystem.DFS_NAMENODE_SNAPSHOT_TRASHROOT_ENABLED;
/**
 * Test Rename with ordered snapshot deletion.
 */
public class TestRenameWithOrderedSnapshotDeletion {
  private final Path snapshottableDir
      = new Path("/" + getClass().getSimpleName());
  private DistributedFileSystem hdfs;
  private MiniDFSCluster cluster;

  @Before
  public void setUp() throws Exception {
    final Configuration conf = new Configuration();
    conf.setBoolean(DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED, true);
    conf.setBoolean(DFS_NAMENODE_SNAPSHOT_TRASHROOT_ENABLED, true);

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
    cluster.waitActive();
    hdfs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test(timeout = 60000)
  public void testRename() throws Exception {
    final Path dir1 = new Path("/dir1");
    final Path dir2 = new Path("/dir2");
    final Path sub0 = new Path(snapshottableDir, "sub0");
    final Path sub1 = new Path(snapshottableDir, "sub1");
    hdfs.mkdirs(sub0);
    hdfs.mkdirs(dir2);
    final Path file1 = new Path(dir1, "file1");
    final Path file2 = new Path(sub0, "file2");
    hdfs.mkdirs(snapshottableDir);
    hdfs.mkdirs(dir1);
    hdfs.mkdirs(dir2);
    hdfs.mkdirs(sub0);
    DFSTestUtil.createFile(hdfs, file1, 0, (short) 1, 0);
    DFSTestUtil.createFile(hdfs, file2, 0, (short) 1, 0);
    hdfs.allowSnapshot(snapshottableDir);
    // rename from non snapshottable dir to snapshottable dir should fail
    validateRename(file1, sub0);
    hdfs.createSnapshot(snapshottableDir, "s0");
    validateRename(file1, sub0);
    // rename across non snapshottable dirs should work
    hdfs.rename(file1, dir2);
    // rename beyond snapshottable root should fail
    validateRename(file2, dir1);
    // rename within snapshottable root should work
    hdfs.rename(file2, snapshottableDir);

    // rename dirs outside snapshottable root should work
    hdfs.rename(dir2, dir1);
    // rename dir into snapshottable root should fail
    validateRename(dir1, snapshottableDir);
    // rename dir outside snapshottable root should fail
    validateRename(sub0, dir2);
    // rename dir within snapshottable root should work
    hdfs.rename(sub0, sub1);
  }

  private void validateRename(Path src, Path dest) {
    try {
      hdfs.rename(src, dest);
      Assert.fail("Expected exception not thrown.");
    } catch (IOException ioe) {
      Assert.assertTrue(ioe.getMessage().contains("are not under the" +
          " same snapshot root."));
    }
  }
}
