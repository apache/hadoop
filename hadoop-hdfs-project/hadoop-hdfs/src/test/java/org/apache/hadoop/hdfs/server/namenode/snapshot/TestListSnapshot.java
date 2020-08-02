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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.protocol.SnapshotStatus;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotManager.
    DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests listSnapshot.
 */
public class TestListSnapshot {

  static final short REPLICATION = 3;

  private final Path dir1 = new Path("/TestSnapshot1");

  Configuration conf;
  MiniDFSCluster cluster;
  FSNamesystem fsn;
  DistributedFileSystem hdfs;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setBoolean(DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED, true);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    hdfs = cluster.getFileSystem();
    hdfs.mkdirs(dir1);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Test listing all the snapshottable directories.
   */
  @Test(timeout = 60000)
  public void testListSnapshot() throws Exception {
    fsn.getSnapshotManager().setAllowNestedSnapshots(true);

    // Initially there is no snapshottable directories in the system
    SnapshotStatus[] snapshotStatuses = null;
    SnapshottableDirectoryStatus[] dirs = hdfs.getSnapshottableDirListing();
    assertNull(dirs);
    LambdaTestUtils.intercept(SnapshotException.class,
        "Directory is not a " + "snapshottable directory",
        () -> hdfs.getSnapshotListing(dir1));
    // Make root as snapshottable
    final Path root = new Path("/");
    hdfs.allowSnapshot(root);
    dirs = hdfs.getSnapshottableDirListing();
    assertEquals(1, dirs.length);
    assertEquals("", dirs[0].getDirStatus().getLocalName());
    assertEquals(root, dirs[0].getFullPath());
    snapshotStatuses = hdfs.getSnapshotListing(root);
    assertTrue(snapshotStatuses.length == 0);
    // Make root non-snaphsottable
    hdfs.disallowSnapshot(root);
    dirs = hdfs.getSnapshottableDirListing();
    assertNull(dirs);
    snapshotStatuses = hdfs.getSnapshotListing(root);
    assertTrue(snapshotStatuses.length == 0);

    // Make dir1 as snapshottable
    hdfs.allowSnapshot(dir1);
    hdfs.createSnapshot(dir1, "s0");
    snapshotStatuses = hdfs.getSnapshotListing(dir1);
    assertEquals(1, snapshotStatuses.length);
    assertEquals("s0", snapshotStatuses[0].getDirStatus().
        getLocalName());
    assertEquals(SnapshotTestHelper.getSnapshotRoot(dir1, "s0"),
        snapshotStatuses[0].getFullPath());
    // snapshot id is zero
    assertEquals(0, snapshotStatuses[0].getSnapshotID());
    // Create a snapshot for dir1
    hdfs.createSnapshot(dir1, "s1");
    hdfs.createSnapshot(dir1, "s2");
    snapshotStatuses = hdfs.getSnapshotListing(dir1);
    // There are now 3 snapshots for dir1
    assertEquals(3, snapshotStatuses.length);
    assertEquals("s0", snapshotStatuses[0].getDirStatus().
        getLocalName());
    assertEquals(SnapshotTestHelper.getSnapshotRoot(dir1, "s0"),
        snapshotStatuses[0].getFullPath());
    assertEquals("s1", snapshotStatuses[1].getDirStatus().
        getLocalName());
    assertEquals(SnapshotTestHelper.getSnapshotRoot(dir1, "s1"),
        snapshotStatuses[1].getFullPath());
    assertEquals("s2", snapshotStatuses[2].getDirStatus().
        getLocalName());
    assertEquals(SnapshotTestHelper.getSnapshotRoot(dir1, "s2"),
        snapshotStatuses[2].getFullPath());
    hdfs.deleteSnapshot(dir1, "s2");
    snapshotStatuses = hdfs.getSnapshotListing(dir1);
    // There are now 2 active snapshots for dir1 and one is marked deleted
    assertEquals(3, snapshotStatuses.length);
    assertTrue(snapshotStatuses[2].isDeleted());
    assertFalse(snapshotStatuses[1].isDeleted());
    assertFalse(snapshotStatuses[0].isDeleted());
    // delete the 1st snapshot
    hdfs.deleteSnapshot(dir1, "s0");
    snapshotStatuses = hdfs.getSnapshotListing(dir1);
    // There are now 2 snapshots now as the 1st one is deleted in order
    assertEquals(2, snapshotStatuses.length);
  }
}