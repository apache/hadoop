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
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.event.Level;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotManager.DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotManager.DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED_GC_PERIOD_MS;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.TestOrderedSnapshotDeletion.assertMarkedAsDeleted;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.TestOrderedSnapshotDeletion.assertNotMarkedAsDeleted;

/**
 * Test {@link SnapshotDeletionGc}.
 */
public class TestOrderedSnapshotDeletionGc {
  private static final int GC_PERIOD = 10;

  private MiniDFSCluster cluster;

  @Before
  public void setUp() throws Exception {
    final Configuration conf = new Configuration();
    conf.setBoolean(DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED, true);
    conf.setInt(DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED_GC_PERIOD_MS, GC_PERIOD);

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
    cluster.waitActive();

    GenericTestUtils.setLogLevel(SnapshotDeletionGc.LOG, Level.TRACE);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test(timeout = 60000)
  public void testSingleDir() throws Exception {
    final DistributedFileSystem hdfs = cluster.getFileSystem();

    final Path snapshottableDir = new Path("/dir");
    hdfs.mkdirs(snapshottableDir);
    hdfs.allowSnapshot(snapshottableDir);

    final Path sub0 = new Path(snapshottableDir, "sub0");
    hdfs.mkdirs(sub0);
    final Path s0path = hdfs.createSnapshot(snapshottableDir, "s0");
    Assert.assertTrue(exist(s0path, hdfs));

    final Path sub1 = new Path(snapshottableDir, "sub1");
    hdfs.mkdirs(sub1);
    final Path s1path = hdfs.createSnapshot(snapshottableDir, "s1");
    Assert.assertTrue(exist(s1path, hdfs));

    final Path sub2 = new Path(snapshottableDir, "sub2");
    hdfs.mkdirs(sub2);
    final Path s2path = hdfs.createSnapshot(snapshottableDir, "s2");
    Assert.assertTrue(exist(s2path, hdfs));

    assertNotMarkedAsDeleted(s0path, cluster);
    assertNotMarkedAsDeleted(s1path, cluster);
    assertNotMarkedAsDeleted(s2path, cluster);

    hdfs.deleteSnapshot(snapshottableDir, "s2");
    assertNotMarkedAsDeleted(s0path, cluster);
    assertNotMarkedAsDeleted(s1path, cluster);
    assertMarkedAsDeleted(s2path, cluster);

    hdfs.deleteSnapshot(snapshottableDir, "s1");
    assertNotMarkedAsDeleted(s0path, cluster);
    assertMarkedAsDeleted(s1path, cluster);
    assertMarkedAsDeleted(s2path, cluster);

    // should not be gc'ed
    Thread.sleep(10*GC_PERIOD);
    assertNotMarkedAsDeleted(s0path, cluster);
    assertMarkedAsDeleted(s1path, cluster);
    assertMarkedAsDeleted(s2path, cluster);

    hdfs.deleteSnapshot(snapshottableDir, "s0");
    Assert.assertFalse(exist(s0path, hdfs));

    waitForGc(Arrays.asList(s1path, s2path), hdfs);
  }

  static boolean exist(Path snapshotRoot, DistributedFileSystem hdfs)
      throws IOException {
    try {
      hdfs.getFileStatus(snapshotRoot);
      return true;
    } catch (FileNotFoundException ignored) {
      return false;
    }
  }

  static void waitForGc(List<Path> snapshotPaths, DistributedFileSystem hdfs)
      throws Exception {
    final Iterator<Path> i = snapshotPaths.iterator();
    for(Path p = i.next();; Thread.sleep(GC_PERIOD)) {
      for(; !exist(p, hdfs); p = i.next()) {
        if (!i.hasNext()) {
          return;
        }
      }
    }
  }

  @Test(timeout = 60000)
  public void testMultipleDirs() throws Exception {
    final int numSnapshottables = 10;
    final DistributedFileSystem hdfs = cluster.getFileSystem();

    final List<Path> snapshottableDirs = new ArrayList<>();
    for(int i = 0; i < numSnapshottables; i++) {
      final Path p = new Path("/dir" + i);
      snapshottableDirs.add(p);
      hdfs.mkdirs(p);
      hdfs.allowSnapshot(p);
    }

    final Random random = new Random();
    final List<Path> snapshotPaths = new ArrayList<>();
    for(Path s : snapshottableDirs) {
      final int numSnapshots = random.nextInt(10) + 1;
      createSnapshots(s, numSnapshots, snapshotPaths, hdfs);
    }

    // Randomly delete snapshots
    Collections.shuffle(snapshotPaths);
    for(Path p : snapshotPaths) {
      hdfs.deleteSnapshot(p.getParent().getParent(), p.getName());
    }

    waitForGc(snapshotPaths, hdfs);
  }

  static void createSnapshots(Path snapshottableDir, int numSnapshots,
      List<Path> snapshotPaths, DistributedFileSystem hdfs)
      throws IOException {
    for(int i = 0; i < numSnapshots; i++) {
      final Path sub = new Path(snapshottableDir, "sub" + i);
      hdfs.mkdirs(sub);
      final Path p = hdfs.createSnapshot(snapshottableDir, "s" + i);
      snapshotPaths.add(p);
      Assert.assertTrue(exist(p, hdfs));
    }
  }
}
