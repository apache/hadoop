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
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;
import org.apache.hadoop.hdfs.util.Holder;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.event.Level;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.EnumMap;
import java.util.ArrayList;

import static org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotManager.DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotManager.DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED_GC_PERIOD_MS;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.TestOrderedSnapshotDeletion.assertMarkedAsDeleted;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.TestOrderedSnapshotDeletion.assertNotMarkedAsDeleted;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.TestOrderedSnapshotDeletion.getDeletedSnapshotName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test {@link SnapshotDeletionGc}.
 */
public class TestOrderedSnapshotDeletionGc {
  private static final int GC_PERIOD = 10;
  private static final int NUM_DATANODES = 0;
  private MiniDFSCluster cluster;

  @Before
  public void setUp() throws Exception {
    final Configuration conf = new Configuration();
    conf.setBoolean(DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED, true);
    conf.setInt(DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED_GC_PERIOD_MS, GC_PERIOD);

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATANODES)
        .build();
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
    assertMarkedAsDeleted(s2path, snapshottableDir, cluster);
    final Path s2pathNew = new Path(s2path.getParent(),
        getDeletedSnapshotName(hdfs, snapshottableDir, s2path.getName()));
    Assert.assertFalse(exist(s2path, hdfs));
    Assert.assertTrue(exist(s2pathNew, hdfs));
    Assert.assertFalse(s2path.equals(s2pathNew));

    hdfs.deleteSnapshot(snapshottableDir, "s1");
    assertNotMarkedAsDeleted(s0path, cluster);
    assertMarkedAsDeleted(s1path, snapshottableDir, cluster);
    assertMarkedAsDeleted(s2path, snapshottableDir, cluster);
    final Path s1pathNew = new Path(s1path.getParent(),
        getDeletedSnapshotName(hdfs, snapshottableDir, s1path.getName()));
    Assert.assertFalse(exist(s1path, hdfs));
    Assert.assertTrue(exist(s1pathNew, hdfs));
    Assert.assertFalse(s1path.equals(s1pathNew));
    // should not be gc'ed
    Thread.sleep(10*GC_PERIOD);
    assertNotMarkedAsDeleted(s0path, cluster);
    assertMarkedAsDeleted(s1path, snapshottableDir, cluster);
    assertMarkedAsDeleted(s2path, snapshottableDir, cluster);

    hdfs.deleteSnapshot(snapshottableDir, "s0");
    Assert.assertFalse(exist(s0path, hdfs));

    waitForGc(Arrays.asList(s1pathNew, s2pathNew), hdfs);
    // total no of edit log records created for delete snapshot will be equal
    // to sum of no of user deleted snapshots and no of snapshots gc'ed with
    // snapshotDeletion gc thread
    doEditLogValidation(cluster, 5);
  }

  static void doEditLogValidation(MiniDFSCluster cluster,
                                  int editLogOpCount) throws Exception {
    final FSNamesystem namesystem = cluster.getNamesystem();
    Configuration conf = cluster.getNameNode().getConf();
    FSImage fsimage = namesystem.getFSImage();
    Storage.StorageDirectory sd = fsimage.getStorage().
        dirIterator(NNStorage.NameNodeDirType.EDITS).next();
    cluster.shutdown();

    File editFile = FSImageTestUtil.findLatestEditsLog(sd).getFile();
    assertTrue("Should exist: " + editFile, editFile.exists());
    EnumMap<FSEditLogOpCodes, Holder<Integer>> counts;
    counts = FSImageTestUtil.countEditLogOpTypes(editFile);
    if (editLogOpCount > 0) {
      assertEquals(editLogOpCount, (int) counts.get(FSEditLogOpCodes.
          OP_DELETE_SNAPSHOT).held);
    }
    // make sure the gc thread doesn't start for a long time after the restart
    conf.setInt(DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED_GC_PERIOD_MS,
        (int)(24 * 60_000L));
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATANODES)
        .build();
    cluster.waitActive();
    // ensure after the edits get replayed , all the snapshots are deleted
    Assert.assertEquals(0,
        cluster.getNamesystem().getSnapshotManager().getNumSnapshots());
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
    // don't do edit log count validation here as gc snapshot
    // deletion count will be random here
    doEditLogValidation(cluster, -1);
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
