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
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.ChildrenDiff;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.DirectoryDiff;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DiffListBySkipList.SkipListNode;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.ToIntBiFunction;
import java.util.function.ToIntFunction;

/**
 * This class tests the DirectoryDiffList API's.
 */
public class TestDiffListBySkipList {
  static final int NUM_SNAPSHOTS = 100;
  static final int MAX_LEVEL = 5;

  static {
    SnapshotTestHelper.disableLogs();
  }

  private static final Configuration CONF = new Configuration();
  private static MiniDFSCluster cluster;
  private static FSNamesystem fsn;
  private static FSDirectory fsdir;
  private static DistributedFileSystem hdfs;

  @Before
  public void setUp() throws Exception {
    cluster =
        new MiniDFSCluster.Builder(CONF).numDataNodes(0).format(true).build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    fsdir = fsn.getFSDirectory();
    hdfs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  static DiffListBySkipList newDiffListBySkipList() {
    DirectoryDiffListFactory.init(3, MAX_LEVEL, DiffListBySkipList.LOG);
    return new DiffListBySkipList(0);
  }

  static void assertList(List<INode> expected, List<INode> computed) {
    Assert.assertEquals(expected.size(), computed.size());
    for (int index = 0; index < expected.size(); index++) {
      Assert.assertEquals(expected.get(index), computed.get(index));
    }
  }

  static void verifyChildrenList(DiffListBySkipList skip, INodeDirectory dir) {
    final int n = skip.size();
    for (int i = 0; i < skip.size(); i++) {
      final List<INode> expected = ReadOnlyList.Util.asList(
          dir.getChildrenList(dir.getDiffs().asList().get(i).getSnapshotId()));
      final List<INode> computed = getChildrenList(skip, i, n, dir);
      try {
        assertList(expected, computed);
      } catch (AssertionError ae) {
        throw new AssertionError(
            "i = " + i + "\ncomputed = " + computed + "\nexpected = "
                + expected + "\n" + skip, ae);
      }
    }
  }

  static void verifyChildrenList(
      DiffList<DirectoryDiff> array, DiffListBySkipList skip,
      INodeDirectory dir, List<INode> childrenList) {
    final int n = array.size();
    Assert.assertEquals(n, skip.size());
    for (int i = 0; i < n - 1; i++) {
      for (int j = i + 1; j < n - 1; j++) {
        final List<INode> expected = getCombined(array, i, j, dir)
            .apply2Previous(childrenList);
        final List<INode> computed = getCombined(skip, i, j, dir)
            .apply2Previous(childrenList);
        try {
          assertList(expected, computed);
        } catch (AssertionError ae) {
          throw new AssertionError(
              "i = " + i + ", j = " + j + "\ncomputed = " + computed
                  + "\nexpected = " + expected + "\n" + skip, ae);
        }
      }
    }

    assertSkipList(skip);
  }

  private static ChildrenDiff getCombined(
      DiffList<DirectoryDiff> list, int from, int to, INodeDirectory dir) {
    final List<DirectoryDiff> minList = list.getMinListForRange(from, to, dir);
    final ChildrenDiff combined = new ChildrenDiff();
    for (DirectoryDiff d : minList) {
      combined.combinePosterior(d.getChildrenDiff(), null);
    }
    return combined;
  }

  static List<INode> getChildrenList(
      DiffList<DirectoryDiff> list, int from, int to, INodeDirectory dir) {
    final ChildrenDiff combined = getCombined(list, from, to, dir);
    return combined.apply2Current(ReadOnlyList.Util.asList(dir.getChildrenList(
        Snapshot.CURRENT_STATE_ID)));
  }

  static Path getChildPath(Path parent, int i) {
    return new Path(parent, "c" + i);
  }

  @Test
  public void testAddLast() throws Exception {
    testAddLast(NUM_SNAPSHOTS);
  }

  static void testAddLast(int n) throws Exception {
    final Path root = new Path("/testAddLast" + n);
    DiffListBySkipList.LOG.info("run " + root);

    final DiffListBySkipList skipList = newDiffListBySkipList();
    final DiffList<DirectoryDiff> arrayList = new DiffListByArrayList<>(0);
    INodeDirectory dir = addDiff(n, skipList, arrayList, root);
    // verify that the both the children list obtained from hdfs and
    // DiffListBySkipList are same
    verifyChildrenList(skipList, dir);
    verifyChildrenList(arrayList, skipList, dir, Collections.emptyList());
  }


  @Test
  public void testAddFirst() throws Exception {
    testAddFirst(NUM_SNAPSHOTS);
  }

  static void testAddFirst(int n) throws Exception {
    final Path root = new Path("/testAddFirst" + n);
    DiffListBySkipList.LOG.info("run " + root);

    hdfs.mkdirs(root);
    for (int i = 1; i < n; i++) {
      final Path child = getChildPath(root, i);
      hdfs.mkdirs(child);
    }
    INodeDirectory dir = fsdir.getINode(root.toString()).asDirectory();
    SnapshotTestHelper.createSnapshot(hdfs, root, "s0");
    for (int i = 1; i < n; i++) {
      final Path child = getChildPath(root, n - i);
      hdfs.delete(child, false);
      hdfs.createSnapshot(root, "s" + i);
    }
    DiffList<DirectoryDiff> diffs = dir.getDiffs().asList();
    List<INode> childrenList = ReadOnlyList.Util.asList(dir.getChildrenList(
        diffs.get(0).getSnapshotId()));
    final DiffListBySkipList skipList = newDiffListBySkipList();
    final DiffList<DirectoryDiff> arrayList = new DiffListByArrayList<>(0);
    for (int i = diffs.size() - 1; i >= 0; i--) {
      final DirectoryDiff d = diffs.get(i);
      skipList.addFirst(d);
      arrayList.addFirst(d);
    }
    // verify that the both the children list obtained from hdfs and
    // DiffListBySkipList are same
    verifyChildrenList(skipList, dir);
    verifyChildrenList(arrayList, skipList, dir, childrenList);
  }

  static INodeDirectory addDiff(int n, DiffList skipList, DiffList arrayList,
      final Path root) throws Exception {
    hdfs.mkdirs(root);
    SnapshotTestHelper.createSnapshot(hdfs, root, "s0");
    for (int i = 1; i < n; i++) {
      final Path child = getChildPath(root, i);
      hdfs.mkdirs(child);
      hdfs.createSnapshot(root, "s" + i);
    }
    INodeDirectory dir = fsdir.getINode(root.toString()).asDirectory();
    DiffList<DirectoryDiff> diffs = dir.getDiffs().asList();
    for (DirectoryDiff d : diffs) {
      skipList.addLast(d);
      arrayList.addLast(d);
    }
    DiffListBySkipList.LOG.info("skipList: " + skipList);
    return dir;
  }

  @Test
  public void testRemoveFromTail() throws Exception {
    final int n = NUM_SNAPSHOTS;
    testRemove("FromTail", n, i -> n - 1 - i);
  }

  @Test
  public void testReomveFromHead() throws Exception {
    testRemove("FromHead", NUM_SNAPSHOTS, i -> 0);
  }

  @Test
  public void testRemoveRandom() throws Exception {
    final int n = NUM_SNAPSHOTS;
    testRemove("Random", n, i -> ThreadLocalRandom.current().nextInt(n - i));
  }

  static void testRemove(String name, int n,
      ToIntFunction<Integer> indexFunction) throws Exception {
    testRemove(name, n, (skipList, i) -> indexFunction.applyAsInt(i));
  }

  static void testRemove(String name, int n,
      ToIntBiFunction<DiffListBySkipList, Integer> indexFunction)
      throws Exception {
    final Path root = new Path("/testRemove" + name + n);
    DiffListBySkipList.LOG.info("run " + root);

    final DiffListBySkipList skipList = newDiffListBySkipList();
    final DiffList<DirectoryDiff> arrayList = new DiffListByArrayList<>(0);
    final INodeDirectory dir = addDiff(n, skipList, arrayList, root);
    Assert.assertEquals(n, arrayList.size());
    Assert.assertEquals(n, skipList.size());

    for (int i = 0; i < n; i++) {
      DiffListBySkipList.LOG.debug("i={}: {}", i, skipList);
      final int index = indexFunction.applyAsInt(skipList, i);
      final DirectoryDiff diff = remove(index, skipList, arrayList);
      hdfs.deleteSnapshot(root, "s" + diff.getSnapshotId());
      verifyChildrenList(skipList, dir);
      verifyChildrenList(arrayList, skipList, dir, Collections.emptyList());
    }
  }

  @Test
  public void testRemoveFromLowerLevel() throws Exception {
    testRemove("FromLowerLevel", NUM_SNAPSHOTS,
        new ToIntBiFunction<DiffListBySkipList, Integer>() {
          private int level = 0;

          @Override
          public int applyAsInt(DiffListBySkipList skipList, Integer integer) {
            for (; level <= MAX_LEVEL; level++) {
              final int index = findIndex(skipList, level);
              if (index != -1) {
                return index;
              }
            }
            return -1;
          }
        });
  }

  @Test
  public void testRemoveFromUpperLevel() throws Exception {
    testRemove("FromUpperLevel", NUM_SNAPSHOTS,
        new ToIntBiFunction<DiffListBySkipList, Integer>() {
      private int level = MAX_LEVEL;
      @Override
      public int applyAsInt(DiffListBySkipList skipList, Integer integer) {
        for(; level >= 0; level--) {
          final int index = findIndex(skipList, level);
          if (index != -1) {
            return index;
          }
          DiffListBySkipList.LOG.info("change from level " + level);
        }
        return -1;
      }
    });
  }

  static int findIndex(DiffListBySkipList skipList, int level) {
    for (int i = 0; i < skipList.size(); i++) {
      if (skipList.getSkipListNode(i).level() == level) {
        return i;
      }
    }
    return -1;
  }

  static DirectoryDiff remove(int i, DiffListBySkipList skip,
      DiffList<DirectoryDiff> array) {
    final DirectoryDiff expected = array.remove(i);
    DiffListBySkipList.LOG
        .info("remove " + i + ", snapshotId=" + expected.getSnapshotId());
    final DirectoryDiff computed = skip.remove(i);
    assertDirectoryDiff(expected, computed);
    return expected;
  }

  static void assertDirectoryDiff(DirectoryDiff expected,
      DirectoryDiff computed) {
    Assert.assertEquals(expected.getSnapshotId(), computed.getSnapshotId());
  }

  static void assertSkipList(DiffListBySkipList skipList) {
    for(int i = 0; i < skipList.size(); i++) {
      assertSkipListNode(skipList.getSkipListNode(i));
    }
  }

  static void assertSkipListNode(SkipListNode n) {
    for (int i = 1; i <= n.level(); i++) {
      final SkipListNode target = n.getSkipNode(i);
      final ChildrenDiff diff = n.getChildrenDiff(i);
      if (target == null) {
        if (diff != null) {
          throw new AssertionError(
              "Target is null but children diff is not at i=" + i + n
                  .appendTo(new StringBuilder(": ")));
        }
      } else if (target == n.getSkipNode(i - 1)) {
        if (diff != n.getChildrenDiff(i - 1)) {
          throw new AssertionError(
              "Same target but different children diff at i=" + i + n
                  .appendTo(new StringBuilder(": ")));
        }
      }
    }
  }
}