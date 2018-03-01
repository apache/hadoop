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
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * This class tests the DirectoryDiffList API's.
 */
public class TestDirectoryDiffList{
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

  static void assertList(List<INode> expected, List<INode> computed) {
    Assert.assertEquals(expected.size(), computed.size());
    for (int index = 0; index < expected.size(); index++) {
      Assert.assertEquals(expected.get(index), computed.get(index));
    }
  }

  static void verifyChildrenList(DirectoryDiffList skip, INodeDirectory dir) {
    final int n = skip.size();
    for (int i = 0; i < skip.size(); i++) {
      final List<INode> expected =
          ReadOnlyList.Util.asList(dir.getChildrenList(i));
      final List<INode> computed = getChildrenList(skip, i, n, dir);
      try {
        assertList(expected, computed);
      } catch (AssertionError ae) {
        throw new AssertionError(
            "i = " + i + "\ncomputed = " + computed + "\nexpected = "
                + expected, ae);
      }
    }
  }

  static void verifyChildrenList(
      DiffList<DirectoryDiff> array, DirectoryDiffList skip,
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
    testAddLast(7);
  }

  static void testAddLast(int n) throws Exception {
    final Path root = new Path("/testAddLast" + n);
    hdfs.mkdirs(root);
    SnapshotTestHelper.createSnapshot(hdfs, root, "s0");
    for (int i = 1; i < n; i++) {
      final Path child = getChildPath(root, i);
      hdfs.mkdirs(child);
      SnapshotTestHelper.createSnapshot(hdfs, root, "s" + i);
    }
    INodeDirectory dir = fsdir.getINode(root.toString()).asDirectory();
    DiffList<DirectoryDiff> diffs = dir.getDiffs().asList();

    final DirectoryDiffList skipList = new DirectoryDiffList(0, 3, 5);
    final DiffList<DirectoryDiff> arrayList = new DiffListByArrayList<>(0);
    for (DirectoryDiff d : diffs) {
      skipList.addLast(d);
      arrayList.addLast(d);
    }

    // verify that the both the children list obtained from hdfs and
    // DirectoryDiffList are same
    verifyChildrenList(skipList, dir);
    verifyChildrenList(arrayList, skipList, dir, Collections.emptyList());
  }


  @Test
  public void testAddFirst() throws Exception {
    testAddFirst(7);
  }

  static void testAddFirst(int n) throws Exception {
    final Path root = new Path("/testAddFirst" + n);
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
      SnapshotTestHelper.createSnapshot(hdfs, root, "s" + i);
    }
    DiffList<DirectoryDiff> diffs = dir.getDiffs().asList();
    List<INode> childrenList = ReadOnlyList.Util.asList(dir.getChildrenList(
        diffs.get(0).getSnapshotId()));
    final DirectoryDiffList skipList = new DirectoryDiffList(0, 3, 5);
    final DiffList<DirectoryDiff> arrayList = new DiffListByArrayList<>(0);
    for (int i = diffs.size() - 1; i >= 0; i--) {
      final DirectoryDiff d = diffs.get(i);
      skipList.addFirst(d);
      arrayList.addFirst(d);
    }
    // verify that the both the children list obtained from hdfs and
    // DirectoryDiffList are same
    verifyChildrenList(skipList, dir);
    verifyChildrenList(arrayList, skipList, dir, childrenList);
  }
}