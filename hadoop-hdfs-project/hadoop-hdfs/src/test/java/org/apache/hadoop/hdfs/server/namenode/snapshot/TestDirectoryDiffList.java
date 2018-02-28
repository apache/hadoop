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
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.DirectoryDiff;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Iterator;

/**
 * This class tests the DirectoryDiffList API's.
 */
public class TestDirectoryDiffList{
  static {
    SnapshotTestHelper.disableLogs();
  }

  private static final long SEED = 0;
  private static final short REPL = 3;
  private static final short REPL_2 = 1;
  private static final long BLOCKSIZE = 1024;

  private static final Configuration CONF = new Configuration();
  private static MiniDFSCluster cluster;
  private static FSNamesystem fsn;
  private static FSDirectory fsdir;
  private static DistributedFileSystem hdfs;

  @Before
  public void setUp() throws Exception {
    CONF.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);
    cluster = new MiniDFSCluster.Builder(CONF).numDataNodes(REPL).format(true)
        .build();
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

  private void compareChildrenList(ReadOnlyList<INode> list1,
      ReadOnlyList<INode> list2) {
    Assert.assertEquals(list1.size(), list2.size());
    for (int index = 0; index < list1.size(); index++) {
      Assert.assertEquals(list1.get(index), list2.get(index));
    }
  }

  private void verifyChildrenListForAllSnapshots(DirectoryDiffList list,
      INodeDirectory dir) {
    for (int index = 0;
         index < list.size(); index++) {
      ReadOnlyList<INode> list1 = dir.getChildrenList(index);
      List<DirectoryWithSnapshotFeature.DirectoryDiff> subList =
          list.getMinListForRange(index, list.size() - 1, dir);
      ReadOnlyList<INode> list2 = getChildrenList(dir, subList);
      compareChildrenList(list1, list2);
    }
  }

  private ReadOnlyList<INode> getChildrenList(INodeDirectory currentINode,
      List<DirectoryWithSnapshotFeature.DirectoryDiff> list) {
    List<INode> children = null;
    final DirectoryWithSnapshotFeature.ChildrenDiff
        combined = new DirectoryWithSnapshotFeature.ChildrenDiff();
    for (DirectoryWithSnapshotFeature.DirectoryDiff d : list) {
      combined.combinePosterior(d.getChildrenDiff(), null);
    }
    children = combined.apply2Current(ReadOnlyList.Util.asList(
        currentINode.getChildrenList(Snapshot.CURRENT_STATE_ID)));
    return ReadOnlyList.Util.asReadOnlyList(children);
  }

  @Test
  public void testDirectoryDiffList() throws Exception {
    final Path sdir1 = new Path("/dir1");
    hdfs.mkdirs(sdir1);
    SnapshotTestHelper.createSnapshot(hdfs, sdir1, "s0");
    for (int i = 1; i < 31; i++) {
      final Path file = new Path(sdir1, "file" + i);
      DFSTestUtil.createFile(hdfs, file, BLOCKSIZE, REPL_2, SEED);
      SnapshotTestHelper.createSnapshot(hdfs, sdir1, "s" + i);
    }
    INodeDirectory sdir1Node = fsdir.getINode(sdir1.toString()).asDirectory();
    DiffList<DirectoryDiff> diffs =
        sdir1Node.getDiffs().asList();

    Iterator<DirectoryDiff> itr = diffs.iterator();
    DirectoryDiffList skipList = DirectoryDiffList.createSkipList(0, 3, 5);
    while (itr.hasNext()) {
      skipList.addLast(itr.next());
    }
    // verify that the both the children list obtained from hdfs and
    // DirectoryDiffList are same
    verifyChildrenListForAllSnapshots(skipList, sdir1Node);
  }

  @Test
  public void testDirectoryDiffList2() throws Exception {
    final Path sdir1 = new Path("/dir1");
    hdfs.mkdirs(sdir1);
    for (int i = 1; i < 31; i++) {
      final Path file = new Path(sdir1, "file" + i);
      DFSTestUtil.createFile(hdfs, file, BLOCKSIZE, REPL_2, SEED);
    }
    SnapshotTestHelper.createSnapshot(hdfs, sdir1, "s0");
    for (int i = 1; i < 31; i++) {
      final Path file = new Path(sdir1, "file" + (31 - i));
      hdfs.delete(file, false);
      SnapshotTestHelper.createSnapshot(hdfs, sdir1, "s" + i);
    }
    INodeDirectory sdir1Node = fsdir.getINode(sdir1.toString()).asDirectory();
    DiffList<DirectoryDiff> diffs = sdir1Node.getDiffs().asList();

    int index = diffs.size() - 1;
    DirectoryDiffList skipList = DirectoryDiffList.createSkipList(0, 3, 5);
    while (index >= 0) {
      skipList.addFirst(diffs.get(index));
      index--;
    }
    // verify that the both the children list obtained from hdfs and
    // DirectoryDiffList are same
    verifyChildrenListForAllSnapshots(skipList, sdir1Node);
  }
}