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
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeReference;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test getting the full path name of a given inode. The INode may be in
 * snapshot.
 */
public class TestFullPathNameWithSnapshot {
  private static final int BLOCKSIZE = 1024;
  private static final short REPLICATION = 1;

  private Configuration conf;
  private MiniDFSCluster cluster;
  private FSDirectory fsdir;
  private DistributedFileSystem dfs;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .build();
    cluster.waitActive();

    fsdir = cluster.getNamesystem().getFSDirectory();
    dfs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Normal case without snapshot involved
   */
  @Test
  public void testNormalINode() throws Exception {
    final Path bar = new Path("/foo/bar");
    dfs.mkdirs(bar);
    final Path file = new Path(bar, "file");
    DFSTestUtil.createFile(dfs, file, BLOCKSIZE, REPLICATION, 0L);
    INode fileNode = fsdir.getINode4Write(file.toString());
    byte[][] pathComponents = FSDirectory
        .getPathComponentsWithSnapshot(fileNode);
    DFSTestUtil.checkComponentsEquals(INode.getPathComponents(file.toString()),
        pathComponents);
  }

  /**
   * INode in deleted list
   */
  @Test
  public void testDeletedINode() throws Exception {
    final Path foo = new Path("/foo");
    final Path f1 = new Path(foo, "f1");
    DFSTestUtil.createFile(dfs, f1, BLOCKSIZE, REPLICATION, 0L);
    final Path bar = new Path(foo, "bar");
    dfs.mkdirs(bar);
    final Path f2 = new Path(bar, "f2");
    DFSTestUtil.createFile(dfs, f2, BLOCKSIZE, REPLICATION, 0L);

    INode f1Node = fsdir.getINode4Write(f1.toString());
    INode f2Node = fsdir.getINode4Write(f2.toString());

    SnapshotTestHelper.createSnapshot(dfs, foo, "s1");
    dfs.delete(bar, true);
    SnapshotTestHelper.createSnapshot(dfs, foo, "s2");
    dfs.delete(f1, true);

    byte[][] f1Components = FSDirectory.getPathComponentsWithSnapshot(f1Node);
    byte[][] f2Components = FSDirectory.getPathComponentsWithSnapshot(f2Node);
    // expected: /foo/.snapshot/s2/f1
    String f1Snapshot = SnapshotTestHelper.getSnapshotPath(foo, "s2",
        f1.getName()).toString();
    // expected: /foo/.snapshot/s1/bar/f2
    String f2Snapshot = SnapshotTestHelper.getSnapshotPath(foo, "s1", "bar/f2")
        .toString();
    DFSTestUtil.checkComponentsEquals(INode.getPathComponents(f1Snapshot),
        f1Components);
    DFSTestUtil.checkComponentsEquals(INode.getPathComponents(f2Snapshot),
        f2Components);

    // delete snapshot s2
    dfs.deleteSnapshot(foo, "s2");
    // expected: /foo/.snapshot/s1/f1
    f1Snapshot = SnapshotTestHelper.getSnapshotPath(foo, "s1", f1.getName())
        .toString();
    f1Components = FSDirectory.getPathComponentsWithSnapshot(f1Node);
    DFSTestUtil.checkComponentsEquals(INode.getPathComponents(f1Snapshot),
        f1Components);
  }

  /**
   * INode after renaming
   */
  @Test
  public void testRenamedINode() throws Exception {
    final Path foo = new Path("/foo");
    final Path bar = new Path(foo, "bar");
    final Path f1 = new Path(bar, "f1");
    final Path f2 = new Path(bar, "f2");
    DFSTestUtil.createFile(dfs, f1, BLOCKSIZE, REPLICATION, 0L);
    DFSTestUtil.createFile(dfs, f2, BLOCKSIZE, REPLICATION, 0L);

    // create snapshot s1
    SnapshotTestHelper.createSnapshot(dfs, foo, "s1");
    INode f2Node = fsdir.getINode4Write(f2.toString());
    // delete /foo/bar/f2
    dfs.delete(f2, true);
    // rename bar to bar2
    final Path bar2 = new Path(foo, "bar2");
    dfs.rename(bar, bar2);
    // create snapshot s2
    SnapshotTestHelper.createSnapshot(dfs, foo, "s2");

    // /foo/.snapshot/s1/bar
    Path barPath = SnapshotTestHelper.getSnapshotPath(foo, "s1", bar.getName());
    INode barNode = fsdir.getINode(barPath.toString());
    Assert.assertTrue(barNode instanceof INodeReference.WithName);
    INode bar2Node = fsdir.getINode(bar2.toString());
    Assert.assertTrue(bar2Node instanceof INodeReference.DstReference);
    byte[][] barComponents = FSDirectory.getPathComponentsWithSnapshot(barNode);
    byte[][] bar2Components = FSDirectory
        .getPathComponentsWithSnapshot(bar2Node);
    DFSTestUtil.checkComponentsEquals(
        INode.getPathComponents(barPath.toString()), barComponents);
    DFSTestUtil.checkComponentsEquals(INode.getPathComponents(bar2.toString()),
        bar2Components);

    byte[][] f2Components = FSDirectory.getPathComponentsWithSnapshot(f2Node);
    // expected: /foo/.snapshot/s1/bar/f2
    Path deletedf2 = SnapshotTestHelper.getSnapshotPath(foo, "s1", "bar/f2");
    DFSTestUtil.checkComponentsEquals(
        INode.getPathComponents(deletedf2.toString()), f2Components);

    final Path newf1 = new Path(bar2, f1.getName());
    INode f1Node = fsdir.getINode(newf1.toString());
    Assert.assertTrue(dfs.delete(newf1, true));
    Path deletedf1 = SnapshotTestHelper.getSnapshotPath(foo, "s2", "bar2/f1");
    byte[][] f1Components = FSDirectory.getPathComponentsWithSnapshot(f1Node);
    DFSTestUtil.checkComponentsEquals(
        INode.getPathComponents(deletedf1.toString()), f1Components);
  }

  /**
   * Similar with testRenamedINode but the rename is across two snapshottable
   * directory.
   */
  @Test
  public void testRenamedINode2() throws Exception {
    final Path foo1 = new Path("/foo1");
    final Path foo2 = new Path("/foo2");
    final Path bar = new Path(foo1, "bar");
    final Path f1 = new Path(bar, "f1");
    final Path f2 = new Path(bar, "f2");
    dfs.mkdirs(foo2);
    DFSTestUtil.createFile(dfs, f1, BLOCKSIZE, REPLICATION, 0L);
    DFSTestUtil.createFile(dfs, f2, BLOCKSIZE, REPLICATION, 0L);

    // create snapshots on foo1 and foo2
    SnapshotTestHelper.createSnapshot(dfs, foo1, "s1");
    SnapshotTestHelper.createSnapshot(dfs, foo2, "s2");
    INode f2Node = fsdir.getINode4Write(f2.toString());
    // delete /foo1/bar/f2
    dfs.delete(f2, true);
    // rename bar to bar2
    final Path bar2 = new Path(foo2, "bar2");
    dfs.rename(bar, bar2);
    // create snapshot s3 and s4 on foo1 and foo2
    SnapshotTestHelper.createSnapshot(dfs, foo1, "s3");
    SnapshotTestHelper.createSnapshot(dfs, foo2, "s4");

    // /foo1/.snapshot/s1/bar
    Path barPath = SnapshotTestHelper
        .getSnapshotPath(foo1, "s1", bar.getName());
    INode barNode = fsdir.getINode(barPath.toString());
    Assert.assertTrue(barNode instanceof INodeReference.WithName);
    INode bar2Node = fsdir.getINode(bar2.toString());
    Assert.assertTrue(bar2Node instanceof INodeReference.DstReference);
    byte[][] barComponents = FSDirectory.getPathComponentsWithSnapshot(barNode);
    byte[][] bar2Components = FSDirectory
        .getPathComponentsWithSnapshot(bar2Node);
    DFSTestUtil.checkComponentsEquals(
        INode.getPathComponents(barPath.toString()), barComponents);
    DFSTestUtil.checkComponentsEquals(INode.getPathComponents(bar2.toString()),
        bar2Components);

    byte[][] f2Components = FSDirectory.getPathComponentsWithSnapshot(f2Node);
    // expected: /foo1/.snapshot/s1/bar/f2
    Path deletedf2 = SnapshotTestHelper.getSnapshotPath(foo1, "s1", "bar/f2");
    DFSTestUtil.checkComponentsEquals(
        INode.getPathComponents(deletedf2.toString()), f2Components);

    final Path newf1 = new Path(bar2, f1.getName());
    INode f1Node = fsdir.getINode(newf1.toString());
    Assert.assertTrue(dfs.delete(newf1, true));
    // /foo2/.snapshot/s4/bar2/f1
    Path deletedf1 = SnapshotTestHelper.getSnapshotPath(foo2, "s4", "bar2/f1");
    byte[][] f1Components = FSDirectory.getPathComponentsWithSnapshot(f1Node);
    DFSTestUtil.checkComponentsEquals(
        INode.getPathComponents(deletedf1.toString()), f1Components);
  }

  /**
   * Rename a directory to its prior descendant
   */
  @Test
  public void testNestedRename() throws Exception {
    final Path sdir1 = new Path("/dir1");
    final Path sdir2 = new Path("/dir2");
    final Path foo = new Path(sdir1, "foo");
    final Path bar = new Path(foo, "bar");
    dfs.mkdirs(bar);
    dfs.mkdirs(sdir2);

    SnapshotTestHelper.createSnapshot(dfs, sdir1, "s1");

    // /dir1/foo/bar -> /dir2/bar
    final Path bar2 = new Path(sdir2, "bar");
    dfs.rename(bar, bar2);

    // /dir1/foo -> /dir2/bar/foo
    final Path foo2 = new Path(bar2, "foo");
    dfs.rename(foo, foo2);

    // /dir2/bar
    INode bar2Node = fsdir.getINode(bar2.toString());
    Assert.assertTrue(bar2Node instanceof INodeReference.DstReference);
    byte[][] bar2Components = FSDirectory
        .getPathComponentsWithSnapshot(bar2Node);
    DFSTestUtil.checkComponentsEquals(INode.getPathComponents(bar2.toString()),
        bar2Components);

    // /dir1/.snapshot/s1/foo/bar
    String oldbar = SnapshotTestHelper.getSnapshotPath(sdir1, "s1", "foo/bar")
        .toString();
    INode oldbarNode = fsdir.getINode(oldbar);
    Assert.assertTrue(oldbarNode instanceof INodeReference.WithName);
    byte[][] oldbarComponents = FSDirectory
        .getPathComponentsWithSnapshot(oldbarNode);
    DFSTestUtil.checkComponentsEquals(INode.getPathComponents(oldbar),
        oldbarComponents);

    // /dir2/bar/foo
    INode foo2Node = fsdir.getINode(foo2.toString());
    Assert.assertTrue(foo2Node instanceof INodeReference.DstReference);
    byte[][] foo2Components = FSDirectory
        .getPathComponentsWithSnapshot(foo2Node);
    DFSTestUtil.checkComponentsEquals(INode.getPathComponents(foo2.toString()),
        foo2Components);

    // /dir1/.snapshot/s1/foo
    String oldfoo = SnapshotTestHelper.getSnapshotPath(sdir1, "s1",
        foo.getName()).toString();
    INode oldfooNode = fsdir.getINode(oldfoo);
    Assert.assertTrue(oldfooNode instanceof INodeReference.WithName);
    byte[][] oldfooComponents = FSDirectory
        .getPathComponentsWithSnapshot(oldfooNode);
    DFSTestUtil.checkComponentsEquals(INode.getPathComponents(oldfoo),
        oldfooComponents);
  }
}
