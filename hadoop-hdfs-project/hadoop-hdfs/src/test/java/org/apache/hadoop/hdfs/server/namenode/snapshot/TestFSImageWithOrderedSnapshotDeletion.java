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
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.visitor.NamespacePrintVisitor;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.event.Level;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import static org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotManager.DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED;
import static org.junit.Assert.assertTrue;

/**
 * Test FSImage correctness with ordered snapshot deletion.
 */
public class TestFSImageWithOrderedSnapshotDeletion {
  {
    SnapshotTestHelper.disableLogs();
    GenericTestUtils.setLogLevel(INode.LOG, Level.TRACE);
  }

  static final long SEED = 0;
  static final short NUM_DATANODES = 1;
  static final int BLOCKSIZE = 1024;

  private final Path dir = new Path("/TestSnapshot");
  private static final String TEST_DIR =
      GenericTestUtils.getTestDir().getAbsolutePath();

  Configuration conf;
  MiniDFSCluster cluster;
  FSNamesystem fsn;
  DistributedFileSystem hdfs;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setBoolean(DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED, true);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATANODES)
        .build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    hdfs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  void rename(Path src, Path dst) throws Exception {
    printTree("Before rename " + src + " -> " + dst);
    hdfs.rename(src, dst);
    printTree("After rename " + src + " -> " + dst);
  }

  void createFile(Path directory, String filename) throws Exception {
    final Path f = new Path(directory, filename);
    DFSTestUtil.createFile(hdfs, f, 0, NUM_DATANODES, SEED);
  }

  void appendFile(Path directory, String filename) throws Exception {
    final Path f = new Path(directory, filename);
    DFSTestUtil.appendFile(hdfs, f, "more data");
    printTree("appended " + f);
  }

  void deleteSnapshot(Path directory, String snapshotName) throws Exception {
    hdfs.deleteSnapshot(directory, snapshotName);
    printTree("deleted snapshot " + snapshotName);
  }

  @Test (timeout=60000)
  public void testDoubleRename() throws Exception {
    final Path parent = new Path("/parent");
    hdfs.mkdirs(parent);
    final Path sub1 = new Path(parent, "sub1");
    final Path sub1foo = new Path(sub1, "foo");
    hdfs.mkdirs(sub1);
    hdfs.mkdirs(sub1foo);
    createFile(sub1foo, "file0");

    printTree("before s0");
    hdfs.allowSnapshot(parent);
    hdfs.createSnapshot(parent, "s0");

    createFile(sub1foo, "file1");
    createFile(sub1foo, "file2");

    final Path sub2 = new Path(parent, "sub2");
    hdfs.mkdirs(sub2);
    final Path sub2foo = new Path(sub2, "foo");
    // mv /parent/sub1/foo to /parent/sub2/foo
    rename(sub1foo, sub2foo);

    hdfs.createSnapshot(parent, "s1");
    hdfs.createSnapshot(parent, "s2");
    printTree("created snapshots: s1, s2");

    appendFile(sub2foo, "file1");
    createFile(sub2foo, "file3");

    final Path sub3 = new Path(parent, "sub3");
    hdfs.mkdirs(sub3);
    // mv /parent/sub2/foo to /parent/sub3/foo
    rename(sub2foo, sub3);

    hdfs.delete(sub3,  true);
    printTree("deleted " + sub3);

    deleteSnapshot(parent, "s1");
    restartCluster();

    deleteSnapshot(parent, "s2");
    restartCluster();
  }

  private File getDumpTreeFile(String directory, String suffix) {
    return new File(directory, String.format("dumpTree_%s", suffix));
  }
  /**
   * Dump the fsdir tree to a temp file.
   * @param fileSuffix suffix of the temp file for dumping
   * @return the temp file
   */
  private File dumpTree2File(String fileSuffix) throws IOException {
    File file = getDumpTreeFile(TEST_DIR, fileSuffix);
    SnapshotTestHelper.dumpTree2File(fsn.getFSDirectory(), file);
    return file;
  }

  void restartCluster() throws Exception {
    final File before = dumpTree2File("before.txt");

    hdfs.setSafeMode(SafeModeAction.ENTER);
    hdfs.saveNamespace();
    hdfs.setSafeMode(SafeModeAction.LEAVE);

    cluster.shutdown();
    cluster = new MiniDFSCluster.Builder(conf).format(false)
        .numDataNodes(NUM_DATANODES).build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    hdfs = cluster.getFileSystem();
    final File after = dumpTree2File("after.txt");
    SnapshotTestHelper.compareDumpedTreeInFile(before, after, true);
  }

  private final PrintWriter output = new PrintWriter(System.out, true);
  private int printTreeCount = 0;

  String printTree(String label) throws Exception {
    output.println();
    output.println();
    output.println("***** " + printTreeCount++ + ": " + label);
    final String b =
        fsn.getFSDirectory().getINode("/").dumpTreeRecursively().toString();
    output.println(b);

    final String s = NamespacePrintVisitor.print2Sting(fsn);
    Assert.assertEquals(b, s);
    return b;
  }

  @Test (timeout=60000)
  public void testFSImageWithDoubleRename() throws Exception {
    final Path dir1 = new Path("/dir1");
    final Path dir2 = new Path("/dir2");
    hdfs.mkdirs(dir1);
    hdfs.mkdirs(dir2);
    Path dira = new Path(dir1, "dira");
    Path dirx = new Path(dir1, "dirx");
    Path dirb = new Path(dira, "dirb");
    hdfs.mkdirs(dira);
    hdfs.mkdirs(dirb);
    hdfs.mkdirs(dirx);
    hdfs.allowSnapshot(dir1);
    hdfs.createSnapshot(dir1, "s0");
    Path file1 = new Path(dirb, "file1");
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, (short) 1, SEED);
    Path rennamePath = new Path(dirx, "dirb");
    // mv /dir1/dira/dirb to /dir1/dirx/dirb
    hdfs.rename(dirb, rennamePath);
    hdfs.createSnapshot(dir1, "s1");
    DFSTestUtil.appendFile(hdfs, new Path("/dir1/dirx/dirb/file1"),
        "more data");
    Path renamePath1 = new Path(dir2, "dira");
    hdfs.mkdirs(renamePath1);
    //mv dirx/dirb to /dir2/dira/dirb
    hdfs.rename(rennamePath, renamePath1);
    hdfs.delete(renamePath1, true);
    hdfs.deleteSnapshot(dir1, "s1");
    // save namespace and restart cluster
    hdfs.setSafeMode(SafeModeAction.ENTER);
    hdfs.saveNamespace();
    hdfs.setSafeMode(SafeModeAction.LEAVE);
    cluster.shutdown();
    cluster = new MiniDFSCluster.Builder(conf).format(false)
        .numDataNodes(NUM_DATANODES).build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    hdfs = cluster.getFileSystem();
  }


  @Test (timeout=60000)
  public void testFSImageWithRename1() throws Exception {
    final Path dir1 = new Path("/dir1");
    final Path dir2 = new Path("/dir2");
    hdfs.mkdirs(dir1);
    hdfs.mkdirs(dir2);
    Path dira = new Path(dir1, "dira");
    Path dirx = new Path(dir1, "dirx");
    Path dirb = new Path(dirx, "dirb");
    hdfs.mkdirs(dira);
    hdfs.mkdirs(dirx);
    hdfs.allowSnapshot(dir1);
    hdfs.createSnapshot(dir1, "s0");
    hdfs.mkdirs(dirb);
    hdfs.createSnapshot(dir1, "s1");
    Path rennamePath = new Path(dira, "dirb");
    // mv /dir1/dirx/dirb to /dir1/dira/dirb
    hdfs.rename(dirb, rennamePath);
    hdfs.createSnapshot(dir1, "s2");
    Path diry = new Path("/dir1/dira/dirb/diry");
    hdfs.mkdirs(diry);
    hdfs.createSnapshot(dir1, "s3");
    Path file1 = new Path("/dir1/dira/dirb/diry/file1");
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, (short) 1, SEED);
    hdfs.createSnapshot(dir1, "s4");
    hdfs.delete(new Path("/dir1/dira/dirb"), true);
    hdfs.deleteSnapshot(dir1, "s1");
    hdfs.deleteSnapshot(dir1, "s3");
    // file1 should exist in the last snapshot
    assertTrue(hdfs.exists(
        new Path("/dir1/.snapshot/s4/dira/dirb/diry/file1")));

    // save namespace and restart cluster
    hdfs.setSafeMode(SafeModeAction.ENTER);
    hdfs.saveNamespace();
    hdfs.setSafeMode(SafeModeAction.LEAVE);

    cluster.shutdown();
    cluster = new MiniDFSCluster.Builder(conf).format(false)
        .numDataNodes(NUM_DATANODES).build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    hdfs = cluster.getFileSystem();
  }

  @Test (timeout=60000)
  public void testFSImageWithRename2() throws Exception {
    final Path dir1 = new Path("/dir1");
    final Path dir2 = new Path("/dir2");
    hdfs.mkdirs(dir1);
    hdfs.mkdirs(dir2);
    Path dira = new Path(dir1, "dira");
    Path dirx = new Path(dir1, "dirx");
    Path dirb = new Path(dirx, "dirb");
    hdfs.mkdirs(dira);
    hdfs.mkdirs(dirx);
    hdfs.allowSnapshot(dir1);
    hdfs.createSnapshot(dir1, "s0");
    hdfs.mkdirs(dirb);
    hdfs.createSnapshot(dir1, "s1");
    Path rennamePath = new Path(dira, "dirb");
    // mv /dir1/dirx/dirb to /dir1/dira/dirb
    hdfs.rename(dirb, rennamePath);
    hdfs.createSnapshot(dir1, "s2");
    Path file1 = new Path("/dir1/dira/dirb/file1");
    DFSTestUtil.createFile(hdfs,
        new Path(
            "/dir1/dira/dirb/file1"), BLOCKSIZE, (short) 1, SEED);
    hdfs.createSnapshot(dir1, "s3");
    hdfs.deleteSnapshot(dir1, "s1");
    hdfs.deleteSnapshot(dir1, "s3");
    assertTrue(hdfs.exists(file1));

    // save namespace and restart cluster
    hdfs.setSafeMode(SafeModeAction.ENTER);
    hdfs.saveNamespace();
    hdfs.setSafeMode(SafeModeAction.LEAVE);

    cluster.shutdown();
    cluster = new MiniDFSCluster.Builder(conf).format(false)
        .numDataNodes(NUM_DATANODES).build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    hdfs = cluster.getFileSystem();
  }

  @Test(timeout = 60000)
  public void testFSImageWithRename3() throws Exception {
    final Path dir1 = new Path("/dir1");
    final Path dir2 = new Path("/dir2");
    hdfs.mkdirs(dir1);
    hdfs.mkdirs(dir2);
    Path dira = new Path(dir1, "dira");
    Path dirx = new Path(dir1, "dirx");
    Path dirb = new Path(dirx, "dirb");
    hdfs.mkdirs(dira);
    hdfs.mkdirs(dirx);
    hdfs.allowSnapshot(dir1);
    hdfs.createSnapshot(dir1, "s0");
    hdfs.mkdirs(dirb);
    hdfs.createSnapshot(dir1, "s1");
    Path rennamePath = new Path(dira, "dirb");
    // mv /dir1/dirx/dirb to /dir1/dira/dirb
    hdfs.rename(dirb, rennamePath);
    hdfs.createSnapshot(dir1, "s2");
    Path diry = new Path("/dir1/dira/dirb/diry");
    hdfs.mkdirs(diry);
    hdfs.createSnapshot(dir1, "s3");
    Path file1 = new Path("/dir1/dira/dirb/diry/file1");
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, (short) 1, SEED);
    hdfs.createSnapshot(dir1, "s4");
    hdfs.delete(new Path("/dir1/dira/dirb"), true);
    hdfs.deleteSnapshot(dir1, "s1");
    hdfs.deleteSnapshot(dir1, "s3");
    // file1 should exist in the last snapshot
    assertTrue(hdfs.exists(new Path(
        "/dir1/.snapshot/s4/dira/dirb/diry/file1")));

    // save namespace and restart cluster
    hdfs.setSafeMode(SafeModeAction.ENTER);
    hdfs.saveNamespace();
    hdfs.setSafeMode(SafeModeAction.LEAVE);

    cluster.shutdown();
    cluster = new MiniDFSCluster.Builder(conf).format(false)
        .numDataNodes(NUM_DATANODES).build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    hdfs = cluster.getFileSystem();
  }

  @Test (timeout=60000)
  public void testFSImageWithRename4() throws Exception {
    final Path dir1 = new Path("/dir1");
    final Path dir2 = new Path("/dir2");
    hdfs.mkdirs(dir1);
    hdfs.mkdirs(dir2);
    Path dira = new Path(dir1, "dira");
    Path dirx = new Path(dir1, "dirx");
    Path dirb = new Path(dirx, "dirb");
    hdfs.mkdirs(dira);
    hdfs.mkdirs(dirx);
    hdfs.allowSnapshot(dir1);
    hdfs.createSnapshot(dir1, "s0");
    hdfs.mkdirs(dirb);
    hdfs.createSnapshot(dir1, "s1");
    Path renamePath = new Path(dira, "dirb");
    // mv /dir1/dirx/dirb to /dir1/dira/dirb
    hdfs.rename(dirb, renamePath);
    hdfs.createSnapshot(dir1, "s2");
    Path diry = new Path("/dir1/dira/dirb/diry");
    hdfs.mkdirs(diry);
    hdfs.createSnapshot(dir1, "s3");
    Path file1 = new Path("/dir1/dira/dirb/diry/file1");
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, (short) 1, SEED);
    hdfs.createSnapshot(dir1, "s4");
    hdfs.delete(new Path("/dir1/dira/dirb/diry/file1"), false);
    hdfs.deleteSnapshot(dir1, "s1");
    hdfs.deleteSnapshot(dir1, "s3");
    // file1 should exist in the last snapshot
    assertTrue(hdfs.exists(
        new Path("/dir1/.snapshot/s4/dira/dirb/diry/file1")));

    // save namespace and restart cluster
    hdfs.setSafeMode(SafeModeAction.ENTER);
    hdfs.saveNamespace();
    hdfs.setSafeMode(SafeModeAction.LEAVE);

    cluster.shutdown();
    cluster = new MiniDFSCluster.Builder(conf).format(false)
        .numDataNodes(NUM_DATANODES).build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    hdfs = cluster.getFileSystem();
  }

  @Test
  public void testFSImageWithRename5() throws Exception {
    final Path dir1 = new Path("/dir1");
    final Path dir2 = new Path("/dir2");
    hdfs.mkdirs(dir1);
    hdfs.mkdirs(dir2);
    Path dira = new Path(dir1, "dira");
    Path dirx = new Path(dir1, "dirx");
    Path dirb = new Path(dira, "dirb");
    Path dirc = new Path(dirb, "dirc");
    hdfs.mkdirs(dira);
    hdfs.mkdirs(dirb);
    hdfs.mkdirs(dirc);
    hdfs.mkdirs(dirx);
    hdfs.allowSnapshot(dir1);
    hdfs.createSnapshot(dir1, "s0");
    Path dird = new Path(dirc, "dird");
    Path dire = new Path(dird, "dire");
    Path file1 = new Path(dire, "file1");
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, (short) 1, SEED);
    Path rennamePath = new Path(dirx, "dirb");
    // mv /dir1/dira/dirb to /dir1/dirx/dirb
    hdfs.rename(dirb, rennamePath);
    hdfs.createSnapshot(dir1, "s1");
    DFSTestUtil.appendFile(hdfs,
        new Path("/dir1/dirx/dirb/dirc/dird/dire/file1"), "more data");
    Path renamePath1 = new Path(dir2, "dira");
    hdfs.mkdirs(renamePath1);
    //mv dirx/dirb to /dir2/dira/dirb
    hdfs.rename(rennamePath, renamePath1);
    hdfs.delete(renamePath1, true);
    hdfs.deleteSnapshot(dir1, "s1");
    // save namespace and restart cluster
    hdfs.setSafeMode(SafeModeAction.ENTER);
    hdfs.saveNamespace();
    hdfs.setSafeMode(SafeModeAction.LEAVE);
    cluster.shutdown();
    cluster = new MiniDFSCluster.Builder(conf).format(false)
        .numDataNodes(NUM_DATANODES).build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    hdfs = cluster.getFileSystem();
  }

  @Test (timeout=60000)
  public void testDoubleRenamesWithSnapshotDelete() throws Exception {
    final Path sub1 = new Path(dir, "sub1");
    hdfs.mkdirs(sub1);
    hdfs.allowSnapshot(sub1);
    final Path dir1 = new Path(sub1, "dir1");
    final Path dir2 = new Path(sub1, "dir2");
    final Path dir3 = new Path(sub1, "dir3");
    final String snap3 = "snap3";
    final String snap4 = "snap4";
    final String snap5 = "snap5";
    final String snap6 = "snap6";
    final Path foo = new Path(dir2, "foo");
    final Path bar = new Path(dir2, "bar");
    hdfs.createSnapshot(sub1, "snap1");
    hdfs.mkdirs(dir1, new FsPermission((short) 0777));
    rename(dir1, dir2);
    hdfs.createSnapshot(sub1, "snap2");
    DFSTestUtil.createFile(hdfs, foo, BLOCKSIZE, (short) 1, SEED);
    DFSTestUtil.createFile(hdfs, bar, BLOCKSIZE, (short) 1, SEED);
    hdfs.createSnapshot(sub1, snap3);
    hdfs.delete(foo, false);
    DFSTestUtil.createFile(hdfs, foo, BLOCKSIZE, (short) 1, SEED);
    hdfs.createSnapshot(sub1, snap4);
    hdfs.delete(foo, false);
    DFSTestUtil.createFile(hdfs, foo, BLOCKSIZE, (short) 1, SEED);
    hdfs.createSnapshot(sub1, snap5);
    rename(dir2, dir3);
    hdfs.createSnapshot(sub1, snap6);
    hdfs.delete(dir3, true);
    deleteSnapshot(sub1, snap6);
    deleteSnapshot(sub1, snap3);
    // save namespace and restart Namenode
    hdfs.setSafeMode(SafeModeAction.ENTER);
    hdfs.saveNamespace();
    hdfs.setSafeMode(SafeModeAction.LEAVE);
    cluster.restartNameNode(true);
  }
}
