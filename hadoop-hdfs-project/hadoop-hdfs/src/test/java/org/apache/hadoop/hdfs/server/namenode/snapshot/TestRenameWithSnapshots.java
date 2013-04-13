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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeReference;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithCount;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileWithSnapshot.FileDiff;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectoryWithSnapshot.DirectoryDiff;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Testing rename with snapshots. */
public class TestRenameWithSnapshots {
  {
    SnapshotTestHelper.disableLogs();
  }
  private static final Log LOG = LogFactory.getLog(TestRenameWithSnapshots.class);
  
  private static final long SEED = 0;
  private static final short REPL = 3;
  private static final short REPL_1 = 2;
  private static final short REPL_2 = 1;
  private static final long BLOCKSIZE = 1024;
  
  private static Configuration conf = new Configuration();
  private static MiniDFSCluster cluster;
  private static FSNamesystem fsn;
  private static FSDirectory fsdir;
  private static DistributedFileSystem hdfs;
  private static String testDir =
      System.getProperty("test.build.data", "build/test/data");
  static private final Path dir = new Path("/testRenameWithSnapshots");
  static private final Path sub1 = new Path(dir, "sub1");
  static private final Path file1 = new Path(sub1, "file1");
  static private final Path file2 = new Path(sub1, "file2");
  static private final Path file3 = new Path(sub1, "file3");
  static private final String snap1 = "snap1";
  static private final String snap2 = "snap2";

  
  @Before
  public void setUp() throws Exception {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPL).format(true)
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
    }
  }

  @Test (timeout=300000)
  public void testRenameFromSDir2NonSDir() throws Exception {
    final String dirStr = "/testRenameWithSnapshot";
    final String abcStr = dirStr + "/abc";
    final Path abc = new Path(abcStr);
    hdfs.mkdirs(abc, new FsPermission((short)0777));
    hdfs.allowSnapshot(abcStr);

    final Path foo = new Path(abc, "foo");
    DFSTestUtil.createFile(hdfs, foo, BLOCKSIZE, REPL, SEED);
    hdfs.createSnapshot(abc, "s0");
    
    try {
      hdfs.rename(abc, new Path(dirStr, "tmp"));
      fail("Expect exception since " + abc
          + " is snapshottable and already has snapshots");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains(abcStr
          + " is snapshottable and already has snapshots", e);
    }

    final String xyzStr = dirStr + "/xyz";
    final Path xyz = new Path(xyzStr);
    hdfs.mkdirs(xyz, new FsPermission((short)0777));
    final Path bar = new Path(xyz, "bar");
    hdfs.rename(foo, bar);
    
    final INode fooRef = fsdir.getINode(
        SnapshotTestHelper.getSnapshotPath(abc, "s0", "foo").toString());
    Assert.assertTrue(fooRef.isReference());
    Assert.assertTrue(fooRef.asReference() instanceof INodeReference.WithName);

    final INodeReference.WithCount withCount
        = (INodeReference.WithCount)fooRef.asReference().getReferredINode();
    Assert.assertEquals(2, withCount.getReferenceCount());

    final INode barRef = fsdir.getINode(bar.toString());
    Assert.assertTrue(barRef.isReference());

    Assert.assertSame(withCount, barRef.asReference().getReferredINode());
    
    hdfs.delete(bar, false);
    Assert.assertEquals(1, withCount.getReferenceCount());
  }
  
  private static boolean existsInDiffReport(List<DiffReportEntry> entries,
      DiffType type, String relativePath) {
    for (DiffReportEntry entry : entries) {
      System.out.println("DiffEntry is:" + entry.getType() + "\""
          + new String(entry.getRelativePath()) + "\"");
      if ((entry.getType() == type)
          && ((new String(entry.getRelativePath())).compareTo(relativePath) == 0)) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * Rename a file under a snapshottable directory, file does not exist
   * in a snapshot.
   */
  @Test (timeout=60000)
  public void testRenameFileNotInSnapshot() throws Exception {
    hdfs.mkdirs(sub1);
    hdfs.allowSnapshot(sub1.toString());
    hdfs.createSnapshot(sub1, snap1);
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPL, SEED);
    hdfs.rename(file1, file2);

    // Query the diff report and make sure it looks as expected.
    SnapshotDiffReport diffReport = hdfs.getSnapshotDiffReport(sub1, snap1, "");
    List<DiffReportEntry> entries = diffReport.getDiffList();
    assertTrue(entries.size() == 2);
    assertTrue(existsInDiffReport(entries, DiffType.MODIFY, ""));
    assertTrue(existsInDiffReport(entries, DiffType.CREATE, file2.getName()));
  }

  /**
   * Rename a file under a snapshottable directory, file exists
   * in a snapshot.
   */
  @Test (timeout=60000)
  public void testRenameFileInSnapshot() throws Exception {
    hdfs.mkdirs(sub1);
    hdfs.allowSnapshot(sub1.toString());
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPL, SEED);
    hdfs.createSnapshot(sub1, snap1);
    hdfs.rename(file1, file2);

    // Query the diff report and make sure it looks as expected.
    SnapshotDiffReport diffReport = hdfs.getSnapshotDiffReport(sub1, snap1, "");
    System.out.println("DiffList is " + diffReport.toString());
    List<DiffReportEntry> entries = diffReport.getDiffList();
    assertTrue(entries.size() == 3);
    assertTrue(existsInDiffReport(entries, DiffType.MODIFY, ""));
    assertTrue(existsInDiffReport(entries, DiffType.CREATE, file2.getName()));
    assertTrue(existsInDiffReport(entries, DiffType.DELETE, file1.getName()));
  }

  @Test (timeout=60000)
  public void testRenameTwiceInSnapshot() throws Exception {
    hdfs.mkdirs(sub1);
    hdfs.allowSnapshot(sub1.toString());
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPL, SEED);
    hdfs.createSnapshot(sub1, snap1);
    hdfs.rename(file1, file2);
    
    hdfs.createSnapshot(sub1, snap2);
    hdfs.rename(file2, file3);

    SnapshotDiffReport diffReport;
    
    // Query the diff report and make sure it looks as expected.
    diffReport = hdfs.getSnapshotDiffReport(sub1, snap1, snap2);
    LOG.info("DiffList is " + diffReport.toString());
    List<DiffReportEntry> entries = diffReport.getDiffList();
    assertTrue(entries.size() == 3);
    assertTrue(existsInDiffReport(entries, DiffType.MODIFY, ""));
    assertTrue(existsInDiffReport(entries, DiffType.CREATE, file2.getName()));
    assertTrue(existsInDiffReport(entries, DiffType.DELETE, file1.getName()));
    
    diffReport = hdfs.getSnapshotDiffReport(sub1, snap2, "");
    LOG.info("DiffList is " + diffReport.toString());
    entries = diffReport.getDiffList();
    assertTrue(entries.size() == 3);
    assertTrue(existsInDiffReport(entries, DiffType.MODIFY, ""));
    assertTrue(existsInDiffReport(entries, DiffType.CREATE, file3.getName()));
    assertTrue(existsInDiffReport(entries, DiffType.DELETE, file2.getName()));
    
    diffReport = hdfs.getSnapshotDiffReport(sub1, snap1, "");
    LOG.info("DiffList is " + diffReport.toString());
    entries = diffReport.getDiffList();
    assertTrue(entries.size() == 3);
    assertTrue(existsInDiffReport(entries, DiffType.MODIFY, ""));
    assertTrue(existsInDiffReport(entries, DiffType.CREATE, file3.getName()));
    assertTrue(existsInDiffReport(entries, DiffType.DELETE, file1.getName()));
  }
  
  @Test (timeout=60000)
  public void testRenameFileInSubDirOfDirWithSnapshot() throws Exception {
    final Path sub2 = new Path(sub1, "sub2");
    final Path sub2file1 = new Path(sub2, "sub2file1");
    final Path sub2file2 = new Path(sub2, "sub2file2");
    final String sub1snap1 = "sub1snap1";
    
    hdfs.mkdirs(sub1);
    hdfs.mkdirs(sub2);
    DFSTestUtil.createFile(hdfs, sub2file1, BLOCKSIZE, REPL, SEED);
    SnapshotTestHelper.createSnapshot(hdfs, sub1, sub1snap1);

    // Rename the file in the subdirectory.
    hdfs.rename(sub2file1, sub2file2);

    // Query the diff report and make sure it looks as expected.
    SnapshotDiffReport diffReport = hdfs.getSnapshotDiffReport(sub1, sub1snap1,
        "");
    LOG.info("DiffList is \n\"" + diffReport.toString() + "\"");
    List<DiffReportEntry> entries = diffReport.getDiffList();
    assertTrue(existsInDiffReport(entries, DiffType.MODIFY, sub2.getName()));
    assertTrue(existsInDiffReport(entries, DiffType.CREATE, sub2.getName()
        + "/" + sub2file2.getName()));
    assertTrue(existsInDiffReport(entries, DiffType.DELETE, sub2.getName()
        + "/" + sub2file1.getName()));
  }

  @Test (timeout=60000)
  public void testRenameDirectoryInSnapshot() throws Exception {
    final Path sub2 = new Path(sub1, "sub2");
    final Path sub3 = new Path(sub1, "sub3");
    final Path sub2file1 = new Path(sub2, "sub2file1");
    final String sub1snap1 = "sub1snap1";
    
    hdfs.mkdirs(sub1);
    hdfs.mkdirs(sub2);
    DFSTestUtil.createFile(hdfs, sub2file1, BLOCKSIZE, REPL, SEED);
    SnapshotTestHelper.createSnapshot(hdfs, sub1, sub1snap1);
    
    // First rename the sub-directory.
    hdfs.rename(sub2, sub3);
    
    // Query the diff report and make sure it looks as expected.
    SnapshotDiffReport diffReport = hdfs.getSnapshotDiffReport(sub1, sub1snap1,
        "");
    LOG.info("DiffList is \n\"" + diffReport.toString() + "\"");
    List<DiffReportEntry> entries = diffReport.getDiffList();
    assertEquals(3, entries.size());
    assertTrue(existsInDiffReport(entries, DiffType.MODIFY, ""));
    assertTrue(existsInDiffReport(entries, DiffType.CREATE, sub3.getName()));
    assertTrue(existsInDiffReport(entries, DiffType.DELETE, sub2.getName()));
  }
  
  /**
   * After the following steps:
   * <pre>
   * 1. Take snapshot s1 on /dir1 at time t1.
   * 2. Take snapshot s2 on /dir2 at time t2.
   * 3. Modify the subtree of /dir2/foo/ to make it a dir with snapshots.
   * 4. Take snapshot s3 on /dir1 at time t3.
   * 5. Rename /dir2/foo/ to /dir1/foo/.
   * </pre>
   * When changes happening on foo, the diff should be recorded in snapshot s2. 
   */
  @Test (timeout=60000)
  public void testRenameDirAcrossSnapshottableDirs() throws Exception {
    final Path sdir1 = new Path("/dir1");
    final Path sdir2 = new Path("/dir2");
    hdfs.mkdirs(sdir1);
    hdfs.mkdirs(sdir2);
    final Path foo = new Path(sdir2, "foo");
    final Path bar = new Path(foo, "bar");
    final Path bar2 = new Path(foo, "bar2");
    DFSTestUtil.createFile(hdfs, bar, BLOCKSIZE, REPL, SEED);
    DFSTestUtil.createFile(hdfs, bar2, BLOCKSIZE, REPL, SEED);
    
    SnapshotTestHelper.createSnapshot(hdfs, sdir1, "s1");
    SnapshotTestHelper.createSnapshot(hdfs, sdir2, "s2");
    
    hdfs.setReplication(bar2, REPL_1);
    hdfs.delete(bar, true);
    
    hdfs.createSnapshot(sdir1, "s3");
    
    final Path newfoo = new Path(sdir1, "foo");
    hdfs.rename(foo, newfoo);
    
    // still can visit the snapshot copy of bar through 
    // /dir2/.snapshot/s2/foo/bar
    final Path snapshotBar = SnapshotTestHelper.getSnapshotPath(sdir2, "s2",
        "foo/bar");
    assertTrue(hdfs.exists(snapshotBar));
    
    // delete bar2
    final Path newBar2 = new Path(newfoo, "bar2");
    assertTrue(hdfs.exists(newBar2));
    hdfs.delete(newBar2, true);
    
    // /dir2/.snapshot/s2/foo/bar2 should still work
    final Path bar2_s2 = SnapshotTestHelper.getSnapshotPath(sdir2, "s2",
        "foo/bar2");
    assertTrue(hdfs.exists(bar2_s2));
    FileStatus status = hdfs.getFileStatus(bar2_s2);
    assertEquals(REPL, status.getReplication());
    final Path bar2_s3 = SnapshotTestHelper.getSnapshotPath(sdir1, "s3",
        "foo/bar2");
    assertFalse(hdfs.exists(bar2_s3));
  }
  
  /**
   * Rename a single file across snapshottable dirs.
   */
  @Test (timeout=60000)
  public void testRenameFileAcrossSnapshottableDirs() throws Exception {
    final Path sdir1 = new Path("/dir1");
    final Path sdir2 = new Path("/dir2");
    hdfs.mkdirs(sdir1);
    hdfs.mkdirs(sdir2);
    final Path foo = new Path(sdir2, "foo");
    DFSTestUtil.createFile(hdfs, foo, BLOCKSIZE, REPL, SEED);
    
    SnapshotTestHelper.createSnapshot(hdfs, sdir1, "s1");
    SnapshotTestHelper.createSnapshot(hdfs, sdir2, "s2");
    hdfs.createSnapshot(sdir1, "s3");
    
    final Path newfoo = new Path(sdir1, "foo");
    hdfs.rename(foo, newfoo);
    
    // change the replication factor of foo
    hdfs.setReplication(newfoo, REPL_1);
    
    // /dir2/.snapshot/s2/foo should still work
    final Path foo_s2 = SnapshotTestHelper.getSnapshotPath(sdir2, "s2",
        "foo");
    assertTrue(hdfs.exists(foo_s2));
    FileStatus status = hdfs.getFileStatus(foo_s2);
    assertEquals(REPL, status.getReplication());
    
    final Path foo_s3 = SnapshotTestHelper.getSnapshotPath(sdir1, "s3",
        "foo");
    assertFalse(hdfs.exists(foo_s3));
    INodeFileWithSnapshot sfoo = (INodeFileWithSnapshot) fsdir.getINode(
        newfoo.toString()).asFile();
    assertEquals("s2", sfoo.getDiffs().getLastSnapshot().getRoot()
        .getLocalName());
  }
  
  /**
   * Test renaming a dir and then delete snapshots.
   */
  @Test
  public void testRenameDirAndDeleteSnapshot() throws Exception {
    final Path sdir1 = new Path("/dir1");
    final Path sdir2 = new Path("/dir2");
    hdfs.mkdirs(sdir1);
    hdfs.mkdirs(sdir2);
    final Path foo = new Path(sdir2, "foo");
    final Path bar = new Path(foo, "bar");
    final Path bar2 = new Path(foo, "bar2");
    DFSTestUtil.createFile(hdfs, bar, BLOCKSIZE, REPL, SEED);
    DFSTestUtil.createFile(hdfs, bar2, BLOCKSIZE, REPL, SEED);
    
    SnapshotTestHelper.createSnapshot(hdfs, sdir1, "s1");
    SnapshotTestHelper.createSnapshot(hdfs, sdir2, "s2");
    hdfs.createSnapshot(sdir1, "s3");
    
    final Path newfoo = new Path(sdir1, "foo");
    hdfs.rename(foo, newfoo);
    
    final Path bar3 = new Path(newfoo, "bar3");
    DFSTestUtil.createFile(hdfs, bar3, BLOCKSIZE, REPL, SEED);
    
    hdfs.createSnapshot(sdir1, "s4");
    hdfs.delete(bar, true);
    hdfs.delete(bar3, true);
    
    assertFalse(hdfs.exists(bar3));
    assertFalse(hdfs.exists(bar));
    final Path bar_s4 = SnapshotTestHelper.getSnapshotPath(sdir1, "s4",
        "foo/bar");
    final Path bar3_s4 = SnapshotTestHelper.getSnapshotPath(sdir1, "s4",
        "foo/bar3");
    assertTrue(hdfs.exists(bar_s4));
    assertTrue(hdfs.exists(bar3_s4));
    
    hdfs.createSnapshot(sdir1, "s5");
    hdfs.delete(bar2, true);
    assertFalse(hdfs.exists(bar2));
    final Path bar2_s5 = SnapshotTestHelper.getSnapshotPath(sdir1, "s5",
        "foo/bar2");
    assertTrue(hdfs.exists(bar2_s5));
    
    // delete snapshot s5. The diff of s5 should be combined to s4
    hdfs.deleteSnapshot(sdir1, "s5");
    restartClusterAndCheckImage();
    assertFalse(hdfs.exists(bar2_s5));
    final Path bar2_s4 = SnapshotTestHelper.getSnapshotPath(sdir1, "s4",
        "foo/bar2");
    assertTrue(hdfs.exists(bar2_s4));
    
    // delete snapshot s4. The diff of s4 should be combined to s2 instead of
    // s3.
    hdfs.deleteSnapshot(sdir1, "s4");
    
    assertFalse(hdfs.exists(bar_s4));
    Path bar_s3 = SnapshotTestHelper.getSnapshotPath(sdir1, "s3", "foo/bar");
    assertFalse(hdfs.exists(bar_s3));
    bar_s3 = SnapshotTestHelper.getSnapshotPath(sdir2, "s3", "foo/bar");
    assertFalse(hdfs.exists(bar_s3));
    final Path bar_s2 = SnapshotTestHelper.getSnapshotPath(sdir2, "s2",
        "foo/bar");
    assertTrue(hdfs.exists(bar_s2));
    
    assertFalse(hdfs.exists(bar2_s4));
    Path bar2_s3 = SnapshotTestHelper.getSnapshotPath(sdir1, "s3", "foo/bar2");
    assertFalse(hdfs.exists(bar2_s3));
    bar2_s3 = SnapshotTestHelper.getSnapshotPath(sdir2, "s3", "foo/bar2");
    assertFalse(hdfs.exists(bar2_s3));
    final Path bar2_s2 = SnapshotTestHelper.getSnapshotPath(sdir2, "s2",
        "foo/bar2");
    assertTrue(hdfs.exists(bar2_s2));
    
    assertFalse(hdfs.exists(bar3_s4));
    Path bar3_s3 = SnapshotTestHelper.getSnapshotPath(sdir1, "s3", "foo/bar3");
    assertFalse(hdfs.exists(bar3_s3));
    bar3_s3 = SnapshotTestHelper.getSnapshotPath(sdir2, "s3", "foo/bar3");
    assertFalse(hdfs.exists(bar3_s3));
    final Path bar3_s2 = SnapshotTestHelper.getSnapshotPath(sdir2, "s2",
        "foo/bar3");
    assertFalse(hdfs.exists(bar3_s2));
    
    // restart the cluster and check fsimage
    restartClusterAndCheckImage();
    
    // delete snapshot s2.
    hdfs.deleteSnapshot(sdir2, "s2");
    assertFalse(hdfs.exists(bar_s2));
    assertFalse(hdfs.exists(bar2_s2));
    
    // restart the cluster and check fsimage
    restartClusterAndCheckImage();
    hdfs.deleteSnapshot(sdir1, "s3");
    restartClusterAndCheckImage();
    hdfs.deleteSnapshot(sdir1, "s1");
    restartClusterAndCheckImage();
  }
  
  private void restartClusterAndCheckImage() throws IOException {
    File fsnBefore = new File(testDir, "dumptree_before");
    File fsnMiddle = new File(testDir, "dumptree_middle");
    File fsnAfter = new File(testDir, "dumptree_after");
    
    SnapshotTestHelper.dumpTree2File(fsdir, fsnBefore);
    
    cluster.shutdown();
    cluster = new MiniDFSCluster.Builder(conf).format(false)
        .numDataNodes(REPL).build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    fsdir = fsn.getFSDirectory();
    hdfs = cluster.getFileSystem();
    // later check fsnMiddle to see if the edit log is applied correctly 
    SnapshotTestHelper.dumpTree2File(fsdir, fsnMiddle);
   
    // save namespace and restart cluster
    hdfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
    hdfs.saveNamespace();
    hdfs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
    cluster.shutdown();
    cluster = new MiniDFSCluster.Builder(conf).format(false)
        .numDataNodes(REPL).build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    fsdir = fsn.getFSDirectory();
    hdfs = cluster.getFileSystem();
    // dump the namespace loaded from fsimage
    SnapshotTestHelper.dumpTree2File(fsdir, fsnAfter);
    
    SnapshotTestHelper.compareDumpedTreeInFile(fsnBefore, fsnMiddle, false);
    SnapshotTestHelper.compareDumpedTreeInFile(fsnBefore, fsnAfter, false);
  }
  
  /**
   * Test renaming a file and then delete snapshots.
   */
  @Test
  public void testRenameFileAndDeleteSnapshot() throws Exception {
    final Path sdir1 = new Path("/dir1");
    final Path sdir2 = new Path("/dir2");
    hdfs.mkdirs(sdir1);
    hdfs.mkdirs(sdir2);
    final Path foo = new Path(sdir2, "foo");
    DFSTestUtil.createFile(hdfs, foo, BLOCKSIZE, REPL, SEED);
    
    SnapshotTestHelper.createSnapshot(hdfs, sdir1, "s1");
    SnapshotTestHelper.createSnapshot(hdfs, sdir2, "s2");
    hdfs.createSnapshot(sdir1, "s3");
    
    final Path newfoo = new Path(sdir1, "foo");
    hdfs.rename(foo, newfoo);
    
    hdfs.setReplication(newfoo, REPL_1);
    
    hdfs.createSnapshot(sdir1, "s4");
    hdfs.setReplication(newfoo, REPL_2);
    
    FileStatus status = hdfs.getFileStatus(newfoo);
    assertEquals(REPL_2, status.getReplication());
    final Path foo_s4 = SnapshotTestHelper.getSnapshotPath(sdir1, "s4", "foo");
    status = hdfs.getFileStatus(foo_s4);
    assertEquals(REPL_1, status.getReplication());
    
    hdfs.createSnapshot(sdir1, "s5");
    final Path foo_s5 = SnapshotTestHelper.getSnapshotPath(sdir1, "s5", "foo");
    status = hdfs.getFileStatus(foo_s5);
    assertEquals(REPL_2, status.getReplication());
    
    // delete snapshot s5.
    hdfs.deleteSnapshot(sdir1, "s5");
    restartClusterAndCheckImage();
    assertFalse(hdfs.exists(foo_s5));
    status = hdfs.getFileStatus(foo_s4);
    assertEquals(REPL_1, status.getReplication());
    
    // delete snapshot s4.
    hdfs.deleteSnapshot(sdir1, "s4");
    
    assertFalse(hdfs.exists(foo_s4));
    Path foo_s3 = SnapshotTestHelper.getSnapshotPath(sdir1, "s3", "foo");
    assertFalse(hdfs.exists(foo_s3));
    foo_s3 = SnapshotTestHelper.getSnapshotPath(sdir2, "s3", "foo");
    assertFalse(hdfs.exists(foo_s3));
    final Path foo_s2 = SnapshotTestHelper.getSnapshotPath(sdir2, "s2", "foo");
    assertTrue(hdfs.exists(foo_s2));
    status = hdfs.getFileStatus(foo_s2);
    assertEquals(REPL, status.getReplication());
    
    INodeFileWithSnapshot snode = (INodeFileWithSnapshot) fsdir.getINode(
        newfoo.toString()).asFile();
    assertEquals(1, snode.getDiffs().asList().size());
    assertEquals("s2", snode.getDiffs().getLastSnapshot().getRoot()
        .getLocalName());
    
    // restart cluster
    restartClusterAndCheckImage();
    
    // delete snapshot s2.
    hdfs.deleteSnapshot(sdir2, "s2");
    assertFalse(hdfs.exists(foo_s2));
    
    // restart the cluster and check fsimage
    restartClusterAndCheckImage();
    hdfs.deleteSnapshot(sdir1, "s3");
    restartClusterAndCheckImage();
    hdfs.deleteSnapshot(sdir1, "s1");
    restartClusterAndCheckImage();
  }
  
  /**
   * Test rename a dir and a file multiple times across snapshottable 
   * directories: /dir1/foo -> /dir2/foo -> /dir3/foo -> /dir2/foo -> /dir1/foo
   * 
   * Only create snapshots in the beginning (before the rename).
   */
  @Test
  public void testRenameMoreThanOnceAcrossSnapDirs() throws Exception {
    final Path sdir1 = new Path("/dir1");
    final Path sdir2 = new Path("/dir2");
    final Path sdir3 = new Path("/dir3");
    hdfs.mkdirs(sdir1);
    hdfs.mkdirs(sdir2);
    hdfs.mkdirs(sdir3);
    
    final Path foo_dir1 = new Path(sdir1, "foo");
    final Path bar1_dir1 = new Path(foo_dir1, "bar1");
    final Path bar2_dir1 = new Path(sdir1, "bar");
    DFSTestUtil.createFile(hdfs, bar1_dir1, BLOCKSIZE, REPL, SEED);
    DFSTestUtil.createFile(hdfs, bar2_dir1, BLOCKSIZE, REPL, SEED);
    
    SnapshotTestHelper.createSnapshot(hdfs, sdir1, "s1");
    SnapshotTestHelper.createSnapshot(hdfs, sdir2, "s2");
    SnapshotTestHelper.createSnapshot(hdfs, sdir3, "s3");
    
    // 1. /dir1/foo -> /dir2/foo, /dir1/bar -> /dir2/bar
    final Path foo_dir2 = new Path(sdir2, "foo");
    hdfs.rename(foo_dir1, foo_dir2);
    final Path bar2_dir2 = new Path(sdir2, "bar");
    hdfs.rename(bar2_dir1, bar2_dir2);
    
    // restart the cluster and check fsimage
    restartClusterAndCheckImage();
    
    // modification on /dir2/foo and /dir2/bar
    final Path bar1_dir2 = new Path(foo_dir2, "bar1");
    hdfs.setReplication(bar1_dir2, REPL_1);
    hdfs.setReplication(bar2_dir2, REPL_1);
    
    // check
    final Path bar1_s1 = SnapshotTestHelper.getSnapshotPath(sdir1, "s1",
        "foo/bar1");
    final Path bar2_s1 = SnapshotTestHelper.getSnapshotPath(sdir1, "s1",
        "bar");
    final Path bar1_s2 = SnapshotTestHelper.getSnapshotPath(sdir2, "s2",
        "foo/bar1");
    final Path bar2_s2 = SnapshotTestHelper.getSnapshotPath(sdir2, "s2",
        "bar");
    assertTrue(hdfs.exists(bar1_s1));
    assertTrue(hdfs.exists(bar2_s1));
    assertFalse(hdfs.exists(bar1_s2));
    assertFalse(hdfs.exists(bar2_s2));
    FileStatus statusBar1 = hdfs.getFileStatus(bar1_s1);
    assertEquals(REPL, statusBar1.getReplication());
    statusBar1 = hdfs.getFileStatus(bar1_dir2);
    assertEquals(REPL_1, statusBar1.getReplication());
    FileStatus statusBar2 = hdfs.getFileStatus(bar2_s1);
    assertEquals(REPL, statusBar2.getReplication());
    statusBar2 = hdfs.getFileStatus(bar2_dir2);
    assertEquals(REPL_1, statusBar2.getReplication());
    
    // 2. /dir2/foo -> /dir3/foo, /dir2/bar -> /dir3/bar
    final Path foo_dir3 = new Path(sdir3, "foo");
    hdfs.rename(foo_dir2, foo_dir3);
    final Path bar2_dir3 = new Path(sdir3, "bar");
    hdfs.rename(bar2_dir2, bar2_dir3);
    
    // restart the cluster and check fsimage
    restartClusterAndCheckImage();
    
    // modification on /dir3/foo and /dir3/bar
    final Path bar1_dir3 = new Path(foo_dir3, "bar1");
    hdfs.setReplication(bar1_dir3, REPL_2);
    hdfs.setReplication(bar2_dir3, REPL_2);
    
    // check
    final Path bar1_s3 = SnapshotTestHelper.getSnapshotPath(sdir3, "s3",
        "foo/bar1");
    final Path bar2_s3 = SnapshotTestHelper.getSnapshotPath(sdir3, "s3",
        "bar");
    assertTrue(hdfs.exists(bar1_s1));
    assertTrue(hdfs.exists(bar2_s1));
    assertFalse(hdfs.exists(bar1_s2));
    assertFalse(hdfs.exists(bar2_s2));
    assertFalse(hdfs.exists(bar1_s3));
    assertFalse(hdfs.exists(bar2_s3));
    statusBar1 = hdfs.getFileStatus(bar1_s1);
    assertEquals(REPL, statusBar1.getReplication());
    statusBar1 = hdfs.getFileStatus(bar1_dir3);
    assertEquals(REPL_2, statusBar1.getReplication());
    statusBar2 = hdfs.getFileStatus(bar2_s1);
    assertEquals(REPL, statusBar2.getReplication());
    statusBar2 = hdfs.getFileStatus(bar2_dir3);
    assertEquals(REPL_2, statusBar2.getReplication());
    
    // 3. /dir3/foo -> /dir2/foo, /dir3/bar -> /dir2/bar
    hdfs.rename(foo_dir3, foo_dir2);
    hdfs.rename(bar2_dir3, bar2_dir2);
    
    // restart the cluster and check fsimage
    restartClusterAndCheckImage();
    
    // modification on /dir2/foo
    hdfs.setReplication(bar1_dir2, REPL);
    hdfs.setReplication(bar2_dir2, REPL);
    
    // check
    assertTrue(hdfs.exists(bar1_s1));
    assertTrue(hdfs.exists(bar2_s1));
    assertFalse(hdfs.exists(bar1_s2));
    assertFalse(hdfs.exists(bar2_s2));
    assertFalse(hdfs.exists(bar1_s3));
    assertFalse(hdfs.exists(bar2_s3));
    statusBar1 = hdfs.getFileStatus(bar1_s1);
    assertEquals(REPL, statusBar1.getReplication());
    statusBar1 = hdfs.getFileStatus(bar1_dir2);
    assertEquals(REPL, statusBar1.getReplication());
    statusBar2 = hdfs.getFileStatus(bar2_s1);
    assertEquals(REPL, statusBar2.getReplication());
    statusBar2 = hdfs.getFileStatus(bar2_dir2);
    assertEquals(REPL, statusBar2.getReplication());
    
    // 4. /dir2/foo -> /dir1/foo, /dir2/bar -> /dir1/bar
    hdfs.rename(foo_dir2, foo_dir1);
    hdfs.rename(bar2_dir2, bar2_dir1);
    
    // check the internal details
    INodeReference fooRef = fsdir.getINode4Write(foo_dir1.toString())
        .asReference();
    INodeReference.WithCount fooWithCount = (WithCount) fooRef
        .getReferredINode();
    // only 2 references: one in deleted list of sdir1, one in created list of
    // sdir1
    assertEquals(2, fooWithCount.getReferenceCount());
    INodeDirectoryWithSnapshot foo = (INodeDirectoryWithSnapshot) fooWithCount
        .asDirectory();
    assertEquals(1, foo.getDiffs().asList().size());
    assertEquals("s1", foo.getLastSnapshot().getRoot().getLocalName());
    INodeFileWithSnapshot bar1 = (INodeFileWithSnapshot) fsdir.getINode4Write(
        bar1_dir1.toString()).asFile();
    assertEquals(1, bar1.getDiffs().asList().size());
    assertEquals("s1", bar1.getDiffs().getLastSnapshot().getRoot()
        .getLocalName());
    
    INodeReference barRef = fsdir.getINode4Write(bar2_dir1.toString())
        .asReference();
    INodeReference.WithCount barWithCount = (WithCount) barRef
        .getReferredINode();
    assertEquals(2, barWithCount.getReferenceCount());
    INodeFileWithSnapshot bar = (INodeFileWithSnapshot) barWithCount.asFile();
    assertEquals(1, bar.getDiffs().asList().size());
    assertEquals("s1", bar.getDiffs().getLastSnapshot().getRoot()
        .getLocalName());
    
    // restart the cluster and check fsimage
    restartClusterAndCheckImage();
    
    // delete foo
    hdfs.delete(foo_dir1, true);
    hdfs.delete(bar2_dir1, true);
    
    // restart the cluster and check fsimage
    restartClusterAndCheckImage();
    
    // check
    assertTrue(hdfs.exists(bar1_s1));
    assertTrue(hdfs.exists(bar2_s1));
    assertFalse(hdfs.exists(bar1_s2));
    assertFalse(hdfs.exists(bar2_s2));
    assertFalse(hdfs.exists(bar1_s3));
    assertFalse(hdfs.exists(bar2_s3));
    assertFalse(hdfs.exists(foo_dir1));
    assertFalse(hdfs.exists(bar1_dir1));
    assertFalse(hdfs.exists(bar2_dir1));
    statusBar1 = hdfs.getFileStatus(bar1_s1);
    assertEquals(REPL, statusBar1.getReplication());
    statusBar2 = hdfs.getFileStatus(bar2_s1);
    assertEquals(REPL, statusBar2.getReplication());
    
    final Path foo_s1 = SnapshotTestHelper.getSnapshotPath(sdir1, "s1", "foo");
    fooRef = fsdir.getINode(foo_s1.toString()).asReference();
    fooWithCount = (WithCount) fooRef.getReferredINode();
    assertEquals(1, fooWithCount.getReferenceCount());
    
    barRef = fsdir.getINode(bar2_s1.toString()).asReference();
    barWithCount = (WithCount) barRef.getReferredINode();
    assertEquals(1, barWithCount.getReferenceCount());
  }
  
  /**
   * Test rename a dir multiple times across snapshottable directories: 
   * /dir1/foo -> /dir2/foo -> /dir3/foo -> /dir2/foo -> /dir1/foo
   * 
   * Create snapshots after each rename.
   */
  @Test
  public void testRenameMoreThanOnceAcrossSnapDirs_2() throws Exception {
    final Path sdir1 = new Path("/dir1");
    final Path sdir2 = new Path("/dir2");
    final Path sdir3 = new Path("/dir3");
    hdfs.mkdirs(sdir1);
    hdfs.mkdirs(sdir2);
    hdfs.mkdirs(sdir3);
    
    final Path foo_dir1 = new Path(sdir1, "foo");
    final Path bar1_dir1 = new Path(foo_dir1, "bar1");
    final Path bar_dir1 = new Path(sdir1, "bar");
    DFSTestUtil.createFile(hdfs, bar1_dir1, BLOCKSIZE, REPL, SEED);
    DFSTestUtil.createFile(hdfs, bar_dir1, BLOCKSIZE, REPL, SEED);
    
    SnapshotTestHelper.createSnapshot(hdfs, sdir1, "s1");
    SnapshotTestHelper.createSnapshot(hdfs, sdir2, "s2");
    SnapshotTestHelper.createSnapshot(hdfs, sdir3, "s3");
    
    // 1. /dir1/foo -> /dir2/foo, /dir1/bar -> /dir2/bar
    final Path foo_dir2 = new Path(sdir2, "foo");
    hdfs.rename(foo_dir1, foo_dir2);
    final Path bar_dir2 = new Path(sdir2, "bar");
    hdfs.rename(bar_dir1, bar_dir2);
    
    // modification on /dir2/foo and /dir2/bar
    final Path bar1_dir2 = new Path(foo_dir2, "bar1");
    hdfs.setReplication(bar1_dir2, REPL_1);
    hdfs.setReplication(bar_dir2, REPL_1);
    
    // create snapshots
    SnapshotTestHelper.createSnapshot(hdfs, sdir1, "s11");
    SnapshotTestHelper.createSnapshot(hdfs, sdir2, "s22");
    SnapshotTestHelper.createSnapshot(hdfs, sdir3, "s33");
    
    // 2. /dir2/foo -> /dir3/foo
    final Path foo_dir3 = new Path(sdir3, "foo");
    hdfs.rename(foo_dir2, foo_dir3);
    final Path bar_dir3 = new Path(sdir3, "bar");
    hdfs.rename(bar_dir2, bar_dir3);
    
    // modification on /dir3/foo
    final Path bar1_dir3 = new Path(foo_dir3, "bar1");
    hdfs.setReplication(bar1_dir3, REPL_2);
    hdfs.setReplication(bar_dir3, REPL_2);
    
    // create snapshots
    SnapshotTestHelper.createSnapshot(hdfs, sdir1, "s111");
    SnapshotTestHelper.createSnapshot(hdfs, sdir2, "s222");
    SnapshotTestHelper.createSnapshot(hdfs, sdir3, "s333");
    
    // check
    final Path bar1_s1 = SnapshotTestHelper.getSnapshotPath(sdir1, "s1",
        "foo/bar1");
    final Path bar1_s22 = SnapshotTestHelper.getSnapshotPath(sdir2, "s22",
        "foo/bar1");
    final Path bar1_s333 = SnapshotTestHelper.getSnapshotPath(sdir3, "s333",
        "foo/bar1");
    final Path bar_s1 = SnapshotTestHelper.getSnapshotPath(sdir1, "s1",
        "bar");
    final Path bar_s22 = SnapshotTestHelper.getSnapshotPath(sdir2, "s22",
        "bar");
    final Path bar_s333 = SnapshotTestHelper.getSnapshotPath(sdir3, "s333",
        "bar");
    assertTrue(hdfs.exists(bar1_s1));
    assertTrue(hdfs.exists(bar1_s22));
    assertTrue(hdfs.exists(bar1_s333));
    assertTrue(hdfs.exists(bar_s1));
    assertTrue(hdfs.exists(bar_s22));
    assertTrue(hdfs.exists(bar_s333));
    
    FileStatus statusBar1 = hdfs.getFileStatus(bar1_s1);
    assertEquals(REPL, statusBar1.getReplication());
    statusBar1 = hdfs.getFileStatus(bar1_dir3);
    assertEquals(REPL_2, statusBar1.getReplication());
    statusBar1 = hdfs.getFileStatus(bar1_s22);
    assertEquals(REPL_1, statusBar1.getReplication());
    statusBar1 = hdfs.getFileStatus(bar1_s333);
    assertEquals(REPL_2, statusBar1.getReplication());
    
    FileStatus statusBar = hdfs.getFileStatus(bar_s1);
    assertEquals(REPL, statusBar.getReplication());
    statusBar = hdfs.getFileStatus(bar_dir3);
    assertEquals(REPL_2, statusBar.getReplication());
    statusBar = hdfs.getFileStatus(bar_s22);
    assertEquals(REPL_1, statusBar.getReplication());
    statusBar = hdfs.getFileStatus(bar_s333);
    assertEquals(REPL_2, statusBar.getReplication());
    
    // 3. /dir3/foo -> /dir2/foo
    hdfs.rename(foo_dir3, foo_dir2);
    hdfs.rename(bar_dir3, bar_dir2);
    
    // modification on /dir2/foo
    hdfs.setReplication(bar1_dir2, REPL);
    hdfs.setReplication(bar_dir2, REPL);
    
    // create snapshots
    SnapshotTestHelper.createSnapshot(hdfs, sdir1, "s1111");
    SnapshotTestHelper.createSnapshot(hdfs, sdir2, "s2222");
    
    // check
    final Path bar1_s2222 = SnapshotTestHelper.getSnapshotPath(sdir2, "s2222",
        "foo/bar1");
    final Path bar_s2222 = SnapshotTestHelper.getSnapshotPath(sdir2, "s2222",
        "bar");
    assertTrue(hdfs.exists(bar1_s1));
    assertTrue(hdfs.exists(bar1_s22));
    assertTrue(hdfs.exists(bar1_s333));
    assertTrue(hdfs.exists(bar1_s2222));
    assertTrue(hdfs.exists(bar_s1));
    assertTrue(hdfs.exists(bar_s22));
    assertTrue(hdfs.exists(bar_s333));
    assertTrue(hdfs.exists(bar_s2222));
    
    statusBar1 = hdfs.getFileStatus(bar1_s1);
    assertEquals(REPL, statusBar1.getReplication());
    statusBar1 = hdfs.getFileStatus(bar1_dir2);
    assertEquals(REPL, statusBar1.getReplication());
    statusBar1 = hdfs.getFileStatus(bar1_s22);
    assertEquals(REPL_1, statusBar1.getReplication());
    statusBar1 = hdfs.getFileStatus(bar1_s333);
    assertEquals(REPL_2, statusBar1.getReplication());
    statusBar1 = hdfs.getFileStatus(bar1_s2222);
    assertEquals(REPL, statusBar1.getReplication());
    
    statusBar = hdfs.getFileStatus(bar_s1);
    assertEquals(REPL, statusBar.getReplication());
    statusBar = hdfs.getFileStatus(bar_dir2);
    assertEquals(REPL, statusBar.getReplication());
    statusBar = hdfs.getFileStatus(bar_s22);
    assertEquals(REPL_1, statusBar.getReplication());
    statusBar = hdfs.getFileStatus(bar_s333);
    assertEquals(REPL_2, statusBar.getReplication());
    statusBar = hdfs.getFileStatus(bar_s2222);
    assertEquals(REPL, statusBar.getReplication());
    
    // 4. /dir2/foo -> /dir1/foo
    hdfs.rename(foo_dir2, foo_dir1);
    hdfs.rename(bar_dir2, bar_dir1);
    
    // check the internal details
    INodeReference fooRef = fsdir.getINode4Write(foo_dir1.toString())
        .asReference();
    INodeReference.WithCount fooWithCount = (WithCount) fooRef.getReferredINode();
    // 5 references: s1, s22, s333, s2222, current tree of sdir1
    assertEquals(5, fooWithCount.getReferenceCount());
    INodeDirectoryWithSnapshot foo = (INodeDirectoryWithSnapshot) fooWithCount
        .asDirectory();
    List<DirectoryDiff> fooDiffs = foo.getDiffs().asList();
    assertEquals(4, fooDiffs.size());
    assertEquals("s2222", fooDiffs.get(3).snapshot.getRoot().getLocalName());
    assertEquals("s333", fooDiffs.get(2).snapshot.getRoot().getLocalName());
    assertEquals("s22", fooDiffs.get(1).snapshot.getRoot().getLocalName());
    assertEquals("s1", fooDiffs.get(0).snapshot.getRoot().getLocalName());
    INodeFileWithSnapshot bar1 = (INodeFileWithSnapshot) fsdir.getINode4Write(
        bar1_dir1.toString()).asFile();
    List<FileDiff> bar1Diffs = bar1.getDiffs().asList();
    assertEquals(3, bar1Diffs.size());
    assertEquals("s333", bar1Diffs.get(2).snapshot.getRoot().getLocalName());
    assertEquals("s22", bar1Diffs.get(1).snapshot.getRoot().getLocalName());
    assertEquals("s1", bar1Diffs.get(0).snapshot.getRoot().getLocalName());
    
    INodeReference barRef = fsdir.getINode4Write(bar_dir1.toString())
        .asReference();
    INodeReference.WithCount barWithCount = (WithCount) barRef.getReferredINode();
    // 5 references: s1, s22, s333, s2222, current tree of sdir1
    assertEquals(5, barWithCount.getReferenceCount());
    INodeFileWithSnapshot bar = (INodeFileWithSnapshot) barWithCount.asFile();
    List<FileDiff> barDiffs = bar.getDiffs().asList();
    assertEquals(4, barDiffs.size());
    assertEquals("s2222", barDiffs.get(3).snapshot.getRoot().getLocalName());
    assertEquals("s333", barDiffs.get(2).snapshot.getRoot().getLocalName());
    assertEquals("s22", barDiffs.get(1).snapshot.getRoot().getLocalName());
    assertEquals("s1", barDiffs.get(0).snapshot.getRoot().getLocalName());
    
    // restart the cluster and check fsimage
    restartClusterAndCheckImage();
    
    // delete foo
    hdfs.delete(foo_dir1, true);
    hdfs.delete(bar_dir1, true);
    
    // restart the cluster and check fsimage
    restartClusterAndCheckImage();
    
    // check
    final Path bar1_s1111 = SnapshotTestHelper.getSnapshotPath(sdir1, "s1111",
        "foo/bar1");
    final Path bar_s1111 = SnapshotTestHelper.getSnapshotPath(sdir1, "s1111",
        "bar");
    assertTrue(hdfs.exists(bar1_s1));
    assertTrue(hdfs.exists(bar1_s22));
    assertTrue(hdfs.exists(bar1_s333));
    assertTrue(hdfs.exists(bar1_s2222));
    assertFalse(hdfs.exists(bar1_s1111));
    assertTrue(hdfs.exists(bar_s1));
    assertTrue(hdfs.exists(bar_s22));
    assertTrue(hdfs.exists(bar_s333));
    assertTrue(hdfs.exists(bar_s2222));
    assertFalse(hdfs.exists(bar_s1111));
    
    final Path foo_s2222 = SnapshotTestHelper.getSnapshotPath(sdir2, "s2222",
        "foo");
    fooRef = fsdir.getINode(foo_s2222.toString()).asReference();
    fooWithCount = (WithCount) fooRef.getReferredINode();
    assertEquals(4, fooWithCount.getReferenceCount());
    foo = (INodeDirectoryWithSnapshot) fooWithCount.asDirectory();
    fooDiffs = foo.getDiffs().asList();
    assertEquals(4, fooDiffs.size());
    assertEquals("s2222", fooDiffs.get(3).snapshot.getRoot().getLocalName());
    bar1Diffs = bar1.getDiffs().asList();
    assertEquals(3, bar1Diffs.size());
    assertEquals("s333", bar1Diffs.get(2).snapshot.getRoot().getLocalName());
    
    barRef = fsdir.getINode(bar_s2222.toString()).asReference();
    barWithCount = (WithCount) barRef.getReferredINode();
    assertEquals(4, barWithCount.getReferenceCount());
    bar = (INodeFileWithSnapshot) barWithCount.asFile();
    barDiffs = bar.getDiffs().asList();
    assertEquals(4, barDiffs.size());
    assertEquals("s2222", barDiffs.get(3).snapshot.getRoot().getLocalName());
  }
  
  /**
   * Test rename from a non-snapshottable dir to a snapshottable dir
   */
  @Test (timeout=60000)
  public void testRenameFromNonSDir2SDir() throws Exception {
    final Path sdir1 = new Path("/dir1");
    final Path sdir2 = new Path("/dir2");
    hdfs.mkdirs(sdir1);
    hdfs.mkdirs(sdir2);
    final Path foo = new Path(sdir1, "foo");
    final Path bar = new Path(foo, "bar");
    DFSTestUtil.createFile(hdfs, bar, BLOCKSIZE, REPL, SEED);
    
    SnapshotTestHelper.createSnapshot(hdfs, sdir2, snap1);
    
    final Path newfoo = new Path(sdir2, "foo");
    hdfs.rename(foo, newfoo);
    
    INode fooNode = fsdir.getINode4Write(newfoo.toString());
    assertTrue(fooNode instanceof INodeDirectory);
  }
  
  /**
   * Test rename where the src/dst directories are both snapshottable 
   * directories without snapshots. In such case we need to update the 
   * snapshottable dir list in SnapshotManager.
   */
  @Test (timeout=60000)
  public void testRenameAndUpdateSnapshottableDirs() throws Exception {
    final Path sdir1 = new Path("/dir1");
    final Path sdir2 = new Path("/dir2");
    final Path foo = new Path(sdir1, "foo");
    final Path bar = new Path(sdir2, "bar");
    hdfs.mkdirs(foo);
    hdfs.mkdirs(bar);
    
    hdfs.allowSnapshot(foo.toString());
    SnapshotTestHelper.createSnapshot(hdfs, bar, snap1);
    assertEquals(2, fsn.getSnapshottableDirListing().length);
    
    INodeDirectory fooNode = fsdir.getINode4Write(foo.toString()).asDirectory();
    long fooId = fooNode.getId();
    
    try {
      hdfs.rename(foo, bar, Rename.OVERWRITE);
      fail("Expect exception since " + bar
          + " is snapshottable and already has snapshots");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains(bar.toString()
          + " is snapshottable and already has snapshots", e);
    }
    
    hdfs.deleteSnapshot(bar, snap1);
    hdfs.rename(foo, bar, Rename.OVERWRITE);
    SnapshottableDirectoryStatus[] dirs = fsn.getSnapshottableDirListing();
    assertEquals(1, dirs.length);
    assertEquals(bar, dirs[0].getFullPath());
    assertEquals(fooId, dirs[0].getDirStatus().getFileId());
  }
  
  /**
   * After rename, delete the snapshot in src
   */
  @Test
  public void testRenameDirAndDeleteSnapshot_2() throws Exception {
    final Path sdir1 = new Path("/dir1");
    final Path sdir2 = new Path("/dir2");
    hdfs.mkdirs(sdir1);
    hdfs.mkdirs(sdir2);
    final Path foo = new Path(sdir2, "foo");
    final Path bar = new Path(foo, "bar");
    DFSTestUtil.createFile(hdfs, bar, BLOCKSIZE, REPL, SEED);
    
    SnapshotTestHelper.createSnapshot(hdfs, sdir1, "s1");
    SnapshotTestHelper.createSnapshot(hdfs, sdir2, "s2");
    SnapshotTestHelper.createSnapshot(hdfs, sdir2, "s3");
    
    final Path newfoo = new Path(sdir1, "foo");
    hdfs.rename(foo, newfoo);
    
    // restart the cluster and check fsimage
    restartClusterAndCheckImage();
    
    final Path bar2 = new Path(newfoo, "bar2");
    DFSTestUtil.createFile(hdfs, bar2, BLOCKSIZE, REPL, SEED);
    
    hdfs.createSnapshot(sdir1, "s4");
    hdfs.delete(newfoo, true);
    
    final Path bar2_s4 = SnapshotTestHelper.getSnapshotPath(sdir1, "s4",
        "foo/bar2");
    assertTrue(hdfs.exists(bar2_s4));
    final Path bar_s4 = SnapshotTestHelper.getSnapshotPath(sdir1, "s4",
        "foo/bar");
    assertTrue(hdfs.exists(bar_s4));
        
    // delete snapshot s4. The diff of s4 should be combined to s3
    hdfs.deleteSnapshot(sdir1, "s4");
    // restart the cluster and check fsimage
    restartClusterAndCheckImage();
    
    Path bar_s3 = SnapshotTestHelper.getSnapshotPath(sdir1, "s3", "foo/bar");
    assertFalse(hdfs.exists(bar_s3));
    bar_s3 = SnapshotTestHelper.getSnapshotPath(sdir2, "s3", "foo/bar");
    assertTrue(hdfs.exists(bar_s3));
    Path bar2_s3 = SnapshotTestHelper.getSnapshotPath(sdir1, "s3", "foo/bar2");
    assertFalse(hdfs.exists(bar2_s3));
    bar2_s3 = SnapshotTestHelper.getSnapshotPath(sdir2, "s3", "foo/bar2");
    assertFalse(hdfs.exists(bar2_s3));
    
    // delete snapshot s3
    hdfs.deleteSnapshot(sdir2, "s3");
    final Path bar_s2 = SnapshotTestHelper.getSnapshotPath(sdir2, "s2",
        "foo/bar");
    assertTrue(hdfs.exists(bar_s2));
    
    // check internal details
    final Path foo_s2 = SnapshotTestHelper.getSnapshotPath(sdir2, "s2", "foo");
    INodeReference fooRef = fsdir.getINode(foo_s2.toString()).asReference();
    assertTrue(fooRef instanceof INodeReference.WithName);
    INodeReference.WithCount fooWC = (WithCount) fooRef.getReferredINode();
    assertEquals(1, fooWC.getReferenceCount());
    INodeDirectoryWithSnapshot fooDir = (INodeDirectoryWithSnapshot) fooWC
        .getReferredINode().asDirectory();
    List<DirectoryDiff> diffs = fooDir.getDiffs().asList();
    assertEquals(1, diffs.size());
    assertEquals("s2", diffs.get(0).snapshot.getRoot().getLocalName());
    
    // restart the cluster and check fsimage
    restartClusterAndCheckImage();
    
    // delete snapshot s2.
    hdfs.deleteSnapshot(sdir2, "s2");
    assertFalse(hdfs.exists(bar_s2));
    
    // restart the cluster and check fsimage
    restartClusterAndCheckImage();
    hdfs.deleteSnapshot(sdir1, "s1");
    restartClusterAndCheckImage();
  }
}
