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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeReference;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithCount;
import org.apache.hadoop.hdfs.server.namenode.INodesInPath;
import org.apache.hadoop.hdfs.server.namenode.QuotaCounts;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.ChildrenDiff;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.DirectoryDiff;
import org.apache.hadoop.hdfs.util.Diff.ListType;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

/** Testing rename with snapshots. */
public class TestRenameWithSnapshots {
  static {
    SnapshotTestHelper.disableLogs();
  }
  private static final Log LOG = LogFactory.getLog(TestRenameWithSnapshots.class);
  
  private static final long SEED = 0;
  private static final short REPL = 3;
  private static final short REPL_1 = 2;
  private static final short REPL_2 = 1;
  private static final long BLOCKSIZE = 1024;
  
  private static final Configuration conf = new Configuration();
  private static MiniDFSCluster cluster;
  private static FSNamesystem fsn;
  private static FSDirectory fsdir;
  private static DistributedFileSystem hdfs;
  private static final String testDir =
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
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);
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
    hdfs.allowSnapshot(abc);

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
      DiffType type, String sourcePath, String targetPath) {
    for (DiffReportEntry entry : entries) {
      if (entry.equals(new DiffReportEntry(type, DFSUtil
          .string2Bytes(sourcePath), targetPath == null ? null : DFSUtil
          .string2Bytes(targetPath)))) {
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
    hdfs.allowSnapshot(sub1);
    hdfs.createSnapshot(sub1, snap1);
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPL, SEED);
    hdfs.rename(file1, file2);

    // Query the diff report and make sure it looks as expected.
    SnapshotDiffReport diffReport = hdfs.getSnapshotDiffReport(sub1, snap1, "");
    List<DiffReportEntry> entries = diffReport.getDiffList();
    assertTrue(entries.size() == 2);
    assertTrue(existsInDiffReport(entries, DiffType.MODIFY, "", null));
    assertTrue(existsInDiffReport(entries, DiffType.CREATE, file2.getName(),
        null));
  }

  /**
   * Rename a file under a snapshottable directory, file exists
   * in a snapshot.
   */
  @Test
  public void testRenameFileInSnapshot() throws Exception {
    hdfs.mkdirs(sub1);
    hdfs.allowSnapshot(sub1);
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPL, SEED);
    hdfs.createSnapshot(sub1, snap1);
    hdfs.rename(file1, file2);

    // Query the diff report and make sure it looks as expected.
    SnapshotDiffReport diffReport = hdfs.getSnapshotDiffReport(sub1, snap1, "");
    System.out.println("DiffList is " + diffReport.toString());
    List<DiffReportEntry> entries = diffReport.getDiffList();
    assertTrue(entries.size() == 2);
    assertTrue(existsInDiffReport(entries, DiffType.MODIFY, "", null));
    assertTrue(existsInDiffReport(entries, DiffType.RENAME, file1.getName(),
        file2.getName()));
  }

  @Test (timeout=60000)
  public void testRenameTwiceInSnapshot() throws Exception {
    hdfs.mkdirs(sub1);
    hdfs.allowSnapshot(sub1);
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
    assertTrue(entries.size() == 2);
    assertTrue(existsInDiffReport(entries, DiffType.MODIFY, "", null));
    assertTrue(existsInDiffReport(entries, DiffType.RENAME, file1.getName(),
        file2.getName()));
    
    diffReport = hdfs.getSnapshotDiffReport(sub1, snap2, "");
    LOG.info("DiffList is " + diffReport.toString());
    entries = diffReport.getDiffList();
    assertTrue(entries.size() == 2);
    assertTrue(existsInDiffReport(entries, DiffType.MODIFY, "", null));
    assertTrue(existsInDiffReport(entries, DiffType.RENAME, file2.getName(),
        file3.getName()));
    
    diffReport = hdfs.getSnapshotDiffReport(sub1, snap1, "");
    LOG.info("DiffList is " + diffReport.toString());
    entries = diffReport.getDiffList();
    assertTrue(entries.size() == 2);
    assertTrue(existsInDiffReport(entries, DiffType.MODIFY, "", null));
    assertTrue(existsInDiffReport(entries, DiffType.RENAME, file1.getName(),
        file3.getName()));
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
    assertTrue(existsInDiffReport(entries, DiffType.MODIFY, sub2.getName(),
        null));
    assertTrue(existsInDiffReport(entries, DiffType.RENAME, sub2.getName()
        + "/" + sub2file1.getName(), sub2.getName() + "/" + sub2file2.getName()));
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
    assertEquals(2, entries.size());
    assertTrue(existsInDiffReport(entries, DiffType.MODIFY, "", null));
    assertTrue(existsInDiffReport(entries, DiffType.RENAME, sub2.getName(),
        sub3.getName()));
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
    INodeDirectory sdir2Node = fsdir.getINode(sdir2.toString()).asDirectory();
    Snapshot s2 = sdir2Node.getSnapshot(DFSUtil.string2Bytes("s2"));
    INodeFile sfoo = fsdir.getINode(newfoo.toString()).asFile();
    assertEquals(s2.getId(), sfoo.getDiffs().getLastSnapshotId());
  }
  
  /**
   * Test renaming a dir and then delete snapshots.
   */
  @Test
  public void testRenameDirAndDeleteSnapshot_1() throws Exception {
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
    
    final Path newbar = new Path(newfoo, bar.getName());
    final Path newbar2 = new Path(newfoo, bar2.getName());
    final Path newbar3 = new Path(newfoo, "bar3");
    DFSTestUtil.createFile(hdfs, newbar3, BLOCKSIZE, REPL, SEED);
    
    hdfs.createSnapshot(sdir1, "s4");
    hdfs.delete(newbar, true);
    hdfs.delete(newbar3, true);
    
    assertFalse(hdfs.exists(newbar3));
    assertFalse(hdfs.exists(bar));
    final Path bar_s4 = SnapshotTestHelper.getSnapshotPath(sdir1, "s4",
        "foo/bar");
    final Path bar3_s4 = SnapshotTestHelper.getSnapshotPath(sdir1, "s4",
        "foo/bar3");
    assertTrue(hdfs.exists(bar_s4));
    assertTrue(hdfs.exists(bar3_s4));
    
    hdfs.createSnapshot(sdir1, "s5");
    hdfs.delete(newbar2, true);
    assertFalse(hdfs.exists(bar2));
    final Path bar2_s5 = SnapshotTestHelper.getSnapshotPath(sdir1, "s5",
        "foo/bar2");
    assertTrue(hdfs.exists(bar2_s5));
    
    // delete snapshot s5. The diff of s5 should be combined to s4
    hdfs.deleteSnapshot(sdir1, "s5");
    restartClusterAndCheckImage(true);
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
    restartClusterAndCheckImage(true);
    
    // delete snapshot s2.
    hdfs.deleteSnapshot(sdir2, "s2");
    assertFalse(hdfs.exists(bar_s2));
    assertFalse(hdfs.exists(bar2_s2));
    
    // restart the cluster and check fsimage
    restartClusterAndCheckImage(true);
    hdfs.deleteSnapshot(sdir1, "s3");
    restartClusterAndCheckImage(true);
    hdfs.deleteSnapshot(sdir1, "s1");
    restartClusterAndCheckImage(true);
  }
  
  private void restartClusterAndCheckImage(boolean compareQuota)
      throws IOException {
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
    
    SnapshotTestHelper.compareDumpedTreeInFile(fsnBefore, fsnMiddle,
        compareQuota);
    SnapshotTestHelper.compareDumpedTreeInFile(fsnBefore, fsnAfter,
        compareQuota);
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
    restartClusterAndCheckImage(true);
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
    
    INodeFile snode = fsdir.getINode(newfoo.toString()).asFile();
    assertEquals(1, snode.getDiffs().asList().size());
    INodeDirectory sdir2Node = fsdir.getINode(sdir2.toString()).asDirectory();
    Snapshot s2 = sdir2Node.getSnapshot(DFSUtil.string2Bytes("s2"));
    assertEquals(s2.getId(), snode.getDiffs().getLastSnapshotId());
    
    // restart cluster
    restartClusterAndCheckImage(true);
    
    // delete snapshot s2.
    hdfs.deleteSnapshot(sdir2, "s2");
    assertFalse(hdfs.exists(foo_s2));
    
    // restart the cluster and check fsimage
    restartClusterAndCheckImage(true);
    hdfs.deleteSnapshot(sdir1, "s3");
    restartClusterAndCheckImage(true);
    hdfs.deleteSnapshot(sdir1, "s1");
    restartClusterAndCheckImage(true);
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
    restartClusterAndCheckImage(true);
    
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
    restartClusterAndCheckImage(true);
    
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
    restartClusterAndCheckImage(true);
    
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
    INodeDirectory foo = fooWithCount.asDirectory();
    assertEquals(1, foo.getDiffs().asList().size());
    INodeDirectory sdir1Node = fsdir.getINode(sdir1.toString()).asDirectory();
    Snapshot s1 = sdir1Node.getSnapshot(DFSUtil.string2Bytes("s1"));
    assertEquals(s1.getId(), foo.getDirectoryWithSnapshotFeature()
        .getLastSnapshotId());
    INodeFile bar1 = fsdir.getINode4Write(bar1_dir1.toString()).asFile();
    assertEquals(1, bar1.getDiffs().asList().size());
    assertEquals(s1.getId(), bar1.getDiffs().getLastSnapshotId());
    
    INodeReference barRef = fsdir.getINode4Write(bar2_dir1.toString())
        .asReference();
    INodeReference.WithCount barWithCount = (WithCount) barRef
        .getReferredINode();
    assertEquals(2, barWithCount.getReferenceCount());
    INodeFile bar = barWithCount.asFile();
    assertEquals(1, bar.getDiffs().asList().size());
    assertEquals(s1.getId(), bar.getDiffs().getLastSnapshotId());
    
    // restart the cluster and check fsimage
    restartClusterAndCheckImage(true);
    
    // delete foo
    hdfs.delete(foo_dir1, true);
    hdfs.delete(bar2_dir1, true);
    
    // restart the cluster and check fsimage
    restartClusterAndCheckImage(true);
    
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
    
    // restart the cluster and check fsimage
    restartClusterAndCheckImage(true);
    
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
    
    // restart the cluster and check fsimage
    restartClusterAndCheckImage(true);
    
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
    
    // restart the cluster and check fsimage
    restartClusterAndCheckImage(true);
    
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
    INodeDirectory sdir1Node = fsdir.getINode(sdir1.toString()).asDirectory();
    INodeDirectory sdir2Node = fsdir.getINode(sdir2.toString()).asDirectory();
    INodeDirectory sdir3Node = fsdir.getINode(sdir3.toString()).asDirectory();
    
    INodeReference fooRef = fsdir.getINode4Write(foo_dir1.toString())
        .asReference();
    INodeReference.WithCount fooWithCount = (WithCount) fooRef.getReferredINode();
    // 5 references: s1, s22, s333, s2222, current tree of sdir1
    assertEquals(5, fooWithCount.getReferenceCount());
    INodeDirectory foo = fooWithCount.asDirectory();
    List<DirectoryDiff> fooDiffs = foo.getDiffs().asList();
    assertEquals(4, fooDiffs.size());
    
    Snapshot s2222 = sdir2Node.getSnapshot(DFSUtil.string2Bytes("s2222"));
    Snapshot s333 = sdir3Node.getSnapshot(DFSUtil.string2Bytes("s333"));
    Snapshot s22 = sdir2Node.getSnapshot(DFSUtil.string2Bytes("s22"));
    Snapshot s1 = sdir1Node.getSnapshot(DFSUtil.string2Bytes("s1"));
    
    assertEquals(s2222.getId(), fooDiffs.get(3).getSnapshotId());
    assertEquals(s333.getId(), fooDiffs.get(2).getSnapshotId());
    assertEquals(s22.getId(), fooDiffs.get(1).getSnapshotId());
    assertEquals(s1.getId(), fooDiffs.get(0).getSnapshotId());
    INodeFile bar1 = fsdir.getINode4Write(bar1_dir1.toString()).asFile();
    List<FileDiff> bar1Diffs = bar1.getDiffs().asList();
    assertEquals(3, bar1Diffs.size());
    assertEquals(s333.getId(), bar1Diffs.get(2).getSnapshotId());
    assertEquals(s22.getId(), bar1Diffs.get(1).getSnapshotId());
    assertEquals(s1.getId(), bar1Diffs.get(0).getSnapshotId());
    
    INodeReference barRef = fsdir.getINode4Write(bar_dir1.toString())
        .asReference();
    INodeReference.WithCount barWithCount = (WithCount) barRef.getReferredINode();
    // 5 references: s1, s22, s333, s2222, current tree of sdir1
    assertEquals(5, barWithCount.getReferenceCount());
    INodeFile bar = barWithCount.asFile();
    List<FileDiff> barDiffs = bar.getDiffs().asList();
    assertEquals(4, barDiffs.size());
    assertEquals(s2222.getId(), barDiffs.get(3).getSnapshotId());
    assertEquals(s333.getId(), barDiffs.get(2).getSnapshotId());
    assertEquals(s22.getId(), barDiffs.get(1).getSnapshotId());
    assertEquals(s1.getId(), barDiffs.get(0).getSnapshotId());
    
    // restart the cluster and check fsimage
    restartClusterAndCheckImage(true);
    
    // delete foo
    hdfs.delete(foo_dir1, true);
    hdfs.delete(bar_dir1, true);
    
    // restart the cluster and check fsimage
    restartClusterAndCheckImage(true);
    
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
    foo = fooWithCount.asDirectory();
    fooDiffs = foo.getDiffs().asList();
    assertEquals(4, fooDiffs.size());
    assertEquals(s2222.getId(), fooDiffs.get(3).getSnapshotId());
    bar1Diffs = bar1.getDiffs().asList();
    assertEquals(3, bar1Diffs.size());
    assertEquals(s333.getId(), bar1Diffs.get(2).getSnapshotId());
    
    barRef = fsdir.getINode(bar_s2222.toString()).asReference();
    barWithCount = (WithCount) barRef.getReferredINode();
    assertEquals(4, barWithCount.getReferenceCount());
    bar = barWithCount.asFile();
    barDiffs = bar.getDiffs().asList();
    assertEquals(4, barDiffs.size());
    assertEquals(s2222.getId(), barDiffs.get(3).getSnapshotId());
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
    
    hdfs.allowSnapshot(foo);
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
    restartClusterAndCheckImage(true);
    
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
    restartClusterAndCheckImage(true);
    
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
    INodeDirectory sdir2Node = fsdir.getINode(sdir2.toString()).asDirectory();
    Snapshot s2 = sdir2Node.getSnapshot(DFSUtil.string2Bytes("s2"));
    final Path foo_s2 = SnapshotTestHelper.getSnapshotPath(sdir2, "s2", "foo");
    INodeReference fooRef = fsdir.getINode(foo_s2.toString()).asReference();
    assertTrue(fooRef instanceof INodeReference.WithName);
    INodeReference.WithCount fooWC = (WithCount) fooRef.getReferredINode();
    assertEquals(1, fooWC.getReferenceCount());
    INodeDirectory fooDir = fooWC.getReferredINode().asDirectory();
    List<DirectoryDiff> diffs = fooDir.getDiffs().asList();
    assertEquals(1, diffs.size());
    assertEquals(s2.getId(), diffs.get(0).getSnapshotId());
    
    // restart the cluster and check fsimage
    restartClusterAndCheckImage(true);
    
    // delete snapshot s2.
    hdfs.deleteSnapshot(sdir2, "s2");
    assertFalse(hdfs.exists(bar_s2));
    restartClusterAndCheckImage(true);
    // make sure the whole referred subtree has been destroyed
    QuotaCounts q = fsdir.getRoot().getDirectoryWithQuotaFeature().getSpaceConsumed();
    assertEquals(3, q.getNameSpace());
    assertEquals(0, q.getStorageSpace());
    
    hdfs.deleteSnapshot(sdir1, "s1");
    restartClusterAndCheckImage(true);
    q = fsdir.getRoot().getDirectoryWithQuotaFeature().getSpaceConsumed();
    assertEquals(3, q.getNameSpace());
    assertEquals(0, q.getStorageSpace());
  }
  
  /**
   * Rename a file and then append the same file. 
   */
  @Test
  public void testRenameAndAppend() throws Exception {
    final Path sdir1 = new Path("/dir1");
    final Path sdir2 = new Path("/dir2");
    hdfs.mkdirs(sdir1);
    hdfs.mkdirs(sdir2);
    
    final Path foo = new Path(sdir1, "foo");
    DFSTestUtil.createFile(hdfs, foo, BLOCKSIZE, REPL, SEED);
    
    SnapshotTestHelper.createSnapshot(hdfs, sdir1, snap1);

    final Path foo2 = new Path(sdir2, "foo");
    hdfs.rename(foo, foo2);
    
    INode fooRef = fsdir.getINode4Write(foo2.toString());
    assertTrue(fooRef instanceof INodeReference.DstReference);
    
    FSDataOutputStream out = hdfs.append(foo2);
    try {
      byte[] content = new byte[1024];
      (new Random()).nextBytes(content);
      out.write(content);
      fooRef = fsdir.getINode4Write(foo2.toString());
      assertTrue(fooRef instanceof INodeReference.DstReference);
      INodeFile fooNode = fooRef.asFile();
      assertTrue(fooNode.isWithSnapshot());
      assertTrue(fooNode.isUnderConstruction());
    } finally {
      if (out != null) {
        out.close();
      }
    }
    
    fooRef = fsdir.getINode4Write(foo2.toString());
    assertTrue(fooRef instanceof INodeReference.DstReference);
    INodeFile fooNode = fooRef.asFile();
    assertTrue(fooNode.isWithSnapshot());
    assertFalse(fooNode.isUnderConstruction());
    
    restartClusterAndCheckImage(true);
  }
  
  /**
   * Test the undo section of rename. Before the rename, we create the renamed 
   * file/dir before taking the snapshot.
   */
  @Test
  public void testRenameUndo_1() throws Exception {
    final Path sdir1 = new Path("/dir1");
    final Path sdir2 = new Path("/dir2");
    hdfs.mkdirs(sdir1);
    hdfs.mkdirs(sdir2);
    final Path foo = new Path(sdir1, "foo");
    final Path bar = new Path(foo, "bar");
    DFSTestUtil.createFile(hdfs, bar, BLOCKSIZE, REPL, SEED);
    final Path dir2file = new Path(sdir2, "file");
    DFSTestUtil.createFile(hdfs, dir2file, BLOCKSIZE, REPL, SEED);
    
    SnapshotTestHelper.createSnapshot(hdfs, sdir1, "s1");
    
    INodeDirectory dir2 = fsdir.getINode4Write(sdir2.toString()).asDirectory();
    INodeDirectory mockDir2 = spy(dir2);
    doReturn(false).when(mockDir2).addChild((INode) anyObject(), anyBoolean(),
           Mockito.anyInt());
    INodeDirectory root = fsdir.getINode4Write("/").asDirectory();
    root.replaceChild(dir2, mockDir2, fsdir.getINodeMap());
    
    final Path newfoo = new Path(sdir2, "foo");
    boolean result = hdfs.rename(foo, newfoo);
    assertFalse(result);
    
    // check the current internal details
    INodeDirectory dir1Node = fsdir.getINode4Write(sdir1.toString())
        .asDirectory();
    Snapshot s1 = dir1Node.getSnapshot(DFSUtil.string2Bytes("s1"));
    ReadOnlyList<INode> dir1Children = dir1Node
        .getChildrenList(Snapshot.CURRENT_STATE_ID);
    assertEquals(1, dir1Children.size());
    assertEquals(foo.getName(), dir1Children.get(0).getLocalName());
    List<DirectoryDiff> dir1Diffs = dir1Node.getDiffs().asList();
    assertEquals(1, dir1Diffs.size());
    assertEquals(s1.getId(), dir1Diffs.get(0).getSnapshotId());
    
    // after the undo of rename, both the created and deleted list of sdir1
    // should be empty
    ChildrenDiff childrenDiff = dir1Diffs.get(0).getChildrenDiff();
    assertEquals(0, childrenDiff.getList(ListType.DELETED).size());
    assertEquals(0, childrenDiff.getList(ListType.CREATED).size());
    
    INode fooNode = fsdir.getINode4Write(foo.toString());
    assertTrue(fooNode.isDirectory() && fooNode.asDirectory().isWithSnapshot());
    List<DirectoryDiff> fooDiffs = fooNode.asDirectory().getDiffs().asList();
    assertEquals(1, fooDiffs.size());
    assertEquals(s1.getId(), fooDiffs.get(0).getSnapshotId());
    
    final Path foo_s1 = SnapshotTestHelper.getSnapshotPath(sdir1, "s1", "foo");
    INode fooNode_s1 = fsdir.getINode(foo_s1.toString());
    assertTrue(fooNode_s1 == fooNode);
    
    // check sdir2
    assertFalse(hdfs.exists(newfoo));
    INodeDirectory dir2Node = fsdir.getINode4Write(sdir2.toString())
        .asDirectory();
    assertFalse(dir2Node.isWithSnapshot());
    ReadOnlyList<INode> dir2Children = dir2Node
        .getChildrenList(Snapshot.CURRENT_STATE_ID);
    assertEquals(1, dir2Children.size());
    assertEquals(dir2file.getName(), dir2Children.get(0).getLocalName());
  }

  /**
   * Test the undo section of rename. Before the rename, we create the renamed 
   * file/dir after taking the snapshot.
   */
  @Test
  public void testRenameUndo_2() throws Exception {
    final Path sdir1 = new Path("/dir1");
    final Path sdir2 = new Path("/dir2");
    hdfs.mkdirs(sdir1);
    hdfs.mkdirs(sdir2);
    final Path dir2file = new Path(sdir2, "file");
    DFSTestUtil.createFile(hdfs, dir2file, BLOCKSIZE, REPL, SEED);
    
    SnapshotTestHelper.createSnapshot(hdfs, sdir1, "s1");
    
    // create foo after taking snapshot
    final Path foo = new Path(sdir1, "foo");
    final Path bar = new Path(foo, "bar");
    DFSTestUtil.createFile(hdfs, bar, BLOCKSIZE, REPL, SEED);
    
    INodeDirectory dir2 = fsdir.getINode4Write(sdir2.toString()).asDirectory();
    INodeDirectory mockDir2 = spy(dir2);
    doReturn(false).when(mockDir2).addChild((INode) anyObject(), anyBoolean(),
            Mockito.anyInt());
    INodeDirectory root = fsdir.getINode4Write("/").asDirectory();
    root.replaceChild(dir2, mockDir2, fsdir.getINodeMap());
    
    final Path newfoo = new Path(sdir2, "foo");
    boolean result = hdfs.rename(foo, newfoo);
    assertFalse(result);
    
    // check the current internal details
    INodeDirectory dir1Node = fsdir.getINode4Write(sdir1.toString())
        .asDirectory();
    Snapshot s1 = dir1Node.getSnapshot(DFSUtil.string2Bytes("s1"));
    ReadOnlyList<INode> dir1Children = dir1Node
        .getChildrenList(Snapshot.CURRENT_STATE_ID);
    assertEquals(1, dir1Children.size());
    assertEquals(foo.getName(), dir1Children.get(0).getLocalName());
    List<DirectoryDiff> dir1Diffs = dir1Node.getDiffs().asList();
    assertEquals(1, dir1Diffs.size());
    assertEquals(s1.getId(), dir1Diffs.get(0).getSnapshotId());
    
    // after the undo of rename, the created list of sdir1 should contain 
    // 1 element
    ChildrenDiff childrenDiff = dir1Diffs.get(0).getChildrenDiff();
    assertEquals(0, childrenDiff.getList(ListType.DELETED).size());
    assertEquals(1, childrenDiff.getList(ListType.CREATED).size());
    
    INode fooNode = fsdir.getINode4Write(foo.toString());
    assertTrue(fooNode instanceof INodeDirectory);
    assertTrue(childrenDiff.getList(ListType.CREATED).get(0) == fooNode);
    
    final Path foo_s1 = SnapshotTestHelper.getSnapshotPath(sdir1, "s1", "foo");
    assertFalse(hdfs.exists(foo_s1));
    
    // check sdir2
    assertFalse(hdfs.exists(newfoo));
    INodeDirectory dir2Node = fsdir.getINode4Write(sdir2.toString())
        .asDirectory();
    assertFalse(dir2Node.isWithSnapshot());
    ReadOnlyList<INode> dir2Children = dir2Node
        .getChildrenList(Snapshot.CURRENT_STATE_ID);
    assertEquals(1, dir2Children.size());
    assertEquals(dir2file.getName(), dir2Children.get(0).getLocalName());
  }
  
  /**
   * Test the undo section of the second-time rename.
   */
  @Test
  public void testRenameUndo_3() throws Exception {
    final Path sdir1 = new Path("/dir1");
    final Path sdir2 = new Path("/dir2");
    final Path sdir3 = new Path("/dir3");
    hdfs.mkdirs(sdir1);
    hdfs.mkdirs(sdir2);
    hdfs.mkdirs(sdir3);
    final Path foo = new Path(sdir1, "foo");
    final Path bar = new Path(foo, "bar");
    DFSTestUtil.createFile(hdfs, bar, BLOCKSIZE, REPL, SEED);
    
    SnapshotTestHelper.createSnapshot(hdfs, sdir1, "s1");
    SnapshotTestHelper.createSnapshot(hdfs, sdir2, "s2");
    
    INodeDirectory dir3 = fsdir.getINode4Write(sdir3.toString()).asDirectory();
    INodeDirectory mockDir3 = spy(dir3);
    doReturn(false).when(mockDir3).addChild((INode) anyObject(), anyBoolean(),
            Mockito.anyInt());
    INodeDirectory root = fsdir.getINode4Write("/").asDirectory();
    root.replaceChild(dir3, mockDir3, fsdir.getINodeMap());
    
    final Path foo_dir2 = new Path(sdir2, "foo2");
    final Path foo_dir3 = new Path(sdir3, "foo3");
    hdfs.rename(foo, foo_dir2);
    boolean result = hdfs.rename(foo_dir2, foo_dir3);
    assertFalse(result);
    
    // check the current internal details
    INodeDirectory dir1Node = fsdir.getINode4Write(sdir1.toString())
        .asDirectory();
    Snapshot s1 = dir1Node.getSnapshot(DFSUtil.string2Bytes("s1"));
    INodeDirectory dir2Node = fsdir.getINode4Write(sdir2.toString())
        .asDirectory();
    Snapshot s2 = dir2Node.getSnapshot(DFSUtil.string2Bytes("s2"));
    ReadOnlyList<INode> dir2Children = dir2Node
        .getChildrenList(Snapshot.CURRENT_STATE_ID);
    assertEquals(1, dir2Children.size());
    List<DirectoryDiff> dir2Diffs = dir2Node.getDiffs().asList();
    assertEquals(1, dir2Diffs.size());
    assertEquals(s2.getId(), dir2Diffs.get(0).getSnapshotId());
    ChildrenDiff childrenDiff = dir2Diffs.get(0).getChildrenDiff();
    assertEquals(0, childrenDiff.getList(ListType.DELETED).size());
    assertEquals(1, childrenDiff.getList(ListType.CREATED).size());
    final Path foo_s2 = SnapshotTestHelper.getSnapshotPath(sdir2, "s2", "foo2");
    assertFalse(hdfs.exists(foo_s2));
    
    INode fooNode = fsdir.getINode4Write(foo_dir2.toString());
    assertTrue(childrenDiff.getList(ListType.CREATED).get(0) == fooNode);
    assertTrue(fooNode instanceof INodeReference.DstReference);
    List<DirectoryDiff> fooDiffs = fooNode.asDirectory().getDiffs().asList();
    assertEquals(1, fooDiffs.size());
    assertEquals(s1.getId(), fooDiffs.get(0).getSnapshotId());
    
    // create snapshot on sdir2 and rename again
    hdfs.createSnapshot(sdir2, "s3");
    result = hdfs.rename(foo_dir2, foo_dir3);
    assertFalse(result);

    // check internal details again
    dir2Node = fsdir.getINode4Write(sdir2.toString()).asDirectory();
    Snapshot s3 = dir2Node.getSnapshot(DFSUtil.string2Bytes("s3"));
    fooNode = fsdir.getINode4Write(foo_dir2.toString());
    dir2Children = dir2Node.getChildrenList(Snapshot.CURRENT_STATE_ID);
    assertEquals(1, dir2Children.size());
    dir2Diffs = dir2Node.getDiffs().asList();
    assertEquals(2, dir2Diffs.size());
    assertEquals(s2.getId(), dir2Diffs.get(0).getSnapshotId());
    assertEquals(s3.getId(), dir2Diffs.get(1).getSnapshotId());
    
    childrenDiff = dir2Diffs.get(0).getChildrenDiff();
    assertEquals(0, childrenDiff.getList(ListType.DELETED).size());
    assertEquals(1, childrenDiff.getList(ListType.CREATED).size());
    assertTrue(childrenDiff.getList(ListType.CREATED).get(0) == fooNode);
    
    childrenDiff = dir2Diffs.get(1).getChildrenDiff();
    assertEquals(0, childrenDiff.getList(ListType.DELETED).size());
    assertEquals(0, childrenDiff.getList(ListType.CREATED).size());
    
    final Path foo_s3 = SnapshotTestHelper.getSnapshotPath(sdir2, "s3", "foo2");
    assertFalse(hdfs.exists(foo_s2));
    assertTrue(hdfs.exists(foo_s3));
    
    assertTrue(fooNode instanceof INodeReference.DstReference);
    fooDiffs = fooNode.asDirectory().getDiffs().asList();
    assertEquals(2, fooDiffs.size());
    assertEquals(s1.getId(), fooDiffs.get(0).getSnapshotId());
    assertEquals(s3.getId(), fooDiffs.get(1).getSnapshotId());
  }
  
  /**
   * Test undo where dst node being overwritten is a reference node
   */
  @Test
  public void testRenameUndo_4() throws Exception {
    final Path sdir1 = new Path("/dir1");
    final Path sdir2 = new Path("/dir2");
    final Path sdir3 = new Path("/dir3");
    hdfs.mkdirs(sdir1);
    hdfs.mkdirs(sdir2);
    hdfs.mkdirs(sdir3);
    
    final Path foo = new Path(sdir1, "foo");
    final Path bar = new Path(foo, "bar");
    DFSTestUtil.createFile(hdfs, bar, BLOCKSIZE, REPL, SEED);
    
    final Path foo2 = new Path(sdir2, "foo2");
    hdfs.mkdirs(foo2);
    
    SnapshotTestHelper.createSnapshot(hdfs, sdir1, "s1");
    SnapshotTestHelper.createSnapshot(hdfs, sdir2, "s2");
    
    // rename foo2 to foo3, so that foo3 will be a reference node
    final Path foo3 = new Path(sdir3, "foo3");
    hdfs.rename(foo2, foo3);
    
    INode foo3Node = fsdir.getINode4Write(foo3.toString());
    assertTrue(foo3Node.isReference());
    
    INodeDirectory dir3 = fsdir.getINode4Write(sdir3.toString()).asDirectory();
    INodeDirectory mockDir3 = spy(dir3);
    // fail the rename but succeed in undo
    doReturn(false).when(mockDir3).addChild((INode) Mockito.isNull(),
        anyBoolean(), Mockito.anyInt());
    Mockito.when(mockDir3.addChild((INode) Mockito.isNotNull(), anyBoolean(), 
        Mockito.anyInt())).thenReturn(false).thenCallRealMethod();
    INodeDirectory root = fsdir.getINode4Write("/").asDirectory();
    root.replaceChild(dir3, mockDir3, fsdir.getINodeMap());
    foo3Node.setParent(mockDir3);
    
    try {
      hdfs.rename(foo, foo3, Rename.OVERWRITE);
      fail("the rename from " + foo + " to " + foo3 + " should fail");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("rename from " + foo + " to "
          + foo3 + " failed.", e);
    }
    
    // make sure the undo is correct
    final INode foo3Node_undo = fsdir.getINode4Write(foo3.toString());
    assertSame(foo3Node, foo3Node_undo);
    INodeReference.WithCount foo3_wc = (WithCount) foo3Node.asReference()
        .getReferredINode();
    assertEquals(2, foo3_wc.getReferenceCount());
    assertSame(foo3Node, foo3_wc.getParentReference());
  }
  
  /**
   * Test rename while the rename operation will exceed the quota in the dst
   * tree.
   */
  @Test
  public void testRenameUndo_5() throws Exception {
    final Path test = new Path("/test");
    final Path dir1 = new Path(test, "dir1");
    final Path dir2 = new Path(test, "dir2");
    final Path subdir2 = new Path(dir2, "subdir2");
    hdfs.mkdirs(dir1);
    hdfs.mkdirs(subdir2);
    
    final Path foo = new Path(dir1, "foo");
    final Path bar = new Path(foo, "bar");
    DFSTestUtil.createFile(hdfs, bar, BLOCKSIZE, REPL, SEED);
    
    SnapshotTestHelper.createSnapshot(hdfs, dir1, "s1");
    SnapshotTestHelper.createSnapshot(hdfs, dir2, "s2");
    
    // set ns quota of dir2 to 4, so the current remaining is 2 (already has
    // dir2, and subdir2)
    hdfs.setQuota(dir2, 4, Long.MAX_VALUE - 1);
    
    final Path foo2 = new Path(subdir2, foo.getName());
    FSDirectory fsdir2 = Mockito.spy(fsdir);
    Mockito.doThrow(new NSQuotaExceededException("fake exception")).when(fsdir2)
        .addLastINode((INodesInPath) Mockito.anyObject(),
            (INode) Mockito.anyObject(), Mockito.anyBoolean());
    Whitebox.setInternalState(fsn, "dir", fsdir2);
    // rename /test/dir1/foo to /test/dir2/subdir2/foo. 
    // FSDirectory#verifyQuota4Rename will pass since the remaining quota is 2.
    // However, the rename operation will fail since we let addLastINode throw
    // NSQuotaExceededException
    boolean rename = hdfs.rename(foo, foo2);
    assertFalse(rename);
    
    // check the undo
    assertTrue(hdfs.exists(foo));
    assertTrue(hdfs.exists(bar));
    INodeDirectory dir1Node = fsdir2.getINode4Write(dir1.toString())
        .asDirectory();
    List<INode> childrenList = ReadOnlyList.Util.asList(dir1Node
        .getChildrenList(Snapshot.CURRENT_STATE_ID));
    assertEquals(1, childrenList.size());
    INode fooNode = childrenList.get(0);
    assertTrue(fooNode.asDirectory().isWithSnapshot());
    INode barNode = fsdir2.getINode4Write(bar.toString());
    assertTrue(barNode.getClass() == INodeFile.class);
    assertSame(fooNode, barNode.getParent());
    List<DirectoryDiff> diffList = dir1Node
        .getDiffs().asList();
    assertEquals(1, diffList.size());
    DirectoryDiff diff = diffList.get(0);
    assertTrue(diff.getChildrenDiff().getList(ListType.CREATED).isEmpty());
    assertTrue(diff.getChildrenDiff().getList(ListType.DELETED).isEmpty());
    
    // check dir2
    INodeDirectory dir2Node = fsdir2.getINode4Write(dir2.toString()).asDirectory();
    assertTrue(dir2Node.isSnapshottable());
    QuotaCounts counts = dir2Node.computeQuotaUsage(fsdir.getBlockStoragePolicySuite());
    assertEquals(2, counts.getNameSpace());
    assertEquals(0, counts.getStorageSpace());
    childrenList = ReadOnlyList.Util.asList(dir2Node.asDirectory()
        .getChildrenList(Snapshot.CURRENT_STATE_ID));
    assertEquals(1, childrenList.size());
    INode subdir2Node = childrenList.get(0);
    assertSame(dir2Node, subdir2Node.getParent());
    assertSame(subdir2Node, fsdir2.getINode4Write(subdir2.toString()));
    diffList = dir2Node.getDiffs().asList();
    assertEquals(1, diffList.size());
    diff = diffList.get(0);
    assertTrue(diff.getChildrenDiff().getList(ListType.CREATED).isEmpty());
    assertTrue(diff.getChildrenDiff().getList(ListType.DELETED).isEmpty());
  }
  
  /**
   * Test the rename undo when removing dst node fails
   */
  @Test
  public void testRenameUndo_6() throws Exception {
    final Path test = new Path("/test");
    final Path dir1 = new Path(test, "dir1");
    final Path dir2 = new Path(test, "dir2");
    final Path sub_dir2 = new Path(dir2, "subdir");
    final Path subsub_dir2 = new Path(sub_dir2, "subdir");
    hdfs.mkdirs(dir1);
    hdfs.mkdirs(subsub_dir2);
    
    final Path foo = new Path(dir1, "foo");
    hdfs.mkdirs(foo);
    
    SnapshotTestHelper.createSnapshot(hdfs, dir1, "s1");
    SnapshotTestHelper.createSnapshot(hdfs, dir2, "s2");
    
    // set ns quota of dir2 to 4, so the current remaining is 1 (already has
    // dir2, sub_dir2, and subsub_dir2)
    hdfs.setQuota(dir2, 4, Long.MAX_VALUE - 1);
    FSDirectory fsdir2 = Mockito.spy(fsdir);
    Mockito.doThrow(new RuntimeException("fake exception")).when(fsdir2)
        .removeLastINode((INodesInPath) Mockito.anyObject());
    Whitebox.setInternalState(fsn, "dir", fsdir2);
    // rename /test/dir1/foo to /test/dir2/sub_dir2/subsub_dir2. 
    // FSDirectory#verifyQuota4Rename will pass since foo only be counted 
    // as 1 in NS quota. However, the rename operation will fail when removing
    // subsub_dir2.
    try {
      hdfs.rename(foo, subsub_dir2, Rename.OVERWRITE);
      fail("Expect QuotaExceedException");
    } catch (Exception e) {
      String msg = "fake exception";
      GenericTestUtils.assertExceptionContains(msg, e);
    }
    
    // check the undo
    assertTrue(hdfs.exists(foo));
    INodeDirectory dir1Node = fsdir2.getINode4Write(dir1.toString())
        .asDirectory();
    List<INode> childrenList = ReadOnlyList.Util.asList(dir1Node
        .getChildrenList(Snapshot.CURRENT_STATE_ID));
    assertEquals(1, childrenList.size());
    INode fooNode = childrenList.get(0);
    assertTrue(fooNode.asDirectory().isWithSnapshot());
    assertSame(dir1Node, fooNode.getParent());
    List<DirectoryDiff> diffList = dir1Node
        .getDiffs().asList();
    assertEquals(1, diffList.size());
    DirectoryDiff diff = diffList.get(0);
    assertTrue(diff.getChildrenDiff().getList(ListType.CREATED).isEmpty());
    assertTrue(diff.getChildrenDiff().getList(ListType.DELETED).isEmpty());
    
    // check dir2
    INodeDirectory dir2Node = fsdir2.getINode4Write(dir2.toString()).asDirectory();
    assertTrue(dir2Node.isSnapshottable());
    QuotaCounts counts = dir2Node.computeQuotaUsage(fsdir.getBlockStoragePolicySuite());
    assertEquals(3, counts.getNameSpace());
    assertEquals(0, counts.getStorageSpace());
    childrenList = ReadOnlyList.Util.asList(dir2Node.asDirectory()
        .getChildrenList(Snapshot.CURRENT_STATE_ID));
    assertEquals(1, childrenList.size());
    INode subdir2Node = childrenList.get(0);
    assertSame(dir2Node, subdir2Node.getParent());
    assertSame(subdir2Node, fsdir2.getINode4Write(sub_dir2.toString()));
    INode subsubdir2Node = fsdir2.getINode4Write(subsub_dir2.toString());
    assertTrue(subsubdir2Node.getClass() == INodeDirectory.class);
    assertSame(subdir2Node, subsubdir2Node.getParent());
    
    diffList = (  dir2Node).getDiffs().asList();
    assertEquals(1, diffList.size());
    diff = diffList.get(0);
    assertTrue(diff.getChildrenDiff().getList(ListType.CREATED).isEmpty());
    assertTrue(diff.getChildrenDiff().getList(ListType.DELETED).isEmpty());
  }
  
  /**
   * Test rename to an invalid name (xxx/.snapshot)
   */
  @Test
  public void testRenameUndo_7() throws Exception {
    final Path root = new Path("/");
    final Path foo = new Path(root, "foo");
    final Path bar = new Path(foo, "bar");
    DFSTestUtil.createFile(hdfs, bar, BLOCKSIZE, REPL, SEED);
    
    // create a snapshot on root
    SnapshotTestHelper.createSnapshot(hdfs, root, snap1);
    
    // rename bar to /foo/.snapshot which is invalid
    final Path invalid = new Path(foo, HdfsConstants.DOT_SNAPSHOT_DIR);
    try {
      hdfs.rename(bar, invalid);
      fail("expect exception since invalid name is used for rename");
    } catch (Exception e) {
      GenericTestUtils.assertExceptionContains("\"" +
          HdfsConstants.DOT_SNAPSHOT_DIR + "\" is a reserved name", e);
    }
    
    // check
    INodeDirectory rootNode = fsdir.getINode4Write(root.toString())
        .asDirectory();
    INodeDirectory fooNode = fsdir.getINode4Write(foo.toString()).asDirectory();
    ReadOnlyList<INode> children = fooNode
        .getChildrenList(Snapshot.CURRENT_STATE_ID);
    assertEquals(1, children.size());
    List<DirectoryDiff> diffList = fooNode.getDiffs().asList();
    assertEquals(1, diffList.size());
    DirectoryDiff diff = diffList.get(0);
    // this diff is generated while renaming
    Snapshot s1 = rootNode.getSnapshot(DFSUtil.string2Bytes(snap1));
    assertEquals(s1.getId(), diff.getSnapshotId());
    // after undo, the diff should be empty
    assertTrue(diff.getChildrenDiff().getList(ListType.DELETED).isEmpty());
    assertTrue(diff.getChildrenDiff().getList(ListType.CREATED).isEmpty());
    
    // bar was converted to filewithsnapshot while renaming
    INodeFile barNode = fsdir.getINode4Write(bar.toString()).asFile();
    assertSame(barNode, children.get(0));
    assertSame(fooNode, barNode.getParent());
    List<FileDiff> barDiffList = barNode.getDiffs().asList();
    assertEquals(1, barDiffList.size());
    FileDiff barDiff = barDiffList.get(0);
    assertEquals(s1.getId(), barDiff.getSnapshotId());
    
    // restart cluster multiple times to make sure the fsimage and edits log are
    // correct. Note that when loading fsimage, foo and bar will be converted 
    // back to normal INodeDirectory and INodeFile since they do not store any 
    // snapshot data
    hdfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
    hdfs.saveNamespace();
    hdfs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
    cluster.shutdown();
    cluster = new MiniDFSCluster.Builder(conf).format(false)
        .numDataNodes(REPL).build();
    cluster.waitActive();
    restartClusterAndCheckImage(true);
  }
  
  /**
   * Test the rename undo when quota of dst tree is exceeded after rename.
   */
  @Test
  public void testRenameExceedQuota() throws Exception {
    final Path test = new Path("/test");
    final Path dir1 = new Path(test, "dir1");
    final Path dir2 = new Path(test, "dir2");
    final Path sub_dir2 = new Path(dir2, "subdir");
    final Path subfile_dir2 = new Path(sub_dir2, "subfile");
    hdfs.mkdirs(dir1);
    DFSTestUtil.createFile(hdfs, subfile_dir2, BLOCKSIZE, REPL, SEED);
    
    final Path foo = new Path(dir1, "foo");
    DFSTestUtil.createFile(hdfs, foo, BLOCKSIZE, REPL, SEED);
    
    SnapshotTestHelper.createSnapshot(hdfs, dir1, "s1");
    SnapshotTestHelper.createSnapshot(hdfs, dir2, "s2");
    
    // set ns quota of dir2 to 4, so the current remaining is 1 (already has
    // dir2, sub_dir2, subfile_dir2, and s2)
    hdfs.setQuota(dir2, 5, Long.MAX_VALUE - 1);
    
    // rename /test/dir1/foo to /test/dir2/sub_dir2/subfile_dir2. 
    // FSDirectory#verifyQuota4Rename will pass since foo only be counted 
    // as 1 in NS quota. The rename operation will succeed while the real quota 
    // of dir2 will become 7 (dir2, s2 in dir2, sub_dir2, s2 in sub_dir2,
    // subfile_dir2 in deleted list, new subfile, s1 in new subfile).
    hdfs.rename(foo, subfile_dir2, Rename.OVERWRITE);
    
    // check dir2
    INode dir2Node = fsdir.getINode4Write(dir2.toString());
    assertTrue(dir2Node.asDirectory().isSnapshottable());
    QuotaCounts counts = dir2Node.computeQuotaUsage(
        fsdir.getBlockStoragePolicySuite());
    assertEquals(4, counts.getNameSpace());
    assertEquals(BLOCKSIZE * REPL * 2, counts.getStorageSpace());
  }
  
  @Test
  public void testRename2PreDescendant() throws Exception {
    final Path sdir1 = new Path("/dir1");
    final Path sdir2 = new Path("/dir2");
    final Path foo = new Path(sdir1, "foo");
    final Path bar = new Path(foo, "bar");
    hdfs.mkdirs(bar);
    hdfs.mkdirs(sdir2);
    
    SnapshotTestHelper.createSnapshot(hdfs, sdir1, snap1);
    
    // /dir1/foo/bar -> /dir2/bar
    final Path bar2 = new Path(sdir2, "bar");
    hdfs.rename(bar, bar2);
    
    // /dir1/foo -> /dir2/bar/foo
    final Path foo2 = new Path(bar2, "foo");
    hdfs.rename(foo, foo2);
    
    restartClusterAndCheckImage(true);
    
    // delete snap1
    hdfs.deleteSnapshot(sdir1, snap1);
    
    restartClusterAndCheckImage(true);
  }
  
  /**
   * move a directory to its prior descendant
   */
  @Test
  public void testRename2PreDescendant_2() throws Exception {
    final Path root = new Path("/");
    final Path sdir1 = new Path("/dir1");
    final Path sdir2 = new Path("/dir2");
    final Path foo = new Path(sdir1, "foo");
    final Path bar = new Path(foo, "bar");
    final Path file1InBar = new Path(bar, "file1");
    final Path file2InBar = new Path(bar, "file2");
    hdfs.mkdirs(bar);
    hdfs.mkdirs(sdir2);
    DFSTestUtil.createFile(hdfs, file1InBar, BLOCKSIZE, REPL, SEED);
    DFSTestUtil.createFile(hdfs, file2InBar, BLOCKSIZE, REPL, SEED);
    
    hdfs.setQuota(sdir1, Long.MAX_VALUE - 1, Long.MAX_VALUE - 1);
    hdfs.setQuota(sdir2, Long.MAX_VALUE - 1, Long.MAX_VALUE - 1);
    hdfs.setQuota(foo, Long.MAX_VALUE - 1, Long.MAX_VALUE - 1);
    hdfs.setQuota(bar, Long.MAX_VALUE - 1, Long.MAX_VALUE - 1);
    
    // create snapshot on root
    SnapshotTestHelper.createSnapshot(hdfs, root, snap1);
    // delete file1InBar
    hdfs.delete(file1InBar, true);
    
    // create another snapshot on root
    SnapshotTestHelper.createSnapshot(hdfs, root, snap2);
    // delete file2InBar
    hdfs.delete(file2InBar, true);
    
    // /dir1/foo/bar -> /dir2/bar
    final Path bar2 = new Path(sdir2, "bar2");
    hdfs.rename(bar, bar2);
    
    // /dir1/foo -> /dir2/bar/foo
    final Path foo2 = new Path(bar2, "foo2");
    hdfs.rename(foo, foo2);
    
    restartClusterAndCheckImage(true);
    
    // delete snapshot snap2
    hdfs.deleteSnapshot(root, snap2);
    
    // after deleteing snap2, the WithName node "bar", which originally was 
    // stored in the deleted list of "foo" for snap2, is moved to its deleted 
    // list for snap1. In that case, it will not be counted when calculating 
    // quota for "foo". However, we do not update this quota usage change while 
    // deleting snap2.
    restartClusterAndCheckImage(false);
  }
  
  /**
   * move a directory to its prior descedant
   */
  @Test
  public void testRename2PreDescendant_3() throws Exception {
    final Path root = new Path("/");
    final Path sdir1 = new Path("/dir1");
    final Path sdir2 = new Path("/dir2");
    final Path foo = new Path(sdir1, "foo");
    final Path bar = new Path(foo, "bar");
    final Path fileInBar = new Path(bar, "file");
    hdfs.mkdirs(bar);
    hdfs.mkdirs(sdir2);
    DFSTestUtil.createFile(hdfs, fileInBar, BLOCKSIZE, REPL, SEED);
    
    hdfs.setQuota(sdir1, Long.MAX_VALUE - 1, Long.MAX_VALUE - 1);
    hdfs.setQuota(sdir2, Long.MAX_VALUE - 1, Long.MAX_VALUE - 1);
    hdfs.setQuota(foo, Long.MAX_VALUE - 1, Long.MAX_VALUE - 1);
    hdfs.setQuota(bar, Long.MAX_VALUE - 1, Long.MAX_VALUE - 1);
    
    // create snapshot on root
    SnapshotTestHelper.createSnapshot(hdfs, root, snap1);
    // delete fileInBar
    hdfs.delete(fileInBar, true);
    // create another snapshot on root
    SnapshotTestHelper.createSnapshot(hdfs, root, snap2);
    
    // /dir1/foo/bar -> /dir2/bar
    final Path bar2 = new Path(sdir2, "bar2");
    hdfs.rename(bar, bar2);
    
    // /dir1/foo -> /dir2/bar/foo
    final Path foo2 = new Path(bar2, "foo2");
    hdfs.rename(foo, foo2);
    
    restartClusterAndCheckImage(true);
    
    // delete snapshot snap1
    hdfs.deleteSnapshot(root, snap1);
    
    restartClusterAndCheckImage(true);
  }
  
  /**
   * After the following operations:
   * Rename a dir -> create a snapshot s on dst tree -> delete the renamed dir
   * -> delete snapshot s on dst tree
   * 
   * Make sure we destroy everything created after the rename under the renamed
   * dir.
   */
  @Test
  public void testRenameDirAndDeleteSnapshot_3() throws Exception {
    final Path sdir1 = new Path("/dir1");
    final Path sdir2 = new Path("/dir2");
    final Path foo = new Path(sdir1, "foo");
    final Path bar = new Path(foo, "bar");
    DFSTestUtil.createFile(hdfs, bar, BLOCKSIZE, REPL, SEED);
    hdfs.mkdirs(sdir2);
    
    SnapshotTestHelper.createSnapshot(hdfs, sdir1, "s1");
    SnapshotTestHelper.createSnapshot(hdfs, sdir2, "s2");
    
    final Path foo2 = new Path(sdir2, "foo");
    hdfs.rename(foo, foo2);
    
    // create two new files under foo2
    final Path bar2 = new Path(foo2, "bar2");
    DFSTestUtil.createFile(hdfs, bar2, BLOCKSIZE, REPL, SEED);
    final Path bar3 = new Path(foo2, "bar3");
    DFSTestUtil.createFile(hdfs, bar3, BLOCKSIZE, REPL, SEED);
    
    // create a new snapshot on sdir2
    hdfs.createSnapshot(sdir2, "s3");
    
    // delete foo2
    hdfs.delete(foo2, true);
    // delete s3
    hdfs.deleteSnapshot(sdir2, "s3");
    
    // check
    final INodeDirectory dir1Node = fsdir.getINode4Write(sdir1.toString())
        .asDirectory();
    QuotaCounts q1 = dir1Node.getDirectoryWithQuotaFeature().getSpaceConsumed();
    assertEquals(3, q1.getNameSpace());
    final INodeDirectory dir2Node = fsdir.getINode4Write(sdir2.toString())
        .asDirectory();
    QuotaCounts q2 = dir2Node.getDirectoryWithQuotaFeature().getSpaceConsumed();
    assertEquals(1, q2.getNameSpace());
    
    final Path foo_s1 = SnapshotTestHelper.getSnapshotPath(sdir1, "s1",
        foo.getName());
    INode fooRef = fsdir.getINode(foo_s1.toString());
    assertTrue(fooRef instanceof INodeReference.WithName);
    INodeReference.WithCount wc = 
        (WithCount) fooRef.asReference().getReferredINode();
    assertEquals(1, wc.getReferenceCount());
    INodeDirectory fooNode = wc.getReferredINode().asDirectory();
    ReadOnlyList<INode> children = fooNode
        .getChildrenList(Snapshot.CURRENT_STATE_ID);
    assertEquals(1, children.size());
    assertEquals(bar.getName(), children.get(0).getLocalName());
    List<DirectoryDiff> diffList = fooNode.getDiffs().asList();
    assertEquals(1, diffList.size());
    Snapshot s1 = dir1Node.getSnapshot(DFSUtil.string2Bytes("s1"));
    assertEquals(s1.getId(), diffList.get(0).getSnapshotId());
    ChildrenDiff diff = diffList.get(0).getChildrenDiff();
    assertEquals(0, diff.getList(ListType.CREATED).size());
    assertEquals(0, diff.getList(ListType.DELETED).size());
    
    restartClusterAndCheckImage(true);
  }
  
  /**
   * After the following operations:
   * Rename a dir -> create a snapshot s on dst tree -> rename the renamed dir
   * again -> delete snapshot s on dst tree
   * 
   * Make sure we only delete the snapshot s under the renamed dir.
   */
  @Test
  public void testRenameDirAndDeleteSnapshot_4() throws Exception {
    final Path sdir1 = new Path("/dir1");
    final Path sdir2 = new Path("/dir2");
    final Path foo = new Path(sdir1, "foo");
    final Path bar = new Path(foo, "bar");
    DFSTestUtil.createFile(hdfs, bar, BLOCKSIZE, REPL, SEED);
    hdfs.mkdirs(sdir2);
    
    SnapshotTestHelper.createSnapshot(hdfs, sdir1, "s1");
    SnapshotTestHelper.createSnapshot(hdfs, sdir2, "s2");
    
    final Path foo2 = new Path(sdir2, "foo");
    hdfs.rename(foo, foo2);
    
    // create two new files under foo2
    final Path bar2 = new Path(foo2, "bar2");
    DFSTestUtil.createFile(hdfs, bar2, BLOCKSIZE, REPL, SEED);
    final Path bar3 = new Path(foo2, "bar3");
    DFSTestUtil.createFile(hdfs, bar3, BLOCKSIZE, REPL, SEED);
    
    // create a new snapshot on sdir2
    hdfs.createSnapshot(sdir2, "s3");
    
    // rename foo2 again
    hdfs.rename(foo2, foo);
    // delete snapshot s3
    hdfs.deleteSnapshot(sdir2, "s3");
    
    // check
    final INodeDirectory dir1Node = fsdir.getINode4Write(sdir1.toString())
        .asDirectory();
    // sdir1 + s1 + foo_s1 (foo) + foo (foo + s1 + bar~bar3)
    QuotaCounts q1 = dir1Node.getDirectoryWithQuotaFeature().getSpaceConsumed();
    assertEquals(7, q1.getNameSpace());
    final INodeDirectory dir2Node = fsdir.getINode4Write(sdir2.toString())
        .asDirectory();
    QuotaCounts q2 = dir2Node.getDirectoryWithQuotaFeature().getSpaceConsumed();
    assertEquals(1, q2.getNameSpace());
    
    final Path foo_s1 = SnapshotTestHelper.getSnapshotPath(sdir1, "s1",
        foo.getName());
    final INode fooRef = fsdir.getINode(foo_s1.toString());
    assertTrue(fooRef instanceof INodeReference.WithName);
    INodeReference.WithCount wc = 
        (WithCount) fooRef.asReference().getReferredINode();
    assertEquals(2, wc.getReferenceCount());
    INodeDirectory fooNode = wc.getReferredINode().asDirectory();
    ReadOnlyList<INode> children = fooNode
        .getChildrenList(Snapshot.CURRENT_STATE_ID);
    assertEquals(3, children.size());
    assertEquals(bar.getName(), children.get(0).getLocalName());
    assertEquals(bar2.getName(), children.get(1).getLocalName());
    assertEquals(bar3.getName(), children.get(2).getLocalName());
    List<DirectoryDiff> diffList = fooNode.getDiffs().asList();
    assertEquals(1, diffList.size());
    Snapshot s1 = dir1Node.getSnapshot(DFSUtil.string2Bytes("s1"));
    assertEquals(s1.getId(), diffList.get(0).getSnapshotId());
    ChildrenDiff diff = diffList.get(0).getChildrenDiff();
    // bar2 and bar3 in the created list
    assertEquals(2, diff.getList(ListType.CREATED).size());
    assertEquals(0, diff.getList(ListType.DELETED).size());
    
    final INode fooRef2 = fsdir.getINode4Write(foo.toString());
    assertTrue(fooRef2 instanceof INodeReference.DstReference);
    INodeReference.WithCount wc2 = 
        (WithCount) fooRef2.asReference().getReferredINode();
    assertSame(wc, wc2);
    assertSame(fooRef2, wc.getParentReference());
    
    restartClusterAndCheckImage(true);
  }
  
  /**
   * This test demonstrates that 
   * {@link INodeDirectory#removeChild}
   * and 
   * {@link INodeDirectory#addChild}
   * should use {@link INode#isInLatestSnapshot} to check if the
   * added/removed child should be recorded in snapshots.
   */
  @Test
  public void testRenameDirAndDeleteSnapshot_5() throws Exception {
    final Path dir1 = new Path("/dir1");
    final Path dir2 = new Path("/dir2");
    final Path dir3 = new Path("/dir3");
    hdfs.mkdirs(dir1);
    hdfs.mkdirs(dir2);
    hdfs.mkdirs(dir3);
    
    final Path foo = new Path(dir1, "foo");
    hdfs.mkdirs(foo);
    SnapshotTestHelper.createSnapshot(hdfs, dir1, "s1");
    final Path bar = new Path(foo, "bar");
    // create file bar, and foo will become an INodeDirectory with snapshot
    DFSTestUtil.createFile(hdfs, bar, BLOCKSIZE, REPL, SEED);
    // delete snapshot s1. now foo is not in any snapshot
    hdfs.deleteSnapshot(dir1, "s1");
    
    SnapshotTestHelper.createSnapshot(hdfs, dir2, "s2");
    // rename /dir1/foo to /dir2/foo
    final Path foo2 = new Path(dir2, foo.getName());
    hdfs.rename(foo, foo2);
    // rename /dir2/foo/bar to /dir3/foo/bar
    final Path bar2 = new Path(dir2, "foo/bar");
    final Path bar3 = new Path(dir3, "bar");
    hdfs.rename(bar2, bar3);
    
    // delete /dir2/foo. Since it is not in any snapshot, we will call its 
    // destroy function. If we do not use isInLatestSnapshot in removeChild and
    // addChild methods in INodeDirectory (with snapshot), the file bar will be 
    // stored in the deleted list of foo, and will be destroyed.
    hdfs.delete(foo2, true);
    
    // check if /dir3/bar still exists
    assertTrue(hdfs.exists(bar3));
    INodeFile barNode = (INodeFile) fsdir.getINode4Write(bar3.toString());
    assertSame(fsdir.getINode4Write(dir3.toString()), barNode.getParent());
  }
  
  /**
   * Rename and deletion snapshot under the same the snapshottable directory.
   */
  @Test
  public void testRenameDirAndDeleteSnapshot_6() throws Exception {
    final Path test = new Path("/test");
    final Path dir1 = new Path(test, "dir1");
    final Path dir2 = new Path(test, "dir2");
    hdfs.mkdirs(dir1);
    hdfs.mkdirs(dir2);
    
    final Path foo = new Path(dir2, "foo");
    final Path bar = new Path(foo, "bar");
    final Path file = new Path(bar, "file");
    DFSTestUtil.createFile(hdfs, file, BLOCKSIZE, REPL, SEED);
    
    // take a snapshot on /test
    SnapshotTestHelper.createSnapshot(hdfs, test, "s0");
    
    // delete /test/dir2/foo/bar/file after snapshot s0, so that there is a 
    // snapshot copy recorded in bar
    hdfs.delete(file, true);
    
    // rename foo from dir2 to dir1
    final Path newfoo = new Path(dir1, foo.getName());
    hdfs.rename(foo, newfoo);
    
    final Path foo_s0 = SnapshotTestHelper.getSnapshotPath(test, "s0",
        "dir2/foo");
    assertTrue("the snapshot path " + foo_s0 + " should exist",
        hdfs.exists(foo_s0));
    
    // delete snapshot s0. The deletion will first go down through dir1, and 
    // find foo in the created list of dir1. Then it will use null as the prior
    // snapshot and continue the snapshot deletion process in the subtree of 
    // foo. We need to make sure the snapshot s0 can be deleted cleanly in the
    // foo subtree.
    hdfs.deleteSnapshot(test, "s0");
    // check the internal
    assertFalse("after deleting s0, " + foo_s0 + " should not exist",
        hdfs.exists(foo_s0));
    INodeDirectory dir2Node = fsdir.getINode4Write(dir2.toString())
        .asDirectory();
    assertTrue("the diff list of " + dir2
        + " should be empty after deleting s0", dir2Node.getDiffs().asList()
        .isEmpty());
    
    assertTrue(hdfs.exists(newfoo));
    INode fooRefNode = fsdir.getINode4Write(newfoo.toString());
    assertTrue(fooRefNode instanceof INodeReference.DstReference);
    INodeDirectory fooNode = fooRefNode.asDirectory();
    // fooNode should be still INodeDirectory (With Snapshot) since we call
    // recordModification before the rename
    assertTrue(fooNode.isWithSnapshot());
    assertTrue(fooNode.getDiffs().asList().isEmpty());
    INodeDirectory barNode = fooNode.getChildrenList(Snapshot.CURRENT_STATE_ID)
        .get(0).asDirectory();
    // bar should also be INodeDirectory (With Snapshot), and both of its diff 
    // list and children list are empty 
    assertTrue(barNode.getDiffs().asList().isEmpty());
    assertTrue(barNode.getChildrenList(Snapshot.CURRENT_STATE_ID).isEmpty());
    
    restartClusterAndCheckImage(true);
  }
  
  /**
   * Unit test for HDFS-4842.
   */
  @Test
  public void testRenameDirAndDeleteSnapshot_7() throws Exception {
    fsn.getSnapshotManager().setAllowNestedSnapshots(true);
    final Path test = new Path("/test");
    final Path dir1 = new Path(test, "dir1");
    final Path dir2 = new Path(test, "dir2");
    hdfs.mkdirs(dir1);
    hdfs.mkdirs(dir2);
    
    final Path foo = new Path(dir2, "foo");
    final Path bar = new Path(foo, "bar");
    final Path file = new Path(bar, "file");
    DFSTestUtil.createFile(hdfs, file, BLOCKSIZE, REPL, SEED);
    
    // take a snapshot s0 and s1 on /test
    SnapshotTestHelper.createSnapshot(hdfs, test, "s0");
    SnapshotTestHelper.createSnapshot(hdfs, test, "s1");
    // delete file so we have a snapshot copy for s1 in bar
    hdfs.delete(file, true);
    
    // create another snapshot on dir2
    SnapshotTestHelper.createSnapshot(hdfs, dir2, "s2");
    
    // rename foo from dir2 to dir1
    final Path newfoo = new Path(dir1, foo.getName());
    hdfs.rename(foo, newfoo);
    
    // delete snapshot s1
    hdfs.deleteSnapshot(test, "s1");
    
    // make sure the snapshot copy of file in s1 is merged to s0. For 
    // HDFS-4842, we need to make sure that we do not wrongly use s2 as the
    // prior snapshot of s1.
    final Path file_s2 = SnapshotTestHelper.getSnapshotPath(dir2, "s2",
        "foo/bar/file");
    assertFalse(hdfs.exists(file_s2));
    final Path file_s0 = SnapshotTestHelper.getSnapshotPath(test, "s0",
        "dir2/foo/bar/file");
    assertTrue(hdfs.exists(file_s0));
    
    // check dir1: foo should be in the created list of s0
    INodeDirectory dir1Node = fsdir.getINode4Write(dir1.toString())
        .asDirectory();
    List<DirectoryDiff> dir1DiffList = dir1Node.getDiffs().asList();
    assertEquals(1, dir1DiffList.size());
    List<INode> dList = dir1DiffList.get(0).getChildrenDiff()
        .getList(ListType.DELETED);
    assertTrue(dList.isEmpty());
    List<INode> cList = dir1DiffList.get(0).getChildrenDiff()
        .getList(ListType.CREATED);
    assertEquals(1, cList.size());
    INode cNode = cList.get(0);
    INode fooNode = fsdir.getINode4Write(newfoo.toString());
    assertSame(cNode, fooNode);
    
    // check foo and its subtree
    final Path newbar = new Path(newfoo, bar.getName());
    INodeDirectory barNode = fsdir.getINode4Write(newbar.toString())
        .asDirectory();
    assertSame(fooNode.asDirectory(), barNode.getParent());
    // bar should only have a snapshot diff for s0
    List<DirectoryDiff> barDiffList = barNode.getDiffs().asList();
    assertEquals(1, barDiffList.size());
    DirectoryDiff diff = barDiffList.get(0);
    INodeDirectory testNode = fsdir.getINode4Write(test.toString())
        .asDirectory();
    Snapshot s0 = testNode.getSnapshot(DFSUtil.string2Bytes("s0"));
    assertEquals(s0.getId(), diff.getSnapshotId());
    // and file should be stored in the deleted list of this snapshot diff
    assertEquals("file", diff.getChildrenDiff().getList(ListType.DELETED)
        .get(0).getLocalName());
    
    // check dir2: a WithName instance for foo should be in the deleted list
    // of the snapshot diff for s2
    INodeDirectory dir2Node = fsdir.getINode4Write(dir2.toString())
        .asDirectory();
    List<DirectoryDiff> dir2DiffList = dir2Node.getDiffs().asList();
    // dir2Node should contain 1 snapshot diffs for s2
    assertEquals(1, dir2DiffList.size());
    dList = dir2DiffList.get(0).getChildrenDiff().getList(ListType.DELETED);
    assertEquals(1, dList.size());
    final Path foo_s2 = SnapshotTestHelper.getSnapshotPath(dir2, "s2", 
        foo.getName());
    INodeReference.WithName fooNode_s2 = 
        (INodeReference.WithName) fsdir.getINode(foo_s2.toString());
    assertSame(dList.get(0), fooNode_s2);
    assertSame(fooNode.asReference().getReferredINode(),
        fooNode_s2.getReferredINode());
    
    restartClusterAndCheckImage(true);
  }
  
  /**
   * Make sure we clean the whole subtree under a DstReference node after 
   * deleting a snapshot.
   * see HDFS-5476.
   */
  @Test
  public void testCleanDstReference() throws Exception {
    final Path test = new Path("/test");
    final Path foo = new Path(test, "foo");
    final Path bar = new Path(foo, "bar");
    hdfs.mkdirs(bar);
    SnapshotTestHelper.createSnapshot(hdfs, test, "s0");
    
    // create file after s0 so that the file should not be included in s0
    final Path fileInBar = new Path(bar, "file");
    DFSTestUtil.createFile(hdfs, fileInBar, BLOCKSIZE, REPL, SEED);
    // rename foo --> foo2
    final Path foo2 = new Path(test, "foo2");
    hdfs.rename(foo, foo2);
    // create snapshot s1, note the file is included in s1
    hdfs.createSnapshot(test, "s1");
    // delete bar and foo2
    hdfs.delete(new Path(foo2, "bar"), true);
    hdfs.delete(foo2, true);
    
    final Path sfileInBar = SnapshotTestHelper.getSnapshotPath(test, "s1",
        "foo2/bar/file");
    assertTrue(hdfs.exists(sfileInBar));
    
    hdfs.deleteSnapshot(test, "s1");
    assertFalse(hdfs.exists(sfileInBar));
    
    restartClusterAndCheckImage(true);
    // make sure the file under bar is deleted 
    final Path barInS0 = SnapshotTestHelper.getSnapshotPath(test, "s0",
        "foo/bar");
    INodeDirectory barNode = fsdir.getINode(barInS0.toString()).asDirectory();
    assertEquals(0, barNode.getChildrenList(Snapshot.CURRENT_STATE_ID).size());
    List<DirectoryDiff> diffList = barNode.getDiffs().asList();
    assertEquals(1, diffList.size());
    DirectoryDiff diff = diffList.get(0);
    assertEquals(0, diff.getChildrenDiff().getList(ListType.DELETED).size());
    assertEquals(0, diff.getChildrenDiff().getList(ListType.CREATED).size());
  }

  /**
   * Rename of the underconstruction file in snapshot should not fail NN restart
   * after checkpoint. Unit test for HDFS-5425.
   */
  @Test
  public void testRenameUCFileInSnapshot() throws Exception {
    final Path test = new Path("/test");
    final Path foo = new Path(test, "foo");
    final Path bar = new Path(foo, "bar");
    hdfs.mkdirs(foo);
    // create a file and keep it as underconstruction.
    hdfs.create(bar);
    SnapshotTestHelper.createSnapshot(hdfs, test, "s0");
    // rename bar --> bar2
    final Path bar2 = new Path(foo, "bar2");
    hdfs.rename(bar, bar2);

    // save namespace and restart
    restartClusterAndCheckImage(true);
  }
  
  /**
   * Similar with testRenameUCFileInSnapshot, but do renaming first and then 
   * append file without closing it. Unit test for HDFS-5425.
   */
  @Test
  public void testAppendFileAfterRenameInSnapshot() throws Exception {
    final Path test = new Path("/test");
    final Path foo = new Path(test, "foo");
    final Path bar = new Path(foo, "bar");
    DFSTestUtil.createFile(hdfs, bar, BLOCKSIZE, REPL, SEED);
    SnapshotTestHelper.createSnapshot(hdfs, test, "s0");
    // rename bar --> bar2
    final Path bar2 = new Path(foo, "bar2");
    hdfs.rename(bar, bar2);
    // append file and keep it as underconstruction.
    FSDataOutputStream out = hdfs.append(bar2);
    out.writeByte(0);
    ((DFSOutputStream) out.getWrappedStream()).hsync(
        EnumSet.of(SyncFlag.UPDATE_LENGTH));

    // save namespace and restart
    restartClusterAndCheckImage(true);
  }

  @Test
  public void testRenameWithOverWrite() throws Exception {
    final Path root = new Path("/");
    final Path foo = new Path(root, "foo");
    final Path file1InFoo = new Path(foo, "file1");
    final Path file2InFoo = new Path(foo, "file2");
    final Path file3InFoo = new Path(foo, "file3");
    DFSTestUtil.createFile(hdfs, file1InFoo, 1L, REPL, SEED);
    DFSTestUtil.createFile(hdfs, file2InFoo, 1L, REPL, SEED);
    DFSTestUtil.createFile(hdfs, file3InFoo, 1L, REPL, SEED);
    final Path bar = new Path(root, "bar");
    hdfs.mkdirs(bar);

    SnapshotTestHelper.createSnapshot(hdfs, root, "s0");
    // move file1 from foo to bar
    final Path fileInBar = new Path(bar, "file1");
    hdfs.rename(file1InFoo, fileInBar);
    // rename bar to newDir
    final Path newDir = new Path(root, "newDir");
    hdfs.rename(bar, newDir);
    // move file2 from foo to newDir
    final Path file2InNewDir = new Path(newDir, "file2");
    hdfs.rename(file2InFoo, file2InNewDir);
    // move file3 from foo to newDir and rename it to file1, this will overwrite
    // the original file1
    final Path file1InNewDir = new Path(newDir, "file1");
    hdfs.rename(file3InFoo, file1InNewDir, Rename.OVERWRITE);
    SnapshotTestHelper.createSnapshot(hdfs, root, "s1");

    SnapshotDiffReport report = hdfs.getSnapshotDiffReport(root, "s0", "s1");
    LOG.info("DiffList is \n\"" + report.toString() + "\"");
    List<DiffReportEntry> entries = report.getDiffList();
    assertEquals(7, entries.size());
    assertTrue(existsInDiffReport(entries, DiffType.MODIFY, "", null));
    assertTrue(existsInDiffReport(entries, DiffType.MODIFY, foo.getName(), null));
    assertTrue(existsInDiffReport(entries, DiffType.MODIFY, bar.getName(), null));
    assertTrue(existsInDiffReport(entries, DiffType.DELETE, "foo/file1", null));
    assertTrue(existsInDiffReport(entries, DiffType.RENAME, "bar", "newDir"));
    assertTrue(existsInDiffReport(entries, DiffType.RENAME, "foo/file2", "newDir/file2"));
    assertTrue(existsInDiffReport(entries, DiffType.RENAME, "foo/file3", "newDir/file1"));
  }
}
