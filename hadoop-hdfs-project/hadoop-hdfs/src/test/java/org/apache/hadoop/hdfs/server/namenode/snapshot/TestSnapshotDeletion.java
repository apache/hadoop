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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectoryWithSnapshot.DirectoryDiffList;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests snapshot deletion.
 */
public class TestSnapshotDeletion {
  protected static final long seed = 0;
  protected static final short REPLICATION = 3;
  protected static final short REPLICATION_1 = 2;
  protected static final long BLOCKSIZE = 1024;
  public static final int SNAPSHOTNUMBER = 10;
  
  private final Path dir = new Path("/TestSnapshot");
  private final Path sub = new Path(dir, "sub1");
  private final Path subsub = new Path(sub, "subsub1");
  
  protected Configuration conf;
  protected MiniDFSCluster cluster;
  protected FSNamesystem fsn;
  protected FSDirectory fsdir;
  protected DistributedFileSystem hdfs;
  
  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .format(true).build();
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
    
  /**
   * Deleting snapshottable directory with snapshots must fail.
   */
  @Test
  public void testDeleteDirectoryWithSnapshot() throws Exception {
    Path file0 = new Path(sub, "file0");
    Path file1 = new Path(sub, "file1");
    DFSTestUtil.createFile(hdfs, file0, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPLICATION, seed);

    // Allow snapshot for sub1, and create snapshot for it
    hdfs.allowSnapshot(sub.toString());
    hdfs.createSnapshot(sub, "s1");

    // Deleting a snapshottable dir with snapshots should fail
    exception.expect(RemoteException.class);
    String error = "The direcotry " + sub.toString()
        + " cannot be deleted since " + sub.toString()
        + " is snapshottable and already has snapshots";
    exception.expectMessage(error);
    hdfs.delete(sub, true);
  }
  
  /**
   * Deleting directory with snapshottable descendant with snapshots must fail.
   */
  @Test
  public void testDeleteDirectoryWithSnapshot2() throws Exception {
    Path file0 = new Path(sub, "file0");
    Path file1 = new Path(sub, "file1");
    DFSTestUtil.createFile(hdfs, file0, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPLICATION, seed);
    
    Path subfile1 = new Path(subsub, "file0");
    Path subfile2 = new Path(subsub, "file1");
    DFSTestUtil.createFile(hdfs, subfile1, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, subfile2, BLOCKSIZE, REPLICATION, seed);

    // Allow snapshot for subsub1, and create snapshot for it
    hdfs.allowSnapshot(subsub.toString());
    hdfs.createSnapshot(subsub, "s1");

    // Deleting dir while its descedant subsub1 having snapshots should fail
    exception.expect(RemoteException.class);
    String error = "The direcotry " + dir.toString()
        + " cannot be deleted since " + subsub.toString()
        + " is snapshottable and already has snapshots";
    exception.expectMessage(error);
    hdfs.delete(dir, true);
  }
  
  /**
   * Test deleting a directory which is a descendant of a snapshottable
   * directory. In the test we need to cover the following cases:
   * 
   * <pre>
   * 1. Delete current INodeFile/INodeDirectory without taking any snapshot.
   * 2. Delete current INodeFile/INodeDirectory while snapshots have been taken 
   *    on ancestor(s).
   * 3. Delete current INodeFileWithSnapshot.
   * 4. Delete current INodeDirectoryWithSnapshot.
   * </pre>
   */
  @Test
  public void testDeleteCurrentFileDirectory() throws Exception {
    // create a folder which will be deleted before taking snapshots
    Path deleteDir = new Path(subsub, "deleteDir");
    Path deleteFile = new Path(deleteDir, "deleteFile");
    // create a directory that we will not change during the whole process.
    Path noChangeDirParent = new Path(sub, "noChangeDirParent");
    Path noChangeDir = new Path(noChangeDirParent, "noChangeDir");
    // create a file that we will not change in the future
    Path noChangeFile = new Path(noChangeDir, "noChangeFile");
    DFSTestUtil.createFile(hdfs, deleteFile, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, noChangeFile, BLOCKSIZE, REPLICATION, seed);
    // we will change this file's metadata in the future
    Path metaChangeFile1 = new Path(subsub, "metaChangeFile1");
    DFSTestUtil.createFile(hdfs, metaChangeFile1, BLOCKSIZE, REPLICATION, seed);
    // another file, created under noChangeDir, whose metadata will be changed
    Path metaChangeFile2 = new Path(noChangeDir, "metaChangeFile2");
    DFSTestUtil.createFile(hdfs, metaChangeFile2, BLOCKSIZE, REPLICATION, seed);
    
    // Case 1: delete deleteDir before taking snapshots
    hdfs.delete(deleteDir, true);
    
    // create snapshot s0
    SnapshotTestHelper.createSnapshot(hdfs, dir, "s0");
    // make a change: create a new file under subsub
    Path newFileAfterS0 = new Path(subsub, "newFile");
    DFSTestUtil.createFile(hdfs, newFileAfterS0, BLOCKSIZE, REPLICATION, seed);
    // further change: change the replicator factor of metaChangeFile
    hdfs.setReplication(metaChangeFile1, REPLICATION_1);
    hdfs.setReplication(metaChangeFile2, REPLICATION_1);
    // create snapshot s1
    SnapshotTestHelper.createSnapshot(hdfs, dir, "s1");
    
    // get two snapshots for later use
    Snapshot snapshot0 = ((INodeDirectorySnapshottable) fsdir.getINode(dir
        .toString())).getSnapshot(DFSUtil.string2Bytes("s0"));
    Snapshot snapshot1 = ((INodeDirectorySnapshottable) fsdir.getINode(dir
        .toString())).getSnapshot(DFSUtil.string2Bytes("s1"));
    
    // Case 2 + Case 3: delete noChangeDirParent, noChangeFile, and
    // metaChangeFile2. Note that when we directly delete a directory, the 
    // directory will be converted to an INodeDirectoryWithSnapshot. To make
    // sure the deletion goes through an INodeDirectory, we delete the parent
    // of noChangeDir
    hdfs.delete(noChangeDirParent, true);
    
    // check the snapshot copy of noChangeDir 
    Path snapshotNoChangeDir = SnapshotTestHelper.getSnapshotPath(dir, "s1",
        sub.getName() + "/" + noChangeDirParent.getName() + "/"
            + noChangeDir.getName());
    INodeDirectory snapshotNode = 
        (INodeDirectory) fsdir.getINode(snapshotNoChangeDir.toString());
    // should still be an INodeDirectory
    assertEquals(INodeDirectory.class, snapshotNode.getClass());
    ReadOnlyList<INode> children = snapshotNode.getChildrenList(null);
    // check 2 children: noChangeFile and metaChangeFile1
    assertEquals(2, children.size());
    INode noChangeFileSCopy = children.get(1);
    assertEquals(noChangeFile.getName(), noChangeFileSCopy.getLocalName());
    assertEquals(INodeFile.class, noChangeFileSCopy.getClass());
    INodeFileWithSnapshot metaChangeFile2SCopy = 
        (INodeFileWithSnapshot) children.get(0);
    assertEquals(metaChangeFile2.getName(), metaChangeFile2SCopy.getLocalName());
    assertEquals(INodeFileWithSnapshot.class, metaChangeFile2SCopy.getClass());
    // check the replication factor of metaChangeFile1SCopy
    assertEquals(REPLICATION_1,
        metaChangeFile2SCopy.getFileReplication(null));
    assertEquals(REPLICATION_1,
        metaChangeFile2SCopy.getFileReplication(snapshot1));
    assertEquals(REPLICATION,
        metaChangeFile2SCopy.getFileReplication(snapshot0));
    
    // Case 4: delete directory sub
    // before deleting sub, we first create a new file under sub
    Path newFile = new Path(sub, "newFile");
    DFSTestUtil.createFile(hdfs, newFile, BLOCKSIZE, REPLICATION, seed);
    hdfs.delete(sub, true);
    
    // make sure the whole subtree of sub is stored correctly in snapshot
    Path snapshotSub = SnapshotTestHelper.getSnapshotPath(dir, "s1",
        sub.getName());
    INodeDirectoryWithSnapshot snapshotNode4Sub = 
        (INodeDirectoryWithSnapshot) fsdir.getINode(snapshotSub.toString());
    assertEquals(INodeDirectoryWithSnapshot.class, snapshotNode4Sub.getClass());
    // the snapshot copy of sub has only one child subsub.
    // newFile should have been destroyed
    assertEquals(1, snapshotNode4Sub.getChildrenList(null).size());
    // but should have two children, subsub and noChangeDir, when s1 was taken  
    assertEquals(2, snapshotNode4Sub.getChildrenList(snapshot1).size());
    
    // check the snapshot copy of subsub, which is contained in the subtree of
    // sub's snapshot copy
    INode snapshotNode4Subsub = snapshotNode4Sub.getChildrenList(null).get(0);
    assertEquals(INodeDirectoryWithSnapshot.class,
        snapshotNode4Subsub.getClass());
    assertTrue(snapshotNode4Sub == snapshotNode4Subsub.getParent());
    // check the children of subsub
    INodeDirectory snapshotSubsubDir = (INodeDirectory) snapshotNode4Subsub;
    children = snapshotSubsubDir.getChildrenList(null);
    assertEquals(2, children.size());
    assertEquals(children.get(0).getLocalName(), metaChangeFile1.getName());
    assertEquals(children.get(1).getLocalName(), newFileAfterS0.getName());
    // only one child before snapshot s0 
    children = snapshotSubsubDir.getChildrenList(snapshot0);
    assertEquals(1, children.size());
    INode child = children.get(0);
    assertEquals(child.getLocalName(), metaChangeFile1.getName());
    // check snapshot copy of metaChangeFile1
    assertEquals(INodeFileWithSnapshot.class, child.getClass());
    INodeFileWithSnapshot metaChangeFile1SCopy = (INodeFileWithSnapshot) child;
    assertEquals(REPLICATION_1,
        metaChangeFile1SCopy.getFileReplication(null));
    assertEquals(REPLICATION_1,
        metaChangeFile1SCopy.getFileReplication(snapshot1));
    assertEquals(REPLICATION,
        metaChangeFile1SCopy.getFileReplication(snapshot0));
  }
  
  /**
   * Test deleting the earliest (first) snapshot. In this simplest scenario, the 
   * snapshots are taken on the same directory, and we do not need to combine
   * snapshot diffs.
   */
  @Test
  public void testDeleteEarliestSnapshot1() throws Exception {
    // create files under sub
    Path file0 = new Path(sub, "file0");
    Path file1 = new Path(sub, "file1");
    DFSTestUtil.createFile(hdfs, file0, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPLICATION, seed);
    
    String snapshotName = "s1";
    try {
      hdfs.deleteSnapshot(sub, snapshotName);
      fail("SnapshotException expected: " + sub.toString()
          + " is not snapshottable yet");
    } catch (Exception e) {
      GenericTestUtils.assertExceptionContains(
          "Directory is not a snapshottable directory: " + sub, e);
    }
    
    // make sub snapshottable
    hdfs.allowSnapshot(sub.toString());
    try {
      hdfs.deleteSnapshot(sub, snapshotName);
      fail("SnapshotException expected: snapshot " + snapshotName
          + " does not exist for " + sub.toString());
    } catch (Exception e) {
      GenericTestUtils.assertExceptionContains("Cannot delete snapshot "
          + snapshotName + " from path " + sub.toString()
          + ": the snapshot does not exist.", e);
    }
    
    // create snapshot s1 for sub
    hdfs.createSnapshot(sub, snapshotName);
    // delete s1
    hdfs.deleteSnapshot(sub, snapshotName);
    // now we can create a snapshot with the same name
    hdfs.createSnapshot(sub, snapshotName);
    
    // create a new file under sub
    Path newFile = new Path(sub, "newFile");
    DFSTestUtil.createFile(hdfs, newFile, BLOCKSIZE, REPLICATION, seed);
    // create another snapshot s2
    String snapshotName2 = "s2";
    hdfs.createSnapshot(sub, snapshotName2);
    // Get the filestatus of sub under snapshot s2
    Path ss = SnapshotTestHelper
        .getSnapshotPath(sub, snapshotName2, "newFile");
    FileStatus statusBeforeDeletion = hdfs.getFileStatus(ss);
    // delete s1
    hdfs.deleteSnapshot(sub, snapshotName);
    FileStatus statusAfterDeletion = hdfs.getFileStatus(ss);
    System.out.println("Before deletion: " + statusBeforeDeletion.toString()
        + "\n" + "After deletion: " + statusAfterDeletion.toString());
    assertEquals(statusBeforeDeletion.toString(),
        statusAfterDeletion.toString());
  }
  
  /**
   * Test deleting the earliest (first) snapshot. In this more complicated 
   * scenario, the snapshots are taken across directories.
   * <pre>
   * The test covers the following scenarios:
   * 1. delete the first diff in the diff list of a directory
   * 2. delete the first diff in the diff list of a file
   * </pre>
   * Also, the recursive cleanTree process should cover both INodeFile and 
   * INodeDirectory.
   */
  @Test
  public void testDeleteEarliestSnapshot2() throws Exception {
    Path noChangeDir = new Path(sub, "noChangeDir");
    Path noChangeFile = new Path(noChangeDir, "noChangeFile");
    Path metaChangeFile = new Path(noChangeDir, "metaChangeFile");
    Path metaChangeDir = new Path(noChangeDir, "metaChangeDir");
    Path toDeleteFile = new Path(metaChangeDir, "toDeleteFile");
    DFSTestUtil.createFile(hdfs, noChangeFile, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, metaChangeFile, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, toDeleteFile, BLOCKSIZE, REPLICATION, seed);
    
    // create snapshot s0 on dir
    SnapshotTestHelper.createSnapshot(hdfs, dir, "s0");
    
    // delete /TestSnapshot/sub/noChangeDir/metaChangeDir/toDeleteFile
    hdfs.delete(toDeleteFile, true);
    // change metadata of /TestSnapshot/sub/noChangeDir/metaChangeDir and
    // /TestSnapshot/sub/noChangeDir/metaChangeFile
    hdfs.setReplication(metaChangeFile, REPLICATION_1);
    hdfs.setOwner(metaChangeDir, "unknown", "unknown");
    
    // create snapshot s1 on dir
    hdfs.createSnapshot(dir, "s1");
    
    // delete snapshot s0
    hdfs.deleteSnapshot(dir, "s0");
    
    // check 1. there is no snapshot s0
    final INodeDirectorySnapshottable dirNode = 
        (INodeDirectorySnapshottable) fsdir.getINode(dir.toString());
    Snapshot snapshot0 = dirNode.getSnapshot(DFSUtil.string2Bytes("s0"));
    assertNull(snapshot0);
    DirectoryDiffList diffList = dirNode.getDiffs();
    assertEquals(1, diffList.asList().size());
    assertEquals("s1", diffList.getLast().snapshot.getRoot().getLocalName());
    diffList = ((INodeDirectoryWithSnapshot) fsdir.getINode(
        metaChangeDir.toString())).getDiffs();
    assertEquals(0, diffList.asList().size());
    
    // check 2. noChangeDir and noChangeFile are still there
    final INodeDirectory noChangeDirNode = 
        (INodeDirectory) fsdir.getINode(noChangeDir.toString());
    assertEquals(INodeDirectory.class, noChangeDirNode.getClass());
    final INodeFile noChangeFileNode = 
        (INodeFile) fsdir.getINode(noChangeFile.toString());
    assertEquals(INodeFile.class, noChangeFileNode.getClass());
    
    // check 3: current metadata of metaChangeFile and metaChangeDir
    FileStatus status = hdfs.getFileStatus(metaChangeDir);
    assertEquals("unknown", status.getOwner());
    assertEquals("unknown", status.getGroup());
    status = hdfs.getFileStatus(metaChangeFile);
    assertEquals(REPLICATION_1, status.getReplication());
    
    // check 4: no snapshot copy for toDeleteFile
    try {
      status = hdfs.getFileStatus(toDeleteFile);
      fail("should throw FileNotFoundException");
    } catch (FileNotFoundException e) {
      GenericTestUtils.assertExceptionContains("File does not exist: "
          + toDeleteFile.toString(), e);
    }
    
    final Path toDeleteFileInSnapshot = SnapshotTestHelper.getSnapshotPath(dir,
        "s0", toDeleteFile.toString().substring(dir.toString().length()));
    try {
      status = hdfs.getFileStatus(toDeleteFileInSnapshot);
      fail("should throw FileNotFoundException");
    } catch (FileNotFoundException e) {
      GenericTestUtils.assertExceptionContains("File does not exist: "
          + toDeleteFileInSnapshot.toString(), e);
    }
  }
  
  /**
   * Test deleting snapshots in a more complicated scenario: need to combine
   * snapshot diffs, but no need to handle diffs distributed in a dir tree
   */
  @Test
  public void testCombineSnapshotDiff1() throws Exception {
    testCombineSnapshotDiffImpl(sub, "");
  }
  
  /**
   * Test deleting snapshots in more complicated scenarios (snapshot diffs are
   * distributed in the directory sub-tree)
   */
  @Test
  public void testCombineSnapshotDiff2() throws Exception {
    testCombineSnapshotDiffImpl(sub, "subsub1/subsubsub1/");
  }
  
  /**
   * Test snapshot deletion
   * @param snapshotRoot The dir where the snapshots are created
   * @param modDirStr The snapshotRoot itself or one of its sub-directory, 
   *        where the modifications happen. It is represented as a relative 
   *        path to the snapshotRoot.
   */
  private void testCombineSnapshotDiffImpl(Path snapshotRoot, String modDirStr)
      throws Exception {
    Path modDir = modDirStr.isEmpty() ? snapshotRoot : new Path(snapshotRoot,
        modDirStr);
    Path file10 = new Path(modDir, "file10");
    Path file11 = new Path(modDir, "file11");
    Path file12 = new Path(modDir, "file12");
    Path file13 = new Path(modDir, "file13");
    Path file14 = new Path(modDir, "file14");
    Path file15 = new Path(modDir, "file15");
    final short REP_1 = REPLICATION - 1;
    DFSTestUtil.createFile(hdfs, file10, BLOCKSIZE, REP_1, seed);
    DFSTestUtil.createFile(hdfs, file11, BLOCKSIZE, REP_1, seed);
    DFSTestUtil.createFile(hdfs, file12, BLOCKSIZE, REP_1, seed);
    DFSTestUtil.createFile(hdfs, file13, BLOCKSIZE, REP_1, seed);

    // create snapshot s1 for snapshotRoot
    SnapshotTestHelper.createSnapshot(hdfs, snapshotRoot, "s1");
    
    // delete file11
    hdfs.delete(file11, true);
    // modify file12
    hdfs.setReplication(file12, REPLICATION);
    // modify file13
    hdfs.setReplication(file13, REPLICATION);
    // create file14
    DFSTestUtil.createFile(hdfs, file14, BLOCKSIZE, REPLICATION, seed);
    // create file15
    DFSTestUtil.createFile(hdfs, file15, BLOCKSIZE, REPLICATION, seed);
    
    // create snapshot s2 for snapshotRoot
    hdfs.createSnapshot(snapshotRoot, "s2");
    
    // create file11 again: (0, d) + (c, 0)
    DFSTestUtil.createFile(hdfs, file11, BLOCKSIZE, REPLICATION, seed);
    // delete file12
    hdfs.delete(file12, true);
    // modify file13
    hdfs.setReplication(file13, (short) (REPLICATION - 2));
    // delete file14: (c, 0) + (0, d)
    hdfs.delete(file14, true);
    // modify file15
    hdfs.setReplication(file15, REP_1);
    
    // create snapshot s3 for snapshotRoot
    hdfs.createSnapshot(snapshotRoot, "s3");
    // modify file10, to check if the posterior diff was set correctly
    hdfs.setReplication(file10, REP_1);
    
    Path file10_s1 = SnapshotTestHelper.getSnapshotPath(snapshotRoot, "s1",
        modDirStr + "file10");
    Path file11_s1 = SnapshotTestHelper.getSnapshotPath(snapshotRoot, "s1",
        modDirStr + "file11");
    Path file12_s1 = SnapshotTestHelper.getSnapshotPath(snapshotRoot, "s1",
        modDirStr + "file12");
    Path file13_s1 = SnapshotTestHelper.getSnapshotPath(snapshotRoot, "s1",
        modDirStr + "file13");
    FileStatus statusBeforeDeletion10 = hdfs.getFileStatus(file10_s1);
    FileStatus statusBeforeDeletion11 = hdfs.getFileStatus(file11_s1);
    FileStatus statusBeforeDeletion12 = hdfs.getFileStatus(file12_s1);
    FileStatus statusBeforeDeletion13 = hdfs.getFileStatus(file13_s1);
    
    // delete s2, in which process we need to combine the diff in s2 to s1
    hdfs.deleteSnapshot(snapshotRoot, "s2");
    // check the correctness of s1
    FileStatus statusAfterDeletion10 = hdfs.getFileStatus(file10_s1);
    FileStatus statusAfterDeletion11 = hdfs.getFileStatus(file11_s1);
    FileStatus statusAfterDeletion12 = hdfs.getFileStatus(file12_s1);
    FileStatus statusAfterDeletion13 = hdfs.getFileStatus(file13_s1);
    assertEquals(statusBeforeDeletion10.toString(),
        statusAfterDeletion10.toString());
    assertEquals(statusBeforeDeletion11.toString(),
        statusAfterDeletion11.toString());
    assertEquals(statusBeforeDeletion12.toString(),
        statusAfterDeletion12.toString());
    assertEquals(statusBeforeDeletion13.toString(),
        statusAfterDeletion13.toString());
    
    // make sure file14 and file15 are not included in s1
    Path file14_s1 = SnapshotTestHelper.getSnapshotPath(snapshotRoot, "s1",
        modDirStr + "file14");
    assertFalse(hdfs.exists(file14_s1));
    Path file15_s1 = SnapshotTestHelper.getSnapshotPath(snapshotRoot, "s1",
        modDirStr + "file15");
    assertFalse(hdfs.exists(file15_s1));
    
    INodeFile nodeFile13 = (INodeFile) fsdir.getINode(file13.toString());
    assertEquals(REP_1, nodeFile13.getBlockReplication());

    INodeFile nodeFile12 = (INodeFile) fsdir.getINode(file12_s1.toString());
    assertEquals(REP_1, nodeFile12.getBlockReplication());
  }
  
  /** Test deleting snapshots with modification on the metadata of directory */ 
  @Test
  public void testDeleteSnapshotWithDirModification() throws Exception {
    Path file = new Path(sub, "file");
    DFSTestUtil.createFile(hdfs, file, BLOCKSIZE, REPLICATION, seed);
    hdfs.setOwner(sub, "user1", "group1");
    
    // create snapshot s1 for sub1, and change the metadata of sub1
    SnapshotTestHelper.createSnapshot(hdfs, sub, "s1");
    hdfs.setOwner(sub, "user2", "group2");
    
    // create snapshot s2 for sub1, but do not modify sub1 afterwards
    hdfs.createSnapshot(sub, "s2");
    
    // create snapshot s3 for sub1, and change the metadata of sub1
    hdfs.createSnapshot(sub, "s3");
    hdfs.setOwner(sub, "user3", "group3");
    
    // delete snapshot s3
    hdfs.deleteSnapshot(sub, "s3");
    // check sub1's metadata in snapshot s2
    FileStatus statusOfS2 = hdfs.getFileStatus(new Path(sub,
        HdfsConstants.DOT_SNAPSHOT_DIR + "/s2"));
    assertEquals("user2", statusOfS2.getOwner());
    assertEquals("group2", statusOfS2.getGroup());
    
    // delete snapshot s2
    hdfs.deleteSnapshot(sub, "s2");
    // check sub1's metadata in snapshot s1
    FileStatus statusOfS1 = hdfs.getFileStatus(new Path(sub,
        HdfsConstants.DOT_SNAPSHOT_DIR + "/s1"));
    assertEquals("user1", statusOfS1.getOwner());
    assertEquals("group1", statusOfS1.getGroup());
  }
  
  /** 
   * A test covering the case where the snapshot diff to be deleted is renamed 
   * to its previous snapshot. 
   */
  @Test
  public void testRenameSnapshotDiff() throws Exception {
    final Path subFile0 = new Path(sub, "file0");
    final Path subsubFile0 = new Path(subsub, "file0");
    DFSTestUtil.createFile(hdfs, subFile0, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, subsubFile0, BLOCKSIZE, REPLICATION, seed);
    hdfs.setOwner(subsub, "owner", "group");
    
    // create snapshot s0 on sub
    SnapshotTestHelper.createSnapshot(hdfs, sub, "s0");
    // make some changes on both sub and subsub
    final Path subFile1 = new Path(sub, "file1");
    final Path subsubFile1 = new Path(subsub, "file1");
    DFSTestUtil.createFile(hdfs, subFile1, BLOCKSIZE, REPLICATION_1, seed);
    DFSTestUtil.createFile(hdfs, subsubFile1, BLOCKSIZE, REPLICATION, seed);
    
    // create snapshot s1 on sub
    SnapshotTestHelper.createSnapshot(hdfs, sub, "s1");
    
    // create snapshot s2 on dir
    SnapshotTestHelper.createSnapshot(hdfs, dir, "s2");
    // make changes on subsub and subsubFile1
    hdfs.setOwner(subsub, "unknown", "unknown");
    hdfs.setReplication(subsubFile1, REPLICATION_1);
    // make changes on sub
    hdfs.delete(subFile1, true);
    
    Path subsubSnapshotCopy = SnapshotTestHelper.getSnapshotPath(dir, "s2",
        sub.getName() + Path.SEPARATOR + subsub.getName());
    Path subsubFile1SCopy = SnapshotTestHelper.getSnapshotPath(dir, "s2",
        sub.getName() + Path.SEPARATOR + subsub.getName() + Path.SEPARATOR
            + subsubFile1.getName());
    Path subFile1SCopy = SnapshotTestHelper.getSnapshotPath(dir, "s2",
        sub.getName() + Path.SEPARATOR + subFile1.getName());
    FileStatus subsubStatus = hdfs.getFileStatus(subsubSnapshotCopy);
    assertEquals("owner", subsubStatus.getOwner());
    assertEquals("group", subsubStatus.getGroup());
    FileStatus subsubFile1Status = hdfs.getFileStatus(subsubFile1SCopy);
    assertEquals(REPLICATION, subsubFile1Status.getReplication());
    FileStatus subFile1Status = hdfs.getFileStatus(subFile1SCopy);
    assertEquals(REPLICATION_1, subFile1Status.getReplication());
    
    // delete snapshot s2
    hdfs.deleteSnapshot(dir, "s2");
    
    // no snapshot copy for s2
    try {
      hdfs.getFileStatus(subsubSnapshotCopy);
      fail("should throw FileNotFoundException");
    } catch (FileNotFoundException e) {
      GenericTestUtils.assertExceptionContains("File does not exist: "
          + subsubSnapshotCopy.toString(), e);
    }
    try {
      hdfs.getFileStatus(subsubFile1SCopy);
      fail("should throw FileNotFoundException");
    } catch (FileNotFoundException e) {
      GenericTestUtils.assertExceptionContains("File does not exist: "
          + subsubFile1SCopy.toString(), e);
    }
    try {
      hdfs.getFileStatus(subFile1SCopy);
      fail("should throw FileNotFoundException");
    } catch (FileNotFoundException e) {
      GenericTestUtils.assertExceptionContains("File does not exist: "
          + subFile1SCopy.toString(), e);
    }
    
    // the snapshot copy of s2 should now be renamed to s1 under sub
    subsubSnapshotCopy = SnapshotTestHelper.getSnapshotPath(sub, "s1",
        subsub.getName());
    subsubFile1SCopy = SnapshotTestHelper.getSnapshotPath(sub, "s1",
        subsub.getName() + Path.SEPARATOR + subsubFile1.getName());
    subFile1SCopy = SnapshotTestHelper.getSnapshotPath(sub, "s1",
        subFile1.getName());
    subsubStatus = hdfs.getFileStatus(subsubSnapshotCopy);
    assertEquals("owner", subsubStatus.getOwner());
    assertEquals("group", subsubStatus.getGroup());
    subsubFile1Status = hdfs.getFileStatus(subsubFile1SCopy);
    assertEquals(REPLICATION, subsubFile1Status.getReplication());
    // also subFile1's snapshot copy should have been moved to diff of s1 as 
    // combination
    subFile1Status = hdfs.getFileStatus(subFile1SCopy);
    assertEquals(REPLICATION_1, subFile1Status.getReplication());
  }
}
