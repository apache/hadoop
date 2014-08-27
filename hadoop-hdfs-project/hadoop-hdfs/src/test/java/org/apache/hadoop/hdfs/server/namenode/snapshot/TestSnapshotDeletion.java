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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.security.PrivilegedAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.namenode.Quota;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.DirectoryDiffList;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
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
  
  private final Path dir = new Path("/TestSnapshot");
  private final Path sub = new Path(dir, "sub1");
  private final Path subsub = new Path(sub, "subsub1");
  
  protected Configuration conf;
  protected MiniDFSCluster cluster;
  protected FSNamesystem fsn;
  protected FSDirectory fsdir;
  protected BlockManager blockmanager;
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
    blockmanager = fsn.getBlockManager();
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
  @Test (timeout=300000)
  public void testDeleteDirectoryWithSnapshot() throws Exception {
    Path file0 = new Path(sub, "file0");
    Path file1 = new Path(sub, "file1");
    DFSTestUtil.createFile(hdfs, file0, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPLICATION, seed);

    // Allow snapshot for sub1, and create snapshot for it
    hdfs.allowSnapshot(sub);
    hdfs.createSnapshot(sub, "s1");

    // Deleting a snapshottable dir with snapshots should fail
    exception.expect(RemoteException.class);
    String error = "The directory " + sub.toString()
        + " cannot be deleted since " + sub.toString()
        + " is snapshottable and already has snapshots";
    exception.expectMessage(error);
    hdfs.delete(sub, true);
  }

  /**
   * Test applying editlog of operation which deletes a snapshottable directory
   * without snapshots. The snapshottable dir list in snapshot manager should be
   * updated.
   */
  @Test (timeout=300000)
  public void testApplyEditLogForDeletion() throws Exception {
    final Path foo = new Path("/foo");
    final Path bar1 = new Path(foo, "bar1");
    final Path bar2 = new Path(foo, "bar2");
    hdfs.mkdirs(bar1);
    hdfs.mkdirs(bar2);

    // allow snapshots on bar1 and bar2
    hdfs.allowSnapshot(bar1);
    hdfs.allowSnapshot(bar2);
    assertEquals(2, cluster.getNamesystem().getSnapshotManager()
        .getNumSnapshottableDirs());
    assertEquals(2, cluster.getNamesystem().getSnapshotManager()
        .getSnapshottableDirs().length);

    // delete /foo
    hdfs.delete(foo, true);
    cluster.restartNameNode(0);
    // the snapshottable dir list in snapshot manager should be empty
    assertEquals(0, cluster.getNamesystem().getSnapshotManager()
        .getNumSnapshottableDirs());
    assertEquals(0, cluster.getNamesystem().getSnapshotManager()
        .getSnapshottableDirs().length);
    hdfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
    hdfs.saveNamespace();
    hdfs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
    cluster.restartNameNode(0);
  }

  /**
   * Deleting directory with snapshottable descendant with snapshots must fail.
   */
  @Test (timeout=300000)
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
    hdfs.allowSnapshot(subsub);
    hdfs.createSnapshot(subsub, "s1");

    // Deleting dir while its descedant subsub1 having snapshots should fail
    exception.expect(RemoteException.class);
    String error = subsub.toString()
        + " is snapshottable and already has snapshots";
    exception.expectMessage(error);
    hdfs.delete(dir, true);
  }
  
  private static INodeDirectory getDir(final FSDirectory fsdir, final Path dir)
      throws IOException {
    final String dirStr = dir.toString();
    return INodeDirectory.valueOf(fsdir.getINode(dirStr), dirStr);
  }

  private void checkQuotaUsageComputation(final Path dirPath,
      final long expectedNs, final long expectedDs) throws IOException {
    INodeDirectory dirNode = getDir(fsdir, dirPath);
    assertTrue(dirNode.isQuotaSet());
    Quota.Counts q = dirNode.getDirectoryWithQuotaFeature().getSpaceConsumed();
    assertEquals(dirNode.dumpTreeRecursively().toString(), expectedNs,
        q.get(Quota.NAMESPACE));
    assertEquals(dirNode.dumpTreeRecursively().toString(), expectedDs,
        q.get(Quota.DISKSPACE));
    Quota.Counts counts = Quota.Counts.newInstance();
    dirNode.computeQuotaUsage(counts, false);
    assertEquals(dirNode.dumpTreeRecursively().toString(), expectedNs,
        counts.get(Quota.NAMESPACE));
    assertEquals(dirNode.dumpTreeRecursively().toString(), expectedDs,
        counts.get(Quota.DISKSPACE));
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
  @Test (timeout=300000)
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
    
    // after creating snapshot s0, create a directory tempdir under dir and then
    // delete dir immediately
    Path tempDir = new Path(dir, "tempdir");
    Path tempFile = new Path(tempDir, "tempfile");
    DFSTestUtil.createFile(hdfs, tempFile, BLOCKSIZE, REPLICATION, seed);
    final INodeFile temp = TestSnapshotBlocksMap.assertBlockCollection(
        tempFile.toString(), 1, fsdir, blockmanager);
    BlockInfo[] blocks = temp.getBlocks();
    hdfs.delete(tempDir, true);
    // check dir's quota usage
    checkQuotaUsageComputation(dir, 9L, BLOCKSIZE * REPLICATION * 3);
    // check blocks of tempFile
    for (BlockInfo b : blocks) {
      assertNull(blockmanager.getBlockCollection(b));
    }
    
    // make a change: create a new file under subsub
    Path newFileAfterS0 = new Path(subsub, "newFile");
    DFSTestUtil.createFile(hdfs, newFileAfterS0, BLOCKSIZE, REPLICATION, seed);
    // further change: change the replicator factor of metaChangeFile
    hdfs.setReplication(metaChangeFile1, REPLICATION_1);
    hdfs.setReplication(metaChangeFile2, REPLICATION_1);
    
    // create snapshot s1
    SnapshotTestHelper.createSnapshot(hdfs, dir, "s1");
    // check dir's quota usage
    checkQuotaUsageComputation(dir, 14L, BLOCKSIZE * REPLICATION * 4);
    
    // get two snapshots for later use
    Snapshot snapshot0 = fsdir.getINode(dir.toString()).asDirectory()
        .getSnapshot(DFSUtil.string2Bytes("s0"));
    Snapshot snapshot1 = fsdir.getINode(dir.toString()).asDirectory()
        .getSnapshot(DFSUtil.string2Bytes("s1"));
    
    // Case 2 + Case 3: delete noChangeDirParent, noChangeFile, and
    // metaChangeFile2. Note that when we directly delete a directory, the 
    // directory will be converted to an INodeDirectoryWithSnapshot. To make
    // sure the deletion goes through an INodeDirectory, we delete the parent
    // of noChangeDir
    hdfs.delete(noChangeDirParent, true);
    // while deletion, we add a diff for metaChangeFile2 as its snapshot copy
    // for s1, we also add diffs for both sub and noChangeDirParent
    checkQuotaUsageComputation(dir, 17L, BLOCKSIZE * REPLICATION * 4);
    
    // check the snapshot copy of noChangeDir 
    Path snapshotNoChangeDir = SnapshotTestHelper.getSnapshotPath(dir, "s1",
        sub.getName() + "/" + noChangeDirParent.getName() + "/"
            + noChangeDir.getName());
    INodeDirectory snapshotNode = 
        (INodeDirectory) fsdir.getINode(snapshotNoChangeDir.toString());
    // should still be an INodeDirectory
    assertEquals(INodeDirectory.class, snapshotNode.getClass());
    ReadOnlyList<INode> children = snapshotNode
        .getChildrenList(Snapshot.CURRENT_STATE_ID);
    // check 2 children: noChangeFile and metaChangeFile2
    assertEquals(2, children.size());
    INode noChangeFileSCopy = children.get(1);
    assertEquals(noChangeFile.getName(), noChangeFileSCopy.getLocalName());
    assertEquals(INodeFile.class, noChangeFileSCopy.getClass());
    TestSnapshotBlocksMap.assertBlockCollection(new Path(snapshotNoChangeDir,
        noChangeFileSCopy.getLocalName()).toString(), 1, fsdir, blockmanager);
    
    INodeFile metaChangeFile2SCopy = children.get(0).asFile();
    assertEquals(metaChangeFile2.getName(), metaChangeFile2SCopy.getLocalName());
    assertTrue(metaChangeFile2SCopy.isWithSnapshot());
    assertFalse(metaChangeFile2SCopy.isUnderConstruction());
    TestSnapshotBlocksMap.assertBlockCollection(new Path(snapshotNoChangeDir,
        metaChangeFile2SCopy.getLocalName()).toString(), 1, fsdir, blockmanager);
    
    // check the replication factor of metaChangeFile2SCopy
    assertEquals(REPLICATION_1,
        metaChangeFile2SCopy.getFileReplication(Snapshot.CURRENT_STATE_ID));
    assertEquals(REPLICATION_1,
        metaChangeFile2SCopy.getFileReplication(snapshot1.getId()));
    assertEquals(REPLICATION,
        metaChangeFile2SCopy.getFileReplication(snapshot0.getId()));
    
    // Case 4: delete directory sub
    // before deleting sub, we first create a new file under sub
    Path newFile = new Path(sub, "newFile");
    DFSTestUtil.createFile(hdfs, newFile, BLOCKSIZE, REPLICATION, seed);
    final INodeFile newFileNode = TestSnapshotBlocksMap.assertBlockCollection(
        newFile.toString(), 1, fsdir, blockmanager);
    blocks = newFileNode.getBlocks();
    checkQuotaUsageComputation(dir, 18L, BLOCKSIZE * REPLICATION * 5);
    hdfs.delete(sub, true);
    // while deletion, we add diff for subsub and metaChangeFile1, and remove
    // newFile
    checkQuotaUsageComputation(dir, 19L, BLOCKSIZE * REPLICATION * 4);
    for (BlockInfo b : blocks) {
      assertNull(blockmanager.getBlockCollection(b));
    }
    
    // make sure the whole subtree of sub is stored correctly in snapshot
    Path snapshotSub = SnapshotTestHelper.getSnapshotPath(dir, "s1",
        sub.getName());
    INodeDirectory snapshotNode4Sub = fsdir.getINode(snapshotSub.toString())
        .asDirectory();
    assertTrue(snapshotNode4Sub.isWithSnapshot());
    // the snapshot copy of sub has only one child subsub.
    // newFile should have been destroyed
    assertEquals(1, snapshotNode4Sub.getChildrenList(Snapshot.CURRENT_STATE_ID)
        .size());
    // but should have two children, subsub and noChangeDir, when s1 was taken  
    assertEquals(2, snapshotNode4Sub.getChildrenList(snapshot1.getId()).size());
    
    // check the snapshot copy of subsub, which is contained in the subtree of
    // sub's snapshot copy
    INode snapshotNode4Subsub = snapshotNode4Sub.getChildrenList(
        Snapshot.CURRENT_STATE_ID).get(0);
    assertTrue(snapshotNode4Subsub.asDirectory().isWithSnapshot());
    assertTrue(snapshotNode4Sub == snapshotNode4Subsub.getParent());
    // check the children of subsub
    INodeDirectory snapshotSubsubDir = (INodeDirectory) snapshotNode4Subsub;
    children = snapshotSubsubDir.getChildrenList(Snapshot.CURRENT_STATE_ID);
    assertEquals(2, children.size());
    assertEquals(children.get(0).getLocalName(), metaChangeFile1.getName());
    assertEquals(children.get(1).getLocalName(), newFileAfterS0.getName());
    // only one child before snapshot s0 
    children = snapshotSubsubDir.getChildrenList(snapshot0.getId());
    assertEquals(1, children.size());
    INode child = children.get(0);
    assertEquals(child.getLocalName(), metaChangeFile1.getName());
    // check snapshot copy of metaChangeFile1
    INodeFile metaChangeFile1SCopy = child.asFile();
    assertTrue(metaChangeFile1SCopy.isWithSnapshot());
    assertFalse(metaChangeFile1SCopy.isUnderConstruction());
    assertEquals(REPLICATION_1,
        metaChangeFile1SCopy.getFileReplication(Snapshot.CURRENT_STATE_ID));
    assertEquals(REPLICATION_1,
        metaChangeFile1SCopy.getFileReplication(snapshot1.getId()));
    assertEquals(REPLICATION,
        metaChangeFile1SCopy.getFileReplication(snapshot0.getId()));
  }
  
  /**
   * Test deleting the earliest (first) snapshot. In this simplest scenario, the 
   * snapshots are taken on the same directory, and we do not need to combine
   * snapshot diffs.
   */
  @Test (timeout=300000)
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
    hdfs.allowSnapshot(sub);
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
    SnapshotTestHelper.createSnapshot(hdfs, sub, snapshotName);
    // check quota usage computation
    checkQuotaUsageComputation(sub, 4, BLOCKSIZE * REPLICATION * 2);
    // delete s1
    hdfs.deleteSnapshot(sub, snapshotName);
    checkQuotaUsageComputation(sub, 3, BLOCKSIZE * REPLICATION * 2);
    // now we can create a snapshot with the same name
    hdfs.createSnapshot(sub, snapshotName);
    checkQuotaUsageComputation(sub, 4, BLOCKSIZE * REPLICATION * 2);
    
    // create a new file under sub
    Path newFile = new Path(sub, "newFile");
    DFSTestUtil.createFile(hdfs, newFile, BLOCKSIZE, REPLICATION, seed);
    // create another snapshot s2
    String snapshotName2 = "s2";
    hdfs.createSnapshot(sub, snapshotName2);
    checkQuotaUsageComputation(sub, 6, BLOCKSIZE * REPLICATION * 3);
    // Get the filestatus of sub under snapshot s2
    Path ss = SnapshotTestHelper
        .getSnapshotPath(sub, snapshotName2, "newFile");
    FileStatus statusBeforeDeletion = hdfs.getFileStatus(ss);
    // delete s1
    hdfs.deleteSnapshot(sub, snapshotName);
    checkQuotaUsageComputation(sub, 5, BLOCKSIZE * REPLICATION * 3);
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
  @Test (timeout=300000)
  public void testDeleteEarliestSnapshot2() throws Exception {
    Path noChangeDir = new Path(sub, "noChangeDir");
    Path noChangeFile = new Path(noChangeDir, "noChangeFile");
    Path metaChangeFile = new Path(noChangeDir, "metaChangeFile");
    Path metaChangeDir = new Path(noChangeDir, "metaChangeDir");
    Path toDeleteFile = new Path(metaChangeDir, "toDeleteFile");
    DFSTestUtil.createFile(hdfs, noChangeFile, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, metaChangeFile, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, toDeleteFile, BLOCKSIZE, REPLICATION, seed);
    
    final INodeFile toDeleteFileNode = TestSnapshotBlocksMap
        .assertBlockCollection(toDeleteFile.toString(), 1, fsdir, blockmanager);
    BlockInfo[] blocks = toDeleteFileNode.getBlocks();
    
    // create snapshot s0 on dir
    SnapshotTestHelper.createSnapshot(hdfs, dir, "s0");
    checkQuotaUsageComputation(dir, 8, 3 * BLOCKSIZE * REPLICATION);
    
    // delete /TestSnapshot/sub/noChangeDir/metaChangeDir/toDeleteFile
    hdfs.delete(toDeleteFile, true);
    // the deletion adds diff of toDeleteFile and metaChangeDir
    checkQuotaUsageComputation(dir, 10, 3 * BLOCKSIZE * REPLICATION);
    // change metadata of /TestSnapshot/sub/noChangeDir/metaChangeDir and
    // /TestSnapshot/sub/noChangeDir/metaChangeFile
    hdfs.setReplication(metaChangeFile, REPLICATION_1);
    hdfs.setOwner(metaChangeDir, "unknown", "unknown");
    checkQuotaUsageComputation(dir, 11, 3 * BLOCKSIZE * REPLICATION);
    
    // create snapshot s1 on dir
    hdfs.createSnapshot(dir, "s1");
    checkQuotaUsageComputation(dir, 12, 3 * BLOCKSIZE * REPLICATION);
    
    // delete snapshot s0
    hdfs.deleteSnapshot(dir, "s0");
    // namespace: remove toDeleteFile and its diff, metaChangeFile's diff, 
    // metaChangeDir's diff, dir's diff. diskspace: remove toDeleteFile, and 
    // metaChangeFile's replication factor decreases
    checkQuotaUsageComputation(dir, 7, 2 * BLOCKSIZE * REPLICATION - BLOCKSIZE);
    for (BlockInfo b : blocks) {
      assertNull(blockmanager.getBlockCollection(b));
    }
    
    // check 1. there is no snapshot s0
    final INodeDirectory dirNode = fsdir.getINode(dir.toString()).asDirectory();
    Snapshot snapshot0 = dirNode.getSnapshot(DFSUtil.string2Bytes("s0"));
    assertNull(snapshot0);
    Snapshot snapshot1 = dirNode.getSnapshot(DFSUtil.string2Bytes("s1"));
    DirectoryDiffList diffList = dirNode.getDiffs();
    assertEquals(1, diffList.asList().size());
    assertEquals(snapshot1.getId(), diffList.getLast().getSnapshotId());
    diffList = fsdir.getINode(metaChangeDir.toString()).asDirectory()
        .getDiffs();
    assertEquals(0, diffList.asList().size());
    
    // check 2. noChangeDir and noChangeFile are still there
    final INodeDirectory noChangeDirNode = 
        (INodeDirectory) fsdir.getINode(noChangeDir.toString());
    assertEquals(INodeDirectory.class, noChangeDirNode.getClass());
    final INodeFile noChangeFileNode = 
        (INodeFile) fsdir.getINode(noChangeFile.toString());
    assertEquals(INodeFile.class, noChangeFileNode.getClass());
    TestSnapshotBlocksMap.assertBlockCollection(noChangeFile.toString(), 1,
        fsdir, blockmanager);
    
    // check 3: current metadata of metaChangeFile and metaChangeDir
    FileStatus status = hdfs.getFileStatus(metaChangeDir);
    assertEquals("unknown", status.getOwner());
    assertEquals("unknown", status.getGroup());
    status = hdfs.getFileStatus(metaChangeFile);
    assertEquals(REPLICATION_1, status.getReplication());
    TestSnapshotBlocksMap.assertBlockCollection(metaChangeFile.toString(), 1,
        fsdir, blockmanager);
    
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
   * Delete a snapshot that is taken before a directory deletion,
   * directory diff list should be combined correctly.
   */
  @Test (timeout=60000)
  public void testDeleteSnapshot1() throws Exception {
    final Path root = new Path("/");

    Path dir = new Path("/dir1");
    Path file1 = new Path(dir, "file1");
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPLICATION, seed);

    hdfs.allowSnapshot(root);
    hdfs.createSnapshot(root, "s1");

    Path file2 = new Path(dir, "file2");
    DFSTestUtil.createFile(hdfs, file2, BLOCKSIZE, REPLICATION, seed);

    hdfs.createSnapshot(root, "s2");

    // delete file
    hdfs.delete(file1, true);
    hdfs.delete(file2, true);

    // delete directory
    assertTrue(hdfs.delete(dir, false));

    // delete second snapshot
    hdfs.deleteSnapshot(root, "s2");

    NameNodeAdapter.enterSafeMode(cluster.getNameNode(), false);
    NameNodeAdapter.saveNamespace(cluster.getNameNode());

    // restart NN
    cluster.restartNameNodes();
  }

  /**
   * Delete a snapshot that is taken before a directory deletion (recursively),
   * directory diff list should be combined correctly.
   */
  @Test (timeout=60000)
  public void testDeleteSnapshot2() throws Exception {
    final Path root = new Path("/");

    Path dir = new Path("/dir1");
    Path file1 = new Path(dir, "file1");
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPLICATION, seed);

    hdfs.allowSnapshot(root);
    hdfs.createSnapshot(root, "s1");

    Path file2 = new Path(dir, "file2");
    DFSTestUtil.createFile(hdfs, file2, BLOCKSIZE, REPLICATION, seed);
    INodeFile file2Node = fsdir.getINode(file2.toString()).asFile();
    long file2NodeId = file2Node.getId();

    hdfs.createSnapshot(root, "s2");

    // delete directory recursively
    assertTrue(hdfs.delete(dir, true));
    assertNotNull(fsdir.getInode(file2NodeId));

    // delete second snapshot
    hdfs.deleteSnapshot(root, "s2");
    assertTrue(fsdir.getInode(file2NodeId) == null);

    NameNodeAdapter.enterSafeMode(cluster.getNameNode(), false);
    NameNodeAdapter.saveNamespace(cluster.getNameNode());

    // restart NN
    cluster.restartNameNodes();
  }

  /**
   * Test deleting snapshots in a more complicated scenario: need to combine
   * snapshot diffs, but no need to handle diffs distributed in a dir tree
   */
  @Test (timeout=300000)
  public void testCombineSnapshotDiff1() throws Exception {
    testCombineSnapshotDiffImpl(sub, "", 1);
  }
  
  /**
   * Test deleting snapshots in more complicated scenarios (snapshot diffs are
   * distributed in the directory sub-tree)
   */
  @Test (timeout=300000)
  public void testCombineSnapshotDiff2() throws Exception {
    testCombineSnapshotDiffImpl(sub, "subsub1/subsubsub1/", 3);
  }
  
  /**
   * When combine two snapshots, make sure files/directories created after the 
   * prior snapshot get destroyed.
   */
  @Test (timeout=300000)
  public void testCombineSnapshotDiff3() throws Exception {
    // create initial dir and subdir
    Path dir = new Path("/dir");
    Path subDir1 = new Path(dir, "subdir1");
    Path subDir2 = new Path(dir, "subdir2");
    hdfs.mkdirs(subDir2);
    Path subsubDir = new Path(subDir1, "subsubdir");
    hdfs.mkdirs(subsubDir);
    
    // take snapshots on subdir and dir
    SnapshotTestHelper.createSnapshot(hdfs, dir, "s1");
    
    // create new dir under initial dir
    Path newDir = new Path(subsubDir, "newdir");
    Path newFile = new Path(newDir, "newfile");
    DFSTestUtil.createFile(hdfs, newFile, BLOCKSIZE, REPLICATION, seed);
    Path newFile2 = new Path(subDir2, "newfile");
    DFSTestUtil.createFile(hdfs, newFile2, BLOCKSIZE, REPLICATION, seed);
    
    // create another snapshot
    SnapshotTestHelper.createSnapshot(hdfs, dir, "s2");
    
    checkQuotaUsageComputation(dir, 11, BLOCKSIZE * 2 * REPLICATION);
    
    // delete subsubdir and subDir2
    hdfs.delete(subsubDir, true);
    hdfs.delete(subDir2, true);
    
    // add diff of s2 to subDir1, subsubDir, and subDir2
    checkQuotaUsageComputation(dir, 14, BLOCKSIZE * 2 * REPLICATION);
    
    // delete snapshot s2
    hdfs.deleteSnapshot(dir, "s2");
    
    // delete s2 diff in dir, subDir2, and subsubDir. Delete newFile, newDir,
    // and newFile2. Rename s2 diff to s1 for subDir1 
    checkQuotaUsageComputation(dir, 8, 0);
    // Check rename of snapshot diff in subDir1
    Path subdir1_s1 = SnapshotTestHelper.getSnapshotPath(dir, "s1",
        subDir1.getName());
    Path subdir1_s2 = SnapshotTestHelper.getSnapshotPath(dir, "s2",
        subDir1.getName());
    assertTrue(hdfs.exists(subdir1_s1));
    assertFalse(hdfs.exists(subdir1_s2));
  }
  
  /**
   * Test snapshot deletion
   * @param snapshotRoot The dir where the snapshots are created
   * @param modDirStr The snapshotRoot itself or one of its sub-directory, 
   *        where the modifications happen. It is represented as a relative 
   *        path to the snapshotRoot.
   */
  private void testCombineSnapshotDiffImpl(Path snapshotRoot, String modDirStr,
      int dirNodeNum) throws Exception {
    Path modDir = modDirStr.isEmpty() ? snapshotRoot : new Path(snapshotRoot,
        modDirStr);
    final int delta = modDirStr.isEmpty() ? 0 : 1;
    Path file10 = new Path(modDir, "file10");
    Path file11 = new Path(modDir, "file11");
    Path file12 = new Path(modDir, "file12");
    Path file13 = new Path(modDir, "file13");
    Path file14 = new Path(modDir, "file14");
    Path file15 = new Path(modDir, "file15");
    DFSTestUtil.createFile(hdfs, file10, BLOCKSIZE, REPLICATION_1, seed);
    DFSTestUtil.createFile(hdfs, file11, BLOCKSIZE, REPLICATION_1, seed);
    DFSTestUtil.createFile(hdfs, file12, BLOCKSIZE, REPLICATION_1, seed);
    DFSTestUtil.createFile(hdfs, file13, BLOCKSIZE, REPLICATION_1, seed);

    // create snapshot s1 for snapshotRoot
    SnapshotTestHelper.createSnapshot(hdfs, snapshotRoot, "s1");
    checkQuotaUsageComputation(snapshotRoot, dirNodeNum + 5, 8 * BLOCKSIZE);
    
    // delete file11
    hdfs.delete(file11, true);
    checkQuotaUsageComputation(snapshotRoot, dirNodeNum + 6 + delta,
        8 * BLOCKSIZE);
    
    // modify file12
    hdfs.setReplication(file12, REPLICATION);
    checkQuotaUsageComputation(snapshotRoot, dirNodeNum + 7 + delta,
        9 * BLOCKSIZE);
    
    // modify file13
    hdfs.setReplication(file13, REPLICATION);
    checkQuotaUsageComputation(snapshotRoot, dirNodeNum + 8 + delta,
        10 * BLOCKSIZE);
    
    // create file14
    DFSTestUtil.createFile(hdfs, file14, BLOCKSIZE, REPLICATION, seed);
    checkQuotaUsageComputation(snapshotRoot, dirNodeNum + 9 + delta,
        13 * BLOCKSIZE);
    
    // create file15
    DFSTestUtil.createFile(hdfs, file15, BLOCKSIZE, REPLICATION, seed);
    checkQuotaUsageComputation(snapshotRoot, dirNodeNum + 10 + delta,
        16 * BLOCKSIZE);
    
    // create snapshot s2 for snapshotRoot
    hdfs.createSnapshot(snapshotRoot, "s2");
    checkQuotaUsageComputation(snapshotRoot, dirNodeNum + 11 + delta,
        16 * BLOCKSIZE);
    
    // create file11 again: (0, d) + (c, 0)
    DFSTestUtil.createFile(hdfs, file11, BLOCKSIZE, REPLICATION, seed);
    checkQuotaUsageComputation(snapshotRoot, dirNodeNum + 12 + delta * 2,
        19 * BLOCKSIZE);
    
    // delete file12
    hdfs.delete(file12, true);
    checkQuotaUsageComputation(snapshotRoot, dirNodeNum + 13 + delta * 2,
        19 * BLOCKSIZE);
    
    // modify file13
    hdfs.setReplication(file13, (short) (REPLICATION - 2));
    checkQuotaUsageComputation(snapshotRoot, dirNodeNum + 14 + delta * 2,
        19 * BLOCKSIZE);
    
    // delete file14: (c, 0) + (0, d)
    hdfs.delete(file14, true);
    checkQuotaUsageComputation(snapshotRoot, dirNodeNum + 15 + delta * 2,
        19 * BLOCKSIZE);
    
    // modify file15
    hdfs.setReplication(file15, REPLICATION_1);
    checkQuotaUsageComputation(snapshotRoot, dirNodeNum + 16 + delta * 2,
        19 * BLOCKSIZE);
    
    // create snapshot s3 for snapshotRoot
    hdfs.createSnapshot(snapshotRoot, "s3");
    checkQuotaUsageComputation(snapshotRoot, dirNodeNum + 17 + delta * 2,
        19 * BLOCKSIZE);
    
    // modify file10, to check if the posterior diff was set correctly
    hdfs.setReplication(file10, REPLICATION);
    checkQuotaUsageComputation(snapshotRoot, dirNodeNum + 18 + delta * 2,
        20 * BLOCKSIZE);
    
    Path file10_s1 = SnapshotTestHelper.getSnapshotPath(snapshotRoot, "s1",
        modDirStr + "file10");
    Path file11_s1 = SnapshotTestHelper.getSnapshotPath(snapshotRoot, "s1",
        modDirStr + "file11");
    Path file12_s1 = SnapshotTestHelper.getSnapshotPath(snapshotRoot, "s1",
        modDirStr + "file12");
    Path file13_s1 = SnapshotTestHelper.getSnapshotPath(snapshotRoot, "s1",
        modDirStr + "file13");
    Path file14_s2 = SnapshotTestHelper.getSnapshotPath(snapshotRoot, "s2",
        modDirStr + "file14");
    Path file15_s2 = SnapshotTestHelper.getSnapshotPath(snapshotRoot, "s2",
        modDirStr + "file15");
    FileStatus statusBeforeDeletion10 = hdfs.getFileStatus(file10_s1);
    FileStatus statusBeforeDeletion11 = hdfs.getFileStatus(file11_s1);
    FileStatus statusBeforeDeletion12 = hdfs.getFileStatus(file12_s1);
    FileStatus statusBeforeDeletion13 = hdfs.getFileStatus(file13_s1);
    INodeFile file14Node = TestSnapshotBlocksMap.assertBlockCollection(
        file14_s2.toString(), 1, fsdir, blockmanager);
    BlockInfo[] blocks_14 = file14Node.getBlocks();
    TestSnapshotBlocksMap.assertBlockCollection(file15_s2.toString(), 1, fsdir,
        blockmanager);
    
    // delete s2, in which process we need to combine the diff in s2 to s1
    hdfs.deleteSnapshot(snapshotRoot, "s2");
    checkQuotaUsageComputation(snapshotRoot, dirNodeNum + 12 + delta,
        14 * BLOCKSIZE);
    
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
    TestSnapshotBlocksMap.assertBlockCollection(file10_s1.toString(), 1, fsdir,
        blockmanager);
    TestSnapshotBlocksMap.assertBlockCollection(file11_s1.toString(), 1, fsdir,
        blockmanager);
    TestSnapshotBlocksMap.assertBlockCollection(file12_s1.toString(), 1, fsdir,
        blockmanager);
    TestSnapshotBlocksMap.assertBlockCollection(file13_s1.toString(), 1, fsdir,
        blockmanager);
    
    // make sure file14 and file15 are not included in s1
    Path file14_s1 = SnapshotTestHelper.getSnapshotPath(snapshotRoot, "s1",
        modDirStr + "file14");
    Path file15_s1 = SnapshotTestHelper.getSnapshotPath(snapshotRoot, "s1",
        modDirStr + "file15");
    assertFalse(hdfs.exists(file14_s1));
    assertFalse(hdfs.exists(file15_s1));
    for (BlockInfo b : blocks_14) {
      assertNull(blockmanager.getBlockCollection(b));
    }
    
    INodeFile nodeFile13 = (INodeFile) fsdir.getINode(file13.toString());
    assertEquals(REPLICATION_1, nodeFile13.getBlockReplication());
    TestSnapshotBlocksMap.assertBlockCollection(file13.toString(), 1, fsdir,
        blockmanager);
    
    INodeFile nodeFile12 = (INodeFile) fsdir.getINode(file12_s1.toString());
    assertEquals(REPLICATION_1, nodeFile12.getBlockReplication());
  }
  
  /** Test deleting snapshots with modification on the metadata of directory */ 
  @Test (timeout=300000)
  public void testDeleteSnapshotWithDirModification() throws Exception {
    Path file = new Path(sub, "file");
    DFSTestUtil.createFile(hdfs, file, BLOCKSIZE, REPLICATION, seed);
    hdfs.setOwner(sub, "user1", "group1");
    
    // create snapshot s1 for sub1, and change the metadata of sub1
    SnapshotTestHelper.createSnapshot(hdfs, sub, "s1");
    checkQuotaUsageComputation(sub, 3, BLOCKSIZE * 3);
    hdfs.setOwner(sub, "user2", "group2");
    checkQuotaUsageComputation(sub, 3, BLOCKSIZE * 3);
    
    // create snapshot s2 for sub1, but do not modify sub1 afterwards
    hdfs.createSnapshot(sub, "s2");
    checkQuotaUsageComputation(sub, 4, BLOCKSIZE * 3);
    
    // create snapshot s3 for sub1, and change the metadata of sub1
    hdfs.createSnapshot(sub, "s3");
    checkQuotaUsageComputation(sub, 5, BLOCKSIZE * 3);
    hdfs.setOwner(sub, "user3", "group3");
    checkQuotaUsageComputation(sub, 5, BLOCKSIZE * 3);
    
    // delete snapshot s3
    hdfs.deleteSnapshot(sub, "s3");
    checkQuotaUsageComputation(sub, 4, BLOCKSIZE * 3);
    
    // check sub1's metadata in snapshot s2
    FileStatus statusOfS2 = hdfs.getFileStatus(new Path(sub,
        HdfsConstants.DOT_SNAPSHOT_DIR + "/s2"));
    assertEquals("user2", statusOfS2.getOwner());
    assertEquals("group2", statusOfS2.getGroup());
    
    // delete snapshot s2
    hdfs.deleteSnapshot(sub, "s2");
    checkQuotaUsageComputation(sub, 3, BLOCKSIZE * 3);
    
    // check sub1's metadata in snapshot s1
    FileStatus statusOfS1 = hdfs.getFileStatus(new Path(sub,
        HdfsConstants.DOT_SNAPSHOT_DIR + "/s1"));
    assertEquals("user1", statusOfS1.getOwner());
    assertEquals("group1", statusOfS1.getGroup());
  }

  @Test
  public void testDeleteSnapshotWithPermissionsDisabled() throws Exception {
    cluster.shutdown();
    Configuration newConf = new Configuration(conf);
    newConf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, false);
    cluster = new MiniDFSCluster.Builder(newConf).numDataNodes(0).build();
    cluster.waitActive();
    hdfs = cluster.getFileSystem();

    final Path path = new Path("/dir");
    hdfs.mkdirs(path);
    hdfs.allowSnapshot(path);
    hdfs.mkdirs(new Path(path, "/test"));
    hdfs.createSnapshot(path, "s1");
    UserGroupInformation anotherUser = UserGroupInformation
        .createRemoteUser("anotheruser");
    anotherUser.doAs(new PrivilegedAction<Object>() {
      @Override
      public Object run() {
        DistributedFileSystem anotherUserFS = null;
        try {
          anotherUserFS = cluster.getFileSystem();
          anotherUserFS.deleteSnapshot(path, "s1");
        } catch (IOException e) {
          fail("Failed to delete snapshot : " + e.getLocalizedMessage());
        } finally {
          IOUtils.closeStream(anotherUserFS);
        }
        return null;
      }
    });
  }

  /** 
   * A test covering the case where the snapshot diff to be deleted is renamed 
   * to its previous snapshot. 
   */
  @Test (timeout=300000)
  public void testRenameSnapshotDiff() throws Exception {
    cluster.getNamesystem().getSnapshotManager().setAllowNestedSnapshots(true);

    final Path subFile0 = new Path(sub, "file0");
    final Path subsubFile0 = new Path(subsub, "file0");
    DFSTestUtil.createFile(hdfs, subFile0, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, subsubFile0, BLOCKSIZE, REPLICATION, seed);
    hdfs.setOwner(subsub, "owner", "group");
    
    // create snapshot s0 on sub
    SnapshotTestHelper.createSnapshot(hdfs, sub, "s0");
    checkQuotaUsageComputation(sub, 5, BLOCKSIZE * 6);
    // make some changes on both sub and subsub
    final Path subFile1 = new Path(sub, "file1");
    final Path subsubFile1 = new Path(subsub, "file1");
    DFSTestUtil.createFile(hdfs, subFile1, BLOCKSIZE, REPLICATION_1, seed);
    DFSTestUtil.createFile(hdfs, subsubFile1, BLOCKSIZE, REPLICATION, seed);
    checkQuotaUsageComputation(sub, 8, BLOCKSIZE * 11);
    
    // create snapshot s1 on sub
    SnapshotTestHelper.createSnapshot(hdfs, sub, "s1");
    checkQuotaUsageComputation(sub, 9, BLOCKSIZE * 11);
    
    // create snapshot s2 on dir
    SnapshotTestHelper.createSnapshot(hdfs, dir, "s2");
    checkQuotaUsageComputation(dir, 11, BLOCKSIZE * 11);
    checkQuotaUsageComputation(sub, 9, BLOCKSIZE * 11);
    
    // make changes on subsub and subsubFile1
    hdfs.setOwner(subsub, "unknown", "unknown");
    hdfs.setReplication(subsubFile1, REPLICATION_1);
    checkQuotaUsageComputation(dir, 13, BLOCKSIZE * 11);
    checkQuotaUsageComputation(sub, 11, BLOCKSIZE * 11);
    
    // make changes on sub
    hdfs.delete(subFile1, true);
    checkQuotaUsageComputation(new Path("/"), 16, BLOCKSIZE * 11);
    checkQuotaUsageComputation(dir, 15, BLOCKSIZE * 11);
    checkQuotaUsageComputation(sub, 13, BLOCKSIZE * 11);
    
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
    checkQuotaUsageComputation(new Path("/"), 14, BLOCKSIZE * 11);
    checkQuotaUsageComputation(dir, 13, BLOCKSIZE * 11);
    checkQuotaUsageComputation(sub, 12, BLOCKSIZE * 11);
    
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
  
  @Test
  public void testDeleteSnapshotCommandWithIllegalArguments() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream psOut = new PrintStream(out);
    System.setOut(psOut);
    System.setErr(psOut);
    FsShell shell = new FsShell();
    shell.setConf(conf);
    
    String[] argv1 = {"-deleteSnapshot", "/tmp"};
    int val = shell.run(argv1);
    assertTrue(val == -1);
    assertTrue(out.toString().contains(
        argv1[0] + ": Incorrect number of arguments."));
    out.reset();
    
    String[] argv2 = {"-deleteSnapshot", "/tmp", "s1", "s2"};
    val = shell.run(argv2);
    assertTrue(val == -1);
    assertTrue(out.toString().contains(
        argv2[0] + ": Incorrect number of arguments."));
    psOut.close();
    out.close();
  }

  /*
   * OP_DELETE_SNAPSHOT edits op was not decrementing the safemode threshold on
   * restart in HA mode. HDFS-5504
   */
  @Test(timeout = 60000)
  public void testHANNRestartAfterSnapshotDeletion() throws Exception {
    hdfs.close();
    cluster.shutdown();
    conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology()).numDataNodes(1)
        .build();
    cluster.transitionToActive(0);
    // stop the standby namenode
    NameNode snn = cluster.getNameNode(1);
    snn.stop();

    hdfs = (DistributedFileSystem) HATestUtil
        .configureFailoverFs(cluster, conf);
    Path dir = new Path("/dir");
    Path subDir = new Path(dir, "sub");
    hdfs.mkdirs(dir);
    hdfs.allowSnapshot(dir);
    for (int i = 0; i < 5; i++) {
      DFSTestUtil.createFile(hdfs, new Path(subDir, "" + i), 100, (short) 1,
          1024L);
    }

    // take snapshot
    hdfs.createSnapshot(dir, "s0");

    // delete the subdir
    hdfs.delete(subDir, true);

    // roll the edit log
    NameNode ann = cluster.getNameNode(0);
    ann.getRpcServer().rollEditLog();

    hdfs.deleteSnapshot(dir, "s0");
    // wait for the blocks deletion at namenode
    Thread.sleep(2000);

    NameNodeAdapter.abortEditLogs(ann);
    cluster.restartNameNode(0, false);
    cluster.transitionToActive(0);

    // wait till the cluster becomes active
    cluster.waitClusterUp();
  }
}
