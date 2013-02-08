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
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
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
  protected static final long BLOCKSIZE = 1024;
  public static final int SNAPSHOTNUMBER = 10;
  
  private final Path dir = new Path("/TestSnapshot");
  private final Path sub1 = new Path(dir, "sub1");
  private final Path subsub1 = new Path(sub1, "subsub1");
  
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
    Path file0 = new Path(sub1, "file0");
    Path file1 = new Path(sub1, "file1");
    DFSTestUtil.createFile(hdfs, file0, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPLICATION, seed);

    // Allow snapshot for sub1, and create snapshot for it
    hdfs.allowSnapshot(sub1.toString());
    hdfs.createSnapshot(sub1, "s1");

    // Deleting a snapshottable dir with snapshots should fail
    exception.expect(RemoteException.class);
    String error = "The direcotry " + sub1.toString()
        + " cannot be deleted since " + sub1.toString()
        + " is snapshottable and already has snapshots";
    exception.expectMessage(error);
    hdfs.delete(sub1, true);
  }
  
  /**
   * Deleting directory with snapshottable descendant with snapshots must fail.
   */
  @Test
  public void testDeleteDirectoryWithSnapshot2() throws Exception {
    Path file0 = new Path(sub1, "file0");
    Path file1 = new Path(sub1, "file1");
    DFSTestUtil.createFile(hdfs, file0, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPLICATION, seed);
    
    Path subfile1 = new Path(subsub1, "file0");
    Path subfile2 = new Path(subsub1, "file1");
    DFSTestUtil.createFile(hdfs, subfile1, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, subfile2, BLOCKSIZE, REPLICATION, seed);

    // Allow snapshot for subsub1, and create snapshot for it
    hdfs.allowSnapshot(subsub1.toString());
    hdfs.createSnapshot(subsub1, "s1");

    // Deleting dir while its descedant subsub1 having snapshots should fail
    exception.expect(RemoteException.class);
    String error = "The direcotry " + dir.toString()
        + " cannot be deleted since " + subsub1.toString()
        + " is snapshottable and already has snapshots";
    exception.expectMessage(error);
    hdfs.delete(dir, true);
  }
  
  /**
   * Test deleting the oldest (first) snapshot. We do not need to combine
   * snapshot diffs in this simplest scenario.
   */
  @Test
  public void testDeleteOldestSnapshot() throws Exception {
    // create files under sub1
    Path file0 = new Path(sub1, "file0");
    Path file1 = new Path(sub1, "file1");
    DFSTestUtil.createFile(hdfs, file0, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPLICATION, seed);
    
    String snapshotName = "s1";
    try {
      hdfs.deleteSnapshot(sub1, snapshotName);
      fail("SnapshotException expected: " + sub1.toString()
          + " is not snapshottable yet");
    } catch (Exception e) {
      GenericTestUtils.assertExceptionContains(
          "Directory is not a snapshottable directory: " + sub1, e);
    }
    
    // make sub1 snapshottable
    hdfs.allowSnapshot(sub1.toString());
    try {
      hdfs.deleteSnapshot(sub1, snapshotName);
      fail("SnapshotException expected: snapshot " + snapshotName
          + " does not exist for " + sub1.toString());
    } catch (Exception e) {
      GenericTestUtils.assertExceptionContains("Cannot delete snapshot "
          + snapshotName + " from path " + sub1.toString()
          + ": the snapshot does not exist.", e);
    }
    
    // create snapshot s1 for sub1
    hdfs.createSnapshot(sub1, snapshotName);
    // delete s1
    hdfs.deleteSnapshot(sub1, snapshotName);
    // now we can create a snapshot with the same name
    hdfs.createSnapshot(sub1, snapshotName);
    
    // create a new file under sub1
    Path newFile = new Path(sub1, "newFile");
    DFSTestUtil.createFile(hdfs, newFile, BLOCKSIZE, REPLICATION, seed);
    // create another snapshot s2
    String snapshotName2 = "s2";
    hdfs.createSnapshot(sub1, snapshotName2);
    // Get the filestatus of sub1 under snapshot s2
    Path ss = SnapshotTestHelper
        .getSnapshotPath(sub1, snapshotName2, "newFile");
    FileStatus statusBeforeDeletion = hdfs.getFileStatus(ss);
    // delete s1
    hdfs.deleteSnapshot(sub1, snapshotName);
    FileStatus statusAfterDeletion = hdfs.getFileStatus(ss);
    assertEquals(statusBeforeDeletion.toString(),
        statusAfterDeletion.toString());
  }
  
  /**
   * Test deleting snapshots in a more complicated scenario: need to combine
   * snapshot diffs, but no need to handle diffs distributed in a dir tree
   */
  @Test
  public void testDeleteSnapshot1() throws Exception {
    testDeleteSnapshot(sub1, "");
  }
  
  /**
   * Test deleting snapshots in more complicated scenarios (snapshot diffs are
   * distributed in the directory sub-tree)
   */
  @Test
  public void testDeleteSnapshot2() throws Exception {
    testDeleteSnapshot(sub1, "subsub1/subsubsub1/");
  }
  
  /**
   * Test snapshot deletion
   * @param snapshotRoot The dir where the snapshots are created
   * @param modDirStr The snapshotRoot itself or one of its sub-directory, 
   *        where the modifications happen. It is represented as a relative 
   *        path to the snapshotRoot.
   */
  private void testDeleteSnapshot(Path snapshotRoot, String modDirStr)
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
    hdfs.allowSnapshot(snapshotRoot.toString());
    hdfs.createSnapshot(snapshotRoot, "s1");
    
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
    // delete file12: (c, d) + (0, d)
    hdfs.delete(file12, true);
    // modify file13: (c, d) + (c, d)
    hdfs.setReplication(file13, (short) (REPLICATION - 2));
    // delete file14: (c, 0) + (0, d)
    hdfs.delete(file14, true);
    // modify file15: (c, 0) + (c, d)
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
    
    // call getBlockReplication, check circular list after snapshot deletion
    INodeFile nodeFile13 = (INodeFile)fsdir.getINode(file13.toString());
    assertEquals(REP_1, nodeFile13.getBlockReplication());

    INodeFile nodeFile12 = (INodeFile)fsdir.getINode(file12_s1.toString());
    assertEquals(REP_1, nodeFile12.getBlockReplication());
  }
  
  /** Test deleting snapshots with modification on the metadata of directory */ 
  @Test
  public void testDeleteSnapshotWithDirModification() throws Exception {
    Path file = new Path(sub1, "file");
    DFSTestUtil.createFile(hdfs, file, BLOCKSIZE, REPLICATION, seed);
    hdfs.setOwner(sub1, "user1", "group1");
    
    // create snapshot s1 for sub1, and change the metadata of sub1
    SnapshotTestHelper.createSnapshot(hdfs, sub1, "s1");
    hdfs.setOwner(sub1, "user2", "group2");
    
    // create snapshot s2 for sub1, but do not modify sub1 afterwards
    hdfs.createSnapshot(sub1, "s2");
    
    // create snapshot s3 for sub1, and change the metadata of sub1
    hdfs.createSnapshot(sub1, "s3");
    hdfs.setOwner(sub1, "user3", "group3");
    
    // delete snapshot s3
    hdfs.deleteSnapshot(sub1, "s3");
    // check sub1's metadata in snapshot s2
    FileStatus statusOfS2 = hdfs.getFileStatus(new Path(sub1,
        HdfsConstants.DOT_SNAPSHOT_DIR + "/s2"));
    assertEquals("user2", statusOfS2.getOwner());
    assertEquals("group2", statusOfS2.getGroup());
    
    // delete snapshot s2
    hdfs.deleteSnapshot(sub1, "s2");
    // check sub1's metadata in snapshot s1
    FileStatus statusOfS1 = hdfs.getFileStatus(new Path(sub1,
        HdfsConstants.DOT_SNAPSHOT_DIR + "/s1"));
    assertEquals("user1", statusOfS1.getOwner());
    assertEquals("group1", statusOfS1.getGroup());
  }
  
}
