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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.DirectoryDiff;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Test for renaming snapshot
 */
public class TestSnapshotRename {

  static final long seed = 0;
  static final short REPLICATION = 3;
  static final long BLOCKSIZE = 1024;

  private final Path dir = new Path("/TestSnapshot");
  private final Path sub1 = new Path(dir, "sub1");
  private final Path file1 = new Path(sub1, "file1");
  
  Configuration conf;
  MiniDFSCluster cluster;
  FSNamesystem fsn;
  DistributedFileSystem hdfs;
  FSDirectory fsdir;
  
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    hdfs = cluster.getFileSystem();
    fsdir = fsn.getFSDirectory();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }
  
  @Rule
  public ExpectedException exception = ExpectedException.none();
  
  /**
   * Check the correctness of snapshot list within snapshottable dir
   */
  private void checkSnapshotList(INodeDirectory srcRoot,
      String[] sortedNames, String[] names) {
    assertTrue(srcRoot.isSnapshottable());
    ReadOnlyList<Snapshot> listByName = srcRoot
        .getDirectorySnapshottableFeature().getSnapshotList();
    assertEquals(sortedNames.length, listByName.size());
    for (int i = 0; i < listByName.size(); i++) {
      assertEquals(sortedNames[i], listByName.get(i).getRoot().getLocalName());
    }
    DiffList<DirectoryDiff> listByTime = srcRoot.getDiffs().asList();
    assertEquals(names.length, listByTime.size());
    for (int i = 0; i < listByTime.size(); i++) {
      Snapshot s = srcRoot.getDirectorySnapshottableFeature().getSnapshotById(
          listByTime.get(i).getSnapshotId());
      assertEquals(names[i], s.getRoot().getLocalName());
    }
  }
  
  /**
   * Rename snapshot(s), and check the correctness of the snapshot list within
   * {@link INodeDirectorySnapshottable}
   */
  @Test (timeout=60000)
  public void testSnapshotList() throws Exception {
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPLICATION, seed);
    // Create three snapshots for sub1
    SnapshotTestHelper.createSnapshot(hdfs, sub1, "s1");
    SnapshotTestHelper.createSnapshot(hdfs, sub1, "s2");
    SnapshotTestHelper.createSnapshot(hdfs, sub1, "s3");
    
    // Rename s3 to s22
    hdfs.renameSnapshot(sub1, "s3", "s22");
    // Check the snapshots list
    INodeDirectory srcRoot = fsdir.getINode(sub1.toString()).asDirectory();
    checkSnapshotList(srcRoot, new String[] { "s1", "s2", "s22" },
        new String[] { "s1", "s2", "s22" });
    
    // Rename s1 to s4
    hdfs.renameSnapshot(sub1, "s1", "s4");
    checkSnapshotList(srcRoot, new String[] { "s2", "s22", "s4" },
        new String[] { "s4", "s2", "s22" });
    
    // Rename s22 to s0
    hdfs.renameSnapshot(sub1, "s22", "s0");
    checkSnapshotList(srcRoot, new String[] { "s0", "s2", "s4" },
        new String[] { "s4", "s2", "s0" });
  }
  
  /**
   * Test FileStatus of snapshot file before/after rename
   */
  @Test (timeout=60000)
  public void testSnapshotRename() throws Exception {
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPLICATION, seed);
    // Create snapshot for sub1
    Path snapshotRoot = SnapshotTestHelper.createSnapshot(hdfs, sub1, "s1");
    Path ssPath = new Path(snapshotRoot, file1.getName());
    assertTrue(hdfs.exists(ssPath));
    FileStatus statusBeforeRename = hdfs.getFileStatus(ssPath);
    
    // Rename the snapshot
    hdfs.renameSnapshot(sub1, "s1", "s2");
    // <sub1>/.snapshot/s1/file1 should no longer exist
    assertFalse(hdfs.exists(ssPath));
    snapshotRoot = SnapshotTestHelper.getSnapshotRoot(sub1, "s2");
    ssPath = new Path(snapshotRoot, file1.getName());
    
    // Instead, <sub1>/.snapshot/s2/file1 should exist
    assertTrue(hdfs.exists(ssPath));
    FileStatus statusAfterRename = hdfs.getFileStatus(ssPath);
    
    // FileStatus of the snapshot should not change except the path
    assertFalse(statusBeforeRename.equals(statusAfterRename));
    statusBeforeRename.setPath(statusAfterRename.getPath());
    assertEquals(statusBeforeRename.toString(), statusAfterRename.toString());
  }
  
  /**
   * Test rename a non-existing snapshot
   */
  @Test (timeout=60000)
  public void testRenameNonExistingSnapshot() throws Exception {
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPLICATION, seed);
    // Create snapshot for sub1
    SnapshotTestHelper.createSnapshot(hdfs, sub1, "s1");
    
    exception.expect(SnapshotException.class);
    String error = "The snapshot wrongName does not exist for directory "
        + sub1.toString();
    exception.expectMessage(error);
    hdfs.renameSnapshot(sub1, "wrongName", "s2");
  }

  /**
   * Test rename a non-existing snapshot to itself.
   */
  @Test (timeout=60000)
  public void testRenameNonExistingSnapshotToItself() throws Exception {
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPLICATION, seed);
    // Create snapshot for sub1
    SnapshotTestHelper.createSnapshot(hdfs, sub1, "s1");

    exception.expect(SnapshotException.class);
    String error = "The snapshot wrongName does not exist for directory "
        + sub1.toString();
    exception.expectMessage(error);
    hdfs.renameSnapshot(sub1, "wrongName", "wrongName");
  }
  
  /**
   * Test rename a snapshot to another existing snapshot 
   */
  @Test (timeout=60000)
  public void testRenameToExistingSnapshot() throws Exception {
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPLICATION, seed);
    // Create snapshots for sub1
    SnapshotTestHelper.createSnapshot(hdfs, sub1, "s1");
    SnapshotTestHelper.createSnapshot(hdfs, sub1, "s2");
    
    exception.expect(SnapshotException.class);
    String error = "The snapshot s2 already exists for directory "
        + sub1.toString();
    exception.expectMessage(error);
    hdfs.renameSnapshot(sub1, "s1", "s2");
  }
  
  /**
   * Test renaming a snapshot with illegal name
   */
  @Test
  public void testRenameWithIllegalName() throws Exception {
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPLICATION, seed);
    // Create snapshots for sub1
    SnapshotTestHelper.createSnapshot(hdfs, sub1, "s1");
    
    final String name1 = HdfsConstants.DOT_SNAPSHOT_DIR;
    try {
      hdfs.renameSnapshot(sub1, "s1", name1);
      fail("Exception expected when an illegal name is given for rename");
    } catch (RemoteException e) {
      String errorMsg = "\"" + HdfsConstants.DOT_SNAPSHOT_DIR
          + "\" is a reserved name.";
      GenericTestUtils.assertExceptionContains(errorMsg, e);
    }
    
    String errorMsg = "Snapshot name cannot contain \"" + Path.SEPARATOR + "\"";
    final String[] badNames = new String[] { "foo" + Path.SEPARATOR,
        Path.SEPARATOR + "foo", Path.SEPARATOR, "foo" + Path.SEPARATOR + "bar" };
    for (String badName : badNames) {
      try {
        hdfs.renameSnapshot(sub1, "s1", badName);
        fail("Exception expected when an illegal name is given");
      } catch (RemoteException e) {
        GenericTestUtils.assertExceptionContains(errorMsg, e);
      }
    }
  }
  
  @Test
  public void testRenameSnapshotCommandWithIllegalArguments() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream psOut = new PrintStream(out);
    PrintStream oldOut = System.out;
    PrintStream oldErr = System.err;
    try {
      System.setOut(psOut);
      System.setErr(psOut);
      FsShell shell = new FsShell();
      shell.setConf(conf);

      String[] argv1 = { "-renameSnapshot", "/tmp", "s1" };
      int val = shell.run(argv1);
      assertTrue(val == -1);
      assertTrue(out.toString()
          .contains(argv1[0] + ": Incorrect number of arguments."));
      out.reset();

      String[] argv2 = { "-renameSnapshot", "/tmp", "s1", "s2", "s3" };
      val = shell.run(argv2);
      assertTrue(val == -1);
      assertTrue(out.toString()
          .contains(argv2[0] + ": Incorrect number of arguments."));
      psOut.close();
      out.close();
    } finally {
      System.setOut(oldOut);
      System.setErr(oldErr);
    }
  }
}
