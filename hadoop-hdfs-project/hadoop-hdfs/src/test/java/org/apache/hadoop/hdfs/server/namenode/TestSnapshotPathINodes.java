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
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test snapshot related operations. */
public class TestSnapshotPathINodes {
  private static final long seed = 0;
  private static final short REPLICATION = 3;

  static private final Path dir = new Path("/TestSnapshot");
  
  static private final Path sub1 = new Path(dir, "sub1");
  static private final Path file1 = new Path(sub1, "file1");
  static private final Path file2 = new Path(sub1, "file2");

  static private MiniDFSCluster cluster;
  static private FSDirectory fsdir;

  static private DistributedFileSystem hdfs;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(REPLICATION)
      .build();
    cluster.waitActive();

    FSNamesystem fsn = cluster.getNamesystem();
    fsdir = fsn.getFSDirectory();
    
    hdfs = cluster.getFileSystem();
  }

  @Before
  public void reset() throws Exception {
    DFSTestUtil.createFile(hdfs, file1, 1024, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, file2, 1024, REPLICATION, seed);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /** Test allow-snapshot operation. */
  @Test (timeout=15000)
  public void testAllowSnapshot() throws Exception {
    final String pathStr = sub1.toString();
    final INode before = fsdir.getINode(pathStr);
    
    // Before a directory is snapshottable
    Assert.assertFalse(before.asDirectory().isSnapshottable());

    // After a directory is snapshottable
    final Path path = new Path(pathStr);
    hdfs.allowSnapshot(path);
    {
      final INode after = fsdir.getINode(pathStr);
      Assert.assertTrue(after.asDirectory().isSnapshottable());
    }
    
    hdfs.disallowSnapshot(path);
    {
      final INode after = fsdir.getINode(pathStr);
      Assert.assertFalse(after.asDirectory().isSnapshottable());
    }
  }
  
  static Snapshot getSnapshot(INodesInPath inodesInPath, String name,
      int index) {
    if (name == null) {
      return null;
    }
    final INode inode = inodesInPath.getINode(index - 1);
    return inode.asDirectory().getSnapshot(DFSUtil.string2Bytes(name));
  }

  static void assertSnapshot(INodesInPath inodesInPath, boolean isSnapshot,
      final Snapshot snapshot, int index) {
    assertEquals(isSnapshot, inodesInPath.isSnapshot());
    assertEquals(Snapshot.getSnapshotId(isSnapshot ? snapshot : null),
        inodesInPath.getPathSnapshotId());
    if (!isSnapshot) {
      assertEquals(Snapshot.getSnapshotId(snapshot),
          inodesInPath.getLatestSnapshotId());
    }
    if (isSnapshot && index >= 0) {
      assertEquals(Snapshot.Root.class, inodesInPath.getINode(index).getClass());
    }
  }

  static void assertINodeFile(INode inode, Path path) {
    assertEquals(path.getName(), inode.getLocalName());
    assertEquals(INodeFile.class, inode.getClass());
  }

  /** 
   * for normal (non-snapshot) file.
   */
  @Test (timeout=15000)
  public void testNonSnapshotPathINodes() throws Exception {
    // Get the inodes by resolving the path of a normal file
    byte[][] components = INode.getPathComponents(file1.toString());
    INodesInPath nodesInPath = INodesInPath.resolve(fsdir.rootDir,
        components, false);
    // The number of inodes should be equal to components.length
    assertEquals(nodesInPath.length(), components.length);
    // The returned nodesInPath should be non-snapshot
    assertSnapshot(nodesInPath, false, null, -1);

    // The last INode should be associated with file1
    assertTrue("file1=" + file1 + ", nodesInPath=" + nodesInPath,
        nodesInPath.getINode(components.length - 1) != null);
    assertEquals(nodesInPath.getINode(components.length - 1).getFullPathName(),
        file1.toString());
    assertEquals(nodesInPath.getINode(components.length - 2).getFullPathName(),
        sub1.toString());
    assertEquals(nodesInPath.getINode(components.length - 3).getFullPathName(),
        dir.toString());

    assertEquals(Path.SEPARATOR, nodesInPath.getPath(0));
    assertEquals(dir.toString(), nodesInPath.getPath(1));
    assertEquals(sub1.toString(), nodesInPath.getPath(2));
    assertEquals(file1.toString(), nodesInPath.getPath(3));

    nodesInPath = INodesInPath.resolve(fsdir.rootDir, components, false);
    assertEquals(nodesInPath.length(), components.length);
    assertSnapshot(nodesInPath, false, null, -1);
    assertEquals(nodesInPath.getLastINode().getFullPathName(), file1.toString());
  }
  
  /** 
   * for snapshot file.
   */
  @Test (timeout=15000)
  public void testSnapshotPathINodes() throws Exception {
    // Create a snapshot for the dir, and check the inodes for the path
    // pointing to a snapshot file
    hdfs.allowSnapshot(sub1);
    hdfs.createSnapshot(sub1, "s1");
    // The path when accessing the snapshot file of file1 is
    // /TestSnapshot/sub1/.snapshot/s1/file1
    String snapshotPath = sub1.toString() + "/.snapshot/s1/file1";
    byte[][] components = INode.getPathComponents(snapshotPath);
    INodesInPath nodesInPath = INodesInPath.resolve(fsdir.rootDir,
        components, false);
    // Length of inodes should be (components.length - 1), since we will ignore
    // ".snapshot" 
    assertEquals(nodesInPath.length(), components.length - 1);
    // SnapshotRootIndex should be 3: {root, Testsnapshot, sub1, s1, file1}
    final Snapshot snapshot = getSnapshot(nodesInPath, "s1", 3);
    assertSnapshot(nodesInPath, true, snapshot, 3);
    // Check the INode for file1 (snapshot file)
    INode snapshotFileNode = nodesInPath.getLastINode();
    assertINodeFile(snapshotFileNode, file1);
    assertTrue(snapshotFileNode.getParent().isWithSnapshot());
    
    // Call getExistingPathINodes and request only one INode.
    nodesInPath = INodesInPath.resolve(fsdir.rootDir, components, false);
    assertEquals(nodesInPath.length(), components.length - 1);
    assertSnapshot(nodesInPath, true, snapshot, 3);
    // Check the INode for file1 (snapshot file)
    assertINodeFile(nodesInPath.getLastINode(), file1);

    // Resolve the path "/TestSnapshot/sub1/.snapshot"  
    String dotSnapshotPath = sub1.toString() + "/.snapshot";
    components = INode.getPathComponents(dotSnapshotPath);
    nodesInPath = INodesInPath.resolve(fsdir.rootDir, components, false);
    // The number of INodes returned should still be components.length
    // since we put a null in the inode array for ".snapshot"
    assertEquals(nodesInPath.length(), components.length);

    // No SnapshotRoot dir is included in the resolved inodes  
    assertSnapshot(nodesInPath, true, snapshot, -1);
    // The last INode should be null, the last but 1 should be sub1
    assertNull(nodesInPath.getLastINode());
    assertEquals(nodesInPath.getINode(-2).getFullPathName(), sub1.toString());
    assertTrue(nodesInPath.getINode(-2).isDirectory());
    
    String[] invalidPathComponent = {"invalidDir", "foo", ".snapshot", "bar"};
    Path invalidPath = new Path(invalidPathComponent[0]);
    for(int i = 1; i < invalidPathComponent.length; i++) {
      invalidPath = new Path(invalidPath, invalidPathComponent[i]);
      try {
        hdfs.getFileStatus(invalidPath);
        Assert.fail();
      } catch(FileNotFoundException fnfe) {
        System.out.println("The exception is expected: " + fnfe);
      }
    }
    hdfs.deleteSnapshot(sub1, "s1");
    hdfs.disallowSnapshot(sub1);
  }
  
  /** 
   * for snapshot file after deleting the original file.
   */
  @Test (timeout=15000)
  public void testSnapshotPathINodesAfterDeletion() throws Exception {
    // Create a snapshot for the dir, and check the inodes for the path
    // pointing to a snapshot file
    hdfs.allowSnapshot(sub1);
    hdfs.createSnapshot(sub1, "s2");
    
    // Delete the original file /TestSnapshot/sub1/file1
    hdfs.delete(file1, false);
    
    final Snapshot snapshot;
    {
      // Resolve the path for the snapshot file
      // /TestSnapshot/sub1/.snapshot/s2/file1
      String snapshotPath = sub1.toString() + "/.snapshot/s2/file1";
      byte[][] components = INode.getPathComponents(snapshotPath);
      INodesInPath nodesInPath = INodesInPath.resolve(fsdir.rootDir,
          components, false);
      // Length of inodes should be (components.length - 1), since we will ignore
      // ".snapshot" 
      assertEquals(nodesInPath.length(), components.length - 1);
      // SnapshotRootIndex should be 3: {root, Testsnapshot, sub1, s2, file1}
      snapshot = getSnapshot(nodesInPath, "s2", 3);
      assertSnapshot(nodesInPath, true, snapshot, 3);
  
      // Check the INode for file1 (snapshot file)
      final INode inode = nodesInPath.getLastINode();
      assertEquals(file1.getName(), inode.getLocalName());
      assertTrue(inode.asFile().isWithSnapshot());
    }

    // Check the INodes for path /TestSnapshot/sub1/file1
    byte[][] components = INode.getPathComponents(file1.toString());
    INodesInPath nodesInPath = INodesInPath.resolve(fsdir.rootDir,
        components, false);
    // The length of inodes should be equal to components.length
    assertEquals(nodesInPath.length(), components.length);
    // The number of non-null elements should be components.length - 1 since
    // file1 has been deleted
    assertEquals(getNumNonNull(nodesInPath), components.length - 1);
    // The returned nodesInPath should be non-snapshot
    assertSnapshot(nodesInPath, false, snapshot, -1);
    // The last INode should be null, and the one before should be associated
    // with sub1
    assertNull(nodesInPath.getINode(components.length - 1));
    assertEquals(nodesInPath.getINode(components.length - 2).getFullPathName(),
        sub1.toString());
    assertEquals(nodesInPath.getINode(components.length - 3).getFullPathName(),
        dir.toString());
    hdfs.deleteSnapshot(sub1, "s2");
    hdfs.disallowSnapshot(sub1);
  }

  private int getNumNonNull(INodesInPath iip) {
    List<INode> inodes = iip.getReadOnlyINodes();
    for (int i = inodes.size() - 1; i >= 0; i--) {
      if (inodes.get(i) != null) {
        return i+1;
      }
    }
    return 0;
  }

  /**
   * for snapshot file while adding a new file after snapshot.
   */
  @Test (timeout=15000)
  public void testSnapshotPathINodesWithAddedFile() throws Exception {
    // Create a snapshot for the dir, and check the inodes for the path
    // pointing to a snapshot file
    hdfs.allowSnapshot(sub1);
    hdfs.createSnapshot(sub1, "s4");
    
    // Add a new file /TestSnapshot/sub1/file3
    final Path file3 = new Path(sub1, "file3");
    DFSTestUtil.createFile(hdfs, file3, 1024, REPLICATION, seed);

    Snapshot s4;
    {
      // Check the inodes for /TestSnapshot/sub1/.snapshot/s4/file3
      String snapshotPath = sub1.toString() + "/.snapshot/s4/file3";
      byte[][] components = INode.getPathComponents(snapshotPath);
      INodesInPath nodesInPath = INodesInPath.resolve(fsdir.rootDir,
          components, false);
      // Length of inodes should be (components.length - 1), since we will ignore
      // ".snapshot" 
      assertEquals(nodesInPath.length(), components.length - 1);
      // The number of non-null inodes should be components.length - 2, since
      // snapshot of file3 does not exist
      assertEquals(getNumNonNull(nodesInPath), components.length - 2);
      s4 = getSnapshot(nodesInPath, "s4", 3);

      // SnapshotRootIndex should still be 3: {root, Testsnapshot, sub1, s4, null}
      assertSnapshot(nodesInPath, true, s4, 3);
  
      // Check the last INode in inodes, which should be null
      assertNull(nodesInPath.getINode(nodesInPath.length() - 1));
    }

    // Check the inodes for /TestSnapshot/sub1/file3
    byte[][] components = INode.getPathComponents(file3.toString());
    INodesInPath nodesInPath = INodesInPath.resolve(fsdir.rootDir,
        components, false);
    // The number of inodes should be equal to components.length
    assertEquals(nodesInPath.length(), components.length);

    // The returned nodesInPath should be non-snapshot
    assertSnapshot(nodesInPath, false, s4, -1);

    // The last INode should be associated with file3
    assertEquals(nodesInPath.getINode(components.length - 1).getFullPathName(),
        file3.toString());
    assertEquals(nodesInPath.getINode(components.length - 2).getFullPathName(),
        sub1.toString());
    assertEquals(nodesInPath.getINode(components.length - 3).getFullPathName(),
        dir.toString());
    hdfs.deleteSnapshot(sub1, "s4");
    hdfs.disallowSnapshot(sub1);
  }
  
  /** 
   * for snapshot file while modifying file after snapshot.
   */
  @Test (timeout=15000)
  public void testSnapshotPathINodesAfterModification() throws Exception {
    // First check the INode for /TestSnapshot/sub1/file1
    byte[][] components = INode.getPathComponents(file1.toString());
    INodesInPath nodesInPath = INodesInPath.resolve(fsdir.rootDir,
        components, false);
    // The number of inodes should be equal to components.length
    assertEquals(nodesInPath.length(), components.length);

    // The last INode should be associated with file1
    assertEquals(nodesInPath.getINode(components.length - 1).getFullPathName(),
        file1.toString());
    // record the modification time of the inode
    final long modTime = nodesInPath.getINode(nodesInPath.length() - 1)
        .getModificationTime();
    
    // Create a snapshot for the dir, and check the inodes for the path
    // pointing to a snapshot file
    hdfs.allowSnapshot(sub1);
    hdfs.createSnapshot(sub1, "s3");
    
    // Modify file1
    DFSTestUtil.appendFile(hdfs, file1, "the content for appending");

    // Check the INodes for snapshot of file1
    String snapshotPath = sub1.toString() + "/.snapshot/s3/file1";
    components = INode.getPathComponents(snapshotPath);
    INodesInPath ssNodesInPath = INodesInPath.resolve(fsdir.rootDir,
        components, false);
    // Length of ssInodes should be (components.length - 1), since we will
    // ignore ".snapshot" 
    assertEquals(ssNodesInPath.length(), components.length - 1);
    final Snapshot s3 = getSnapshot(ssNodesInPath, "s3", 3);
    assertSnapshot(ssNodesInPath, true, s3, 3);
    // Check the INode for snapshot of file1
    INode snapshotFileNode = ssNodesInPath.getLastINode();
    assertEquals(snapshotFileNode.getLocalName(), file1.getName());
    assertTrue(snapshotFileNode.asFile().isWithSnapshot());
    // The modification time of the snapshot INode should be the same with the
    // original INode before modification
    assertEquals(modTime,
        snapshotFileNode.getModificationTime(ssNodesInPath.getPathSnapshotId()));

    // Check the INode for /TestSnapshot/sub1/file1 again
    components = INode.getPathComponents(file1.toString());
    INodesInPath newNodesInPath = INodesInPath.resolve(fsdir.rootDir,
        components, false);
    assertSnapshot(newNodesInPath, false, s3, -1);
    // The number of inodes should be equal to components.length
    assertEquals(newNodesInPath.length(), components.length);
    // The last INode should be associated with file1
    final int last = components.length - 1;
    assertEquals(newNodesInPath.getINode(last).getFullPathName(),
        file1.toString());
    // The modification time of the INode for file3 should have been changed
    Assert.assertFalse(modTime == newNodesInPath.getINode(last).getModificationTime());
    hdfs.deleteSnapshot(sub1, "s3");
    hdfs.disallowSnapshot(sub1);
  }
}
