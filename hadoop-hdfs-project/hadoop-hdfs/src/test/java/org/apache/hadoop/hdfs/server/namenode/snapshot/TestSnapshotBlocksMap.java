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

import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for snapshot-related information in blocksMap.
 */
public class TestSnapshotBlocksMap {
  private static final long seed = 0;
  private static final short REPLICATION = 3;
  private static final int BLOCKSIZE = 1024;
  
  private final Path dir = new Path("/TestSnapshot");
  private final Path sub1 = new Path(dir, "sub1");
  
  protected Configuration conf;
  protected MiniDFSCluster cluster;
  protected FSNamesystem fsn;
  FSDirectory fsdir;
  BlockManager blockmanager;
  protected DistributedFileSystem hdfs;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .build();
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

  void assertAllNull(INodeFile inode, Path path, String[] snapshots) throws Exception { 
    Assert.assertNull(inode.getBlocks());
    assertINodeNull(path.toString());
    assertINodeNullInSnapshots(path, snapshots);
  }

  void assertINodeNull(String path) throws Exception {
    Assert.assertNull(fsdir.getINode(path));
  }

  void assertINodeNullInSnapshots(Path path, String... snapshots) throws Exception {
    for(String s : snapshots) {
      assertINodeNull(SnapshotTestHelper.getSnapshotPath(
          path.getParent(), s, path.getName()).toString());
    }
  }

  static INodeFile assertBlockCollection(String path, int numBlocks,
     final FSDirectory dir, final BlockManager blkManager) throws Exception {
    final INodeFile file = INodeFile.valueOf(dir.getINode(path), path);
    assertEquals(numBlocks, file.getBlocks().length);
    for(BlockInfoContiguous b : file.getBlocks()) {
      assertBlockCollection(blkManager, file, b);
    }
    return file;
  }

  static void assertBlockCollection(final BlockManager blkManager,
      final INodeFile file, final BlockInfoContiguous b) {
    Assert.assertSame(b, blkManager.getStoredBlock(b));
    Assert.assertSame(file, blkManager.getBlockCollection(b));
    Assert.assertSame(file, b.getBlockCollection());
  }

  /**
   * Test deleting a file with snapshots. Need to check the blocksMap to make
   * sure the corresponding record is updated correctly.
   */
  @Test (timeout=60000)
  public void testDeletionWithSnapshots() throws Exception {
    Path file0 = new Path(sub1, "file0");
    Path file1 = new Path(sub1, "file1");
    
    Path sub2 = new Path(sub1, "sub2");
    Path file2 = new Path(sub2, "file2");

    Path file3 = new Path(sub1, "file3");
    Path file4 = new Path(sub1, "file4");
    Path file5 = new Path(sub1, "file5");
    
    // Create file under sub1
    DFSTestUtil.createFile(hdfs, file0, 4*BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, file1, 2*BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, file2, 3*BLOCKSIZE, REPLICATION, seed);
    
    // Normal deletion
    {
      final INodeFile f2 = assertBlockCollection(file2.toString(), 3, fsdir,
          blockmanager);
      BlockInfoContiguous[] blocks = f2.getBlocks();
      hdfs.delete(sub2, true);
      // The INode should have been removed from the blocksMap
      for(BlockInfoContiguous b : blocks) {
        assertNull(blockmanager.getBlockCollection(b));
      }
    }
    
    // Create snapshots for sub1
    final String[] snapshots = {"s0", "s1", "s2"};
    DFSTestUtil.createFile(hdfs, file3, 5*BLOCKSIZE, REPLICATION, seed);
    SnapshotTestHelper.createSnapshot(hdfs, sub1, snapshots[0]);
    DFSTestUtil.createFile(hdfs, file4, 1*BLOCKSIZE, REPLICATION, seed);
    SnapshotTestHelper.createSnapshot(hdfs, sub1, snapshots[1]);
    DFSTestUtil.createFile(hdfs, file5, 7*BLOCKSIZE, REPLICATION, seed);
    SnapshotTestHelper.createSnapshot(hdfs, sub1, snapshots[2]);

    // set replication so that the inode should be replaced for snapshots
    {
      INodeFile f1 = assertBlockCollection(file1.toString(), 2, fsdir,
          blockmanager);
      Assert.assertSame(INodeFile.class, f1.getClass());
      hdfs.setReplication(file1, (short)2);
      f1 = assertBlockCollection(file1.toString(), 2, fsdir, blockmanager);
      assertTrue(f1.isWithSnapshot());
      assertFalse(f1.isUnderConstruction());
    }
    
    // Check the block information for file0
    final INodeFile f0 = assertBlockCollection(file0.toString(), 4, fsdir,
        blockmanager);
    BlockInfoContiguous[] blocks0 = f0.getBlocks();
    
    // Also check the block information for snapshot of file0
    Path snapshotFile0 = SnapshotTestHelper.getSnapshotPath(sub1, "s0",
        file0.getName());
    assertBlockCollection(snapshotFile0.toString(), 4, fsdir, blockmanager);
    
    // Delete file0
    hdfs.delete(file0, true);
    // Make sure the blocks of file0 is still in blocksMap
    for(BlockInfoContiguous b : blocks0) {
      assertNotNull(blockmanager.getBlockCollection(b));
    }
    assertBlockCollection(snapshotFile0.toString(), 4, fsdir, blockmanager);
    
    // Compare the INode in the blocksMap with INodes for snapshots
    String s1f0 = SnapshotTestHelper.getSnapshotPath(sub1, "s1",
        file0.getName()).toString();
    assertBlockCollection(s1f0, 4, fsdir, blockmanager);
    
    // Delete snapshot s1
    hdfs.deleteSnapshot(sub1, "s1");

    // Make sure the first block of file0 is still in blocksMap
    for(BlockInfoContiguous b : blocks0) {
      assertNotNull(blockmanager.getBlockCollection(b));
    }
    assertBlockCollection(snapshotFile0.toString(), 4, fsdir, blockmanager);

    try {
      INodeFile.valueOf(fsdir.getINode(s1f0), s1f0);
      fail("Expect FileNotFoundException when identifying the INode in a deleted Snapshot");
    } catch (IOException e) {
      assertExceptionContains("File does not exist: " + s1f0, e);
    }
  }

  /*
   * Try to read the files inside snapshot but deleted in original place after
   * restarting post checkpoint. refer HDFS-5427
   */
  @Test(timeout = 30000)
  public void testReadSnapshotFileWithCheckpoint() throws Exception {
    Path foo = new Path("/foo");
    hdfs.mkdirs(foo);
    hdfs.allowSnapshot(foo);
    Path bar = new Path("/foo/bar");
    DFSTestUtil.createFile(hdfs, bar, 100, (short) 2, 100024L);
    hdfs.createSnapshot(foo, "s1");
    assertTrue(hdfs.delete(bar, true));

    // checkpoint
    NameNode nameNode = cluster.getNameNode();
    NameNodeAdapter.enterSafeMode(nameNode, false);
    NameNodeAdapter.saveNamespace(nameNode);
    NameNodeAdapter.leaveSafeMode(nameNode);

    // restart namenode to load snapshot files from fsimage
    cluster.restartNameNode(true);
    String snapshotPath = Snapshot.getSnapshotPath(foo.toString(), "s1/bar");
    DFSTestUtil.readFile(hdfs, new Path(snapshotPath));
  }

  /*
   * Try to read the files inside snapshot but renamed to different file and
   * deleted after restarting post checkpoint. refer HDFS-5427
   */
  @Test(timeout = 30000)
  public void testReadRenamedSnapshotFileWithCheckpoint() throws Exception {
    final Path foo = new Path("/foo");
    final Path foo2 = new Path("/foo2");
    hdfs.mkdirs(foo);
    hdfs.mkdirs(foo2);

    hdfs.allowSnapshot(foo);
    hdfs.allowSnapshot(foo2);
    final Path bar = new Path(foo, "bar");
    final Path bar2 = new Path(foo2, "bar");
    DFSTestUtil.createFile(hdfs, bar, 100, (short) 2, 100024L);
    hdfs.createSnapshot(foo, "s1");
    // rename to another snapshottable directory and take snapshot
    assertTrue(hdfs.rename(bar, bar2));
    hdfs.createSnapshot(foo2, "s2");
    // delete the original renamed file to make sure blocks are not updated by
    // the original file
    assertTrue(hdfs.delete(bar2, true));

    // checkpoint
    NameNode nameNode = cluster.getNameNode();
    NameNodeAdapter.enterSafeMode(nameNode, false);
    NameNodeAdapter.saveNamespace(nameNode);
    NameNodeAdapter.leaveSafeMode(nameNode);
    // restart namenode to load snapshot files from fsimage
    cluster.restartNameNode(true);
    // file in first snapshot
    String barSnapshotPath = Snapshot.getSnapshotPath(foo.toString(), "s1/bar");
    DFSTestUtil.readFile(hdfs, new Path(barSnapshotPath));
    // file in second snapshot after rename+delete
    String bar2SnapshotPath = Snapshot.getSnapshotPath(foo2.toString(),
        "s2/bar");
    DFSTestUtil.readFile(hdfs, new Path(bar2SnapshotPath));
  }

  /**
   * Make sure we delete 0-sized block when deleting an INodeFileUCWithSnapshot
   */
  @Test
  public void testDeletionWithZeroSizeBlock() throws Exception {
    final Path foo = new Path("/foo");
    final Path bar = new Path(foo, "bar");
    DFSTestUtil.createFile(hdfs, bar, BLOCKSIZE, REPLICATION, 0L);

    SnapshotTestHelper.createSnapshot(hdfs, foo, "s0");
    hdfs.append(bar);

    INodeFile barNode = fsdir.getINode4Write(bar.toString()).asFile();
    BlockInfoContiguous[] blks = barNode.getBlocks();
    assertEquals(1, blks.length);
    assertEquals(BLOCKSIZE, blks[0].getNumBytes());
    ExtendedBlock previous = new ExtendedBlock(fsn.getBlockPoolId(), blks[0]);
    cluster.getNameNodeRpc()
        .addBlock(bar.toString(), hdfs.getClient().getClientName(), previous,
            null, barNode.getId(), null);

    SnapshotTestHelper.createSnapshot(hdfs, foo, "s1");

    barNode = fsdir.getINode4Write(bar.toString()).asFile();
    blks = barNode.getBlocks();
    assertEquals(2, blks.length);
    assertEquals(BLOCKSIZE, blks[0].getNumBytes());
    assertEquals(0, blks[1].getNumBytes());

    hdfs.delete(bar, true);
    final Path sbar = SnapshotTestHelper.getSnapshotPath(foo, "s1",
        bar.getName());
    barNode = fsdir.getINode(sbar.toString()).asFile();
    blks = barNode.getBlocks();
    assertEquals(1, blks.length);
    assertEquals(BLOCKSIZE, blks[0].getNumBytes());
  }

  /**
   * Make sure we delete 0-sized block when deleting an under-construction file
   */
  @Test
  public void testDeletionWithZeroSizeBlock2() throws Exception {
    final Path foo = new Path("/foo");
    final Path subDir = new Path(foo, "sub");
    final Path bar = new Path(subDir, "bar");
    DFSTestUtil.createFile(hdfs, bar, BLOCKSIZE, REPLICATION, 0L);

    hdfs.append(bar);

    INodeFile barNode = fsdir.getINode4Write(bar.toString()).asFile();
    BlockInfoContiguous[] blks = barNode.getBlocks();
    assertEquals(1, blks.length);
    ExtendedBlock previous = new ExtendedBlock(fsn.getBlockPoolId(), blks[0]);
    cluster.getNameNodeRpc()
        .addBlock(bar.toString(), hdfs.getClient().getClientName(), previous,
            null, barNode.getId(), null);

    SnapshotTestHelper.createSnapshot(hdfs, foo, "s1");

    barNode = fsdir.getINode4Write(bar.toString()).asFile();
    blks = barNode.getBlocks();
    assertEquals(2, blks.length);
    assertEquals(BLOCKSIZE, blks[0].getNumBytes());
    assertEquals(0, blks[1].getNumBytes());

    hdfs.delete(subDir, true);
    final Path sbar = SnapshotTestHelper.getSnapshotPath(foo, "s1", "sub/bar");
    barNode = fsdir.getINode(sbar.toString()).asFile();
    blks = barNode.getBlocks();
    assertEquals(1, blks.length);
    assertEquals(BLOCKSIZE, blks[0].getNumBytes());
  }
  
  /**
   * 1. rename under-construction file with 0-sized blocks after snapshot.
   * 2. delete the renamed directory.
   * make sure we delete the 0-sized block.
   * see HDFS-5476.
   */
  @Test
  public void testDeletionWithZeroSizeBlock3() throws Exception {
    final Path foo = new Path("/foo");
    final Path subDir = new Path(foo, "sub");
    final Path bar = new Path(subDir, "bar");
    DFSTestUtil.createFile(hdfs, bar, BLOCKSIZE, REPLICATION, 0L);

    hdfs.append(bar);

    INodeFile barNode = fsdir.getINode4Write(bar.toString()).asFile();
    BlockInfoContiguous[] blks = barNode.getBlocks();
    assertEquals(1, blks.length);
    ExtendedBlock previous = new ExtendedBlock(fsn.getBlockPoolId(), blks[0]);
    cluster.getNameNodeRpc()
        .addBlock(bar.toString(), hdfs.getClient().getClientName(), previous,
            null, barNode.getId(), null);

    SnapshotTestHelper.createSnapshot(hdfs, foo, "s1");

    // rename bar
    final Path bar2 = new Path(subDir, "bar2");
    hdfs.rename(bar, bar2);
    
    INodeFile bar2Node = fsdir.getINode4Write(bar2.toString()).asFile();
    blks = bar2Node.getBlocks();
    assertEquals(2, blks.length);
    assertEquals(BLOCKSIZE, blks[0].getNumBytes());
    assertEquals(0, blks[1].getNumBytes());

    // delete subDir
    hdfs.delete(subDir, true);
    
    final Path sbar = SnapshotTestHelper.getSnapshotPath(foo, "s1", "sub/bar");
    barNode = fsdir.getINode(sbar.toString()).asFile();
    blks = barNode.getBlocks();
    assertEquals(1, blks.length);
    assertEquals(BLOCKSIZE, blks[0].getNumBytes());
  }
  
  /**
   * Make sure that a delete of a non-zero-length file which results in a
   * zero-length file in a snapshot works.
   */
  @Test
  public void testDeletionOfLaterBlocksWithZeroSizeFirstBlock() throws Exception {
    final Path foo = new Path("/foo");
    final Path bar = new Path(foo, "bar");
    final byte[] testData = "foo bar baz".getBytes();
    
    // Create a zero-length file.
    DFSTestUtil.createFile(hdfs, bar, 0, REPLICATION, 0L);
    assertEquals(0, fsdir.getINode4Write(bar.toString()).asFile().getBlocks().length);

    // Create a snapshot that includes that file.
    SnapshotTestHelper.createSnapshot(hdfs, foo, "s0");
    
    // Extend that file.
    FSDataOutputStream out = hdfs.append(bar);
    out.write(testData);
    out.close();
    INodeFile barNode = fsdir.getINode4Write(bar.toString()).asFile();
    BlockInfoContiguous[] blks = barNode.getBlocks();
    assertEquals(1, blks.length);
    assertEquals(testData.length, blks[0].getNumBytes());
    
    // Delete the file.
    hdfs.delete(bar, true);
    
    // Now make sure that the NN can still save an fsimage successfully.
    cluster.getNameNode().getRpcServer().setSafeMode(
        SafeModeAction.SAFEMODE_ENTER, false);
    cluster.getNameNode().getRpcServer().saveNamespace();
  }
}
