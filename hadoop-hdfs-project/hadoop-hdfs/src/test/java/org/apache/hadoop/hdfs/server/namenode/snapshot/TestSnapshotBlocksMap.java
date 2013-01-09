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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
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
  protected DistributedFileSystem hdfs;
  
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .build();
    cluster.waitActive();

    fsn = cluster.getNamesystem();
    hdfs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  /**
   * Test deleting a file with snapshots. Need to check the blocksMap to make
   * sure the corresponding record is updated correctly.
   */
  @Test
  public void testDeletionWithSnapshots() throws Exception {
    Path file0 = new Path(sub1, "file0");
    Path file1 = new Path(sub1, "file1");
    
    Path subsub1 = new Path(sub1, "sub1");
    Path subfile0 = new Path(subsub1, "file0");
    
    // Create file under sub1
    DFSTestUtil.createFile(hdfs, file0, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, subfile0, BLOCKSIZE, REPLICATION, seed);
    
    BlockManager bm = fsn.getBlockManager();
    FSDirectory dir = fsn.getFSDirectory();
    
    INodeFile inodeForDeletedFile = INodeFile.valueOf(
        dir.getINode(subfile0.toString()), subfile0.toString());
    BlockInfo[] blocksForDeletedFile = inodeForDeletedFile.getBlocks();
    assertEquals(blocksForDeletedFile.length, 1);
    BlockCollection bcForDeletedFile = bm
        .getBlockCollection(blocksForDeletedFile[0]);
    assertNotNull(bcForDeletedFile);
    // Normal deletion
    hdfs.delete(subsub1, true);
    bcForDeletedFile = bm.getBlockCollection(blocksForDeletedFile[0]);
    // The INode should have been removed from the blocksMap
    assertNull(bcForDeletedFile);
    
    // Create snapshots for sub1
    for (int i = 0; i < 2; i++) {
      SnapshotTestHelper.createSnapshot(hdfs, sub1, "s" + i);
    }
    
    // Check the block information for file0
    // Get the INode for file0
    INodeFile inode = INodeFile.valueOf(dir.getINode(file0.toString()),
        file0.toString());
    BlockInfo[] blocks = inode.getBlocks();
    // Get the INode for the first block from blocksMap 
    BlockCollection bc = bm.getBlockCollection(blocks[0]);
    // The two INode should be the same one
    assertTrue(bc == inode);
    
    // Also check the block information for snapshot of file0
    Path snapshotFile0 = SnapshotTestHelper.getSnapshotPath(sub1, "s0",
        file0.getName());
    INodeFile ssINode0 = INodeFile.valueOf(dir.getINode(snapshotFile0.toString()),
        snapshotFile0.toString());
    BlockInfo[] ssBlocks = ssINode0.getBlocks();
    // The snapshot of file1 should contain 1 block
    assertEquals(1, ssBlocks.length);
    
    // Delete file0
    hdfs.delete(file0, true);
    // Make sure the first block of file0 is still in blocksMap
    BlockInfo blockInfoAfterDeletion = bm.getStoredBlock(blocks[0]);
    assertNotNull(blockInfoAfterDeletion);
    // Check the INode information
    BlockCollection bcAfterDeletion = blockInfoAfterDeletion
        .getBlockCollection();
    
    // Compare the INode in the blocksMap with INodes for snapshots
    Path snapshot1File0 = SnapshotTestHelper.getSnapshotPath(sub1, "s1",
        file0.getName());
    INodeFile ssINode1 = INodeFile.valueOf(
        dir.getINode(snapshot1File0.toString()), snapshot1File0.toString());
    assertTrue(bcAfterDeletion == ssINode0 || bcAfterDeletion == ssINode1);
    assertEquals(1, bcAfterDeletion.getBlocks().length);
    
    // Delete snapshot s1
    hdfs.deleteSnapshot(sub1, "s1");
    // Make sure the first block of file0 is still in blocksMap
    BlockInfo blockInfoAfterSnapshotDeletion = bm.getStoredBlock(blocks[0]);
    assertNotNull(blockInfoAfterSnapshotDeletion);
    BlockCollection bcAfterSnapshotDeletion = blockInfoAfterSnapshotDeletion
        .getBlockCollection();
    assertTrue(bcAfterSnapshotDeletion == ssINode0);
    assertEquals(1, bcAfterSnapshotDeletion.getBlocks().length);
    try {
      ssINode1 = INodeFile.valueOf(
        dir.getINode(snapshot1File0.toString()), snapshot1File0.toString());
      fail("Expect FileNotFoundException when identifying the INode in a deleted Snapshot");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("File does not exist: "
          + snapshot1File0.toString(), e);
    }
  }

  // TODO: test for deletion file which was appended after taking snapshots
  
}
