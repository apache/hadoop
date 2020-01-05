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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeFile.HeaderFormat;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotTestHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test SwapBlockListOp working.
 */
public class TestSwapBlockList {

  private static final short REPLICATION = 3;

  private static final long SEED = 0;
  private final Path rootDir = new Path("/" + getClass().getSimpleName());

  private final Path subDir1 = new Path(rootDir, "dir1");
  private final Path file1 = new Path(subDir1, "file1");
  private final Path file2 = new Path(subDir1, "file2");

  private final Path subDir11 = new Path(subDir1, "dir11");
  private final Path file3 = new Path(subDir11, "file3");

  private final Path subDir2 = new Path(rootDir, "dir2");
  private final Path file4 = new Path(subDir2, "file4");

  private Configuration conf;
  private MiniDFSCluster cluster;
  private FSNamesystem fsn;
  private FSDirectory fsdir;

  private DistributedFileSystem hdfs;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY, 2);
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(REPLICATION)
        .build();
    cluster.waitActive();

    fsn = cluster.getNamesystem();
    fsdir = fsn.getFSDirectory();

    hdfs = cluster.getFileSystem();

    hdfs.mkdirs(subDir2);

    DFSTestUtil.createFile(hdfs, file1, 1024, REPLICATION, SEED);
    DFSTestUtil.createFile(hdfs, file2, 1024, REPLICATION, SEED);
    DFSTestUtil.createFile(hdfs, file3, 1024, REPLICATION, SEED);
    DFSTestUtil.createFile(hdfs, file4, 1024, REPLICATION, SEED);

  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testInputValidation() throws Exception {

    // Source file not found.
    try {
      fsn.swapBlockList("/TestSwapBlockList/dir1/fileXYZ",
          "/TestSwapBlockList/dir1/dir11/file3", 0L);
      Assert.fail();
    } catch (IOException ioEx) {
      Assert.assertTrue(ioEx instanceof FileNotFoundException);
      Assert.assertTrue(
          ioEx.getMessage().contains("/TestSwapBlockList/dir1/fileXYZ"));
    }

    // Destination file not found.
    try {
      fsn.swapBlockList("/TestSwapBlockList/dir1/file1",
          "/TestSwapBlockList/dir1/dir11/fileXYZ", 0L);
      Assert.fail();
    } catch (IOException ioEx) {
      Assert.assertTrue(ioEx instanceof FileNotFoundException);
      Assert.assertTrue(
          ioEx.getMessage().contains("/TestSwapBlockList/dir1/dir11/fileXYZ"));
    }

    // Source is Directory, not a file.
    try {
      fsn.swapBlockList("/TestSwapBlockList/dir1",
          "/TestSwapBlockList/dir1/dir11/file3", 0L);
      Assert.fail();
    } catch (IOException ioEx) {
      Assert.assertTrue(
          ioEx.getMessage().contains("/TestSwapBlockList/dir1 is not a file."));
    }

    String sourceFile = "/TestSwapBlockList/dir1/file1";
    String dstFile = "/TestSwapBlockList/dir1/dir11/file3";

    // Destination file is under construction.
    INodeFile dstInodeFile =
        (INodeFile) fsdir.resolvePath(fsdir.getPermissionChecker(),
        dstFile, FSDirectory.DirOp.WRITE).getLastINode();
    dstInodeFile.toUnderConstruction("TestClient", "TestClientMachine");
    try {
      fsn.swapBlockList(sourceFile, dstFile, 0L);
      Assert.fail();
    } catch (IOException ioEx) {
      Assert.assertTrue(
          ioEx.getMessage().contains(dstFile + " is under construction."));
    }

    // Check if parent directory is in snapshot.
    SnapshotTestHelper.createSnapshot(hdfs, subDir2, "s0");
    dstFile = "/TestSwapBlockList/dir2/file4";
    try {
      fsn.swapBlockList(sourceFile, dstFile, 0L);
      Assert.fail();
    } catch (IOException ioEx) {
      Assert.assertTrue(
          ioEx.getMessage().contains(dstFile + " is in a snapshot directory."));
    }

    // Check if gen timestamp validation works.
    dstFile = "/TestSwapBlockList/dir1/file2";
    dstInodeFile = (INodeFile) fsdir.resolvePath(fsdir.getPermissionChecker(),
            dstFile, FSDirectory.DirOp.WRITE).getLastINode();
    long genStamp = dstInodeFile.getLastBlock().getGenerationStamp();
    dstInodeFile.getLastBlock().setGenerationStamp(genStamp + 1);
    try {
      fsn.swapBlockList(sourceFile, dstFile, genStamp);
      Assert.fail();
    } catch (IOException ioEx) {
      Assert.assertTrue(
          ioEx.getMessage().contains(dstFile +
              " has last block with different gen timestamp."));
    }
  }

  @Test
  public void testSwapBlockListOp() throws Exception {
    String sourceFile = "/TestSwapBlockList/dir1/file1";
    String dstFile = "/TestSwapBlockList/dir1/dir11/file3";

    INodeFile srcInodeFile =
        (INodeFile) fsdir.resolvePath(fsdir.getPermissionChecker(),
            sourceFile, FSDirectory.DirOp.WRITE).getLastINode();
    INodeFile dstInodeFile =
        (INodeFile) fsdir.resolvePath(fsdir.getPermissionChecker(),
            dstFile, FSDirectory.DirOp.WRITE).getLastINode();

    BlockInfo[] srcBlockLocationsBeforeSwap = srcInodeFile.getBlocks();
    long srcHeader = srcInodeFile.getHeaderLong();

    BlockInfo[] dstBlockLocationsBeforeSwap = dstInodeFile.getBlocks();
    long dstHeader = dstInodeFile.getHeaderLong();

    fsn.swapBlockList(sourceFile, dstFile,
        dstInodeFile.getLastBlock().getGenerationStamp());
    assertBlockListEquality(dstBlockLocationsBeforeSwap,
        srcInodeFile.getBlocks(), srcInodeFile.getId());
    assertBlockListEquality(srcBlockLocationsBeforeSwap,
        dstInodeFile.getBlocks(), dstInodeFile.getId());

    // Assert Block Layout
    assertEquals(HeaderFormat.getBlockLayoutPolicy(srcHeader),
        HeaderFormat.getBlockLayoutPolicy(dstInodeFile.getHeaderLong()));
    assertEquals(HeaderFormat.getBlockLayoutPolicy(dstHeader),
        HeaderFormat.getBlockLayoutPolicy(srcInodeFile.getHeaderLong()));

    // Assert Storage policy
    assertEquals(HeaderFormat.getStoragePolicyID(srcHeader),
        HeaderFormat.getStoragePolicyID(dstInodeFile.getHeaderLong()));
    assertEquals(HeaderFormat.getStoragePolicyID(dstHeader),
        HeaderFormat.getStoragePolicyID(srcInodeFile.getHeaderLong()));
  }

  @Test
  public void testSwapBlockListOpRollback() throws Exception {
    // Invoke swap twice and make sure the blocks are back to their original
    // file.
    String sourceFile = "/TestSwapBlockList/dir1/file1";
    String dstFile = "/TestSwapBlockList/dir1/dir11/file3";

    INodeFile srcInodeFile =
        (INodeFile) fsdir.resolvePath(fsdir.getPermissionChecker(),
            sourceFile, FSDirectory.DirOp.WRITE).getLastINode();
    INodeFile dstInodeFile =
        (INodeFile) fsdir.resolvePath(fsdir.getPermissionChecker(),
            dstFile, FSDirectory.DirOp.WRITE).getLastINode();

    BlockInfo[] srcBlockLocationsBeforeSwap = srcInodeFile.getBlocks();
    long srcHeader = srcInodeFile.getHeaderLong();

    BlockInfo[] dstBlockLocationsBeforeSwap = dstInodeFile.getBlocks();
    long dstHeader = dstInodeFile.getHeaderLong();

    testSwapBlockListOp();
    testSwapBlockListOp();

    assertBlockListEquality(dstBlockLocationsBeforeSwap,
        dstInodeFile.getBlocks(), dstInodeFile.getId());
    assertBlockListEquality(srcBlockLocationsBeforeSwap,
        srcInodeFile.getBlocks(), srcInodeFile.getId());

    // Assert Block Layout
    assertEquals(HeaderFormat.getBlockLayoutPolicy(srcHeader),
        HeaderFormat.getBlockLayoutPolicy(srcInodeFile.getHeaderLong()));
    assertEquals(HeaderFormat.getBlockLayoutPolicy(dstHeader),
        HeaderFormat.getBlockLayoutPolicy(dstInodeFile.getHeaderLong()));
  }

  private void assertBlockListEquality(BlockInfo[] expected,
                                       BlockInfo[] actual,
                                       long expectedId) {
    assertEquals(expected.length, actual.length);
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i].getBlockId(), actual[i].getBlockId());
      assertEquals(expectedId, actual[i].getBlockCollectionId());
    }
  }
}
