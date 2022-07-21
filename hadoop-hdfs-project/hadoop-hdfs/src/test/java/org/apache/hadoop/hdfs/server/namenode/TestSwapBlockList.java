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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.namenode.INodeFile.HeaderFormat;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotTestHelper;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

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

  private final Path subDir3 = new Path(rootDir, "dir3");
  private final Path file5 = new Path(subDir3, "file5");

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
        .numDataNodes(9)
        .build();
    cluster.waitActive();

    fsn = cluster.getNamesystem();
    fsdir = fsn.getFSDirectory();

    hdfs = cluster.getFileSystem();

    hdfs.mkdirs(subDir2);
    hdfs.mkdirs(subDir3);

    DFSTestUtil.createFile(hdfs, file1, 1024, REPLICATION, SEED);
    DFSTestUtil.createFile(hdfs, file2, 1024, REPLICATION, SEED);
    DFSTestUtil.createFile(hdfs, file3, 1024, REPLICATION, SEED);
    DFSTestUtil.createFile(hdfs, file4, 1024, REPLICATION, SEED);
    fsn.enableErasureCodingPolicy(
        StripedFileTestUtil.getDefaultECPolicy().getName(), false);
    hdfs.getClient().setErasureCodingPolicy(subDir3.toString(),
        StripedFileTestUtil.getDefaultECPolicy().getName());
    DFSTestUtil.createStripedFile(cluster, file5, null,
          4, 4, false);
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
    LambdaTestUtils.intercept(FileNotFoundException.class,
        "/TestSwapBlockList/dir1/fileXYZ", () -> fsn.swapBlockList(
            "/TestSwapBlockList/dir1/fileXYZ", "/TestSwapBlockList/dir1/dir11" +
                "/file3", 0L, false));

    // Destination file not found.
    LambdaTestUtils.intercept(FileNotFoundException.class,
        "/TestSwapBlockList/dir1/dir11/fileXYZ",
        () -> fsn.swapBlockList("/TestSwapBlockList/dir1/file1",
            "/TestSwapBlockList/dir1/dir11/fileXYZ", 0L, false));

    // Source is Directory, not a file.
    LambdaTestUtils.intercept(IOException.class,
        "/TestSwapBlockList/dir1 is not a file.",
        () -> fsn.swapBlockList("/TestSwapBlockList/dir1",
            "/TestSwapBlockList/dir1/dir11/file3", 0L, false));

    String sourceFile = "/TestSwapBlockList/dir1/file1";
    String dstFile1 = "/TestSwapBlockList/dir1/dir11/file3";

    // Destination file is under construction.
    INodeFile dstInodeFile = fsdir.resolvePath(fsdir.getPermissionChecker(),
        dstFile1, FSDirectory.DirOp.WRITE).getLastINode().asFile();
    dstInodeFile.toUnderConstruction("TestClient", "TestClientMachine");
    LambdaTestUtils.intercept(IOException.class,
        dstFile1 + " is under construction.",
        () -> fsn.swapBlockList(sourceFile, dstFile1, 0L, false));

    // Check if parent directory is in snapshot.
    SnapshotTestHelper.createSnapshot(hdfs, subDir2, "s0");
    String dstFile2 = "/TestSwapBlockList/dir2/file4";
    LambdaTestUtils.intercept(IOException.class,
        dstFile2 + " is in a snapshot directory",
        () -> fsn.swapBlockList(sourceFile, dstFile2, 0L, false));

    // Check if gen timestamp validation works.
    String dstFile3 = "/TestSwapBlockList/dir1/file2";
    dstInodeFile = (INodeFile) fsdir.resolvePath(fsdir.getPermissionChecker(),
        dstFile3, FSDirectory.DirOp.WRITE).getLastINode();
    long genStamp = dstInodeFile.getLastBlock().getGenerationStamp();
    dstInodeFile.getLastBlock().setGenerationStamp(genStamp + 1);
    LambdaTestUtils.intercept(IOException.class,
        dstFile3 + " has last block with different gen timestamp.",
        () -> fsn.swapBlockList(sourceFile, dstFile3, genStamp, false));
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
        dstInodeFile.getLastBlock().getGenerationStamp(), false);
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

  @Test
  public void testSwapBlockListEditLog() throws Exception{
    // start a cluster
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(9)
          .build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      FSNamesystem fns = cluster.getNamesystem();
      FSDirectory fsd = fns.getFSDirectory();
      ErasureCodingPolicy testECPolicy = StripedFileTestUtil.getDefaultECPolicy();
      fns.enableErasureCodingPolicy(testECPolicy.getName(), false);

      final Path rootDir = new Path("/" + getClass().getSimpleName());
      Path srcRepDir =  new Path (rootDir,"dir_replica");
      Path dstECDir =  new Path (rootDir,"dir_ec");
      Path srcFile = new Path(srcRepDir, "file_1");
      Path dstFile = new Path(dstECDir, "file_2");

      fs.mkdirs(srcRepDir);
      fs.mkdirs(dstECDir);

      fs.getClient().setErasureCodingPolicy(dstECDir.toString(),
          testECPolicy.getName());

      DFSTestUtil.createFile(fs, srcFile, 1024, (short) 3, 1);

      DFSTestUtil.createStripedFile(cluster, dstFile, null,
          4, 4, false, testECPolicy);

      INodeFile srcInodeFile =
          (INodeFile) fsd.resolvePath(fsd.getPermissionChecker(),
              srcFile.toString(), FSDirectory.DirOp.WRITE).getLastINode();
      INodeFile dstInodeFile =
          (INodeFile) fsd.resolvePath(fsd.getPermissionChecker(),
              dstFile.toString(), FSDirectory.DirOp.WRITE).getLastINode();

      BlockInfo[] srcBlockLocationsBeforeSwap = srcInodeFile.getBlocks();
      long srcHeader = srcInodeFile.getHeaderLong();

      BlockInfo[] dstBlockLocationsBeforeSwap = dstInodeFile.getBlocks();
      long dstHeader = dstInodeFile.getHeaderLong();

      // swapBlockList srcRepFile dstECFile
      fns.swapBlockList(srcFile.toString(), dstFile.toString(),
          dstInodeFile.getLastBlock().getGenerationStamp(), false);

      // Assert Block Id
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

      //After the namenode restarts if the block by loaded is the same as above
      //(new block size and timestamp) it means that we have successfully
      //applied the edit log to the fsimage.
      cluster.restartNameNodes();
      cluster.waitActive();
      fns = cluster.getNamesystem();

      INodeFile srcInodeLoaded = (INodeFile)fns.getFSDirectory()
          .getINode(srcFile.toString());

      INodeFile dstInodeLoaded = (INodeFile)fns.getFSDirectory()
          .getINode(dstFile.toString());

      // Assert Block Id
      assertBlockListEquality(dstBlockLocationsBeforeSwap,
          srcInodeLoaded.getBlocks(), srcInodeLoaded.getId());
      assertBlockListEquality(srcBlockLocationsBeforeSwap,
          dstInodeLoaded.getBlocks(), dstInodeLoaded.getId());

      // Assert Block Layout
      assertEquals(HeaderFormat.getBlockLayoutPolicy(srcHeader),
          HeaderFormat.getBlockLayoutPolicy(dstInodeLoaded.getHeaderLong()));
      assertEquals(HeaderFormat.getBlockLayoutPolicy(dstHeader),
          HeaderFormat.getBlockLayoutPolicy(srcInodeLoaded.getHeaderLong()));

      // Assert Storage policy
      assertEquals(HeaderFormat.getStoragePolicyID(srcHeader),
          HeaderFormat.getStoragePolicyID(dstInodeLoaded.getHeaderLong()));
      assertEquals(HeaderFormat.getStoragePolicyID(dstHeader),
          HeaderFormat.getStoragePolicyID(srcInodeLoaded.getHeaderLong()));

      cluster.shutdown();
      cluster = null;
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
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
