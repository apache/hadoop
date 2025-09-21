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

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.HOT_STORAGE_POLICY_NAME;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.ONESSD_STORAGE_POLICY_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;

public class TestCorrectnessOfQuotaAfterRenameOp {
  private static MiniDFSCluster cluster;
  private static DistributedFileSystem dfs;

  @BeforeAll
  public static void setUp() throws IOException {
    HdfsConfiguration conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
  }

  @Test
  public void testQuotaUsageWhenRenameWithSameStoragePolicy() throws Exception {
    final int fileLen = 1024;
    final short replication = 3;
    final long spaceQuota = dfs.getClient().getConf().getDefaultBlockSize() * 10;
    final Path root = new Path(PathUtils.getTestDir(getClass()).getPath(),
        GenericTestUtils.getMethodName());
    assertTrue(dfs.mkdirs(root));
    // Init test dir
    Path testParentDir1 = new Path(root, "test1");
    assertTrue(dfs.mkdirs(testParentDir1));
    Path testParentDir2 = new Path(root, "test2");
    assertTrue(dfs.mkdirs(testParentDir2));
    Path testParentDir3 = new Path(root, "test3");
    assertTrue(dfs.mkdirs(testParentDir3));
    // Set quota to update quota cache when rename
    dfs.setQuota(testParentDir1, HdfsConstants.QUOTA_DONT_SET, spaceQuota);
    dfs.setQuota(testParentDir2, HdfsConstants.QUOTA_DONT_SET, spaceQuota);
    dfs.setQuota(testParentDir3, HdfsConstants.QUOTA_DONT_SET, spaceQuota);

    final Path srcDir = new Path(testParentDir1, "src-dir");
    Path file = new Path(srcDir, "file1");
    DFSTestUtil.createFile(dfs, file, fileLen, replication, 0);
    Path file2 = new Path(srcDir, "file2");
    DFSTestUtil.createFile(dfs, file2, fileLen, replication, 0);

    final Path dstDir1 = new Path(testParentDir2, "dst-dir");
    // If dstDir1 not exist, after the rename operation,
    // the root dir's quota usage should remain unchanged.
    QuotaUsage quotaUsage1 = dfs.getQuotaUsage(new Path("/"));
    ContentSummary cs1 = dfs.getContentSummary(testParentDir1);
    // srcDir=/root/test1/src/dir
    // dstDir1=/root/test2/dst-dir dstDir1 not exist
    boolean rename = dfs.rename(srcDir, dstDir1);
    assertEquals(true, rename);
    QuotaUsage quotaUsage2 = dfs.getQuotaUsage(new Path("/"));
    ContentSummary cs2 = dfs.getContentSummary(testParentDir2);
    assertEquals(quotaUsage1, quotaUsage2);
    assertTrue(cs1.equals(cs2));


    final Path dstDir2 = new Path(testParentDir3, "dst-dir");
    assertTrue(dfs.mkdirs(dstDir2));
    QuotaUsage quotaUsage3 = dfs.getQuotaUsage(testParentDir2);
    ContentSummary cs3 = dfs.getContentSummary(testParentDir2);

    //Src and  dst must be same (all file or all dir)
    // dstDir1=/root/test2/dst-dir
    // dstDir2=/root/test3/dst-dir
    dfs.rename(dstDir1, dstDir2, Options.Rename.OVERWRITE);
    QuotaUsage quotaUsage4 = dfs.getQuotaUsage(testParentDir3);
    ContentSummary cs4 = dfs.getContentSummary(testParentDir3);
    assertEquals(quotaUsage3, quotaUsage4);
    assertTrue(cs3.equals(cs4));
  }

  @Test
  public void testQuotaUsageWhenRenameWithDifferStoragePolicy() throws Exception {
    final int fileLen = 1024;
    final short replication = 3;
    final long spaceQuota = dfs.getClient().getConf().getDefaultBlockSize() * 10;
    final Path root = new Path(PathUtils.getTestDir(getClass()).getPath(),
        GenericTestUtils.getMethodName());
    assertTrue(dfs.mkdirs(root));

    // Init test dir
    Path testParentDir1 = new Path(root, "test1");
    assertTrue(dfs.mkdirs(testParentDir1));
    Path testParentDir2 = new Path(root, "test2");
    assertTrue(dfs.mkdirs(testParentDir2));

    final Path srcDir = new Path(testParentDir1, "src-dir");
    Path file = new Path(srcDir, "file1");
    DFSTestUtil.createFile(dfs, file, fileLen, replication, 0);
    Path file2 = new Path(srcDir, "file2");
    DFSTestUtil.createFile(dfs, file2, fileLen, replication, 0);

    // Set quota to update quota cache when rename
    dfs.setStoragePolicy(testParentDir1, HOT_STORAGE_POLICY_NAME);
    dfs.setQuota(testParentDir1, HdfsConstants.QUOTA_DONT_SET, spaceQuota);
    dfs.setStoragePolicy(testParentDir2, ONESSD_STORAGE_POLICY_NAME);
    dfs.setQuota(testParentDir2, HdfsConstants.QUOTA_DONT_SET, spaceQuota);


    final Path dstDir1 = new Path(testParentDir2, "dst-dir");
    assertTrue(dfs.mkdirs(dstDir1));

    FSNamesystem namesystem = cluster.getNameNode().getNamesystem();
    BlockStoragePolicySuite bsps = namesystem.getBlockManager().getStoragePolicySuite();
    INodesInPath iipSrc = namesystem.getFSDirectory().resolvePath(
        null, srcDir.toString(), FSDirectory.DirOp.READ);
    INodesInPath iipDst = namesystem.getFSDirectory().resolvePath(
        null, dstDir1.toString(), FSDirectory.DirOp.READ);

    // Src`s quotaCounts with dst storage policy
    QuotaCounts srcCounts = iipSrc.getLastINode().computeQuotaUsage(bsps,
        iipDst.getLastINode().getStoragePolicyID(),
        false, Snapshot.CURRENT_STATE_ID);


    QuotaCounts dstCountsBeforeRename = iipDst.getLastINode().
        computeQuotaUsage(bsps, iipDst.getLastINode().getStoragePolicyID(),
        false, Snapshot.CURRENT_STATE_ID);

    boolean rename = dfs.rename(srcDir, dstDir1);
    assertEquals(true, rename);

    QuotaCounts dstCountsAfterRename = iipDst.getLastINode().
        computeQuotaUsage(bsps, iipDst.getLastINode().getStoragePolicyID(),
        false, Snapshot.CURRENT_STATE_ID);

    QuotaCounts subtract = dstCountsAfterRename.subtract(dstCountsBeforeRename);
    assertTrue(subtract.equals(srcCounts));
  }

  @Test
  public void testRenameWithoutValidFeature() throws Exception {
    final int fileLen = 1024;
    final short replication = 3;
    final Path root = new Path("/testRename");
    assertTrue(dfs.mkdirs(root));

    Path testParentDir1 = new Path(root, "testDir1");
    assertTrue(dfs.mkdirs(testParentDir1));
    Path testParentDir2 = new Path(root, "testDir2");
    assertTrue(dfs.mkdirs(testParentDir2));
    Path testParentDir3 = new Path(root, "testDir3");
    assertTrue(dfs.mkdirs(testParentDir3));

    final Path srcDir = new Path(testParentDir1, "src-dir");
    for (int i = 0; i < 2; i++) {
      Path file1 = new Path(srcDir, "file" + i);
      DFSTestUtil.createFile(dfs, file1, fileLen, replication, 0);
    }

    // 1. Test rename1
    ContentSummary rootContentSummary1 = dfs.getContentSummary(new Path("/"));
    QuotaUsage rootQuotaUsage1 = dfs.getQuotaUsage(new Path("/"));
    ContentSummary contentSummary1 = dfs.getContentSummary(testParentDir1);
    // srcDir=/testRename/testDir1/src-dir
    // dstDir=/testRename/testDir2/dst-dir dstDir not exist
    final Path dstDir2 = new Path(testParentDir2, "dst-dir");
    assertTrue(dfs.rename(srcDir, dstDir2));
    ContentSummary contentSummary2 = dfs.getContentSummary(testParentDir2);
    assertEquals(contentSummary1, contentSummary2);
    QuotaUsage rootQuotaUsage2 = dfs.getQuotaUsage(new Path("/"));
    assertEquals(rootQuotaUsage1.getFileAndDirectoryCount(),
        rootQuotaUsage2.getFileAndDirectoryCount());
    // The return values of the getContentSummary() and getQuotaUsage() should be consistent
    assertEquals(rootContentSummary1.getFileAndDirectoryCount(),
        rootQuotaUsage2.getFileAndDirectoryCount());

    // 2. Test rename2
    final Path dstDir3 = new Path(testParentDir3, "dst-dir");
    assertTrue(dfs.mkdirs(dstDir3));
    long originDstDirUsage = dfs.getQuotaUsage(dstDir3).getFileAndDirectoryCount();
    // Overwrite the rename destination, the usage of dstDir3 should be excluded
    long expectedCount =
        dfs.getQuotaUsage(new Path("/")).getFileAndDirectoryCount() - originDstDirUsage;
    ContentSummary contentSummary3 = dfs.getContentSummary(testParentDir2);
    // Src and dst must be same
    // dstDir2=/testRename/testDir2/dst-dir
    // dstDir3=/testRename/testDir3/dst-dir
    dfs.rename(dstDir2, dstDir3, Options.Rename.OVERWRITE);
    long actualCount = dfs.getQuotaUsage(new Path("/")).getFileAndDirectoryCount();
    assertEquals(expectedCount, actualCount);
    ContentSummary contentSummary4 = dfs.getContentSummary(testParentDir3);
    assertEquals(contentSummary3, contentSummary4);
  }

  @Test
  public void testRenameUndoWithoutValidFeature() throws Exception {
    final int fileLen = 1024;
    final short replication = 3;
    final Path root = new Path("/testRenameUndo");
    assertTrue(dfs.mkdirs(root));

    Path testParentDir1 = new Path(root, "testDir1");
    assertTrue(dfs.mkdirs(testParentDir1));
    Path testParentDir2 = new Path(root, "testDir2");
    assertTrue(dfs.mkdirs(testParentDir2));
    Path testParentDir3 = new Path(root, "testDir3");
    assertTrue(dfs.mkdirs(testParentDir3));
    Path testParentDir4 = new Path(root, "testDir4");
    assertTrue(dfs.mkdirs(testParentDir4));

    final Path srcDir1 = new Path(testParentDir1, "src-dir");
    for (int i = 0; i < 2; i++) {
      Path file1 = new Path(srcDir1, "file" + i);
      DFSTestUtil.createFile(dfs, file1, fileLen, replication, 0);
    }

    final Path srcDir3 = new Path(testParentDir3, "src-dir");
    for (int i = 0; i < 2; i++) {
      Path file1 = new Path(srcDir3, "file" + i);
      DFSTestUtil.createFile(dfs, file1, fileLen, replication, 0);
    }

    // Test rename1
    ContentSummary rootContentSummary1 = dfs.getContentSummary(new Path("/"));
    QuotaUsage rootQuotaUsage1 = dfs.getQuotaUsage(new Path("/"));
    ContentSummary contentSummary1 = dfs.getContentSummary(testParentDir1);

    FSNamesystem fsn = cluster.getNamesystem();
    FSDirectory fsDirectory = fsn.getFSDirectory();

    // Replace INode, expected addChild return false
    INodeDirectory dir = fsDirectory.getINode4Write(testParentDir2.toString()).asDirectory();
    INodeDirectory mockDir = Mockito.spy(dir);
    INode srcInode = fsDirectory.getINode(srcDir3.toString());
    // Fail the rename but succeed in undo
    Mockito.doReturn(false).when(mockDir).addChild(Mockito.eq(srcInode), anyBoolean(), anyInt());
    INodeDirectory rootDir = fsDirectory.getINode4Write(root.toString()).asDirectory();
    rootDir.replaceChild(dir, mockDir, fsDirectory.getINodeMap());
    mockDir.setParent(rootDir);

    // srcDir=/testRenameUndo/testDir1/src-dir
    // dstDir=/testRenameUndo/testDir2/
    assertFalse(dfs.rename(srcDir3, testParentDir2));

    ContentSummary rootContentSummary2 = dfs.getContentSummary(new Path("/"));
    QuotaUsage rootQuotaUsage2 = dfs.getQuotaUsage(new Path("/"));
    ContentSummary contentSummary2 = dfs.getContentSummary(testParentDir1);
    assertEquals(rootContentSummary1.toString(), rootContentSummary2.toString());
    assertEquals(rootQuotaUsage1.toString(), rootQuotaUsage2.toString());
    assertEquals(contentSummary1.toString(), contentSummary2.toString());
    assertEquals(rootContentSummary1.getFileAndDirectoryCount(),
        rootQuotaUsage2.getFileAndDirectoryCount());

    // Test rename2
    final Path dstDir4 = new Path(testParentDir4, "src-dir");
    assertTrue(dfs.mkdirs(dstDir4));
    ContentSummary rootContentSummary3 = dfs.getContentSummary(new Path("/"));
    QuotaUsage rootQuotaUsage3 = dfs.getQuotaUsage(new Path("/"));
    ContentSummary contentSummary3 = dfs.getContentSummary(testParentDir3);

    // Replace INode, expected addChild return false
    INodeDirectory dir4 = fsDirectory.getINode4Write(testParentDir4.toString()).asDirectory();
    INodeDirectory mockDir4 = Mockito.spy(dir4);
    INode srcInode3 = fsDirectory.getINode(srcDir3.toString());
    Mockito.doReturn(false).when(mockDir4).addChild(Mockito.eq(srcInode3), anyBoolean(), anyInt());
    rootDir.replaceChild(dir4, mockDir4, fsDirectory.getINodeMap());
    mockDir4.setParent(rootDir);

    // srcDir=/testRenameUndo/testDir3/src-dir
    // dstDir=/testRenameUndo/testDir4/src-dir dstDir exist
    assertThrows(RemoteException.class,
        () -> dfs.rename(srcDir3, dstDir4, Options.Rename.OVERWRITE));

    ContentSummary rootContentSummary4 = dfs.getContentSummary(new Path("/"));
    QuotaUsage rootQuotaUsage4 = dfs.getQuotaUsage(new Path("/"));
    ContentSummary contentSummary4 = dfs.getContentSummary(testParentDir3);
    assertEquals(rootContentSummary3.toString(), rootContentSummary4.toString());
    assertEquals(rootQuotaUsage3.toString(), rootQuotaUsage4.toString());
    assertEquals(contentSummary3.toString(), contentSummary4.toString());
    assertEquals(rootContentSummary3.getFileAndDirectoryCount(),
        rootQuotaUsage4.getFileAndDirectoryCount());
  }

  @Test
  public void testRenameFileInSnapshotDirWithoutValidFeature() throws Exception {
    final int fileLen = 1024;
    final short replication = 3;
    final Path root = new Path("/testRenameFileInSnapshotDir");
    assertTrue(dfs.mkdirs(root));

    Path testParentDir1 = new Path(root, "testDir1");
    assertTrue(dfs.mkdirs(testParentDir1));
    Path file = new Path(testParentDir1, "file1");
    DFSTestUtil.createFile(dfs, file, fileLen, replication, 0);
    dfs.allowSnapshot(testParentDir1);
    dfs.createSnapshot(testParentDir1, "snapshot1");

    Path testParentDir2 = new Path(root, "testDir2");
    assertTrue(dfs.mkdirs(testParentDir2));

    ContentSummary contentSummary1 = dfs.getContentSummary(new Path("/"));
    QuotaUsage quotaUsage1 = dfs.getQuotaUsage(new Path("/"));
    assertEquals(contentSummary1.getSpaceConsumed(), quotaUsage1.getSpaceConsumed());
    assertEquals(contentSummary1.getFileAndDirectoryCount(),
        quotaUsage1.getFileAndDirectoryCount());

    // The snapshot of file1 not be cleaned up
    assertTrue(dfs.rename(new Path(testParentDir1, "file1"), testParentDir2));

    ContentSummary contentSummary2 = dfs.getContentSummary(new Path("/"));
    QuotaUsage quotaUsage2 = dfs.getQuotaUsage(new Path("/"));
    assertEquals(quotaUsage1.getFileAndDirectoryCount() + 1,
        quotaUsage2.getFileAndDirectoryCount());
    assertEquals(quotaUsage1.getSpaceConsumed() + fileLen * replication,
        quotaUsage2.getSpaceConsumed());
    // Root directory's quota usage must match actual capacity
    assertEquals(contentSummary2.getFileAndDirectoryCount(),
        quotaUsage2.getFileAndDirectoryCount());
    assertEquals(contentSummary2.getSpaceConsumed(), quotaUsage2.getSpaceConsumed());
  }
}
