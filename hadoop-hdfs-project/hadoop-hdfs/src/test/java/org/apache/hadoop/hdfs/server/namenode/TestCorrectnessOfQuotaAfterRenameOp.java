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
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.HOT_STORAGE_POLICY_NAME;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.ONESSD_STORAGE_POLICY_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
  public void testRenameWithoutValidQuotaFeature() throws Exception {
    final int fileLen = 1024;
    final short replication = 3;
    final Path root = new Path("/testRoot");
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
    QuotaUsage quotaUsage1 = dfs.getQuotaUsage(new Path("/"));
    final Path dstDir1 = new Path(testParentDir2, "dst-dir");
    ContentSummary cs1 = dfs.getContentSummary(testParentDir1);

    // srcDir=/testRoot/testDir1/src-dir
    // dstDir=/testRoot/testDir2/dst-dir dstDir1 not exist
    boolean rename = dfs.rename(srcDir, dstDir1);
    assertTrue(rename);
    ContentSummary cs2 = dfs.getContentSummary(testParentDir2);
    assertEquals(cs1, cs2);

    QuotaUsage quotaUsage2 = dfs.getQuotaUsage(new Path("/"));
    assertEquals(quotaUsage1.getFileAndDirectoryCount(), quotaUsage2.getFileAndDirectoryCount());

    // 2. Test rename2
    final Path dstDir2 = new Path(testParentDir3, "dst-dir");
    assertTrue(dfs.mkdirs(dstDir2));
    long originDstDir2Usage = dfs.getQuotaUsage(dstDir2).getFileAndDirectoryCount();
    // The usage for covering the root dir should not include dstDir2 usage
    long rootINodeCount1 =
        dfs.getQuotaUsage(new Path("/")).getFileAndDirectoryCount() - originDstDir2Usage;
    ContentSummary cs3 = dfs.getContentSummary(testParentDir2);

    // Src and dst must be same (all file or all dir)
    // dstDir1=/testRoot/testDir3/dst-dir
    // dstDir2=/testRoot/testDir3/dst-dir
    dfs.rename(dstDir1, dstDir2, Options.Rename.OVERWRITE);
    long rootINodeCount2 = dfs.getQuotaUsage(new Path("/")).getFileAndDirectoryCount();
    assertEquals(rootINodeCount1, rootINodeCount2);
    ContentSummary cs4 = dfs.getContentSummary(testParentDir3);
    assertEquals(cs3, cs4);
  }
}
