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
    ContentSummary cs1 = dfs.getContentSummary(testParentDir1);
    // srcDir=/root/test1/src/dir
    // dstDir1=/root/test2/dst-dir dstDir1 not exist
    boolean rename = dfs.rename(srcDir, dstDir1);
    assertEquals(true, rename);
    ContentSummary cs2 = dfs.getContentSummary(testParentDir2);
    assertTrue(cs1.equals(cs2));


    final Path dstDir2 = new Path(testParentDir3, "dst-dir");
    assertTrue(dfs.mkdirs(dstDir2));
    ContentSummary cs3 = dfs.getContentSummary(testParentDir2);

    //Src and  dst must be same (all file or all dir)
    // dstDir1=/root/test2/dst-dir
    // dstDir2=/root/test3/dst-dir
    dfs.rename(dstDir1, dstDir2, Options.Rename.OVERWRITE);
    ContentSummary cs4 = dfs.getContentSummary(testParentDir3);
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
}
