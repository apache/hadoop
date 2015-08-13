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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstructionStriped;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

import org.junit.Test;

/**
 * This class tests INodeFile with striped feature.
 */
public class TestStripedINodeFile {
  public static final Log LOG = LogFactory.getLog(TestINodeFile.class);

  private static final PermissionStatus perm = new PermissionStatus(
      "userName", null, FsPermission.getDefault());

  private final BlockStoragePolicySuite defaultSuite =
      BlockStoragePolicySuite.createDefaultSuite();
  private final BlockStoragePolicy defaultPolicy =
      defaultSuite.getDefaultPolicy();

  private static final ErasureCodingPolicy testECPolicy
      = ErasureCodingPolicyManager.getSystemDefaultPolicy();

  private static INodeFile createStripedINodeFile() {
    return new INodeFile(HdfsConstants.GRANDFATHER_INODE_ID, null, perm, 0L, 0L,
        null, (short)0, 1024L, HdfsServerConstants.COLD_STORAGE_POLICY_ID, true);
  }

  @Test
  public void testBlockStripedFeature()
      throws IOException, InterruptedException{
    INodeFile inf = createStripedINodeFile();
    assertTrue(inf.isStriped());
  }

  @Test
  public void testBlockStripedTotalBlockCount() {
    Block blk = new Block(1);
    BlockInfoStriped blockInfoStriped
        = new BlockInfoStriped(blk, testECPolicy);
    assertEquals(9, blockInfoStriped.getTotalBlockNum());
  }

  @Test
  public void testBlockStripedLength()
      throws IOException, InterruptedException {
    INodeFile inf = createStripedINodeFile();
    Block blk = new Block(1);
    BlockInfoStriped blockInfoStriped
        = new BlockInfoStriped(blk, testECPolicy);
    inf.addBlock(blockInfoStriped);
    assertEquals(1, inf.getBlocks().length);
  }

  @Test
  public void testBlockStripedConsumedSpace()
      throws IOException, InterruptedException {
    INodeFile inf = createStripedINodeFile();
    Block blk = new Block(1);
    BlockInfoStriped blockInfoStriped
        = new BlockInfoStriped(blk, testECPolicy);
    blockInfoStriped.setNumBytes(1);
    inf.addBlock(blockInfoStriped);
    //   0. Calculate the total bytes per stripes <Num Bytes per Stripes>
    //   1. Calculate the number of stripes in this block group. <Num Stripes>
    //   2. Calculate the last remaining length which does not make a stripe. <Last Stripe Length>
    //   3. Total consumed space is the total of
    //     a. The total of the full cells of data blocks and parity blocks.
    //     b. The remaining of data block which does not make a stripe.
    //     c. The last parity block cells. These size should be same
    //        to the first cell in this stripe.
    // So the total consumed space is the sum of
    //  a. <Cell Size> * (<Num Stripes> - 1) * <Total Block Num> = 0
    //  b. <Num Bytes> % <Num Bytes per Stripes> = 1
    //  c. <Last Stripe Length> * <Parity Block Num> = 1 * 3
    assertEquals(4, inf.storagespaceConsumedStriped().getStorageSpace());
    assertEquals(4, inf.storagespaceConsumed(defaultPolicy).getStorageSpace());
  }

  @Test
  public void testMultipleBlockStripedConsumedSpace()
      throws IOException, InterruptedException {
    INodeFile inf = createStripedINodeFile();
    Block blk1 = new Block(1);
    BlockInfoStriped blockInfoStriped1
        = new BlockInfoStriped(blk1, testECPolicy);
    blockInfoStriped1.setNumBytes(1);
    Block blk2 = new Block(2);
    BlockInfoStriped blockInfoStriped2
        = new BlockInfoStriped(blk2, testECPolicy);
    blockInfoStriped2.setNumBytes(1);
    inf.addBlock(blockInfoStriped1);
    inf.addBlock(blockInfoStriped2);
    // This is the double size of one block in above case.
    assertEquals(4 * 2, inf.storagespaceConsumedStriped().getStorageSpace());
    assertEquals(4 * 2, inf.storagespaceConsumed(defaultPolicy).getStorageSpace());
  }

  @Test
  public void testBlockStripedFileSize()
      throws IOException, InterruptedException {
    INodeFile inf = createStripedINodeFile();
    Block blk = new Block(1);
    BlockInfoStriped blockInfoStriped
        = new BlockInfoStriped(blk, testECPolicy);
    blockInfoStriped.setNumBytes(100);
    inf.addBlock(blockInfoStriped);
    // Compute file size should return actual data
    // size which is retained by this file.
    assertEquals(100, inf.computeFileSize());
    assertEquals(100, inf.computeFileSize(false, false));
  }

  @Test
  public void testBlockUCStripedFileSize()
      throws IOException, InterruptedException {
    INodeFile inf = createStripedINodeFile();
    Block blk = new Block(1);
    BlockInfoUnderConstructionStriped bInfoUCStriped
        = new BlockInfoUnderConstructionStriped(blk, testECPolicy);
    bInfoUCStriped.setNumBytes(100);
    inf.addBlock(bInfoUCStriped);
    assertEquals(100, inf.computeFileSize());
    assertEquals(0, inf.computeFileSize(false, false));
  }

  @Test
  public void testBlockStripedComputeQuotaUsage()
      throws IOException, InterruptedException {
    INodeFile inf = createStripedINodeFile();
    Block blk = new Block(1);
    BlockInfoStriped blockInfoStriped
        = new BlockInfoStriped(blk, testECPolicy);
    blockInfoStriped.setNumBytes(100);
    inf.addBlock(blockInfoStriped);

    QuotaCounts counts =
        inf.computeQuotaUsageWithStriped(defaultPolicy,
            new QuotaCounts.Builder().build());
    assertEquals(1, counts.getNameSpace());
    // The total consumed space is the sum of
    //  a. <Cell Size> * (<Num Stripes> - 1) * <Total Block Num> = 0
    //  b. <Num Bytes> % <Num Bytes per Stripes> = 100
    //  c. <Last Stripe Length> * <Parity Block Num> = 100 * 3
    assertEquals(400, counts.getStorageSpace());
  }

  @Test
  public void testBlockUCStripedComputeQuotaUsage()
      throws IOException, InterruptedException {
    INodeFile inf = createStripedINodeFile();
    Block blk = new Block(1);
    BlockInfoUnderConstructionStriped bInfoUCStriped
        = new BlockInfoUnderConstructionStriped(blk, testECPolicy);
    bInfoUCStriped.setNumBytes(100);
    inf.addBlock(bInfoUCStriped);

    QuotaCounts counts
        = inf.computeQuotaUsageWithStriped(defaultPolicy,
              new QuotaCounts.Builder().build());
    assertEquals(1024, inf.getPreferredBlockSize());
    assertEquals(1, counts.getNameSpace());
    // Consumed space in the case of BlockInfoUCStriped can be calculated
    // by using preferred block size. This is 1024 and total block num
    // is 9(= 3 + 6). Consumed storage space should be 1024 * 9 = 9216.
    assertEquals(9216, counts.getStorageSpace());
  }

  /**
   * Test the behavior of striped and contiguous block deletions.
   */
  @Test(timeout = 60000)
  public void testDeleteOp() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      final int len = 1024;
      final Path parentDir = new Path("/parentDir");
      final Path zone = new Path(parentDir, "zone");
      final Path zoneFile = new Path(zone, "zoneFile");
      final Path contiguousFile = new Path(parentDir, "someFile");
      final DistributedFileSystem dfs;
      final Configuration conf = new Configuration();
      final short GROUP_SIZE = HdfsConstants.NUM_DATA_BLOCKS
          + HdfsConstants.NUM_PARITY_BLOCKS;
      conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY, 2);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(GROUP_SIZE)
          .build();
      cluster.waitActive();

      FSNamesystem fsn = cluster.getNamesystem();
      dfs = cluster.getFileSystem();
      dfs.mkdirs(zone);

      // create erasure zone
      dfs.createErasureCodingZone(zone, null);
      DFSTestUtil.createFile(dfs, zoneFile, len, (short) 1, 0xFEED);
      DFSTestUtil.createFile(dfs, contiguousFile, len, (short) 1, 0xFEED);
      final FSDirectory fsd = fsn.getFSDirectory();

      // Case-1: Verify the behavior of striped blocks
      // Get blocks of striped file
      INode inodeStriped = fsd.getINode("/parentDir/zone/zoneFile");
      assertTrue("Failed to get INodeFile for /parentDir/zone/zoneFile",
          inodeStriped instanceof INodeFile);
      INodeFile inodeStripedFile = (INodeFile) inodeStriped;
      BlockInfo[] stripedBlks = inodeStripedFile.getBlocks();
      for (BlockInfo blockInfo : stripedBlks) {
        assertFalse("Mistakenly marked the block as deleted!",
            blockInfo.isDeleted());
      }

      // delete erasure zone directory
      dfs.delete(zone, true);
      for (BlockInfo blockInfo : stripedBlks) {
        assertTrue("Didn't mark the block as deleted!", blockInfo.isDeleted());
      }

      // Case-2: Verify the behavior of contiguous blocks
      // Get blocks of contiguous file
      INode inode = fsd.getINode("/parentDir/someFile");
      assertTrue("Failed to get INodeFile for /parentDir/someFile",
          inode instanceof INodeFile);
      INodeFile inodeFile = (INodeFile) inode;
      BlockInfo[] contiguousBlks = inodeFile.getBlocks();
      for (BlockInfo blockInfo : contiguousBlks) {
        assertFalse("Mistakenly marked the block as deleted!",
            blockInfo.isDeleted());
      }

      // delete parent directory
      dfs.delete(parentDir, true);
      for (BlockInfo blockInfo : contiguousBlks) {
        assertTrue("Didn't mark the block as deleted!", blockInfo.isDeleted());
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
