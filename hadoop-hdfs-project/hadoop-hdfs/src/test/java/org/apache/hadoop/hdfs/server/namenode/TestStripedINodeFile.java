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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStripedUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.io.erasurecode.ECSchema;

import org.junit.Test;

/**
 * This class tests INodeFile with striped feature.
 */
public class TestStripedINodeFile {
  public static final Log LOG = LogFactory.getLog(TestINodeFile.class);

  private static final PermissionStatus perm = new PermissionStatus(
      "userName", null, FsPermission.getDefault());

  private static INodeFile createStripedINodeFile() {
    return new INodeFile(INodeId.GRANDFATHER_INODE_ID, null, perm, 0L, 0L,
        null, (short)0, 1024L, HdfsConstants.COLD_STORAGE_POLICY_ID);
  }

  @Test
  public void testBlockStripedFeature()
      throws IOException, InterruptedException{
    INodeFile inf = createStripedINodeFile();
    inf.addStripedBlocksFeature();
    assertTrue(inf.isStriped());
  }

  @Test
  public void testBlockStripedTotalBlockCount() {
    ECSchema defaultSchema = ECSchemaManager.getSystemDefaultSchema();
    Block blk = new Block(1);
    BlockInfoStriped blockInfoStriped
        = new BlockInfoStriped(blk,
            (short)defaultSchema.getNumDataUnits(),
            (short)defaultSchema.getNumParityUnits());
    assertEquals(9, blockInfoStriped.getTotalBlockNum());
  }

  @Test
  public void testBlockStripedLength()
      throws IOException, InterruptedException {
    ECSchema defaultSchema = ECSchemaManager.getSystemDefaultSchema();
    INodeFile inf = createStripedINodeFile();
    inf.addStripedBlocksFeature();
    Block blk = new Block(1);
    BlockInfoStriped blockInfoStriped
        = new BlockInfoStriped(blk,
            (short)defaultSchema.getNumDataUnits(),
            (short)defaultSchema.getNumParityUnits());
    inf.addBlock(blockInfoStriped);
    assertEquals(1, inf.getBlocks().length);
  }

  @Test
  public void testBlockStripedConsumedSpace()
      throws IOException, InterruptedException {
    ECSchema defaultSchema = ECSchemaManager.getSystemDefaultSchema();
    INodeFile inf = createStripedINodeFile();
    inf.addStripedBlocksFeature();
    Block blk = new Block(1);
    BlockInfoStriped blockInfoStriped
        = new BlockInfoStriped(blk,
            (short)defaultSchema.getNumDataUnits(),
            (short)defaultSchema.getNumParityUnits());
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
    assertEquals(4, inf.storagespaceConsumedWithStriped());
    assertEquals(4, inf.storagespaceConsumed());
  }

  @Test
  public void testMultipleBlockStripedConsumedSpace()
      throws IOException, InterruptedException {
    ECSchema defaultSchema = ECSchemaManager.getSystemDefaultSchema();
    INodeFile inf = createStripedINodeFile();
    inf.addStripedBlocksFeature();
    Block blk1 = new Block(1);
    BlockInfoStriped blockInfoStriped1
        = new BlockInfoStriped(blk1,
            (short)defaultSchema.getNumDataUnits(),
            (short)defaultSchema.getNumParityUnits());
    blockInfoStriped1.setNumBytes(1);
    Block blk2 = new Block(2);
    BlockInfoStriped blockInfoStriped2
        = new BlockInfoStriped(blk2,
            (short)defaultSchema.getNumDataUnits(),
            (short)defaultSchema.getNumParityUnits());
    blockInfoStriped2.setNumBytes(1);
    inf.addBlock(blockInfoStriped1);
    inf.addBlock(blockInfoStriped2);
    // This is the double size of one block in above case.
    assertEquals(4 * 2, inf.storagespaceConsumedWithStriped());
    assertEquals(4 * 2, inf.storagespaceConsumed());
  }

  @Test
  public void testBlockStripedFileSize()
      throws IOException, InterruptedException {
    ECSchema defaultSchema = ECSchemaManager.getSystemDefaultSchema();
    INodeFile inf = createStripedINodeFile();
    inf.addStripedBlocksFeature();
    Block blk = new Block(1);
    BlockInfoStriped blockInfoStriped
        = new BlockInfoStriped(blk,
            (short)defaultSchema.getNumDataUnits(),
            (short)defaultSchema.getNumParityUnits());
    blockInfoStriped.setNumBytes(100);
    inf.addBlock(blockInfoStriped);
    // Compute file size should return actual data
    // size which is retained by this file.
    assertEquals(100, inf.computeFileSize());
    assertEquals(100, inf.computeFileSize(false, false));
  }

  @Test
  public void testBlockStripedUCFileSize()
      throws IOException, InterruptedException {
    ECSchema defaultSchema = ECSchemaManager.getSystemDefaultSchema();
    INodeFile inf = createStripedINodeFile();
    inf.addStripedBlocksFeature();
    Block blk = new Block(1);
    BlockInfoStripedUnderConstruction bInfoStripedUC
        = new BlockInfoStripedUnderConstruction(blk,
            (short)defaultSchema.getNumDataUnits(),
            (short)defaultSchema.getNumParityUnits());
    bInfoStripedUC.setNumBytes(100);
    inf.addBlock(bInfoStripedUC);
    assertEquals(100, inf.computeFileSize());
    assertEquals(0, inf.computeFileSize(false, false));
  }

  @Test
  public void testBlockStripedComputeQuotaUsage()
      throws IOException, InterruptedException {
    ECSchema defaultSchema = ECSchemaManager.getSystemDefaultSchema();
    INodeFile inf = createStripedINodeFile();
    inf.addStripedBlocksFeature();
    Block blk = new Block(1);
    BlockInfoStriped blockInfoStriped
        = new BlockInfoStriped(blk,
            (short)defaultSchema.getNumDataUnits(),
            (short)defaultSchema.getNumParityUnits());
    blockInfoStriped.setNumBytes(100);
    inf.addBlock(blockInfoStriped);

    BlockStoragePolicySuite suite =
        BlockStoragePolicySuite.createDefaultSuite();
    QuotaCounts counts =
        inf.computeQuotaUsageWithStriped(suite,
            new QuotaCounts.Builder().build());
    assertEquals(1, counts.getNameSpace());
    // The total consumed space is the sum of
    //  a. <Cell Size> * (<Num Stripes> - 1) * <Total Block Num> = 0
    //  b. <Num Bytes> % <Num Bytes per Stripes> = 100
    //  c. <Last Stripe Length> * <Parity Block Num> = 100 * 3
    assertEquals(400, counts.getStorageSpace());
  }

  @Test
  public void testBlockStripedUCComputeQuotaUsage()
      throws IOException, InterruptedException {
    ECSchema defaultSchema = ECSchemaManager.getSystemDefaultSchema();
    INodeFile inf = createStripedINodeFile();
    inf.addStripedBlocksFeature();
    Block blk = new Block(1);
    BlockInfoStripedUnderConstruction bInfoStripedUC
        = new BlockInfoStripedUnderConstruction(blk,
            (short)defaultSchema.getNumDataUnits(),
            (short)defaultSchema.getNumParityUnits());
    bInfoStripedUC.setNumBytes(100);
    inf.addBlock(bInfoStripedUC);

    BlockStoragePolicySuite suite
        = BlockStoragePolicySuite.createDefaultSuite();
    QuotaCounts counts
        = inf.computeQuotaUsageWithStriped(suite,
              new QuotaCounts.Builder().build());
    assertEquals(1024, inf.getPreferredBlockSize());
    assertEquals(1, counts.getNameSpace());
    // Consumed space in the case of BlockInfoStripedUC can be calculated
    // by using preferred block size. This is 1024 and total block num
    // is 9(= 3 + 6). Consumed storage space should be 1024 * 9 = 9216.
    assertEquals(9216, counts.getStorageSpace());
  }
}
