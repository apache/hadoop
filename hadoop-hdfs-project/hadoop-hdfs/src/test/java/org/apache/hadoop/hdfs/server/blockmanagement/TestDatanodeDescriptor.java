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
package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.EnumSet;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo.AddBlockResult;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.junit.jupiter.api.Test;

/**
 * This class tests that methods in DatanodeDescriptor
 */
public class TestDatanodeDescriptor {
  /**
   * Test that getInvalidateBlocks observes the maxlimit.
   */
  @Test
  public void testGetInvalidateBlocks() throws Exception {
    final int MAX_BLOCKS = 10;
    final int REMAINING_BLOCKS = 2;
    final int MAX_LIMIT = MAX_BLOCKS - REMAINING_BLOCKS;
    
    DatanodeDescriptor dd = DFSTestUtil.getLocalDatanodeDescriptor();
    ArrayList<Block> blockList = new ArrayList<Block>(MAX_BLOCKS);
    for (int i=0; i<MAX_BLOCKS; i++) {
      blockList.add(new Block(i, 0, GenerationStamp.LAST_RESERVED_STAMP));
    }
    dd.addBlocksToBeInvalidated(blockList);
    Block[] bc = dd.getInvalidateBlocks(MAX_LIMIT);
    assertEquals(bc.length, MAX_LIMIT);
    bc = dd.getInvalidateBlocks(MAX_LIMIT);
    assertEquals(bc.length, REMAINING_BLOCKS);
  }
  
  @Test
  public void testBlocksCounter() throws Exception {
    DatanodeDescriptor dd = BlockManagerTestUtil.getLocalDatanodeDescriptor(true);
    assertEquals(0, dd.numBlocks());
    BlockInfo blk = new BlockInfoContiguous(new Block(1L), (short) 1);
    BlockInfo blk1 = new BlockInfoContiguous(new Block(2L), (short) 2);
    DatanodeStorageInfo[] storages = dd.getStorageInfos();
    assertTrue(storages.length > 0);
    // add first block
    assertEquals(AddBlockResult.ADDED, storages[0].addBlock(blk));
    assertEquals(1, dd.numBlocks());
    // remove a non-existent block
    assertFalse(BlocksMap.removeBlock(dd, blk1));
    assertEquals(1, dd.numBlocks());
    // add an existent block
    assertNotEquals(AddBlockResult.ADDED, storages[0].addBlock(blk));
    assertEquals(1, dd.numBlocks());
    // add second block
    assertEquals(AddBlockResult.ADDED, storages[0].addBlock(blk1));
    assertEquals(2, dd.numBlocks());
    // remove first block
    assertTrue(BlocksMap.removeBlock(dd, blk));
    assertEquals(1, dd.numBlocks());
    // remove second block
    assertTrue(BlocksMap.removeBlock(dd, blk1));
    assertEquals(0, dd.numBlocks());
  }

  /**
   * HDFS-17639: hasStorageType() previously called getStorageInfos() which
   * acquired storageMap lock and allocated a new array on every call.  The fix
   * iterates storageMap.values() directly under the lock.  This test verifies
   * correctness: hasStorageType() and getStorageTypes() must accurately reflect
   * which types are present after storages are injected.
   */
  @Test
  public void testHasStorageTypeAndGetStorageTypes() {
    DatanodeDescriptor dd = DFSTestUtil.getLocalDatanodeDescriptor();

    // Before any storage is injected, no type should be present.
    assertFalse(dd.hasStorageType(StorageType.DISK),
        "No DISK storage expected before injection");
    assertFalse(dd.hasStorageType(StorageType.SSD),
        "No SSD storage expected before injection");
    assertTrue(dd.getStorageTypes().isEmpty(),
        "getStorageTypes() should be empty before injection");

    // Inject a DISK storage.
    DatanodeStorageInfo diskStorage = DFSTestUtil.createDatanodeStorageInfo(
        "storage-disk", "127.0.0.1", "/rack1", "host1",
        StorageType.DISK, null);
    dd.injectStorage(diskStorage);

    assertTrue(dd.hasStorageType(StorageType.DISK),
        "DISK storage should be present after injection");
    assertFalse(dd.hasStorageType(StorageType.SSD),
        "SSD storage should still be absent");
    EnumSet<StorageType> types = dd.getStorageTypes();
    assertTrue(types.contains(StorageType.DISK));
    assertEquals(1, types.size());

    // Inject an SSD storage.
    DatanodeStorageInfo ssdStorage = DFSTestUtil.createDatanodeStorageInfo(
        "storage-ssd", "127.0.0.1", "/rack1", "host1",
        StorageType.SSD, null);
    dd.injectStorage(ssdStorage);

    assertTrue(dd.hasStorageType(StorageType.DISK));
    assertTrue(dd.hasStorageType(StorageType.SSD));
    assertEquals(2, dd.getStorageTypes().size());
  }
}
