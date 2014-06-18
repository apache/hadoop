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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.junit.Assert;
import org.junit.Test;

/**
 * This class provides tests for BlockInfo class, which is used in BlocksMap.
 * The test covers BlockList.listMoveToHead, used for faster block report
 * processing in DatanodeDescriptor.reportDiff.
 */

public class TestBlockInfo {

  private static final Log LOG = LogFactory
      .getLog("org.apache.hadoop.hdfs.TestBlockInfo");


  @Test
  public void testAddStorage() throws Exception {
    BlockInfo blockInfo = new BlockInfo(3);

    final DatanodeStorageInfo storage = DFSTestUtil.createDatanodeStorageInfo("storageID", "127.0.0.1");

    boolean added = blockInfo.addStorage(storage);

    Assert.assertTrue(added);
    Assert.assertEquals(storage, blockInfo.getStorageInfo(0));
  }


  @Test
  public void testReplaceStorageIfDifferetnOneAlreadyExistedFromSameDataNode() throws Exception {
    BlockInfo blockInfo = new BlockInfo(3);

    final DatanodeStorageInfo storage1 = DFSTestUtil.createDatanodeStorageInfo("storageID1", "127.0.0.1");
    final DatanodeStorageInfo storage2 = new DatanodeStorageInfo(storage1.getDatanodeDescriptor(), new DatanodeStorage("storageID2"));

    blockInfo.addStorage(storage1);
    boolean added = blockInfo.addStorage(storage2);

    Assert.assertFalse(added);
    Assert.assertEquals(storage2, blockInfo.getStorageInfo(0));
  }

  @Test
  public void testBlockListMoveToHead() throws Exception {
    LOG.info("BlockInfo moveToHead tests...");

    final int MAX_BLOCKS = 10;

    DatanodeStorageInfo dd = DFSTestUtil.createDatanodeStorageInfo("s1", "1.1.1.1");
    ArrayList<Block> blockList = new ArrayList<Block>(MAX_BLOCKS);
    ArrayList<BlockInfo> blockInfoList = new ArrayList<BlockInfo>();
    int headIndex;
    int curIndex;

    LOG.info("Building block list...");
    for (int i = 0; i < MAX_BLOCKS; i++) {
      blockList.add(new Block(i, 0, GenerationStamp.LAST_RESERVED_STAMP));
      blockInfoList.add(new BlockInfo(blockList.get(i), 3));
      dd.addBlock(blockInfoList.get(i));

      // index of the datanode should be 0
      assertEquals("Find datanode should be 0", 0, blockInfoList.get(i)
          .findStorageInfo(dd));
    }

    // list length should be equal to the number of blocks we inserted
    LOG.info("Checking list length...");
    assertEquals("Length should be MAX_BLOCK", MAX_BLOCKS, dd.numBlocks());
    Iterator<BlockInfo> it = dd.getBlockIterator();
    int len = 0;
    while (it.hasNext()) {
      it.next();
      len++;
    }
    assertEquals("There should be MAX_BLOCK blockInfo's", MAX_BLOCKS, len);

    headIndex = dd.getBlockListHeadForTesting().findStorageInfo(dd);

    LOG.info("Moving each block to the head of the list...");
    for (int i = 0; i < MAX_BLOCKS; i++) {
      curIndex = blockInfoList.get(i).findStorageInfo(dd);
      headIndex = dd.moveBlockToHead(blockInfoList.get(i), curIndex, headIndex);
      // the moved element must be at the head of the list
      assertEquals("Block should be at the head of the list now.",
          blockInfoList.get(i), dd.getBlockListHeadForTesting());
    }

    // move head of the list to the head - this should not change the list
    LOG.info("Moving head to the head...");

    BlockInfo temp = dd.getBlockListHeadForTesting();
    curIndex = 0;
    headIndex = 0;
    dd.moveBlockToHead(temp, curIndex, headIndex);
    assertEquals(
        "Moving head to the head of the list shopuld not change the list",
        temp, dd.getBlockListHeadForTesting());

    // check all elements of the list against the original blockInfoList
    LOG.info("Checking elements of the list...");
    temp = dd.getBlockListHeadForTesting();
    assertNotNull("Head should not be null", temp);
    int c = MAX_BLOCKS - 1;
    while (temp != null) {
      assertEquals("Expected element is not on the list",
          blockInfoList.get(c--), temp);
      temp = temp.getNext(0);
    }

    LOG.info("Moving random blocks to the head of the list...");
    headIndex = dd.getBlockListHeadForTesting().findStorageInfo(dd);
    Random rand = new Random();
    for (int i = 0; i < MAX_BLOCKS; i++) {
      int j = rand.nextInt(MAX_BLOCKS);
      curIndex = blockInfoList.get(j).findStorageInfo(dd);
      headIndex = dd.moveBlockToHead(blockInfoList.get(j), curIndex, headIndex);
      // the moved element must be at the head of the list
      assertEquals("Block should be at the head of the list now.",
          blockInfoList.get(j), dd.getBlockListHeadForTesting());
    }
  }
}