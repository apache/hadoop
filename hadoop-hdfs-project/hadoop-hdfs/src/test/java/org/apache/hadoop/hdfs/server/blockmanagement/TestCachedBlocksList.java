/*
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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.CachedBlocksList;
import org.apache.hadoop.hdfs.server.namenode.CachedBlock;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class TestCachedBlocksList {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestCachedBlocksList.class);

  @Test(timeout=60000)
  public void testSingleList() {
    DatanodeDescriptor dn = new DatanodeDescriptor(
      new DatanodeID("127.0.0.1", "localhost", "abcd", 5000, 5001, 5002, 5003));
    CachedBlock[] blocks = new CachedBlock[] {
          new CachedBlock(0L, (short)1, true),
          new CachedBlock(1L, (short)1, true),
          new CachedBlock(2L, (short)1, true),
      };
      // check that lists are empty
      Assertions.assertTrue(
              !dn.getPendingCached().iterator().hasNext(), "expected pending cached list to start off empty.");
      Assertions.assertTrue(
              !dn.getCached().iterator().hasNext(), "expected cached list to start off empty.");
      Assertions.assertTrue(
              !dn.getPendingUncached().iterator().hasNext(), "expected pending uncached list to start off empty.");
    // add a block to the back
    Assertions.assertTrue(dn.getCached().add(blocks[0]));
      Assertions.assertTrue(
              !dn.getPendingCached().iterator().hasNext(), "expected pending cached list to still be empty.");
      Assertions.assertEquals(blocks[0],
              dn.getCached().iterator().next(), "failed to insert blocks[0]");
      Assertions.assertTrue(
              !dn.getPendingUncached().iterator().hasNext(), "expected pending uncached list to still be empty.");
    // add another block to the back
    Assertions.assertTrue(dn.getCached().add(blocks[1]));
    Iterator<CachedBlock> iter = dn.getCached().iterator();
    Assertions.assertEquals(blocks[0], iter.next());
    Assertions.assertEquals(blocks[1], iter.next());
    Assertions.assertTrue(!iter.hasNext());
    // add a block to the front
    Assertions.assertTrue(dn.getCached().addFirst(blocks[2]));
    iter = dn.getCached().iterator();
    Assertions.assertEquals(blocks[2], iter.next());
    Assertions.assertEquals(blocks[0], iter.next());
    Assertions.assertEquals(blocks[1], iter.next());
    Assertions.assertTrue(!iter.hasNext());
    // remove a block from the middle
    Assertions.assertTrue(dn.getCached().remove(blocks[0]));
    iter = dn.getCached().iterator();
    Assertions.assertEquals(blocks[2], iter.next());
    Assertions.assertEquals(blocks[1], iter.next());
    Assertions.assertTrue(!iter.hasNext());
    // remove all blocks
    dn.getCached().clear();
      Assertions.assertTrue(
              !dn.getPendingCached().iterator().hasNext(), "expected cached list to be empty after clear.");
  }

  private void testAddElementsToList(CachedBlocksList list,
      CachedBlock[] blocks) {
      Assertions.assertTrue(
              !list.iterator().hasNext(), "expected list to start off empty.");
    for (CachedBlock block : blocks) {
      Assertions.assertTrue(list.add(block));
    }
  }

  private void testRemoveElementsFromList(Random r,
      CachedBlocksList list, CachedBlock[] blocks) {
    int i = 0;
    for (Iterator<CachedBlock> iter = list.iterator(); iter.hasNext(); ) {
      Assertions.assertEquals(blocks[i], iter.next());
      i++;
    }
    if (r.nextBoolean()) {
      LOG.info("Removing via iterator");
      for (Iterator<CachedBlock> iter = list.iterator(); iter.hasNext() ;) {
        iter.next();
        iter.remove();
      }
    } else {
      LOG.info("Removing in pseudo-random order");
      CachedBlock[] remainingBlocks = Arrays.copyOf(blocks, blocks.length);
      for (int removed = 0; removed < remainingBlocks.length; ) {
        int toRemove = r.nextInt(remainingBlocks.length);
        if (remainingBlocks[toRemove] != null) {
          Assertions.assertTrue(list.remove(remainingBlocks[toRemove]));
          remainingBlocks[toRemove] = null;
          removed++;
        }
      }
    }
      Assertions.assertTrue(!list.iterator().hasNext(), "expected list to be empty after everything " +
              "was removed.");
  }

  @Test(timeout=60000)
  public void testMultipleLists() {
    DatanodeDescriptor[] datanodes = new DatanodeDescriptor[] {
      new DatanodeDescriptor(
        new DatanodeID("127.0.0.1", "localhost", "abcd", 5000, 5001, 5002, 5003)),
      new DatanodeDescriptor(
        new DatanodeID("127.0.1.1", "localhost", "efgh", 6000, 6001, 6002, 6003)),
    };
    CachedBlocksList[] lists = new CachedBlocksList[] {
        datanodes[0].getPendingCached(),
        datanodes[0].getCached(),
        datanodes[1].getPendingCached(),
        datanodes[1].getCached(),
        datanodes[1].getPendingUncached(),
    };
    final int NUM_BLOCKS = 8000;
    CachedBlock[] blocks = new CachedBlock[NUM_BLOCKS];
    for (int i = 0; i < NUM_BLOCKS; i++) {
      blocks[i] = new CachedBlock(i, (short)i, true);
    }
    Random r = new Random(654);
    for (CachedBlocksList list : lists) {
      testAddElementsToList(list, blocks);
    }
    for (CachedBlocksList list : lists) {
      testRemoveElementsFromList(r, list, blocks);
    }
  }
}
