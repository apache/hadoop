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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.CachedBlocksList;
import org.apache.hadoop.hdfs.server.namenode.CachedBlock;
import org.junit.Assert;
import org.junit.Test;

public class TestCachedBlocksList {
  public static final Log LOG = LogFactory.getLog(TestCachedBlocksList.class);

  @Test(timeout=60000)
  public void testSingleList() {
    DatanodeDescriptor dn = new DatanodeDescriptor(
      new DatanodeID("127.0.0.1", "localhost", "abcd",
        5000, 5001, 5002, 5003));
    CachedBlock[] blocks = new CachedBlock[] {
          new CachedBlock(0L, (short)1, true),
          new CachedBlock(1L, (short)1, true),
          new CachedBlock(2L, (short)1, true),
      };
    // check that lists are empty
    Assert.assertTrue("expected pending cached list to start off empty.", 
        !dn.getPendingCached().iterator().hasNext());
    Assert.assertTrue("expected cached list to start off empty.", 
        !dn.getCached().iterator().hasNext());
    Assert.assertTrue("expected pending uncached list to start off empty.", 
        !dn.getPendingUncached().iterator().hasNext());
    // add a block to the back
    Assert.assertTrue(dn.getCached().add(blocks[0]));
    Assert.assertTrue("expected pending cached list to still be empty.", 
        !dn.getPendingCached().iterator().hasNext());
    Assert.assertEquals("failed to insert blocks[0]", blocks[0],
        dn.getCached().iterator().next());
    Assert.assertTrue("expected pending uncached list to still be empty.", 
        !dn.getPendingUncached().iterator().hasNext());
    // add another block to the back
    Assert.assertTrue(dn.getCached().add(blocks[1]));
    Iterator<CachedBlock> iter = dn.getCached().iterator();
    Assert.assertEquals(blocks[0], iter.next());
    Assert.assertEquals(blocks[1], iter.next());
    Assert.assertTrue(!iter.hasNext());
    // add a block to the front
    Assert.assertTrue(dn.getCached().addFirst(blocks[2]));
    iter = dn.getCached().iterator();
    Assert.assertEquals(blocks[2], iter.next());
    Assert.assertEquals(blocks[0], iter.next());
    Assert.assertEquals(blocks[1], iter.next());
    Assert.assertTrue(!iter.hasNext());
    // remove a block from the middle
    Assert.assertTrue(dn.getCached().remove(blocks[0]));
    iter = dn.getCached().iterator();
    Assert.assertEquals(blocks[2], iter.next());
    Assert.assertEquals(blocks[1], iter.next());
    Assert.assertTrue(!iter.hasNext());
    // remove all blocks
    dn.getCached().clear();
    Assert.assertTrue("expected cached list to be empty after clear.", 
        !dn.getPendingCached().iterator().hasNext());
  }

  private void testAddElementsToList(CachedBlocksList list,
      CachedBlock[] blocks) {
    Assert.assertTrue("expected list to start off empty.", 
        !list.iterator().hasNext());
    for (CachedBlock block : blocks) {
      Assert.assertTrue(list.add(block));
    }
  }

  private void testRemoveElementsFromList(Random r,
      CachedBlocksList list, CachedBlock[] blocks) {
    int i = 0;
    for (Iterator<CachedBlock> iter = list.iterator(); iter.hasNext(); ) {
      Assert.assertEquals(blocks[i], iter.next());
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
          Assert.assertTrue(list.remove(remainingBlocks[toRemove]));
          remainingBlocks[toRemove] = null;
          removed++;
        }
      }
    }
    Assert.assertTrue("expected list to be empty after everything " +
        "was removed.", !list.iterator().hasNext());
  }

  @Test(timeout=60000)
  public void testMultipleLists() {
    DatanodeDescriptor[] datanodes = new DatanodeDescriptor[] {
      new DatanodeDescriptor(
        new DatanodeID("127.0.0.1", "localhost", "abcd",
          5000, 5001, 5002, 5003)),
      new DatanodeDescriptor(
        new DatanodeID("127.0.1.1", "localhost", "efgh",
          6000, 6001, 6002, 6003)),
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
