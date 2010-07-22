/**
 * Copyright 2009 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.io.hfile;

import java.nio.ByteBuffer;
import java.util.LinkedList;

import junit.framework.TestCase;

public class TestCachedBlockQueue extends TestCase {

  public void testQueue() throws Exception {

    CachedBlock cb1 = new CachedBlock(1000, "cb1", 1);
    CachedBlock cb2 = new CachedBlock(1500, "cb2", 2);
    CachedBlock cb3 = new CachedBlock(1000, "cb3", 3);
    CachedBlock cb4 = new CachedBlock(1500, "cb4", 4);
    CachedBlock cb5 = new CachedBlock(1000, "cb5", 5);
    CachedBlock cb6 = new CachedBlock(1750, "cb6", 6);
    CachedBlock cb7 = new CachedBlock(1000, "cb7", 7);
    CachedBlock cb8 = new CachedBlock(1500, "cb8", 8);
    CachedBlock cb9 = new CachedBlock(1000, "cb9", 9);
    CachedBlock cb10 = new CachedBlock(1500, "cb10", 10);

    CachedBlockQueue queue = new CachedBlockQueue(10000,1000);

    queue.add(cb1);
    queue.add(cb2);
    queue.add(cb3);
    queue.add(cb4);
    queue.add(cb5);
    queue.add(cb6);
    queue.add(cb7);
    queue.add(cb8);
    queue.add(cb9);
    queue.add(cb10);

    // We expect cb1 through cb8 to be in the queue
    long expectedSize = cb1.heapSize() + cb2.heapSize() + cb3.heapSize() +
      cb4.heapSize() + cb5.heapSize() + cb6.heapSize() + cb7.heapSize() +
      cb8.heapSize();

    assertEquals(queue.heapSize(), expectedSize);

    LinkedList<org.apache.hadoop.hbase.io.hfile.CachedBlock> blocks =
      queue.get();
    assertEquals(blocks.poll().getName(), "cb1");
    assertEquals(blocks.poll().getName(), "cb2");
    assertEquals(blocks.poll().getName(), "cb3");
    assertEquals(blocks.poll().getName(), "cb4");
    assertEquals(blocks.poll().getName(), "cb5");
    assertEquals(blocks.poll().getName(), "cb6");
    assertEquals(blocks.poll().getName(), "cb7");
    assertEquals(blocks.poll().getName(), "cb8");

  }

  public void testQueueSmallBlockEdgeCase() throws Exception {

    CachedBlock cb1 = new CachedBlock(1000, "cb1", 1);
    CachedBlock cb2 = new CachedBlock(1500, "cb2", 2);
    CachedBlock cb3 = new CachedBlock(1000, "cb3", 3);
    CachedBlock cb4 = new CachedBlock(1500, "cb4", 4);
    CachedBlock cb5 = new CachedBlock(1000, "cb5", 5);
    CachedBlock cb6 = new CachedBlock(1750, "cb6", 6);
    CachedBlock cb7 = new CachedBlock(1000, "cb7", 7);
    CachedBlock cb8 = new CachedBlock(1500, "cb8", 8);
    CachedBlock cb9 = new CachedBlock(1000, "cb9", 9);
    CachedBlock cb10 = new CachedBlock(1500, "cb10", 10);

    CachedBlockQueue queue = new CachedBlockQueue(10000,1000);

    queue.add(cb1);
    queue.add(cb2);
    queue.add(cb3);
    queue.add(cb4);
    queue.add(cb5);
    queue.add(cb6);
    queue.add(cb7);
    queue.add(cb8);
    queue.add(cb9);
    queue.add(cb10);

    CachedBlock cb0 = new CachedBlock(10 + CachedBlock.PER_BLOCK_OVERHEAD, "cb0", 0);
    queue.add(cb0);

    // This is older so we must include it, but it will not end up kicking
    // anything out because (heapSize - cb8.heapSize + cb0.heapSize < maxSize)
    // and we must always maintain heapSize >= maxSize once we achieve it.

    // We expect cb0 through cb8 to be in the queue
    long expectedSize = cb1.heapSize() + cb2.heapSize() + cb3.heapSize() +
      cb4.heapSize() + cb5.heapSize() + cb6.heapSize() + cb7.heapSize() +
      cb8.heapSize() + cb0.heapSize();

    assertEquals(queue.heapSize(), expectedSize);

    LinkedList<org.apache.hadoop.hbase.io.hfile.CachedBlock> blocks = queue.get();
    assertEquals(blocks.poll().getName(), "cb0");
    assertEquals(blocks.poll().getName(), "cb1");
    assertEquals(blocks.poll().getName(), "cb2");
    assertEquals(blocks.poll().getName(), "cb3");
    assertEquals(blocks.poll().getName(), "cb4");
    assertEquals(blocks.poll().getName(), "cb5");
    assertEquals(blocks.poll().getName(), "cb6");
    assertEquals(blocks.poll().getName(), "cb7");
    assertEquals(blocks.poll().getName(), "cb8");

  }

  private static class CachedBlock extends org.apache.hadoop.hbase.io.hfile.CachedBlock
  {
    public CachedBlock(long heapSize, String name, long accessTime) {
      super(name,
          ByteBuffer.allocate((int)(heapSize - CachedBlock.PER_BLOCK_OVERHEAD)),
          accessTime,false);
    }
  }
}