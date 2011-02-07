/**
 * Copyright 2011 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MultithreadedTestUtil;
import org.apache.hadoop.hbase.MultithreadedTestUtil.TestThread;
import org.apache.hadoop.hbase.regionserver.MemStoreLAB.Allocation;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

public class TestMemStoreLAB {

  /**
   * Test a bunch of random allocations
   */
  @Test
  public void testLABRandomAllocation() {
    Random rand = new Random();
    MemStoreLAB mslab = new MemStoreLAB();
    int expectedOff = 0;
    byte[] lastBuffer = null;
    // 100K iterations by 0-1K alloc -> 50MB expected
    // should be reasonable for unit test and also cover wraparound
    // behavior
    for (int i = 0; i < 100000; i++) {
      int size = rand.nextInt(1000);
      Allocation alloc = mslab.allocateBytes(size);
      
      if (alloc.getData() != lastBuffer) {
        expectedOff = 0;
        lastBuffer = alloc.getData();
      }
      assertEquals(expectedOff, alloc.getOffset());
      assertTrue("Allocation " + alloc + " overruns buffer",
          alloc.getOffset() + size <= alloc.getData().length);
      expectedOff += size;
    }
  }

  @Test
  public void testLABLargeAllocation() {
    MemStoreLAB mslab = new MemStoreLAB();
    Allocation alloc = mslab.allocateBytes(2*1024*1024);
    assertNull("2MB allocation shouldn't be satisfied by LAB.",
      alloc);
  } 

  /**
   * Test allocation from lots of threads, making sure the results don't
   * overlap in any way
   */
  @Test
  public void testLABThreading() throws Exception {
    Configuration conf = new Configuration();
    MultithreadedTestUtil.TestContext ctx =
      new MultithreadedTestUtil.TestContext(conf);
    
    final AtomicInteger totalAllocated = new AtomicInteger();
    
    final MemStoreLAB mslab = new MemStoreLAB();
    List<List<AllocRecord>> allocations = Lists.newArrayList();
    
    for (int i = 0; i < 10; i++) {
      final List<AllocRecord> allocsByThisThread = Lists.newLinkedList();
      allocations.add(allocsByThisThread);
      
      TestThread t = new MultithreadedTestUtil.RepeatingTestThread(ctx) {
        private Random r = new Random();
        @Override
        public void doAnAction() throws Exception {
          int size = r.nextInt(1000);
          Allocation alloc = mslab.allocateBytes(size);
          totalAllocated.addAndGet(size);
          allocsByThisThread.add(new AllocRecord(alloc, size));
        }
      };
      ctx.addThread(t);
    }
    
    ctx.startThreads();
    while (totalAllocated.get() < 50*1024*1024 && ctx.shouldRun()) {
      Thread.sleep(10);
    }
    ctx.stop();
    
    // Partition the allocations by the actual byte[] they point into,
    // make sure offsets are unique for each chunk
    Map<byte[], Map<Integer, AllocRecord>> mapsByChunk =
      Maps.newHashMap();
    
    int sizeCounted = 0;
    for (AllocRecord rec : Iterables.concat(allocations)) {
      sizeCounted += rec.size;
      if (rec.size == 0) continue;
      
      Map<Integer, AllocRecord> mapForThisByteArray =
        mapsByChunk.get(rec.alloc.getData());
      if (mapForThisByteArray == null) {
        mapForThisByteArray = Maps.newTreeMap();
        mapsByChunk.put(rec.alloc.getData(), mapForThisByteArray);
      }
      AllocRecord oldVal = mapForThisByteArray.put(rec.alloc.getOffset(), rec);
      assertNull("Already had an entry " + oldVal + " for allocation " + rec,
          oldVal);
    }
    assertEquals("Sanity check test", sizeCounted, totalAllocated.get());
    
    // Now check each byte array to make sure allocations don't overlap
    for (Map<Integer, AllocRecord> allocsInChunk : mapsByChunk.values()) {
      int expectedOff = 0;
      for (AllocRecord alloc : allocsInChunk.values()) {
        assertEquals(expectedOff, alloc.alloc.getOffset());
        assertTrue("Allocation " + alloc + " overruns buffer",
            alloc.alloc.getOffset() + alloc.size <= alloc.alloc.getData().length);
        expectedOff += alloc.size;
      }
    }

  }
  
  private static class AllocRecord implements Comparable<AllocRecord>{
    private final Allocation alloc;
    private final int size;
    public AllocRecord(Allocation alloc, int size) {
      super();
      this.alloc = alloc;
      this.size = size;
    }

    @Override
    public int compareTo(AllocRecord e) {
      if (alloc.getData() != e.alloc.getData()) {
        throw new RuntimeException("Can only compare within a particular array");
      }
      return Ints.compare(alloc.getOffset(), e.alloc.getOffset());
    }
    
    @Override
    public String toString() {
      return "AllocRecord(alloc=" + alloc + ", size=" + size + ")";
    }
    
  }
}
