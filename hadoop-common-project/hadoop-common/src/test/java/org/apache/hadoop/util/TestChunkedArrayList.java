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
package org.apache.hadoop.util;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

public class TestChunkedArrayList {

  @Test
  public void testBasics() {
    final int N_ELEMS = 100000;
    ChunkedArrayList<Integer> l = new ChunkedArrayList<Integer>();
    assertTrue(l.isEmpty());
    // Insert a bunch of elements.
    for (int i = 0; i < N_ELEMS; i++) {
      l.add(i);
    }
    assertFalse(l.isEmpty());
    assertEquals(N_ELEMS, l.size());

    // Check that it got chunked.
    assertTrue(l.getNumChunks() > 10);
    assertEquals(8192, l.getMaxChunkSize());
  }
  
  @Test
  public void testIterator() {
    ChunkedArrayList<Integer> l = new ChunkedArrayList<Integer>();
    for (int i = 0; i < 30000; i++) {
      l.add(i);
    }
    
    int i = 0;
    for (int fromList : l) {
      assertEquals(i, fromList);
      i++;
    }
  }
  
  @Test
  public void testPerformance() {
    String obj = "hello world";
    
    final int numElems = 1000000;
    final int numTrials = 5;
    
    for (int trial = 0; trial < numTrials; trial++) {
      System.gc();
      {
        ArrayList<String> arrayList = new ArrayList<String>();
        StopWatch sw = new StopWatch();
        sw.start();
        for (int i = 0; i < numElems; i++) {
          arrayList.add(obj);
        }
        System.out.println("       ArrayList " + sw.now(TimeUnit.MILLISECONDS));
      }
      
      // test ChunkedArrayList
      System.gc();
      {
        ChunkedArrayList<String> chunkedList = new ChunkedArrayList<String>();
        StopWatch sw = new StopWatch();
        sw.start();
        for (int i = 0; i < numElems; i++) {
          chunkedList.add(obj);
        }
        System.out.println("ChunkedArrayList " + sw.now(TimeUnit.MILLISECONDS));
      }
    }
  }

  @Test
  public void testRemovals() throws Exception {
    final int NUM_ELEMS = 100000;
    ChunkedArrayList<Integer> list = new ChunkedArrayList<Integer>();
    for (int i = 0; i < NUM_ELEMS; i++) {
      list.add(i);
    }

    // Iterate through all list elements.
    Iterator<Integer> iter = list.iterator();
    for (int i = 0; i < NUM_ELEMS; i++) {
      Assert.assertTrue(iter.hasNext());
      Integer val = iter.next();
      Assert.assertEquals(Integer.valueOf(i), val);
    }
    Assert.assertFalse(iter.hasNext());
    Assert.assertEquals(NUM_ELEMS, list.size());

    // Remove even elements.
    iter = list.iterator();
    for (int i = 0; i < NUM_ELEMS; i++) {
      Assert.assertTrue(iter.hasNext());
      Integer val = iter.next();
      Assert.assertEquals(Integer.valueOf(i), val);
      if (i % 2 == 0) {
        iter.remove();
      }
    }
    Assert.assertFalse(iter.hasNext());
    Assert.assertEquals(NUM_ELEMS / 2, list.size());

    // Iterate through all odd list elements.
    iter = list.iterator();
    for (int i = 0; i < NUM_ELEMS / 2; i++) {
      Assert.assertTrue(iter.hasNext());
      Integer val = iter.next();
      Assert.assertEquals(Integer.valueOf(1 + (2 * i)), val);
      iter.remove();
    }
    Assert.assertFalse(iter.hasNext());

    // Check that list is now empty.
    Assert.assertEquals(0, list.size());
    Assert.assertTrue(list.isEmpty());
    iter = list.iterator();
    Assert.assertFalse(iter.hasNext());
  }

  @Test
  public void testGet() throws Exception {
    final int NUM_ELEMS = 100001;
    ChunkedArrayList<Integer> list = new ChunkedArrayList<Integer>();
    for (int i = 0; i < NUM_ELEMS; i++) {
      list.add(i);
    }

    Assert.assertEquals(Integer.valueOf(100), list.get(100));
    Assert.assertEquals(Integer.valueOf(1000), list.get(1000));
    Assert.assertEquals(Integer.valueOf(10000), list.get(10000));
    Assert.assertEquals(Integer.valueOf(100000), list.get(100000));

    Iterator<Integer> iter = list.iterator();
    iter.next();
    iter.remove();
    Assert.assertEquals(Integer.valueOf(1), list.get(0));

    iter = list.iterator();
    for (int i = 0; i < 500; i++) {
      iter.next();
    }
    iter.remove();

    Assert.assertEquals(Integer.valueOf(502), list.get(500));
    Assert.assertEquals(Integer.valueOf(602), list.get(600));
  }
}
