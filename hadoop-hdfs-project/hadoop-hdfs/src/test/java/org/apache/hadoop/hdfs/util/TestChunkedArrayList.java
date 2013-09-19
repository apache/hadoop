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
package org.apache.hadoop.hdfs.util;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.Test;

import com.google.common.base.Stopwatch;

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
        Stopwatch sw = new Stopwatch();
        sw.start();
        for (int i = 0; i < numElems; i++) {
          arrayList.add(obj);
        }
        System.out.println("       ArrayList " + sw.elapsedMillis());
      }
      
      // test ChunkedArrayList
      System.gc();
      {
        ChunkedArrayList<String> chunkedList = new ChunkedArrayList<String>();
        Stopwatch sw = new Stopwatch();
        sw.start();
        for (int i = 0; i < numElems; i++) {
          chunkedList.add(obj);
        }
        System.out.println("ChunkedArrayList " + sw.elapsedMillis());
      }
    }
  }
}
