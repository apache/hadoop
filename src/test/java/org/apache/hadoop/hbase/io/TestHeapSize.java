/*
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

package org.apache.hadoop.hbase.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.hfile.CachedBlock;
import org.apache.hadoop.hbase.io.hfile.LruBlockCache;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MemStore;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

/**
 * Testing the sizing that HeapSize offers and compares to the size given by
 * ClassSize.
 */
public class TestHeapSize extends TestCase {
  static final Log LOG = LogFactory.getLog(TestHeapSize.class);
  // List of classes implementing HeapSize
  // BatchOperation, BatchUpdate, BlockIndex, Entry, Entry<K,V>, HStoreKey
  // KeyValue, LruBlockCache, LruHashMap<K,V>, Put, HLogKey

  /**
   * Test our hard-coded sizing of native java objects
   */
  public void testNativeSizes() throws IOException {
    @SuppressWarnings("rawtypes")
    Class cl = null;
    long expected = 0L;
    long actual = 0L;

    // ArrayList
    cl = ArrayList.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.ARRAYLIST;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // ByteBuffer
    cl = ByteBuffer.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.BYTE_BUFFER;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // Integer
    cl = Integer.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.INTEGER;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // Map.Entry
    // Interface is public, all others are not.  Hard to size via ClassSize
//    cl = Map.Entry.class;
//    expected = ClassSize.estimateBase(cl, false);
//    actual = ClassSize.MAP_ENTRY;
//    if(expected != actual) {
//      ClassSize.estimateBase(cl, true);
//      assertEquals(expected, actual);
//    }

    // Object
    cl = Object.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.OBJECT;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // TreeMap
    cl = TreeMap.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.TREEMAP;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // String
    cl = String.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.STRING;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // ConcurrentHashMap
    cl = ConcurrentHashMap.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.CONCURRENT_HASHMAP;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // ConcurrentSkipListMap
    cl = ConcurrentSkipListMap.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.CONCURRENT_SKIPLISTMAP;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // ReentrantReadWriteLock
    cl = ReentrantReadWriteLock.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.REENTRANT_LOCK;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // AtomicLong
    cl = AtomicLong.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.ATOMIC_LONG;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // AtomicInteger
    cl = AtomicInteger.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.ATOMIC_INTEGER;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // AtomicBoolean
    cl = AtomicBoolean.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.ATOMIC_BOOLEAN;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // CopyOnWriteArraySet
    cl = CopyOnWriteArraySet.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.COPYONWRITE_ARRAYSET;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // CopyOnWriteArrayList
    cl = CopyOnWriteArrayList.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.COPYONWRITE_ARRAYLIST;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }


  }

  /**
   * Testing the classes that implements HeapSize and are a part of 0.20.
   * Some are not tested here for example BlockIndex which is tested in
   * TestHFile since it is a non public class
   * @throws IOException
   */
  public void testSizes() throws IOException {
    @SuppressWarnings("rawtypes")
    Class cl = null;
    long expected = 0L;
    long actual = 0L;

    //KeyValue
    cl = KeyValue.class;
    expected = ClassSize.estimateBase(cl, false);
    KeyValue kv = new KeyValue();
    actual = kv.heapSize();
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    //Put
    cl = Put.class;
    expected = ClassSize.estimateBase(cl, false);
    //The actual TreeMap is not included in the above calculation
    expected += ClassSize.TREEMAP;
    Put put = new Put(Bytes.toBytes(""));
    actual = put.heapSize();
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    //LruBlockCache Overhead
    cl = LruBlockCache.class;
    actual = LruBlockCache.CACHE_FIXED_OVERHEAD;
    expected = ClassSize.estimateBase(cl, false);
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // CachedBlock Fixed Overhead
    // We really need "deep" sizing but ClassSize does not do this.
    // Perhaps we should do all these more in this style....
    cl = CachedBlock.class;
    actual = CachedBlock.PER_BLOCK_OVERHEAD;
    expected = ClassSize.estimateBase(cl, false);
    expected += ClassSize.estimateBase(String.class, false);
    expected += ClassSize.estimateBase(ByteBuffer.class, false);
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      ClassSize.estimateBase(String.class, true);
      ClassSize.estimateBase(ByteBuffer.class, true);
      assertEquals(expected, actual);
    }

    // MemStore Overhead
    cl = MemStore.class;
    actual = MemStore.FIXED_OVERHEAD;
    expected = ClassSize.estimateBase(cl, false);
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // MemStore Deep Overhead
    actual = MemStore.DEEP_OVERHEAD;
    expected = ClassSize.estimateBase(cl, false);
    expected += ClassSize.estimateBase(ReentrantReadWriteLock.class, false);
    expected += ClassSize.estimateBase(AtomicLong.class, false);
    expected += ClassSize.estimateBase(ConcurrentSkipListMap.class, false);
    expected += ClassSize.estimateBase(ConcurrentSkipListMap.class, false);
    expected += ClassSize.estimateBase(CopyOnWriteArraySet.class, false);
    expected += ClassSize.estimateBase(CopyOnWriteArrayList.class, false);
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      ClassSize.estimateBase(ReentrantReadWriteLock.class, true);
      ClassSize.estimateBase(AtomicLong.class, true);
      ClassSize.estimateBase(ConcurrentSkipListMap.class, true);
      ClassSize.estimateBase(CopyOnWriteArraySet.class, true);
      ClassSize.estimateBase(CopyOnWriteArrayList.class, true);
      assertEquals(expected, actual);
    }

    // Store Overhead
    cl = Store.class;
    actual = Store.FIXED_OVERHEAD;
    expected = ClassSize.estimateBase(cl, false);
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // Region Overhead
    cl = HRegion.class;
    actual = HRegion.FIXED_OVERHEAD;
    expected = ClassSize.estimateBase(cl, false);
    if (expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // Currently NOT testing Deep Overheads of many of these classes.
    // Deep overheads cover a vast majority of stuff, but will not be 100%
    // accurate because it's unclear when we're referencing stuff that's already
    // accounted for.  But we have satisfied our two core requirements.
    // Sizing is quite accurate now, and our tests will throw errors if
    // any of these classes are modified without updating overhead sizes.
  }
}
