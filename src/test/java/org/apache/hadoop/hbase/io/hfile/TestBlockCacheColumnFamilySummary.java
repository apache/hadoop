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

package org.apache.hadoop.hbase.io.hfile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * Tests the BlockCacheColumnFamilySummary class
 * 
 */
public class TestBlockCacheColumnFamilySummary {


  /**
   * 
   */
  @Test
  public void testEquals()  {

    BlockCacheColumnFamilySummary e1 = new BlockCacheColumnFamilySummary();
    e1.setTable("table1");
    e1.setColumnFamily("cf1");
    
    BlockCacheColumnFamilySummary e2 = new BlockCacheColumnFamilySummary();
    e2.setTable("table1");
    e2.setColumnFamily("cf1");

    assertEquals("bcse", e1, e2);
  }

  /**
   * 
   */
  @Test
  public void testNotEquals() {

    BlockCacheColumnFamilySummary e1 = new BlockCacheColumnFamilySummary();
    e1.setTable("table1");
    e1.setColumnFamily("cf1");
    
    BlockCacheColumnFamilySummary e2 = new BlockCacheColumnFamilySummary();
    e2.setTable("tablexxxxxx");
    e2.setColumnFamily("cf1");

    assertTrue("bcse", ! e1.equals(e2));
  }

  /**
   * 
   */
  @Test
  public void testMapLookup() {
    
    Map<BlockCacheColumnFamilySummary, BlockCacheColumnFamilySummary> bcs = 
      new HashMap<BlockCacheColumnFamilySummary, BlockCacheColumnFamilySummary>();

    BlockCacheColumnFamilySummary e1 = new BlockCacheColumnFamilySummary("table1","cf1");

    BlockCacheColumnFamilySummary lookup = bcs.get(e1);

    if (lookup == null) {
      lookup = BlockCacheColumnFamilySummary.create(e1);
      bcs.put(e1,lookup);
      lookup.incrementBlocks();
      lookup.incrementHeapSize(100L);
    }

    BlockCacheColumnFamilySummary e2 = new BlockCacheColumnFamilySummary("table1","cf1");

    BlockCacheColumnFamilySummary l2 = bcs.get(e2);
    assertEquals("blocks",1,l2.getBlocks());
    assertEquals("heap",100L,l2.getHeapSize());
  }

  /**
   * 
   */
  @Test
  public void testMapEntry() {
    
    Map<BlockCacheColumnFamilySummary, BlockCacheColumnFamilySummary> bcs = 
      new HashMap<BlockCacheColumnFamilySummary, BlockCacheColumnFamilySummary>();

    BlockCacheColumnFamilySummary e1 = new BlockCacheColumnFamilySummary("table1","cf1");
    bcs.put(e1, e1);
    
    BlockCacheColumnFamilySummary e2 = new BlockCacheColumnFamilySummary("table1","cf1");
    bcs.put(e2, e2);
    
    BlockCacheColumnFamilySummary e3 = new BlockCacheColumnFamilySummary("table1","cf1");
    bcs.put(e3, e3);
    
    assertEquals("mapSize",1,bcs.size());
  }

}
