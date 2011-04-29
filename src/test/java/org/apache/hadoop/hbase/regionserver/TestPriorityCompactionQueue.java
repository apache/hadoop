/*
 * Copyright 2010 The Apache Software Foundation
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for the priority compaction queue
 */
public class TestPriorityCompactionQueue {
  static final Log LOG = LogFactory.getLog(TestPriorityCompactionQueue.class);

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {

  }

  class DummyHRegion extends HRegion {
    String name;

    DummyHRegion(String name) {
      super();
      this.name = name;
    }

    public int hashCode() {
      return name.hashCode();
    }

    public boolean equals(DummyHRegion r) {
      return name.equals(r.name);
    }

    public String toString() {
      return "[DummyHRegion " + name + "]";
    }

    public byte[] getRegionName() {
      return Bytes.toBytes(name);
    }

    public String getRegionNameAsString() {
      return name;
    }
  }

  protected void getAndCheckRegion(PriorityCompactionQueue pq,
      HRegion checkRegion) {
    HRegion r = pq.remove().getHRegion();
    if (r != checkRegion) {
      Assert.assertTrue("Didn't get expected " + checkRegion + " got " + r, r
          .equals(checkRegion));
    }
  }

  protected void addRegion(PriorityCompactionQueue pq, HRegion r, int p) {
    pq.add(new CompactionRequest(r, null, p));
    try {
      // Sleep 1 millisecond so 2 things are not put in the queue within the
      // same millisecond. The queue breaks ties arbitrarily between two
      // requests inserted at the same time. We want the ordering to
      // be consistent for our unit test.
      Thread.sleep(1);
    } catch (InterruptedException ex) {
      // continue
    }
  }

  // ////////////////////////////////////////////////////////////////////////////
  // tests
  // ////////////////////////////////////////////////////////////////////////////

  /** tests general functionality of the compaction queue */
  @Test public void testPriorityQueue() throws InterruptedException {
    PriorityCompactionQueue pq = new PriorityCompactionQueue();

    HRegion r1 = new DummyHRegion("r1");
    HRegion r2 = new DummyHRegion("r2");
    HRegion r3 = new DummyHRegion("r3");
    HRegion r4 = new DummyHRegion("r4");
    HRegion r5 = new DummyHRegion("r5");

    // test 1
    // check fifo w/priority
    addRegion(pq, r1, 0);
    addRegion(pq, r2, 0);
    addRegion(pq, r3, 0);
    addRegion(pq, r4, 0);
    addRegion(pq, r5, 0);

    getAndCheckRegion(pq, r1);
    getAndCheckRegion(pq, r2);
    getAndCheckRegion(pq, r3);
    getAndCheckRegion(pq, r4);
    getAndCheckRegion(pq, r5);

    // test 2
    // check fifo w/mixed priority
    addRegion(pq, r1, 0);
    addRegion(pq, r2, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r3, 0);
    addRegion(pq, r4, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r5, 0);

    getAndCheckRegion(pq, r1);
    getAndCheckRegion(pq, r3);
    getAndCheckRegion(pq, r5);
    getAndCheckRegion(pq, r2);
    getAndCheckRegion(pq, r4);

    // test 3
    // check fifo w/mixed priority
    addRegion(pq, r1, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r2, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r3, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r4, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r5, 0);

    getAndCheckRegion(pq, r5);
    getAndCheckRegion(pq, r1);
    getAndCheckRegion(pq, r2);
    getAndCheckRegion(pq, r3);
    getAndCheckRegion(pq, r4);

    // test 4
    // check fifo w/mixed priority elevation time
    addRegion(pq, r1, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r2, 0);
    addRegion(pq, r3, CompactSplitThread.PRIORITY_USER);
    Thread.sleep(1000);
    addRegion(pq, r4, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r5, 0);

    getAndCheckRegion(pq, r2);
    getAndCheckRegion(pq, r5);
    getAndCheckRegion(pq, r1);
    getAndCheckRegion(pq, r3);
    getAndCheckRegion(pq, r4);

    // reset the priority compaction queue back to a normal queue
    pq = new PriorityCompactionQueue();

    // test 5
    // test that lower priority are removed from the queue when a high priority
    // is added
    addRegion(pq, r1, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r2, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r3, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r4, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r5, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r3, 0);

    getAndCheckRegion(pq, r3);
    getAndCheckRegion(pq, r1);
    getAndCheckRegion(pq, r2);
    getAndCheckRegion(pq, r4);
    getAndCheckRegion(pq, r5);

    Assert.assertTrue("Queue should be empty.", pq.size() == 0);

    // test 6
    // don't add the same region more than once
    addRegion(pq, r1, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r2, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r3, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r4, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r5, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r1, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r2, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r3, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r4, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r5, CompactSplitThread.PRIORITY_USER);

    getAndCheckRegion(pq, r1);
    getAndCheckRegion(pq, r2);
    getAndCheckRegion(pq, r3);
    getAndCheckRegion(pq, r4);
    getAndCheckRegion(pq, r5);

    Assert.assertTrue("Queue should be empty.", pq.size() == 0);
    
    // test 7
    // we can handle negative priorities
    addRegion(pq, r1, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r2, -1);
    addRegion(pq, r3, 0);    
    addRegion(pq, r4, -2);
    
    getAndCheckRegion(pq, r4);
    getAndCheckRegion(pq, r2);
    getAndCheckRegion(pq, r3);
    getAndCheckRegion(pq, r1);
    
    Assert.assertTrue("Queue should be empty.", pq.size() == 0);
  }
}
