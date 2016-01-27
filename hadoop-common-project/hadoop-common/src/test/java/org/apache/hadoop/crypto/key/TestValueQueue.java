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
package org.apache.hadoop.crypto.key;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.crypto.key.kms.ValueQueue;
import org.apache.hadoop.crypto.key.kms.ValueQueue.QueueRefiller;
import org.apache.hadoop.crypto.key.kms.ValueQueue.SyncGenerationPolicy;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

public class TestValueQueue {

  private static class FillInfo {
    final int num;
    final String key;
    FillInfo(int num, String key) {
      this.num = num;
      this.key = key;
    }
  }

  private static class MockFiller implements QueueRefiller<String> {
    final LinkedBlockingQueue<FillInfo> fillCalls =
        new LinkedBlockingQueue<FillInfo>();
    @Override
    public void fillQueueForKey(String keyName, Queue<String> keyQueue,
        int numValues) throws IOException {
      fillCalls.add(new FillInfo(numValues, keyName));
      for(int i = 0; i < numValues; i++) {
        keyQueue.add("test");
      }
    }
    public FillInfo getTop() throws InterruptedException {
      return fillCalls.poll(500, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Verifies that Queue is initially filled to "numInitValues"
   */
  @Test
  public void testInitFill() throws Exception {
    MockFiller filler = new MockFiller();
    ValueQueue<String> vq =
        new ValueQueue<String>(10, 0.1f, 300, 1,
            SyncGenerationPolicy.ALL, filler);
    Assert.assertEquals("test", vq.getNext("k1"));
    Assert.assertEquals(1, filler.getTop().num);
    vq.shutdown();
  }

  /**
   * Verifies that Queue is initialized (Warmed-up) for provided keys
   */
  @Test
  public void testWarmUp() throws Exception {
    MockFiller filler = new MockFiller();
    ValueQueue<String> vq =
        new ValueQueue<String>(10, 0.5f, 300, 1,
            SyncGenerationPolicy.ALL, filler);
    vq.initializeQueuesForKeys("k1", "k2", "k3");
    FillInfo[] fillInfos =
      {filler.getTop(), filler.getTop(), filler.getTop()};
    Assert.assertEquals(5, fillInfos[0].num);
    Assert.assertEquals(5, fillInfos[1].num);
    Assert.assertEquals(5, fillInfos[2].num);
    Assert.assertEquals(Sets.newHashSet("k1", "k2", "k3"),
        Sets.newHashSet(fillInfos[0].key,
            fillInfos[1].key,
            fillInfos[2].key));
    vq.shutdown();
  }

  /**
   * Verifies that the refill task is executed after "checkInterval" if
   * num values below "lowWatermark"
   */
  @Test
  public void testRefill() throws Exception {
    MockFiller filler = new MockFiller();
    ValueQueue<String> vq =
        new ValueQueue<String>(10, 0.1f, 300, 1,
            SyncGenerationPolicy.ALL, filler);
    Assert.assertEquals("test", vq.getNext("k1"));
    Assert.assertEquals(1, filler.getTop().num);
    // Trigger refill
    vq.getNext("k1");
    Assert.assertEquals(1, filler.getTop().num);
    Assert.assertEquals(10, filler.getTop().num);
    vq.shutdown();
  }

  /**
   * Verifies that the No refill Happens after "checkInterval" if
   * num values above "lowWatermark"
   */
  @Test
  public void testNoRefill() throws Exception {
    MockFiller filler = new MockFiller();
    ValueQueue<String> vq =
        new ValueQueue<String>(10, 0.5f, 300, 1,
            SyncGenerationPolicy.ALL, filler);
    Assert.assertEquals("test", vq.getNext("k1"));
    Assert.assertEquals(5, filler.getTop().num);
    Assert.assertEquals(null, filler.getTop());
    vq.shutdown();
  }

  /**
   * Verify getAtMost when SyncGeneration Policy = ALL
   */
  @Test
  public void testgetAtMostPolicyALL() throws Exception {
    MockFiller filler = new MockFiller();
    ValueQueue<String> vq =
        new ValueQueue<String>(10, 0.1f, 300, 1,
            SyncGenerationPolicy.ALL, filler);
    Assert.assertEquals("test", vq.getNext("k1"));
    Assert.assertEquals(1, filler.getTop().num);
    // Drain completely
    Assert.assertEquals(10, vq.getAtMost("k1", 10).size());
    // Synchronous call
    Assert.assertEquals(10, filler.getTop().num);
    // Ask for more... return all
    Assert.assertEquals(19, vq.getAtMost("k1", 19).size());
    // Synchronous call (No Async call since num > lowWatermark)
    Assert.assertEquals(19, filler.getTop().num);
    vq.shutdown();
  }

  /**
   * Verify getAtMost when SyncGeneration Policy = ALL
   */
  @Test
  public void testgetAtMostPolicyATLEAST_ONE() throws Exception {
    MockFiller filler = new MockFiller();
    ValueQueue<String> vq =
        new ValueQueue<String>(10, 0.3f, 300, 1,
            SyncGenerationPolicy.ATLEAST_ONE, filler);
    Assert.assertEquals("test", vq.getNext("k1"));
    Assert.assertEquals(3, filler.getTop().num);
    // Drain completely
    Assert.assertEquals(2, vq.getAtMost("k1", 10).size());
    // Asynch Refill call
    Assert.assertEquals(10, filler.getTop().num);
    vq.shutdown();
  }

  /**
   * Verify getAtMost when SyncGeneration Policy = LOW_WATERMARK
   */
  @Test
  public void testgetAtMostPolicyLOW_WATERMARK() throws Exception {
    MockFiller filler = new MockFiller();
    ValueQueue<String> vq =
        new ValueQueue<String>(10, 0.3f, 300, 1,
            SyncGenerationPolicy.LOW_WATERMARK, filler);
    Assert.assertEquals("test", vq.getNext("k1"));
    Assert.assertEquals(3, filler.getTop().num);
    // Drain completely
    Assert.assertEquals(3, vq.getAtMost("k1", 10).size());
    // Synchronous call
    Assert.assertEquals(1, filler.getTop().num);
    // Asynch Refill call
    Assert.assertEquals(10, filler.getTop().num);
    vq.shutdown();
  }

  @Test
  public void testDrain() throws Exception {
    MockFiller filler = new MockFiller();
    ValueQueue<String> vq =
        new ValueQueue<String>(10, 0.1f, 300, 1,
            SyncGenerationPolicy.ALL, filler);
    Assert.assertEquals("test", vq.getNext("k1"));
    Assert.assertEquals(1, filler.getTop().num);
    vq.drain("k1");
    Assert.assertNull(filler.getTop());
    vq.shutdown();
  }

}
