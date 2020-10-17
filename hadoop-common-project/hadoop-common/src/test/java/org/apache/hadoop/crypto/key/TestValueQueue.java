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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.crypto.key.kms.ValueQueue;
import org.apache.hadoop.crypto.key.kms.ValueQueue.QueueRefiller;
import org.apache.hadoop.crypto.key.kms.ValueQueue.SyncGenerationPolicy;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;

public class TestValueQueue {
  Logger LOG = LoggerFactory.getLogger(TestValueQueue.class);

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

  private void waitForRefill(ValueQueue<?> valueQueue, String queueName, int queueSize)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> {
      int size = valueQueue.getSize(queueName);
      if (size != queueSize) {
        LOG.info("Current ValueQueue size is " + size);
        return false;
      }
      return true;
    }, 100, 3000);
  }

  /**
   * Verifies that Queue is initially filled to "numInitValues"
   */
  @Test(timeout=30000)
  public void testInitFill() throws Exception {
    MockFiller filler = new MockFiller();
    ValueQueue<String> vq =
        new ValueQueue<String>(10, 0.1f, 30000, 1,
            SyncGenerationPolicy.ALL, filler);
    Assert.assertEquals("test", vq.getNext("k1"));
    Assert.assertEquals(1, filler.getTop().num);
    vq.shutdown();
  }

  /**
   * Verifies that Queue is initialized (Warmed-up) for provided keys
   */
  @Test(timeout=30000)
  public void testWarmUp() throws Exception {
    MockFiller filler = new MockFiller();
    ValueQueue<String> vq =
        new ValueQueue<String>(10, 0.5f, 30000, 1,
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
  @Test(timeout=30000)
  public void testRefill() throws Exception {
    MockFiller filler = new MockFiller();
    ValueQueue<String> vq =
        new ValueQueue<String>(100, 0.1f, 30000, 1,
            SyncGenerationPolicy.ALL, filler);
    // Trigger a prefill (10) and an async refill (91)
    Assert.assertEquals("test", vq.getNext("k1"));
    Assert.assertEquals(10, filler.getTop().num);

    // Wait for the async task to finish
    waitForRefill(vq, "k1", 100);
    // Refill task should add 91 values to get to a full queue (10 produced by
    // the prefill to the low watermark, 1 consumed by getNext())
    Assert.assertEquals(91, filler.getTop().num);
    vq.shutdown();
  }

  /**
   * Verifies that the No refill Happens after "checkInterval" if
   * num values above "lowWatermark"
   */
  @Test(timeout=30000)
  public void testNoRefill() throws Exception {
    MockFiller filler = new MockFiller();
    ValueQueue<String> vq =
        new ValueQueue<String>(10, 0.5f, 30000, 1,
            SyncGenerationPolicy.ALL, filler);
    // Trigger a prefill (5) and an async refill (6)
    Assert.assertEquals("test", vq.getNext("k1"));
    Assert.assertEquals(5, filler.getTop().num);

    // Wait for the async task to finish
    waitForRefill(vq, "k1", 10);
    // Refill task should add 6 values to get to a full queue (5 produced by
    // the prefill to the low watermark, 1 consumed by getNext())
    Assert.assertEquals(6, filler.getTop().num);

    // Take another value, queue is still above the watermark
    Assert.assertEquals("test", vq.getNext("k1"));

    // Wait a while to make sure that no async refills are triggered
    try {
      waitForRefill(vq, "k1", 10);
    } catch (TimeoutException ignored) {
      // This is the correct outcome - no refill is expected
    }
    Assert.assertEquals(null, filler.getTop());
    vq.shutdown();
  }

  /**
   * Verify getAtMost when SyncGeneration Policy = ALL
   */
  @Test(timeout=30000)
  public void testgetAtMostPolicyALL() throws Exception {
    MockFiller filler = new MockFiller();
    final ValueQueue<String> vq =
        new ValueQueue<String>(10, 0.1f, 30000, 1,
            SyncGenerationPolicy.ALL, filler);
    // Trigger a prefill (1) and an async refill (10)
    Assert.assertEquals("test", vq.getNext("k1"));
    Assert.assertEquals(1, filler.getTop().num);

    // Wait for the async task to finish
    waitForRefill(vq, "k1", 10);
    // Refill task should add 10 values to get to a full queue (1 produced by
    // the prefill to the low watermark, 1 consumed by getNext())
    Assert.assertEquals(10, filler.getTop().num);

    // Drain completely, no further refills triggered
    vq.drain("k1");

    // Wait a while to make sure that no async refills are triggered
    try {
      waitForRefill(vq, "k1", 10);
    } catch (TimeoutException ignored) {
      // This is the correct outcome - no refill is expected
    }
    Assert.assertNull(filler.getTop());

    // Synchronous call:
    // 1. Synchronously fill returned list
    // 2. Start another async task to fill the queue in the cache
    Assert.assertEquals("Failed in sync call.", 10,
        vq.getAtMost("k1", 10).size());
    Assert.assertEquals("Sync call filler got wrong number.", 10,
        filler.getTop().num);

    // Wait for the async task to finish
    waitForRefill(vq, "k1", 10);
    // Refill task should add 10 values to get to a full queue
    Assert.assertEquals("Failed in async call.", 10, filler.getTop().num);

    // Drain completely after filled by the async thread
    vq.drain("k1");
    Assert.assertEquals("Failed to drain completely after async.", 0,
        vq.getSize("k1"));

    // Synchronous call
    Assert.assertEquals("Failed to get all 19.", 19,
        vq.getAtMost("k1", 19).size());
    Assert.assertEquals("Failed in sync call.", 19, filler.getTop().num);
    vq.shutdown();
  }

  /**
   * Verify getAtMost when SyncGeneration Policy = ALL
   */
  @Test(timeout=30000)
  public void testgetAtMostPolicyATLEAST_ONE() throws Exception {
    MockFiller filler = new MockFiller();
    ValueQueue<String> vq =
        new ValueQueue<String>(10, 0.3f, 30000, 1,
            SyncGenerationPolicy.ATLEAST_ONE, filler);
    // Trigger a prefill (3) and an async refill (8)
    Assert.assertEquals("test", vq.getNext("k1"));
    Assert.assertEquals(3, filler.getTop().num);

    // Wait for the async task to finish
    waitForRefill(vq, "k1", 10);
    // Refill task should add 8 values to get to a full queue (3 produced by
    // the prefill to the low watermark, 1 consumed by getNext())
    Assert.assertEquals("Failed in async call.", 8, filler.getTop().num);

    // Drain completely, no further refills triggered
    vq.drain("k1");

    // Queue is empty, sync will return a single value and trigger a refill
    Assert.assertEquals(1, vq.getAtMost("k1", 10).size());
    Assert.assertEquals(1, filler.getTop().num);

    // Wait for the async task to finish
    waitForRefill(vq, "k1", 10);
    // Refill task should add 10 values to get to a full queue
    Assert.assertEquals("Failed in async call.", 10, filler.getTop().num);
    vq.shutdown();
  }

  /**
   * Verify getAtMost when SyncGeneration Policy = LOW_WATERMARK
   */
  @Test(timeout=30000)
  public void testgetAtMostPolicyLOW_WATERMARK() throws Exception {
    MockFiller filler = new MockFiller();
    ValueQueue<String> vq =
        new ValueQueue<String>(10, 0.3f, 30000, 1,
            SyncGenerationPolicy.LOW_WATERMARK, filler);
    // Trigger a prefill (3) and an async refill (8)
    Assert.assertEquals("test", vq.getNext("k1"));
    Assert.assertEquals(3, filler.getTop().num);

    // Wait for the async task to finish
    waitForRefill(vq, "k1", 10);
    // Refill task should add 8 values to get to a full queue (3 produced by
    // the prefill to the low watermark, 1 consumed by getNext())
    Assert.assertEquals("Failed in async call.", 8, filler.getTop().num);

    // Drain completely, no further refills triggered
    vq.drain("k1");

    // Queue is empty, sync will return 3 values and trigger a refill
    Assert.assertEquals(3, vq.getAtMost("k1", 10).size());
    Assert.assertEquals(3, filler.getTop().num);

    // Wait for the async task to finish
    waitForRefill(vq, "k1", 10);
    // Refill task should add 10 values to get to a full queue
    Assert.assertEquals("Failed in async call.", 10, filler.getTop().num);
    vq.shutdown();
  }

  @Test(timeout=30000)
  public void testDrain() throws Exception {
    MockFiller filler = new MockFiller();
    ValueQueue<String> vq =
        new ValueQueue<String>(10, 0.1f, 30000, 1,
            SyncGenerationPolicy.ALL, filler);
    // Trigger a prefill (1) and an async refill (10)
    Assert.assertEquals("test", vq.getNext("k1"));
    Assert.assertEquals(1, filler.getTop().num);

    // Wait for the async task to finish
    waitForRefill(vq, "k1", 10);
    // Refill task should add 10 values to get to a full queue (1 produced by
    // the prefill to the low watermark, 1 consumed by getNext())
    Assert.assertEquals(10, filler.getTop().num);

    // Drain completely, no further refills triggered
    vq.drain("k1");

    // Wait a while to make sure that no async refills are triggered
    try {
      waitForRefill(vq, "k1", 10);
    } catch (TimeoutException ignored) {
      // This is the correct outcome - no refill is expected
    }
    Assert.assertNull(filler.getTop());
    vq.shutdown();
  }

}
