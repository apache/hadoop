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

package org.apache.hadoop.metrics2.impl;

import java.util.ConcurrentModificationException;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.apache.hadoop.metrics2.impl.SinkQueue.*;

/**
 * Test the half-blocking metrics sink queue
 */
public class TestSinkQueue {
  private static final Log LOG = LogFactory.getLog(TestSinkQueue.class);

  /**
   * Test common use case
   * @throws Exception
   */
  @Test public void testCommon() throws Exception {
    final SinkQueue<Integer> q = new SinkQueue<Integer>(2);
    q.enqueue(1);
    assertEquals("queue front", 1, (int) q.front());
    assertEquals("queue back", 1, (int) q.back());
    assertEquals("element", 1, (int) q.dequeue());

    assertTrue("should enqueue", q.enqueue(2));
    q.consume(new Consumer<Integer>() {
      @Override public void consume(Integer e) {
        assertEquals("element", 2, (int) e);
      }
    });
    assertTrue("should enqueue", q.enqueue(3));
    assertEquals("element", 3, (int) q.dequeue());
    assertEquals("queue size", 0, q.size());
    assertEquals("queue front", null, q.front());
    assertEquals("queue back", null, q.back());
  }

  /**
   * Test blocking when queue is empty
   * @throws Exception
   */
  @Test public void testEmptyBlocking() throws Exception {
    testEmptyBlocking(0);
    testEmptyBlocking(100);
  }

  private void testEmptyBlocking(int awhile) throws Exception {
    final SinkQueue<Integer> q = new SinkQueue<Integer>(2);
    final Runnable trigger = mock(Runnable.class);
    // try consuming emtpy equeue and blocking
    Thread t = new Thread() {
      @Override public void run() {
        try {
          assertEquals("element", 1, (int) q.dequeue());
          q.consume(new Consumer<Integer>() {
            @Override public void consume(Integer e) {
              assertEquals("element", 2, (int) e);
              trigger.run();
            }
          });
        }
        catch (InterruptedException e) {
          LOG.warn("Interrupted", e);
        }
      }
    };
    t.start();
    // Should work with or without sleep
    if (awhile > 0) {
      Thread.sleep(awhile);
    }
    q.enqueue(1);
    q.enqueue(2);
    t.join();
    verify(trigger).run();
  }

  /**
   * Test nonblocking enqueue when queue is full
   * @throws Exception
   */
  @Test public void testFull() throws Exception {
    final SinkQueue<Integer> q = new SinkQueue<Integer>(1);
    q.enqueue(1);

    assertTrue("should drop", !q.enqueue(2));
    assertEquals("element", 1, (int) q.dequeue());

    q.enqueue(3);
    q.consume(new Consumer<Integer>() {
      @Override public void consume(Integer e) {
        assertEquals("element", 3, (int) e);
      }
    });
    assertEquals("queue size", 0, q.size());
  }

  /**
   * Test the consumeAll method
   * @throws Exception
   */
  @Test public void testConsumeAll() throws Exception {
    final int capacity = 64;  // arbitrary
    final SinkQueue<Integer> q = new SinkQueue<Integer>(capacity);

    for (int i = 0; i < capacity; ++i) {
      assertTrue("should enqueue", q.enqueue(i));
    }
    assertTrue("should not enqueue", !q.enqueue(capacity));

    final Runnable trigger = mock(Runnable.class);
    q.consumeAll(new Consumer<Integer>() {
      private int expected = 0;
      @Override public void consume(Integer e) {
        assertEquals("element", expected++, (int) e);
        trigger.run();
      }
    });

    verify(trigger, times(capacity)).run();
  }

  /**
   * Test the consumer throwing exceptions
   * @throws Exception
   */
  @Test public void testConsumerException() throws Exception {
    final SinkQueue<Integer> q = new SinkQueue<Integer>(1);
    final RuntimeException ex = new RuntimeException("expected");
    q.enqueue(1);

    try {
      q.consume(new Consumer<Integer>() {
        @Override public void consume(Integer e) {
          throw ex;
        }
      });
    }
    catch (Exception expected) {
      assertSame("consumer exception", ex, expected);
    }
    // The queue should be in consistent state after exception
    assertEquals("queue size", 1, q.size());
    assertEquals("element", 1, (int) q.dequeue());
  }

  /**
   * Test the clear method
   */
  @Test public void testClear() {
    final SinkQueue<Integer> q = new SinkQueue<Integer>(128);
    for (int i = 0; i < q.capacity() + 97; ++i) {
      q.enqueue(i);
    }
    assertEquals("queue size", q.capacity(), q.size());
    q.clear();
    assertEquals("queue size", 0, q.size());
  }

  /**
   * Test consumers that take their time.
   * @throws Exception
   */
  @Test public void testHangingConsumer() throws Exception {
    SinkQueue<Integer> q = newSleepingConsumerQueue(2, 1, 2);
    assertEquals("queue back", 2, (int) q.back());
    assertTrue("should drop", !q.enqueue(3)); // should not block
    assertEquals("queue size", 2, q.size());
    assertEquals("queue head", 1, (int) q.front());
    assertEquals("queue back", 2, (int) q.back());
  }

  /**
   * Test concurrent consumer access, which is illegal
   * @throws Exception
   */
  @Test public void testConcurrentConsumers() throws Exception {
    final SinkQueue<Integer> q = newSleepingConsumerQueue(2, 1);
    assertTrue("should enqueue", q.enqueue(2));
    assertEquals("queue back", 2, (int) q.back());
    assertTrue("should drop", !q.enqueue(3)); // should not block
    shouldThrowCME(new Fun() {
      @Override public void run() {
        q.clear();
      }
    });
    shouldThrowCME(new Fun() {
      @Override public void run() throws Exception {
        q.consume(null);
      }
    });
    shouldThrowCME(new Fun() {
      @Override public void run() throws Exception {
        q.consumeAll(null);
      }
    });
    shouldThrowCME(new Fun() {
      @Override public void run() throws Exception {
        q.dequeue();
      }
    });
    // The queue should still be in consistent state after all the exceptions
    assertEquals("queue size", 2, q.size());
    assertEquals("queue front", 1, (int) q.front());
    assertEquals("queue back", 2, (int) q.back());
  }

  private void shouldThrowCME(Fun callback) throws Exception {
    try {
      callback.run();
    }
    catch (ConcurrentModificationException e) {
      LOG.info(e);
      return;
    }
    LOG.error("should've thrown CME");
    fail("should've thrown CME");
  }

  private SinkQueue<Integer> newSleepingConsumerQueue(int capacity,
      int... values) throws Exception {
    final SinkQueue<Integer> q = new SinkQueue<Integer>(capacity);
    for (int i : values) {
      q.enqueue(i);
    }
    final CountDownLatch barrier = new CountDownLatch(1);
    Thread t = new Thread() {
      @Override public void run() {
        try {
          Thread.sleep(10); // causes failure without barrier
          q.consume(new Consumer<Integer>() {
            @Override
            public void consume(Integer e) throws InterruptedException {
              LOG.info("sleeping");
              barrier.countDown();
              Thread.sleep(1000 * 86400); // a long time
            }
          });
        }
        catch (InterruptedException ex) {
          LOG.warn("Interrupted", ex);
        }
      }
    };
    t.setName("Sleeping consumer");
    t.setDaemon(true);  // so jvm can exit
    t.start();
    barrier.await();
    LOG.debug("Returning new sleeping consumer queue");
    return q;
  }

  static interface Fun {
    void run() throws Exception;
  }
}
