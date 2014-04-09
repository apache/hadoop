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

package org.apache.hadoop.ipc;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Test;

public class TestCallQueueManager {
  private CallQueueManager<FakeCall> manager;

  public class FakeCall {
    public final int tag; // Can be used for unique identification

    public FakeCall(int tag) {
      this.tag = tag;
    }
  }

  /**
   * Putter produces FakeCalls
   */
  public class Putter implements Runnable {
    private final CallQueueManager<FakeCall> cq;

    public final int tag;
    public volatile int callsAdded = 0; // How many calls we added, accurate unless interrupted
    private final int maxCalls;

    private volatile boolean isRunning = true;

    public Putter(CallQueueManager<FakeCall> aCq, int maxCalls, int tag) {
      this.maxCalls = maxCalls;
      this.cq = aCq;
      this.tag = tag;
    }

    public void run() {
      try {
        // Fill up to max (which is infinite if maxCalls < 0)
        while (isRunning && (callsAdded < maxCalls || maxCalls < 0)) {
          cq.put(new FakeCall(this.tag));
          callsAdded++;
        }
      } catch (InterruptedException e) {
        return;
      }
    }

    public void stop() {
      this.isRunning = false;
    }
  }

  /**
   * Taker consumes FakeCalls
   */
  public class Taker implements Runnable {
    private final CallQueueManager<FakeCall> cq;

    public final int tag; // if >= 0 means we will only take the matching tag, and put back
                          // anything else
    public volatile int callsTaken = 0; // total calls taken, accurate if we aren't interrupted
    public volatile FakeCall lastResult = null; // the last thing we took
    private final int maxCalls; // maximum calls to take

    public Taker(CallQueueManager<FakeCall> aCq, int maxCalls, int tag) {
      this.maxCalls = maxCalls;
      this.cq = aCq;
      this.tag = tag;
    }

    public void run() {
      try {
        // Take while we don't exceed maxCalls, or if maxCalls is undefined (< 0)
        while (callsTaken < maxCalls || maxCalls < 0) {
          FakeCall res = cq.take();

          if (tag >= 0 && res.tag != this.tag) {
            // This call does not match our tag, we should put it back and try again
            cq.put(res);
          } else {
            callsTaken++;
            lastResult = res;
          }
        }
      } catch (InterruptedException e) {
        return;
      }
    }
  }

  // Assert we can take exactly the numberOfTakes
  public void assertCanTake(CallQueueManager<FakeCall> cq, int numberOfTakes,
    int takeAttempts) throws InterruptedException {

    Taker taker = new Taker(cq, takeAttempts, -1);
    Thread t = new Thread(taker);
    t.start();
    t.join(100);

    assertEquals(taker.callsTaken, numberOfTakes);
    t.interrupt();
  }

  // Assert we can put exactly the numberOfPuts
  public void assertCanPut(CallQueueManager<FakeCall> cq, int numberOfPuts,
    int putAttempts) throws InterruptedException {

    Putter putter = new Putter(cq, putAttempts, -1);
    Thread t = new Thread(putter);
    t.start();
    t.join(100);

    assertEquals(putter.callsAdded, numberOfPuts);
    t.interrupt();
  }


  private static final Class<? extends BlockingQueue<FakeCall>> queueClass
      = CallQueueManager.convertQueueClass(LinkedBlockingQueue.class, FakeCall.class);

  @Test
  public void testCallQueueCapacity() throws InterruptedException {
    manager = new CallQueueManager<FakeCall>(queueClass, 10, "", null);

    assertCanPut(manager, 10, 20); // Will stop at 10 due to capacity
  }

  @Test
  public void testEmptyConsume() throws InterruptedException {
    manager = new CallQueueManager<FakeCall>(queueClass, 10, "", null);

    assertCanTake(manager, 0, 1); // Fails since it's empty
  }

  @Test(timeout=60000)
  public void testSwapUnderContention() throws InterruptedException {
    manager = new CallQueueManager<FakeCall>(queueClass, 5000, "", null);

    ArrayList<Putter> producers = new ArrayList<Putter>();
    ArrayList<Taker> consumers = new ArrayList<Taker>();

    HashMap<Runnable, Thread> threads = new HashMap<Runnable, Thread>();

    // Create putters and takers
    for (int i=0; i < 50; i++) {
      Putter p = new Putter(manager, -1, -1);
      Thread pt = new Thread(p);
      producers.add(p);
      threads.put(p, pt);

      pt.start();
    }

    for (int i=0; i < 20; i++) {
      Taker t = new Taker(manager, -1, -1);
      Thread tt = new Thread(t);
      consumers.add(t);
      threads.put(t, tt);

      tt.start();
    }

    Thread.sleep(10);

    for (int i=0; i < 5; i++) {
      manager.swapQueue(queueClass, 5000, "", null);
    }

    // Stop the producers
    for (Putter p : producers) {
      p.stop();
    }

    // Wait for consumers to wake up, then consume
    Thread.sleep(2000);
    assertEquals(0, manager.size());

    // Ensure no calls were dropped
    long totalCallsCreated = 0;
    for (Putter p : producers) {
      threads.get(p).interrupt();
    }
    for (Putter p : producers) {
      threads.get(p).join();
      totalCallsCreated += p.callsAdded;
    }
    
    long totalCallsConsumed = 0;
    for (Taker t : consumers) {
      threads.get(t).interrupt();
    }
    for (Taker t : consumers) {
      threads.get(t).join();
      totalCallsConsumed += t.callsTaken;
    }

    assertEquals(totalCallsConsumed, totalCallsCreated);
  }
}