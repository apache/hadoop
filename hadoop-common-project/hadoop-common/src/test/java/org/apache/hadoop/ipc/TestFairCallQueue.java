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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import junit.framework.TestCase;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;
import org.mockito.Matchers;

import static org.apache.hadoop.ipc.FairCallQueue.IPC_CALLQUEUE_PRIORITY_LEVELS_KEY;

public class TestFairCallQueue extends TestCase {
  private FairCallQueue<Schedulable> fcq;

  private Schedulable mockCall(String id) {
    Schedulable mockCall = mock(Schedulable.class);
    UserGroupInformation ugi = mock(UserGroupInformation.class);

    when(ugi.getUserName()).thenReturn(id);
    when(mockCall.getUserGroupInformation()).thenReturn(ugi);

    return mockCall;
  }

  // A scheduler which always schedules into priority zero
  private RpcScheduler alwaysZeroScheduler;
  {
    RpcScheduler sched = mock(RpcScheduler.class);
    when(sched.getPriorityLevel(Matchers.<Schedulable>any())).thenReturn(0); // always queue 0
    alwaysZeroScheduler = sched;
  }

  public void setUp() {
    Configuration conf = new Configuration();
    conf.setInt("ns." + IPC_CALLQUEUE_PRIORITY_LEVELS_KEY, 2);

    fcq = new FairCallQueue<Schedulable>(10, "ns", conf);
  }

  //
  // Ensure that FairCallQueue properly implements BlockingQueue
  //
  public void testPollReturnsNullWhenEmpty() {
    assertNull(fcq.poll());
  }

  public void testPollReturnsTopCallWhenNotEmpty() {
    Schedulable call = mockCall("c");
    assertTrue(fcq.offer(call));

    assertEquals(call, fcq.poll());

    // Poll took it out so the fcq is empty
    assertEquals(0, fcq.size());
  }

  public void testOfferSucceeds() {
    fcq.setScheduler(alwaysZeroScheduler);

    for (int i = 0; i < 5; i++) {
      // We can fit 10 calls
      assertTrue(fcq.offer(mockCall("c")));
    }

    assertEquals(5, fcq.size());
  }

  public void testOfferFailsWhenFull() {
    fcq.setScheduler(alwaysZeroScheduler);
    for (int i = 0; i < 5; i++) { assertTrue(fcq.offer(mockCall("c"))); }

    assertFalse(fcq.offer(mockCall("c"))); // It's full

    assertEquals(5, fcq.size());
  }

  public void testOfferSucceedsWhenScheduledLowPriority() {
    // Scheduler will schedule into queue 0 x 5, then queue 1
    RpcScheduler sched = mock(RpcScheduler.class);
    when(sched.getPriorityLevel(Matchers.<Schedulable>any())).thenReturn(0, 0, 0, 0, 0, 1, 0);
    fcq.setScheduler(sched);
    for (int i = 0; i < 5; i++) { assertTrue(fcq.offer(mockCall("c"))); }

    assertTrue(fcq.offer(mockCall("c")));

    assertEquals(6, fcq.size());
  }

  public void testPeekNullWhenEmpty() {
    assertNull(fcq.peek());
  }

  public void testPeekNonDestructive() {
    Schedulable call = mockCall("c");
    assertTrue(fcq.offer(call));

    assertEquals(call, fcq.peek());
    assertEquals(call, fcq.peek()); // Non-destructive
    assertEquals(1, fcq.size());
  }

  public void testPeekPointsAtHead() {
    Schedulable call = mockCall("c");
    Schedulable next = mockCall("b");
    fcq.offer(call);
    fcq.offer(next);

    assertEquals(call, fcq.peek()); // Peek points at the head
  }

  public void testPollTimeout() throws InterruptedException {
    fcq.setScheduler(alwaysZeroScheduler);

    assertNull(fcq.poll(10, TimeUnit.MILLISECONDS));
  }

  public void testPollSuccess() throws InterruptedException {
    fcq.setScheduler(alwaysZeroScheduler);

    Schedulable call = mockCall("c");
    assertTrue(fcq.offer(call));

    assertEquals(call, fcq.poll(10, TimeUnit.MILLISECONDS));

    assertEquals(0, fcq.size());
  }

  public void testOfferTimeout() throws InterruptedException {
    fcq.setScheduler(alwaysZeroScheduler);
    for (int i = 0; i < 5; i++) {
      assertTrue(fcq.offer(mockCall("c"), 10, TimeUnit.MILLISECONDS));
    }

    assertFalse(fcq.offer(mockCall("e"), 10, TimeUnit.MILLISECONDS)); // It's full

    assertEquals(5, fcq.size());
  }

  public void testDrainTo() {
    Configuration conf = new Configuration();
    conf.setInt("ns." + IPC_CALLQUEUE_PRIORITY_LEVELS_KEY, 2);
    FairCallQueue<Schedulable> fcq2 = new FairCallQueue<Schedulable>(10, "ns", conf);

    fcq.setScheduler(alwaysZeroScheduler);
    fcq2.setScheduler(alwaysZeroScheduler);

    // Start with 3 in fcq, to be drained
    for (int i = 0; i < 3; i++) {
      fcq.offer(mockCall("c"));
    }

    fcq.drainTo(fcq2);

    assertEquals(0, fcq.size());
    assertEquals(3, fcq2.size());
  }

  public void testDrainToWithLimit() {
    Configuration conf = new Configuration();
    conf.setInt("ns." + IPC_CALLQUEUE_PRIORITY_LEVELS_KEY, 2);
    FairCallQueue<Schedulable> fcq2 = new FairCallQueue<Schedulable>(10, "ns", conf);

    fcq.setScheduler(alwaysZeroScheduler);
    fcq2.setScheduler(alwaysZeroScheduler);

    // Start with 3 in fcq, to be drained
    for (int i = 0; i < 3; i++) {
      fcq.offer(mockCall("c"));
    }

    fcq.drainTo(fcq2, 2);

    assertEquals(1, fcq.size());
    assertEquals(2, fcq2.size());
  }

  public void testInitialRemainingCapacity() {
    assertEquals(10, fcq.remainingCapacity());
  }

  public void testFirstQueueFullRemainingCapacity() {
    fcq.setScheduler(alwaysZeroScheduler);
    while (fcq.offer(mockCall("c"))) ; // Queue 0 will fill up first, then queue 1

    assertEquals(5, fcq.remainingCapacity());
  }

  public void testAllQueuesFullRemainingCapacity() {
    RpcScheduler sched = mock(RpcScheduler.class);
    when(sched.getPriorityLevel(Matchers.<Schedulable>any())).thenReturn(0, 0, 0, 0, 0, 1, 1, 1, 1, 1);
    fcq.setScheduler(sched);
    while (fcq.offer(mockCall("c"))) ;

    assertEquals(0, fcq.remainingCapacity());
    assertEquals(10, fcq.size());
  }

  public void testQueuesPartialFilledRemainingCapacity() {
    RpcScheduler sched = mock(RpcScheduler.class);
    when(sched.getPriorityLevel(Matchers.<Schedulable>any())).thenReturn(0, 1, 0, 1, 0);
    fcq.setScheduler(sched);
    for (int i = 0; i < 5; i++) { fcq.offer(mockCall("c")); }

    assertEquals(5, fcq.remainingCapacity());
    assertEquals(5, fcq.size());
  }

  /**
   * Putter produces FakeCalls
   */
  public class Putter implements Runnable {
    private final BlockingQueue<Schedulable> cq;

    public final String tag;
    public volatile int callsAdded = 0; // How many calls we added, accurate unless interrupted
    private final int maxCalls;
    private final CountDownLatch latch;

    public Putter(BlockingQueue<Schedulable> aCq, int maxCalls, String tag,
                  CountDownLatch latch) {
      this.maxCalls = maxCalls;
      this.cq = aCq;
      this.tag = tag;
      this.latch = latch;
    }

    private String getTag() {
      if (this.tag != null) return this.tag;
      return "";
    }

    @Override
    public void run() {
      try {
        // Fill up to max (which is infinite if maxCalls < 0)
        while (callsAdded < maxCalls || maxCalls < 0) {
          cq.put(mockCall(getTag()));
          callsAdded++;
          latch.countDown();
        }
      } catch (InterruptedException e) {
        return;
      }
    }
  }

  /**
   * Taker consumes FakeCalls
   */
  public class Taker implements Runnable {
    private final BlockingQueue<Schedulable> cq;

    public final String tag; // if >= 0 means we will only take the matching tag, and put back
                          // anything else
    public volatile int callsTaken = 0; // total calls taken, accurate if we aren't interrupted
    public volatile Schedulable lastResult = null; // the last thing we took
    private final int maxCalls; // maximum calls to take
    private final CountDownLatch latch;

    private IdentityProvider uip;

    public Taker(BlockingQueue<Schedulable> aCq, int maxCalls, String tag,
                 CountDownLatch latch) {
      this.maxCalls = maxCalls;
      this.cq = aCq;
      this.tag = tag;
      this.uip = new UserIdentityProvider();
      this.latch = latch;
    }

    @Override
    public void run() {
      try {
        // Take while we don't exceed maxCalls, or if maxCalls is undefined (< 0)
        while (callsTaken < maxCalls || maxCalls < 0) {
          Schedulable res = cq.take();
          String identity = uip.makeIdentity(res);

          if (tag != null && this.tag.equals(identity)) {
            // This call does not match our tag, we should put it back and try again
            cq.put(res);
          } else {
            callsTaken++;
            latch.countDown();
            lastResult = res;
          }
        }
      } catch (InterruptedException e) {
        return;
      }
    }
  }

  // Assert we can take exactly the numberOfTakes
  public void assertCanTake(BlockingQueue<Schedulable> cq, int numberOfTakes,
    int takeAttempts) throws InterruptedException {

    CountDownLatch latch = new CountDownLatch(numberOfTakes);
    Taker taker = new Taker(cq, takeAttempts, "default", latch);
    Thread t = new Thread(taker);
    t.start();
    latch.await();

    assertEquals(numberOfTakes, taker.callsTaken);
    t.interrupt();
  }

  // Assert we can put exactly the numberOfPuts
  public void assertCanPut(BlockingQueue<Schedulable> cq, int numberOfPuts,
    int putAttempts) throws InterruptedException {

    CountDownLatch latch = new CountDownLatch(numberOfPuts);
    Putter putter = new Putter(cq, putAttempts, null, latch);
    Thread t = new Thread(putter);
    t.start();
    latch.await();

    assertEquals(numberOfPuts, putter.callsAdded);
    t.interrupt();
  }

  // Make sure put will overflow into lower queues when the top is full
  public void testPutOverflows() throws InterruptedException {
    fcq.setScheduler(alwaysZeroScheduler);

    // We can fit more than 5, even though the scheduler suggests the top queue
    assertCanPut(fcq, 8, 8);
    assertEquals(8, fcq.size());
  }

  public void testPutBlocksWhenAllFull() throws InterruptedException {
    fcq.setScheduler(alwaysZeroScheduler);

    assertCanPut(fcq, 10, 10); // Fill up
    assertEquals(10, fcq.size());

    // Put more which causes overflow
    assertCanPut(fcq, 0, 1); // Will block
  }

  public void testTakeBlocksWhenEmpty() throws InterruptedException {
    fcq.setScheduler(alwaysZeroScheduler);
    assertCanTake(fcq, 0, 1);
  }

  public void testTakeRemovesCall() throws InterruptedException {
    fcq.setScheduler(alwaysZeroScheduler);
    Schedulable call = mockCall("c");
    fcq.offer(call);

    assertEquals(call, fcq.take());
    assertEquals(0, fcq.size());
  }

  public void testTakeTriesNextQueue() throws InterruptedException {
    // Make a FCQ filled with calls in q 1 but empty in q 0
    RpcScheduler q1Scheduler = mock(RpcScheduler.class);
    when(q1Scheduler.getPriorityLevel(Matchers.<Schedulable>any())).thenReturn(1);
    fcq.setScheduler(q1Scheduler);

    // A mux which only draws from q 0
    RpcMultiplexer q0mux = mock(RpcMultiplexer.class);
    when(q0mux.getAndAdvanceCurrentIndex()).thenReturn(0);
    fcq.setMultiplexer(q0mux);

    Schedulable call = mockCall("c");
    fcq.put(call);

    // Take from q1 even though mux said q0, since q0 empty
    assertEquals(call, fcq.take());
    assertEquals(0, fcq.size());
  }

  public void testFairCallQueueMXBean() throws Exception {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName mxbeanName = new ObjectName(
        "Hadoop:service=ns,name=FairCallQueue");

    Schedulable call = mockCall("c");
    fcq.put(call);
    int[] queueSizes = (int[]) mbs.getAttribute(mxbeanName, "QueueSizes");
    assertEquals(1, queueSizes[0]);
    assertEquals(0, queueSizes[1]);
    fcq.take();
    queueSizes = (int[]) mbs.getAttribute(mxbeanName, "QueueSizes");
    assertEquals(0, queueSizes[0]);
    assertEquals(0, queueSizes[1]);
  }
}
