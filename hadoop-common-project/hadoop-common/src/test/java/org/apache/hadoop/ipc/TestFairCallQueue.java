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

import static org.apache.hadoop.test.MetricsAsserts.assertGauge;
import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import org.apache.hadoop.security.UserGroupInformation;
import org.mockito.Mockito;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.CallQueueManager.CallQueueOverflowException;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;

import static org.assertj.core.api.Assertions.assertThat;

public class TestFairCallQueue {
  private FairCallQueue<Schedulable> fcq;

  private Schedulable mockCall(String id, int priority) {
    Schedulable mockCall = mock(Schedulable.class);
    UserGroupInformation ugi = mock(UserGroupInformation.class);

    when(ugi.getUserName()).thenReturn(id);
    when(mockCall.getUserGroupInformation()).thenReturn(ugi);
    when(mockCall.getPriorityLevel()).thenReturn(priority);
    when(mockCall.toString()).thenReturn("id=" + id + " priority=" + priority);

    return mockCall;
  }

  private Schedulable mockCall(String id) {
    return mockCall(id, 0);
  }

  @SuppressWarnings("deprecation")
  @Before
  public void setUp() {
    Configuration conf = new Configuration();
    conf.setInt("ns." + FairCallQueue.IPC_CALLQUEUE_PRIORITY_LEVELS_KEY, 2);

    fcq = new FairCallQueue<Schedulable>(2, 10, "ns", conf);
  }

  // Validate that the total capacity of all subqueues equals
  // the maxQueueSize for different values of maxQueueSize
  @Test
  public void testTotalCapacityOfSubQueues() {
    Configuration conf = new Configuration();
    FairCallQueue<Schedulable> fairCallQueue;
    fairCallQueue = new FairCallQueue<Schedulable>(1, 1000, "ns", conf);
    assertThat(fairCallQueue.remainingCapacity()).isEqualTo(1000);
    fairCallQueue = new FairCallQueue<Schedulable>(4, 1000, "ns", conf);
    assertThat(fairCallQueue.remainingCapacity()).isEqualTo(1000);
    fairCallQueue = new FairCallQueue<Schedulable>(7, 1000, "ns", conf);
    assertThat(fairCallQueue.remainingCapacity()).isEqualTo(1000);
    fairCallQueue = new FairCallQueue<Schedulable>(1, 1025, "ns", conf);
    assertThat(fairCallQueue.remainingCapacity()).isEqualTo(1025);
    fairCallQueue = new FairCallQueue<Schedulable>(4, 1025, "ns", conf);
    assertThat(fairCallQueue.remainingCapacity()).isEqualTo(1025);
    fairCallQueue = new FairCallQueue<Schedulable>(7, 1025, "ns", conf);
    assertThat(fairCallQueue.remainingCapacity()).isEqualTo(1025);
    fairCallQueue = new FairCallQueue<Schedulable>(7, 1025, "ns",
        new int[]{7, 6, 5, 4, 3, 2, 1}, false, conf);
    assertThat(fairCallQueue.remainingCapacity()).isEqualTo(1025);

    // Total capacity is shared queues + reserved queues
    conf.setStrings("ns." + CommonConfigurationKeys.IPC_CALLQUEUE_RESERVED_USERS_KEY, "user1", "user2");
    conf.setStrings("ns." + CommonConfigurationKeys.IPC_CALLQUEUE_RESERVED_USERS_CAPACITIES_KEY, "1000", "1000");
    fairCallQueue = new FairCallQueue<>(4, 1000, "ns", conf);
    assertEquals(fairCallQueue.remainingCapacity(), 3000);
  }

  @Test
  public void testParseReservedQueueCapacities() {
    Configuration conf = new Configuration();
    conf.setStrings("ns." + CommonConfigurationKeys.IPC_CALLQUEUE_RESERVED_USERS_KEY, "user1", "user2");
    conf.setStrings("ns." + CommonConfigurationKeys.IPC_CALLQUEUE_RESERVED_USERS_CAPACITIES_KEY, "1");
    try {
      FairCallQueue<Schedulable> fairCallQueue = new FairCallQueue<>(1, 1000, "ns", conf);
      fail("didn't fail");
    } catch (IllegalArgumentException ex) {
      // expect exception
    }

  }

  @Test
  public void testParseReservedUsersExceedLimit() {
    Configuration conf = new Configuration();
    conf.setStrings("ns." + CommonConfigurationKeys.IPC_CALLQUEUE_RESERVED_USERS_KEY, "user1", "user2", "user3", "user4");
    conf.setStrings("ns." + CommonConfigurationKeys.IPC_CALLQUEUE_RESERVED_USERS_MAX_KEY, "3");
    try {
      FairCallQueue<Schedulable> fairCallQueue = new FairCallQueue<>(4, 1000, "ns", conf);
      fail("didn't fail");
    } catch (IllegalArgumentException ex) {
      // expect exception
    }
  }

  @Test
  public void testPrioritization() {
    int numQueues = 10;
    Configuration conf = new Configuration();
    fcq = new FairCallQueue<Schedulable>(numQueues, numQueues, "ns", conf);

    //Schedulable[] calls = new Schedulable[numCalls];
    List<Schedulable> calls = new ArrayList<>();
    for (int i=0; i < numQueues; i++) {
      Schedulable call = mockCall("u", i);
      calls.add(call);
      fcq.add(call);
    }

    final AtomicInteger currentIndex = new AtomicInteger();
    fcq.setMultiplexer(new RpcMultiplexer(){
      @Override
      public int getAndAdvanceCurrentIndex() {
        return currentIndex.get();
      }
    });

    // if there is no call at a given index, return the next highest
    // priority call available.
    //   v
    //0123456789
    currentIndex.set(3);
    assertSame(calls.get(3), fcq.poll());
    assertSame(calls.get(0), fcq.poll());
    assertSame(calls.get(1), fcq.poll());
    //      v
    //--2-456789
    currentIndex.set(6);
    assertSame(calls.get(6), fcq.poll());
    assertSame(calls.get(2), fcq.poll());
    assertSame(calls.get(4), fcq.poll());
    //        v
    //-----5-789
    currentIndex.set(8);
    assertSame(calls.get(8), fcq.poll());
    //         v
    //-----5-7-9
    currentIndex.set(9);
    assertSame(calls.get(9), fcq.poll());
    assertSame(calls.get(5), fcq.poll());
    assertSame(calls.get(7), fcq.poll());
    //----------
    assertNull(fcq.poll());
    assertNull(fcq.poll());
  }

  @Test
  public void testPrioritizationWithReservedUsers() {
    int numQueues = 10;
    Configuration conf = new Configuration();
    conf.setStrings("ns." + CommonConfigurationKeys.IPC_CALLQUEUE_RESERVED_USERS_KEY, "u1", "u2");
    FairCallQueue<Schedulable> fcq = new FairCallQueue<>(numQueues, numQueues, "ns", conf);

    //Schedulable[] calls = new Schedulable[numCalls];
    List<Schedulable> calls = new ArrayList<>();
    for (int i=0; i < numQueues; i++) {
      Schedulable call = mockCall("u", i);
      calls.add(call);
      fcq.add(call);
    }
    // Reserved users
    Schedulable call1 = mockCall("u1", numQueues);
    Schedulable call2 = mockCall("u2", numQueues + 1);
    fcq.add(call1);
    fcq.add(call2);

    final AtomicInteger currentIndex = new AtomicInteger();
    fcq.setMultiplexer(new RpcMultiplexer(){
      @Override
      public int getAndAdvanceCurrentIndex() {
        return currentIndex.get();
      }
    });

    //   v
    //0123456789
    currentIndex.set(3);
    assertEquals(calls.get(3), fcq.poll());
    assertEquals(calls.get(0), fcq.poll());
    assertEquals(calls.get(1), fcq.poll());

    //            v
    //--2-456789u1u2
    currentIndex.set(10);
    assertEquals(call1, fcq.poll());
    assertEquals(calls.get(2), fcq.poll());
    assertEquals(calls.get(4), fcq.poll());

    //        v
    //-----56789-u2
    currentIndex.set(8);
    assertEquals(calls.get(8), fcq.poll());
    assertEquals(calls.get(5), fcq.poll());
    assertEquals(calls.get(6), fcq.poll());
    assertEquals(calls.get(7), fcq.poll());

    //        v
    //--------9-u2
    currentIndex.set(9);
    assertEquals(calls.get(9), fcq.poll());
    assertEquals(call2, fcq.poll());

    //------------
    assertNull(fcq.poll());
    assertNull(fcq.poll());
  }

  @SuppressWarnings("unchecked") // for mock reset.
  @Test
  public void testInsertionWithReservedUser() throws Exception {
    Configuration conf = new Configuration();
    conf.setStrings("ns." + CommonConfigurationKeys.IPC_CALLQUEUE_RESERVED_USERS_KEY, "u1", "u2");
    FairCallQueue<Schedulable> fcq = Mockito.spy(new FairCallQueue<>(3, 6, "ns", conf));

    Schedulable p0 = mockCall("a", 0);
    Schedulable p1 = mockCall("b", 1);
    Schedulable p2 = mockCall("c", 2);
    Schedulable u1 = mockCall("u1", 3);
    Schedulable u2 = mockCall("u2", 4);

    // add to first queue.
    Mockito.reset(fcq);
    fcq.add(p0);
    Mockito.verify(fcq, times(1)).offerQueue(0, p0);
    Mockito.verify(fcq, times(0)).offerQueue(1, p0);
    Mockito.verify(fcq, times(0)).offerQueue(2, p0);
    Mockito.reset(fcq);
    // 0:x- 1:-- 2:-- u1:-- u2:--

    // add to second queue.
    Mockito.reset(fcq);
    fcq.add(p1);
    Mockito.verify(fcq, times(0)).offerQueue(0, p1);
    Mockito.verify(fcq, times(1)).offerQueue(1, p1);
    Mockito.verify(fcq, times(0)).offerQueue(2, p1);
    Mockito.reset(fcq);
    // 0:x- 1:x- 2:-- u1:-- u2:--

    // add to first queue.
    Mockito.reset(fcq);
    fcq.add(p0);
    Mockito.verify(fcq, times(1)).offerQueue(0, p0);
    Mockito.verify(fcq, times(0)).offerQueue(1, p0);
    Mockito.verify(fcq, times(0)).offerQueue(2, p0);
    // 0:xx 1:x- 2:-- u1:-- u2:--

    // add to first full queue overflow to second.
    Mockito.reset(fcq);
    fcq.add(p0);
    Mockito.verify(fcq, times(1)).offerQueue(0, p0);
    Mockito.verify(fcq, times(1)).offerQueue(1, p0);
    Mockito.verify(fcq, times(0)).offerQueue(2, p0);
    // 0:xx 1:xx 2:-- u1:-- u2:--

    // add to u1 dedicated queue
    Mockito.reset(fcq);
    fcq.add(u1);
    Mockito.verify(fcq, times(0)).offerQueue(0, u1);
    Mockito.verify(fcq, times(0)).offerQueue(1, u1);
    Mockito.verify(fcq, times(0)).offerQueue(2, u1);
    Mockito.verify(fcq, times(1)).offerQueue(3, u1);
    Mockito.verify(fcq, times(0)).offerQueue(4, u2);
    // 0:xx 1:xx 2:-- u1:x- u2:--

    // add to u2 dedicated queue
    Mockito.reset(fcq);
    fcq.add(u2);
    Mockito.verify(fcq, times(0)).offerQueue(0, u2);
    Mockito.verify(fcq, times(0)).offerQueue(1, u2);
    Mockito.verify(fcq, times(0)).offerQueue(2, u2);
    Mockito.verify(fcq, times(0)).offerQueue(3, u2);
    Mockito.verify(fcq, times(1)).offerQueue(4, u2);
    // 0:xx 1:xx 2:-- u1:x- u2:x-

    // add to first full queue overflow to third.
    Mockito.reset(fcq);
    fcq.add(p0);
    Mockito.verify(fcq, times(1)).offerQueue(0, p0);
    Mockito.verify(fcq, times(1)).offerQueue(1, p0);
    Mockito.verify(fcq, times(1)).offerQueue(2, p0);
    // 0:xx 1:xx 2:x- u1:x- u2:x-

    // add to third
    Mockito.reset(fcq);
    fcq.add(p2);
    Mockito.verify(fcq, times(0)).offerQueue(0, p2);
    Mockito.verify(fcq, times(0)).offerQueue(1, p2);
    Mockito.verify(fcq, times(1)).offerQueue(2, p2);
    // 0:xx 1:xx 2:xx u1:x- u2:x-

    // make sure adding a call to full shared queues do not overflow to dedicated queue
    // and throw exception
    Mockito.reset(fcq);
    try {
      fcq.add(p0);
      fail("didn't fail");
    } catch (IllegalStateException ise) {
      checkOverflowException(ise, RpcStatusProto.ERROR, false);
    }
    Mockito.verify(fcq, times(1)).offerQueue(0, p0);
    Mockito.verify(fcq, times(1)).offerQueue(1, p0);
    Mockito.verify(fcq, times(1)).offerQueue(2, p0);
    // 0:xx 1:xx 2:xx u1:x- u2:x-

    // add to u1 dedicated queue
    Mockito.reset(fcq);
    fcq.add(u1);
    Mockito.verify(fcq, times(0)).offerQueue(0, u1);
    Mockito.verify(fcq, times(0)).offerQueue(1, u1);
    Mockito.verify(fcq, times(0)).offerQueue(2, u1);
    Mockito.verify(fcq, times(1)).offerQueue(3, u1);
    Mockito.verify(fcq, times(0)).offerQueue(4, u1);
    // 0:xx 1:xx 2:xx u1:xx u2:x-

    // add to full u1 dedicated queue should throw exception and not overflow
    // to shared queue
    Mockito.reset(fcq);
    try {
      fcq.add(u1);
      fail("didn't fail");
    } catch (IllegalStateException ise) {
      checkOverflowException(ise, RpcStatusProto.ERROR, false);
    }
    Mockito.verify(fcq, times(0)).offerQueue(0, u1);
    Mockito.verify(fcq, times(0)).offerQueue(1, u1);
    Mockito.verify(fcq, times(0)).offerQueue(2, u1);
    Mockito.verify(fcq, times(1)).offerQueue(3, u1);
    Mockito.verify(fcq, times(0)).offerQueue(4, u1);
    // 0:xx 1:xx 2:xx u1:xx u2:x-

    // put to dedicated queue should not offer, just put.
    Mockito.reset(fcq);
    Exception stopPuts = new RuntimeException();
    try {
      doThrow(stopPuts).when(fcq).putQueue(anyInt(), any(Schedulable.class));
      fcq.put(u1);
      fail("didn't fail");
    } catch (Exception e) {
      assertSame(stopPuts, e);
    }
    Mockito.verify(fcq, times(0)).offerQueue(0, u1);
    Mockito.verify(fcq, times(0)).offerQueue(1, u1);
    Mockito.verify(fcq, times(0)).offerQueue(2, u1);
    Mockito.verify(fcq, times(1)).putQueue(3, u1);
    Mockito.verify(fcq, times(0)).putQueue(4, u1);
  }

  @Test
  public void testQueueCapacity() {
    int numQueues = 2;
    int capacity = 4;
    Configuration conf = new Configuration();
    List<Schedulable> calls = new ArrayList<>();

    // default weights i.e. all queues share capacity
    fcq = new FairCallQueue<Schedulable>(numQueues, 4, "ns", conf);
    FairCallQueue<Schedulable> fcq1 = new FairCallQueue<Schedulable>(
        numQueues, capacity, "ns", new int[]{1, 3}, false, conf);

    for (int i=0; i < capacity; i++) {
      Schedulable call = mockCall("u", i%2);
      calls.add(call);
      fcq.add(call);
      fcq1.add(call);
    }

    final AtomicInteger currentIndex = new AtomicInteger();
    fcq.setMultiplexer(new RpcMultiplexer(){
      @Override
      public int getAndAdvanceCurrentIndex() {
        return currentIndex.get();
      }
    });
    fcq1.setMultiplexer(new RpcMultiplexer(){
      @Override
      public int getAndAdvanceCurrentIndex() {
        return currentIndex.get();
      }
    });

    // either queue will have two calls
    //    v
    // 0  1
    // 2  3
    currentIndex.set(1);
    assertSame(calls.get(1), fcq.poll());
    assertSame(calls.get(3), fcq.poll());
    assertSame(calls.get(0), fcq.poll());
    assertSame(calls.get(2), fcq.poll());

    // queues with different number of calls
    //    v
    // 0  1
    //    2
    //    3
    currentIndex.set(1);
    assertSame(calls.get(1), fcq1.poll());
    assertSame(calls.get(2), fcq1.poll());
    assertSame(calls.get(3), fcq1.poll());
    assertSame(calls.get(0), fcq1.poll());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testInsertionWithFailover() {
    Configuration conf = new Configuration();
    // Config for server to throw StandbyException instead of the
    // regular RetriableException if call queue is full.

    // 3 queues, 2 slots each.
    fcq = Mockito.spy(new FairCallQueue<>(3, 6, "ns",
        true, conf));

    Schedulable p0 = mockCall("a", 0);
    Schedulable p1 = mockCall("b", 1);

    // add to first queue.
    addToQueueAndVerify(p0, 1, 0, 0);
    // 0:x- 1:-- 2:--

    // add to second queue.
    addToQueueAndVerify(p1, 0, 1, 0);
    // 0:x- 1:x- 2:--

    // add to first queue.
    addToQueueAndVerify(p0, 1, 0, 0);
    // 0:xx 1:x- 2:--

    // add to first full queue spills over to second.
    addToQueueAndVerify(p0, 1, 1, 0);
    // 0:xx 1:xx 2:--

    // add to second full queue spills over to third.
    addToQueueAndVerify(p1, 0, 1, 1);
    // 0:xx 1:xx 2:x-

    // add to first and second full queue spills over to third.
    addToQueueAndVerify(p0, 1, 1, 1);
    // 0:xx 1:xx 2:xx

    // adding non-lowest priority with all queues full throws a
    // standby exception for client to try another server.
    Mockito.reset(fcq);
    try {
      fcq.add(p0);
      fail("didn't fail");
    } catch (IllegalStateException ise) {
      checkOverflowException(ise, RpcStatusProto.FATAL, true);
    }
  }

  private void addToQueueAndVerify(Schedulable call, int expectedQueue0,
      int expectedQueue1, int expectedQueue2) {
    Mockito.reset(fcq);
    fcq.add(call);
    Mockito.verify(fcq, times(expectedQueue0)).offerQueue(0, call);
    Mockito.verify(fcq, times(expectedQueue1)).offerQueue(1, call);
    Mockito.verify(fcq, times(expectedQueue2)).offerQueue(2, call);
  }

  @SuppressWarnings("unchecked") // for mock reset.
  @Test
  public void testInsertion() throws Exception {
    Configuration conf = new Configuration();
    // 3 queues, 2 slots each.
    fcq = Mockito.spy(new FairCallQueue<Schedulable>(3, 6, "ns", conf));

    Schedulable p0 = mockCall("a", 0);
    Schedulable p1 = mockCall("b", 1);
    Schedulable p2 = mockCall("c", 2);

    // add to first queue.
    Mockito.reset(fcq);
    fcq.add(p0);
    Mockito.verify(fcq, times(1)).offerQueue(0, p0);
    Mockito.verify(fcq, times(0)).offerQueue(1, p0);
    Mockito.verify(fcq, times(0)).offerQueue(2, p0);
    Mockito.reset(fcq);
    // 0:x- 1:-- 2:--

    // add to second queue.
    Mockito.reset(fcq);
    fcq.add(p1);
    Mockito.verify(fcq, times(0)).offerQueue(0, p1);
    Mockito.verify(fcq, times(1)).offerQueue(1, p1);
    Mockito.verify(fcq, times(0)).offerQueue(2, p1);
    // 0:x- 1:x- 2:--

    // add to first queue.
    Mockito.reset(fcq);
    fcq.add(p0);
    Mockito.verify(fcq, times(1)).offerQueue(0, p0);
    Mockito.verify(fcq, times(0)).offerQueue(1, p0);
    Mockito.verify(fcq, times(0)).offerQueue(2, p0);
    // 0:xx 1:x- 2:--

    // add to first full queue spills over to second.
    Mockito.reset(fcq);
    fcq.add(p0);
    Mockito.verify(fcq, times(1)).offerQueue(0, p0);
    Mockito.verify(fcq, times(1)).offerQueue(1, p0);
    Mockito.verify(fcq, times(0)).offerQueue(2, p0);
    // 0:xx 1:xx 2:--

    // add to second full queue spills over to third.
    Mockito.reset(fcq);
    fcq.add(p1);
    Mockito.verify(fcq, times(0)).offerQueue(0, p1);
    Mockito.verify(fcq, times(1)).offerQueue(1, p1);
    Mockito.verify(fcq, times(1)).offerQueue(2, p1);
    // 0:xx 1:xx 2:x-

    // add to first and second full queue spills over to third.
    Mockito.reset(fcq);
    fcq.add(p0);
    Mockito.verify(fcq, times(1)).offerQueue(0, p0);
    Mockito.verify(fcq, times(1)).offerQueue(1, p0);
    Mockito.verify(fcq, times(1)).offerQueue(2, p0);
    // 0:xx 1:xx 2:xx

    // adding non-lowest priority with all queues full throws a
    // non-disconnecting rpc server exception.
    Mockito.reset(fcq);
    try {
      fcq.add(p0);
      fail("didn't fail");
    } catch (IllegalStateException ise) {
      checkOverflowException(ise, RpcStatusProto.ERROR, false);
    }
    Mockito.verify(fcq, times(1)).offerQueue(0, p0);
    Mockito.verify(fcq, times(1)).offerQueue(1, p0);
    Mockito.verify(fcq, times(1)).offerQueue(2, p0);

    // adding non-lowest priority with all queues full throws a
    // non-disconnecting rpc server exception.
    Mockito.reset(fcq);
    try {
      fcq.add(p1);
      fail("didn't fail");
    } catch (IllegalStateException ise) {
      checkOverflowException(ise, RpcStatusProto.ERROR, false);
    }
    Mockito.verify(fcq, times(0)).offerQueue(0, p1);
    Mockito.verify(fcq, times(1)).offerQueue(1, p1);
    Mockito.verify(fcq, times(1)).offerQueue(2, p1);

    // adding lowest priority with all queues full throws a
    // fatal disconnecting rpc server exception.
    Mockito.reset(fcq);
    try {
      fcq.add(p2);
      fail("didn't fail");
    } catch (IllegalStateException ise) {
      checkOverflowException(ise, RpcStatusProto.FATAL, false);
    }
    Mockito.verify(fcq, times(0)).offerQueue(0, p2);
    Mockito.verify(fcq, times(0)).offerQueue(1, p2);
    Mockito.verify(fcq, times(1)).offerQueue(2, p2);
    Mockito.reset(fcq);

    // used to abort what would be a blocking operation.
    Exception stopPuts = new RuntimeException();

    // put should offer to all but last subqueue, only put to last subqueue.
    Mockito.reset(fcq);
    try {
      doThrow(stopPuts).when(fcq).putQueue(anyInt(), any());
      fcq.put(p0);
      fail("didn't fail");
    } catch (Exception e) {
      assertSame(stopPuts, e);
    }
    Mockito.verify(fcq, times(1)).offerQueue(0, p0);
    Mockito.verify(fcq, times(1)).offerQueue(1, p0);
    Mockito.verify(fcq, times(0)).offerQueue(2, p0); // expect put, not offer.
    Mockito.verify(fcq, times(1)).putQueue(2, p0);

    // put with lowest priority should not offer, just put.
    Mockito.reset(fcq);
    try {
      doThrow(stopPuts).when(fcq).putQueue(anyInt(), any());
      fcq.put(p2);
      fail("didn't fail");
    } catch (Exception e) {
      assertSame(stopPuts, e);
    }
    Mockito.verify(fcq, times(0)).offerQueue(0, p2);
    Mockito.verify(fcq, times(0)).offerQueue(1, p2);
    Mockito.verify(fcq, times(0)).offerQueue(2, p2);
    Mockito.verify(fcq, times(1)).putQueue(2, p2);
  }

  private void checkOverflowException(Exception ex, RpcStatusProto status,
      boolean failOverTriggered) {
    // should be an overflow exception
    assertTrue(ex.getClass().getName() + " != CallQueueOverflowException",
        ex instanceof CallQueueOverflowException);
    IOException ioe = ((CallQueueOverflowException)ex).getCause();
    assertNotNull(ioe);
    assertTrue(ioe.getClass().getName() + " != RpcServerException",
        ioe instanceof RpcServerException);
    RpcServerException rse = (RpcServerException)ioe;
    // check error/fatal status and if it embeds a retriable ex or standby ex.
    assertEquals(status, rse.getRpcStatusProto());
    if (failOverTriggered) {
      assertTrue(rse.getClass().getName() + " != RetriableException",
          rse.getCause() instanceof StandbyException);
    } else {
      assertTrue(rse.getClass().getName() + " != RetriableException",
          rse.getCause() instanceof RetriableException);
    }
  }

  //
  // Ensure that FairCallQueue properly implements BlockingQueue
  @Test
  public void testPollReturnsNullWhenEmpty() {
    assertNull(fcq.poll());
  }

  @Test
  public void testPollReturnsTopCallWhenNotEmpty() {
    Schedulable call = mockCall("c");
    assertTrue(fcq.offer(call));

    assertEquals(call, fcq.poll());

    // Poll took it out so the fcq is empty
    assertEquals(0, fcq.size());
  }

  @Test
  public void testOfferSucceeds() {

    for (int i = 0; i < 5; i++) {
      // We can fit 10 calls
      assertTrue(fcq.offer(mockCall("c")));
    }

    assertEquals(5, fcq.size());
  }

  @Test
  public void testOfferFailsWhenFull() {
    for (int i = 0; i < 5; i++) { assertTrue(fcq.offer(mockCall("c"))); }

    assertFalse(fcq.offer(mockCall("c"))); // It's full

    assertEquals(5, fcq.size());
  }

  @Test
  public void testOfferSucceedsWhenScheduledLowPriority() {
    // Scheduler will schedule into queue 0 x 5, then queue 1
    int mockedPriorities[] = {0, 0, 0, 0, 0, 1, 0};
    for (int i = 0; i < 5; i++) { assertTrue(fcq.offer(mockCall("c", mockedPriorities[i]))); }

    assertTrue(fcq.offer(mockCall("c", mockedPriorities[5])));

    assertEquals(6, fcq.size());
  }

  @Test
  public void testPeekNullWhenEmpty() {
    assertNull(fcq.peek());
  }

  @Test
  public void testPeekNonDestructive() {
    Schedulable call = mockCall("c", 0);
    assertTrue(fcq.offer(call));

    assertEquals(call, fcq.peek());
    assertEquals(call, fcq.peek()); // Non-destructive
    assertEquals(1, fcq.size());
  }

  @Test
  public void testPeekPointsAtHead() {
    Schedulable call = mockCall("c", 0);
    Schedulable next = mockCall("b", 0);
    fcq.offer(call);
    fcq.offer(next);

    assertEquals(call, fcq.peek()); // Peek points at the head
  }

  @Test
  public void testPollTimeout() throws InterruptedException {
    assertNull(fcq.poll(10, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testPollSuccess() throws InterruptedException {
    Schedulable call = mockCall("c", 0);
    assertTrue(fcq.offer(call));

    assertEquals(call, fcq.poll(10, TimeUnit.MILLISECONDS));

    assertEquals(0, fcq.size());
  }

  @Test
  public void testOfferTimeout() throws InterruptedException {
    for (int i = 0; i < 5; i++) {
      assertTrue(fcq.offer(mockCall("c"), 10, TimeUnit.MILLISECONDS));
    }

    assertFalse(fcq.offer(mockCall("e"), 10, TimeUnit.MILLISECONDS)); // It's full

    assertEquals(5, fcq.size());
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testDrainTo() {
    Configuration conf = new Configuration();
    conf.setInt("ns." + FairCallQueue.IPC_CALLQUEUE_PRIORITY_LEVELS_KEY, 2);
    FairCallQueue<Schedulable> fcq2 = new FairCallQueue<Schedulable>(2, 10, "ns", conf);

    // Start with 3 in fcq, to be drained
    for (int i = 0; i < 3; i++) {
      fcq.offer(mockCall("c"));
    }

    fcq.drainTo(fcq2);

    assertEquals(0, fcq.size());
    assertEquals(3, fcq2.size());
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testDrainToWithLimit() {
    Configuration conf = new Configuration();
    conf.setInt("ns." + FairCallQueue.IPC_CALLQUEUE_PRIORITY_LEVELS_KEY, 2);
    FairCallQueue<Schedulable> fcq2 = new FairCallQueue<Schedulable>(2, 10, "ns", conf);

    // Start with 3 in fcq, to be drained
    for (int i = 0; i < 3; i++) {
      fcq.offer(mockCall("c"));
    }

    fcq.drainTo(fcq2, 2);

    assertEquals(1, fcq.size());
    assertEquals(2, fcq2.size());
  }

  @Test
  public void testInitialRemainingCapacity() {
    assertEquals(10, fcq.remainingCapacity());
  }

  @Test
  public void testFirstQueueFullRemainingCapacity() {
    while (fcq.offer(mockCall("c"))) ; // Queue 0 will fill up first, then queue 1

    assertEquals(5, fcq.remainingCapacity());
  }

  @Test
  public void testAllQueuesFullRemainingCapacity() {
    int[] mockedPriorities = {0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 0};
    int i = 0;
    while (fcq.offer(mockCall("c", mockedPriorities[i++]))) ;

    assertEquals(0, fcq.remainingCapacity());
    assertEquals(10, fcq.size());
  }

  @Test
  public void testQueuesPartialFilledRemainingCapacity() {
    int[] mockedPriorities = {0, 1, 0, 1, 0};
    for (int i = 0; i < 5; i++) { fcq.offer(mockCall("c", mockedPriorities[i])); }

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
  @Test
  public void testPutOverflows() throws InterruptedException {
    // We can fit more than 5, even though the scheduler suggests the top queue
    assertCanPut(fcq, 8, 8);
    assertEquals(8, fcq.size());
  }

  @Test
  public void testPutBlocksWhenAllFull() throws InterruptedException {
    assertCanPut(fcq, 10, 10); // Fill up
    assertEquals(10, fcq.size());

    // Put more which causes overflow
    assertCanPut(fcq, 0, 1); // Will block
  }

  @Test
  public void testTakeBlocksWhenEmpty() throws InterruptedException {
    assertCanTake(fcq, 0, 1);
  }

  @Test
  public void testTakeRemovesCall() throws InterruptedException {
    Schedulable call = mockCall("c");
    fcq.offer(call);

    assertEquals(call, fcq.take());
    assertEquals(0, fcq.size());
  }

  @Test
  public void testTakeTriesNextQueue() throws InterruptedException {

    // A mux which only draws from q 0
    RpcMultiplexer q0mux = mock(RpcMultiplexer.class);
    when(q0mux.getAndAdvanceCurrentIndex()).thenReturn(0);
    fcq.setMultiplexer(q0mux);

    // Make a FCQ filled with calls in q 1 but empty in q 0
    Schedulable call = mockCall("c", 1);
    fcq.put(call);

    // Take from q1 even though mux said q0, since q0 empty
    assertEquals(call, fcq.take());
    assertEquals(0, fcq.size());
  }

  @Test
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

  @Test
  public void testFairCallQueueMetrics() throws Exception {
    final String fcqMetrics = "ns.FairCallQueue";
    Schedulable p0 = mockCall("a", 0);
    Schedulable p1 = mockCall("b", 1);

    assertGauge("FairCallQueueSize_p0", 0, getMetrics(fcqMetrics));
    assertGauge("FairCallQueueSize_p1", 0, getMetrics(fcqMetrics));
    assertCounter("FairCallQueueOverflowedCalls_p0", 0L,
        getMetrics(fcqMetrics));
    assertCounter("FairCallQueueOverflowedCalls_p1", 0L,
        getMetrics(fcqMetrics));

    for (int i = 0; i < 5; i++) {
      fcq.add(p0);
      fcq.add(p1);
    }

    try {
      fcq.add(p1);
      fail("didn't overflow");
    } catch (IllegalStateException ise) {
      // Expected exception
    }

    assertGauge("FairCallQueueSize_p0", 5, getMetrics(fcqMetrics));
    assertGauge("FairCallQueueSize_p1", 5, getMetrics(fcqMetrics));
    assertCounter("FairCallQueueOverflowedCalls_p0", 0L,
        getMetrics(fcqMetrics));
    assertCounter("FairCallQueueOverflowedCalls_p1", 1L,
        getMetrics(fcqMetrics));
  }
}
