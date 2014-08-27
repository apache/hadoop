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

import java.lang.ref.WeakReference;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.AbstractQueue;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.util.MBeans;

/**
 * A queue with multiple levels for each priority.
 */
public class FairCallQueue<E extends Schedulable> extends AbstractQueue<E>
  implements BlockingQueue<E> {
  // Configuration Keys
  public static final int    IPC_CALLQUEUE_PRIORITY_LEVELS_DEFAULT = 4;
  public static final String IPC_CALLQUEUE_PRIORITY_LEVELS_KEY =
    "faircallqueue.priority-levels";

  public static final Log LOG = LogFactory.getLog(FairCallQueue.class);

  /* The queues */
  private final ArrayList<BlockingQueue<E>> queues;

  /* Read locks */
  private final ReentrantLock takeLock = new ReentrantLock();
  private final Condition notEmpty = takeLock.newCondition();
  private void signalNotEmpty() {
    takeLock.lock();
    try {
      notEmpty.signal();
    } finally {
      takeLock.unlock();
    }
  }

  /* Scheduler picks which queue to place in */
  private RpcScheduler scheduler;

  /* Multiplexer picks which queue to draw from */
  private RpcMultiplexer multiplexer;

  /* Statistic tracking */
  private final ArrayList<AtomicLong> overflowedCalls;

  /**
   * Create a FairCallQueue.
   * @param capacity the maximum size of each sub-queue
   * @param ns the prefix to use for configuration
   * @param conf the configuration to read from
   * Notes: the FairCallQueue has no fixed capacity. Rather, it has a minimum
   * capacity of `capacity` and a maximum capacity of `capacity * number_queues`
   */
  public FairCallQueue(int capacity, String ns, Configuration conf) {
    int numQueues = parseNumQueues(ns, conf);
    LOG.info("FairCallQueue is in use with " + numQueues + " queues.");

    this.queues = new ArrayList<BlockingQueue<E>>(numQueues);
    this.overflowedCalls = new ArrayList<AtomicLong>(numQueues);

    for(int i=0; i < numQueues; i++) {
      this.queues.add(new LinkedBlockingQueue<E>(capacity));
      this.overflowedCalls.add(new AtomicLong(0));
    }

    this.scheduler = new DecayRpcScheduler(numQueues, ns, conf);
    this.multiplexer = new WeightedRoundRobinMultiplexer(numQueues, ns, conf);

    // Make this the active source of metrics
    MetricsProxy mp = MetricsProxy.getInstance(ns);
    mp.setDelegate(this);
  }

  /**
   * Read the number of queues from the configuration.
   * This will affect the FairCallQueue's overall capacity.
   * @throws IllegalArgumentException on invalid queue count
   */
  private static int parseNumQueues(String ns, Configuration conf) {
    int retval = conf.getInt(ns + "." + IPC_CALLQUEUE_PRIORITY_LEVELS_KEY,
      IPC_CALLQUEUE_PRIORITY_LEVELS_DEFAULT);
    if(retval < 1) {
      throw new IllegalArgumentException("numQueues must be at least 1");
    }
    return retval;
  }

  /**
   * Returns the first non-empty queue with equal or lesser priority
   * than <i>startIdx</i>. Wraps around, searching a maximum of N
   * queues, where N is this.queues.size().
   *
   * @param startIdx the queue number to start searching at
   * @return the first non-empty queue with less priority, or null if
   * everything was empty
   */
  private BlockingQueue<E> getFirstNonEmptyQueue(int startIdx) {
    final int numQueues = this.queues.size();
    for(int i=0; i < numQueues; i++) {
      int idx = (i + startIdx) % numQueues; // offset and wrap around
      BlockingQueue<E> queue = this.queues.get(idx);
      if (queue.size() != 0) {
        return queue;
      }
    }

    // All queues were empty
    return null;
  }

  /* AbstractQueue and BlockingQueue methods */

  /**
   * Put and offer follow the same pattern:
   * 1. Get a priorityLevel from the scheduler
   * 2. Get the nth sub-queue matching this priorityLevel
   * 3. delegate the call to this sub-queue.
   *
   * But differ in how they handle overflow:
   * - Put will move on to the next queue until it lands on the last queue
   * - Offer does not attempt other queues on overflow
   */
  @Override
  public void put(E e) throws InterruptedException {
    int priorityLevel = scheduler.getPriorityLevel(e);

    final int numLevels = this.queues.size();
    while (true) {
      BlockingQueue<E> q = this.queues.get(priorityLevel);
      boolean res = q.offer(e);
      if (!res) {
        // Update stats
        this.overflowedCalls.get(priorityLevel).getAndIncrement();

        // If we failed to insert, try again on the next level
        priorityLevel++;

        if (priorityLevel == numLevels) {
          // That was the last one, we will block on put in the last queue
          // Delete this line to drop the call
          this.queues.get(priorityLevel-1).put(e);
          break;
        }
      } else {
        break;
      }
    }


    signalNotEmpty();
  }

  @Override
  public boolean offer(E e, long timeout, TimeUnit unit)
      throws InterruptedException {
    int priorityLevel = scheduler.getPriorityLevel(e);
    BlockingQueue<E> q = this.queues.get(priorityLevel);
    boolean ret = q.offer(e, timeout, unit);

    signalNotEmpty();

    return ret;
  }

  @Override
  public boolean offer(E e) {
    int priorityLevel = scheduler.getPriorityLevel(e);
    BlockingQueue<E> q = this.queues.get(priorityLevel);
    boolean ret = q.offer(e);

    signalNotEmpty();

    return ret;
  }

  @Override
  public E take() throws InterruptedException {
    int startIdx = this.multiplexer.getAndAdvanceCurrentIndex();

    takeLock.lockInterruptibly();
    try {
      // Wait while queue is empty
      for (;;) {
        BlockingQueue<E> q = this.getFirstNonEmptyQueue(startIdx);
        if (q != null) {
          // Got queue, so return if we can poll out an object
          E e = q.poll();
          if (e != null) {
            return e;
          }
        }

        notEmpty.await();
      }
    } finally {
      takeLock.unlock();
    }
  }

  @Override
  public E poll(long timeout, TimeUnit unit)
      throws InterruptedException {

    int startIdx = this.multiplexer.getAndAdvanceCurrentIndex();

    long nanos = unit.toNanos(timeout);
    takeLock.lockInterruptibly();
    try {
      for (;;) {
        BlockingQueue<E> q = this.getFirstNonEmptyQueue(startIdx);
        if (q != null) {
          E e = q.poll();
          if (e != null) {
            // Escape condition: there might be something available
            return e;
          }
        }

        if (nanos <= 0) {
          // Wait has elapsed
          return null;
        }

        try {
          // Now wait on the condition for a bit. If we get
          // spuriously awoken we'll re-loop
          nanos = notEmpty.awaitNanos(nanos);
        } catch (InterruptedException ie) {
          notEmpty.signal(); // propagate to a non-interrupted thread
          throw ie;
        }
      }
    } finally {
      takeLock.unlock();
    }
  }

  /**
   * poll() provides no strict consistency: it is possible for poll to return
   * null even though an element is in the queue.
   */
  @Override
  public E poll() {
    int startIdx = this.multiplexer.getAndAdvanceCurrentIndex();

    BlockingQueue<E> q = this.getFirstNonEmptyQueue(startIdx);
    if (q == null) {
      return null; // everything is empty
    }

    // Delegate to the sub-queue's poll, which could still return null
    return q.poll();
  }

  /**
   * Peek, like poll, provides no strict consistency.
   */
  @Override
  public E peek() {
    BlockingQueue<E> q = this.getFirstNonEmptyQueue(0);
    if (q == null) {
      return null;
    } else {
      return q.peek();
    }
  }

  /**
   * Size returns the sum of all sub-queue sizes, so it may be greater than
   * capacity.
   * Note: size provides no strict consistency, and should not be used to
   * control queue IO.
   */
  @Override
  public int size() {
    int size = 0;
    for (BlockingQueue q : this.queues) {
      size += q.size();
    }
    return size;
  }

  /**
   * Iterator is not implemented, as it is not needed.
   */
  @Override
  public Iterator<E> iterator() {
    throw new NotImplementedException();
  }

  /**
   * drainTo defers to each sub-queue. Note that draining from a FairCallQueue
   * to another FairCallQueue will likely fail, since the incoming calls
   * may be scheduled differently in the new FairCallQueue. Nonetheless this
   * method is provided for completeness.
   */
  @Override
  public int drainTo(Collection<? super E> c, int maxElements) {
    int sum = 0;
    for (BlockingQueue<E> q : this.queues) {
      sum += q.drainTo(c, maxElements);
    }
    return sum;
  }

  @Override
  public int drainTo(Collection<? super E> c) {
    int sum = 0;
    for (BlockingQueue<E> q : this.queues) {
      sum += q.drainTo(c);
    }
    return sum;
  }

  /**
   * Returns maximum remaining capacity. This does not reflect how much you can
   * ideally fit in this FairCallQueue, as that would depend on the scheduler's
   * decisions.
   */
  @Override
  public int remainingCapacity() {
    int sum = 0;
    for (BlockingQueue q : this.queues) {
      sum += q.remainingCapacity();
    }
    return sum;
  }

  /**
   * MetricsProxy is a singleton because we may init multiple
   * FairCallQueues, but the metrics system cannot unregister beans cleanly.
   */
  private static final class MetricsProxy implements FairCallQueueMXBean {
    // One singleton per namespace
    private static final HashMap<String, MetricsProxy> INSTANCES =
      new HashMap<String, MetricsProxy>();

    // Weakref for delegate, so we don't retain it forever if it can be GC'd
    private WeakReference<FairCallQueue> delegate;

    // Keep track of how many objects we registered
    private int revisionNumber = 0;

    private MetricsProxy(String namespace) {
      MBeans.register(namespace, "FairCallQueue", this);
    }

    public static synchronized MetricsProxy getInstance(String namespace) {
      MetricsProxy mp = INSTANCES.get(namespace);
      if (mp == null) {
        // We must create one
        mp = new MetricsProxy(namespace);
        INSTANCES.put(namespace, mp);
      }
      return mp;
    }

    public void setDelegate(FairCallQueue obj) {
      this.delegate = new WeakReference<FairCallQueue>(obj);
      this.revisionNumber++;
    }

    @Override
    public int[] getQueueSizes() {
      FairCallQueue obj = this.delegate.get();
      if (obj == null) {
        return new int[]{};
      }

      return obj.getQueueSizes();
    }

    @Override
    public long[] getOverflowedCalls() {
      FairCallQueue obj = this.delegate.get();
      if (obj == null) {
        return new long[]{};
      }

      return obj.getOverflowedCalls();
    }

    @Override public int getRevision() {
      return revisionNumber;
    }
  }

  // FairCallQueueMXBean
  public int[] getQueueSizes() {
    int numQueues = queues.size();
    int[] sizes = new int[numQueues];
    for (int i=0; i < numQueues; i++) {
      sizes[i] = queues.get(i).size();
    }
    return sizes;
  }

  public long[] getOverflowedCalls() {
    int numQueues = queues.size();
    long[] calls = new long[numQueues];
    for (int i=0; i < numQueues; i++) {
      calls[i] = overflowedCalls.get(i).get();
    }
    return calls;
  }

  // For testing
  @VisibleForTesting
  public void setScheduler(RpcScheduler newScheduler) {
    this.scheduler = newScheduler;
  }

  @VisibleForTesting
  public void setMultiplexer(RpcMultiplexer newMux) {
    this.multiplexer = newMux;
  }
}
