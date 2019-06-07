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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.CallQueueManager.CallQueueOverflowException;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.util.MBeans;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A queue with multiple levels for each priority.
 */
public class FairCallQueue<E extends Schedulable> extends AbstractQueue<E>
    implements BlockingQueue<E> {
  @Deprecated
  public static final int    IPC_CALLQUEUE_PRIORITY_LEVELS_DEFAULT = 4;
  @Deprecated
  public static final String IPC_CALLQUEUE_PRIORITY_LEVELS_KEY =
    "faircallqueue.priority-levels";

  public static final Logger LOG = LoggerFactory.getLogger(FairCallQueue.class);

  /* The queues */
  private final ArrayList<BlockingQueue<E>> queues;

  /* Track available permits for scheduled objects.  All methods that will
   * mutate a subqueue must acquire or release a permit on the semaphore.
   * A semaphore is much faster than an exclusive lock because producers do
   * not contend with consumers and consumers do not block other consumers
   * while polling.
   */
  private final Semaphore semaphore = new Semaphore(0);
  private void signalNotEmpty() {
    semaphore.release();
  }

  /* Multiplexer picks which queue to draw from */
  private RpcMultiplexer multiplexer;

  /* Statistic tracking */
  private final ArrayList<AtomicLong> overflowedCalls;

  /**
   * Create a FairCallQueue.
   * @param capacity the total size of all sub-queues
   * @param ns the prefix to use for configuration
   * @param conf the configuration to read from
   * Notes: Each sub-queue has a capacity of `capacity / numSubqueues`.
   * The first or the highest priority sub-queue has an excess capacity
   * of `capacity % numSubqueues`
   */
  public FairCallQueue(int priorityLevels, int capacity, String ns,
      Configuration conf) {
    if(priorityLevels < 1) {
      throw new IllegalArgumentException("Number of Priority Levels must be " +
          "at least 1");
    }
    int numQueues = priorityLevels;
    LOG.info("FairCallQueue is in use with " + numQueues +
        " queues with total capacity of " + capacity);

    this.queues = new ArrayList<BlockingQueue<E>>(numQueues);
    this.overflowedCalls = new ArrayList<AtomicLong>(numQueues);
    int queueCapacity = capacity / numQueues;
    int capacityForFirstQueue = queueCapacity + (capacity % numQueues);
    for(int i=0; i < numQueues; i++) {
      if (i == 0) {
        this.queues.add(new LinkedBlockingQueue<E>(capacityForFirstQueue));
      } else {
        this.queues.add(new LinkedBlockingQueue<E>(queueCapacity));
      }
      this.overflowedCalls.add(new AtomicLong(0));
    }

    this.multiplexer = new WeightedRoundRobinMultiplexer(numQueues, ns, conf);
    // Make this the active source of metrics
    MetricsProxy mp = MetricsProxy.getInstance(ns);
    mp.setDelegate(this);
  }

  /**
   * Returns an element first non-empty queue equal to the priority returned
   * by the multiplexer or scans from highest to lowest priority queue.
   *
   * Caller must always acquire a semaphore permit before invoking.
   *
   * @return the first non-empty queue with less priority, or null if
   * everything was empty
   */
  private E removeNextElement() {
    int priority = multiplexer.getAndAdvanceCurrentIndex();
    E e = queues.get(priority).poll();
    // a semaphore permit has been acquired, so an element MUST be extracted
    // or the semaphore and queued elements will go out of sync.  loop to
    // avoid race condition if elements are added behind the current position,
    // awakening other threads that poll the elements ahead of our position.
    while (e == null) {
      for (int idx = 0; e == null && idx < queues.size(); idx++) {
        e = queues.get(idx).poll();
      }
    }
    return e;
  }

  /* AbstractQueue and BlockingQueue methods */

  /**
   * Add, put, and offer follow the same pattern:
   * 1. Get the assigned priorityLevel from the call by scheduler
   * 2. Get the nth sub-queue matching this priorityLevel
   * 3. delegate the call to this sub-queue.
   *
   * But differ in how they handle overflow:
   * - Add will move on to the next queue, throw on last queue overflow
   * - Put will move on to the next queue, block on last queue overflow
   * - Offer does not attempt other queues on overflow
   */

  @Override
  public boolean add(E e) {
    final int priorityLevel = e.getPriorityLevel();
    // try offering to all queues.
    if (!offerQueues(priorityLevel, e, true)) {
      // only disconnect the lowest priority users that overflow the queue.
      throw (priorityLevel == queues.size() - 1)
          ? CallQueueOverflowException.DISCONNECT
          : CallQueueOverflowException.KEEPALIVE;
    }
    return true;
  }

  @Override
  public void put(E e) throws InterruptedException {
    final int priorityLevel = e.getPriorityLevel();
    // try offering to all but last queue, put on last.
    if (!offerQueues(priorityLevel, e, false)) {
      putQueue(queues.size() - 1, e);
    }
  }

  /**
   * Put the element in a queue of a specific priority.
   * @param priority - queue priority
   * @param e - element to add
   */
  @VisibleForTesting
  void putQueue(int priority, E e) throws InterruptedException {
    queues.get(priority).put(e);
    signalNotEmpty();
  }

  /**
   * Offer the element to queue of a specific priority.
   * @param priority - queue priority
   * @param e - element to add
   * @return boolean if added to the given queue
   */
  @VisibleForTesting
  boolean offerQueue(int priority, E e) {
    boolean ret = queues.get(priority).offer(e);
    if (ret) {
      signalNotEmpty();
    }
    return ret;
  }

  /**
   * Offer the element to queue of the given or lower priority.
   * @param priority - starting queue priority
   * @param e - element to add
   * @param includeLast - whether to attempt last queue
   * @return boolean if added to a queue
   */
  private boolean offerQueues(int priority, E e, boolean includeLast) {
    int lastPriority = queues.size() - (includeLast ? 1 : 2);
    for (int i=priority; i <= lastPriority; i++) {
      if (offerQueue(i, e)) {
        return true;
      }
      // Update stats
      overflowedCalls.get(i).getAndIncrement();
    }
    return false;
  }

  @Override
  public boolean offer(E e, long timeout, TimeUnit unit)
      throws InterruptedException {
    int priorityLevel = e.getPriorityLevel();
    BlockingQueue<E> q = this.queues.get(priorityLevel);
    boolean ret = q.offer(e, timeout, unit);
    if (ret) {
      signalNotEmpty();
    }
    return ret;
  }

  @Override
  public boolean offer(E e) {
    int priorityLevel = e.getPriorityLevel();
    BlockingQueue<E> q = this.queues.get(priorityLevel);
    boolean ret = q.offer(e);
    if (ret) {
      signalNotEmpty();
    }
    return ret;
  }

  @Override
  public E take() throws InterruptedException {
    semaphore.acquire();
    return removeNextElement();
  }

  @Override
  public E poll(long timeout, TimeUnit unit) throws InterruptedException {
    return semaphore.tryAcquire(timeout, unit) ? removeNextElement() : null;
  }

  /**
   * poll() provides no strict consistency: it is possible for poll to return
   * null even though an element is in the queue.
   */
  @Override
  public E poll() {
    return semaphore.tryAcquire() ? removeNextElement() : null;
  }

  /**
   * Peek, like poll, provides no strict consistency.
   */
  @Override
  public E peek() {
    E e = null;
    for (int i=0; e == null && i < queues.size(); i++) {
      e = queues.get(i).peek();
    }
    return e;
  }

  /**
   * Size returns the sum of all sub-queue sizes, so it may be greater than
   * capacity.
   * Note: size provides no strict consistency, and should not be used to
   * control queue IO.
   */
  @Override
  public int size() {
    return semaphore.availablePermits();
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
    // initially take all permits to stop consumers from modifying queues
    // while draining.  will restore any excess when done draining.
    final int permits = semaphore.drainPermits();
    final int numElements = Math.min(maxElements, permits);
    int numRemaining = numElements;
    for (int i=0; numRemaining > 0 && i < queues.size(); i++) {
      numRemaining -= queues.get(i).drainTo(c, numRemaining);
    }
    int drained = numElements - numRemaining;
    if (permits > drained) { // restore unused permits.
      semaphore.release(permits - drained);
    }
    return drained;
  }

  @Override
  public int drainTo(Collection<? super E> c) {
    return drainTo(c, Integer.MAX_VALUE);
  }

  /**
   * Returns maximum remaining capacity. This does not reflect how much you can
   * ideally fit in this FairCallQueue, as that would depend on the scheduler's
   * decisions.
   */
  @Override
  public int remainingCapacity() {
    int sum = 0;
    for (BlockingQueue<E> q : this.queues) {
      sum += q.remainingCapacity();
    }
    return sum;
  }

  /**
   * MetricsProxy is a singleton because we may init multiple
   * FairCallQueues, but the metrics system cannot unregister beans cleanly.
   */
  private static final class MetricsProxy implements FairCallQueueMXBean,
      MetricsSource {
    // One singleton per namespace
    private static final HashMap<String, MetricsProxy> INSTANCES =
      new HashMap<String, MetricsProxy>();

    // Weakref for delegate, so we don't retain it forever if it can be GC'd
    private WeakReference<FairCallQueue<? extends Schedulable>> delegate;

    // Keep track of how many objects we registered
    private int revisionNumber = 0;

    private String namespace;

    private MetricsProxy(String namespace) {
      this.namespace = namespace;
      MBeans.register(namespace, "FairCallQueue", this);
      final String name = namespace + ".FairCallQueue";
      DefaultMetricsSystem.instance().register(name, name, this);
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

    public void setDelegate(FairCallQueue<? extends Schedulable> obj) {
      this.delegate
          = new WeakReference<FairCallQueue<? extends Schedulable>>(obj);
      this.revisionNumber++;
    }

    /**
     * Fetch the current call queue from the weak reference delegate. If there
     * is no delegate, or the delegate is empty, this will return null.
     */
    private FairCallQueue<? extends Schedulable> getCallQueue() {
      WeakReference<FairCallQueue<? extends Schedulable>> ref = this.delegate;
      if (ref == null) {
        return null;
      }
      return ref.get();
    }

    @Override
    public int[] getQueueSizes() {
      FairCallQueue<? extends Schedulable> obj = getCallQueue();
      if (obj == null) {
        return new int[]{};
      }

      return obj.getQueueSizes();
    }

    @Override
    public long[] getOverflowedCalls() {
      FairCallQueue<? extends Schedulable> obj = getCallQueue();
      if (obj == null) {
        return new long[]{};
      }

      return obj.getOverflowedCalls();
    }

    @Override public int getRevision() {
      return revisionNumber;
    }

    @Override
    public void getMetrics(MetricsCollector collector, boolean all) {
      MetricsRecordBuilder rb = collector.addRecord("FairCallQueue")
          .setContext("rpc")
          .tag(Interns.info("namespace", "Namespace"), namespace);

      final int[] currentQueueSizes = getQueueSizes();
      final long[] currentOverflowedCalls = getOverflowedCalls();

      for (int i = 0; i < currentQueueSizes.length; i++) {
        rb.addGauge(Interns.info("FairCallQueueSize_p" + i, "FCQ Queue Size"),
            currentQueueSizes[i]);
        rb.addCounter(Interns.info("FairCallQueueOverflowedCalls_p" + i,
            "FCQ Overflowed Calls"), currentOverflowedCalls[i]);
      }
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

  @VisibleForTesting
  public void setMultiplexer(RpcMultiplexer newMux) {
    this.multiplexer = newMux;
  }
}
