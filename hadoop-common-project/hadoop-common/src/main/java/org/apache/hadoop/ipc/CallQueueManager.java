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

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CALLQUEUE_SERVER_FAILOVER_ENABLE;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CALLQUEUE_SERVER_FAILOVER_ENABLE_DEFAULT;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.AbstractQueue;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstracts queue operations for different blocking queues.
 */
public class CallQueueManager<E extends Schedulable>
    extends AbstractQueue<E> implements BlockingQueue<E> {
  public static final Logger LOG =
      LoggerFactory.getLogger(CallQueueManager.class);
  // Number of checkpoints for empty queue.
  private static final int CHECKPOINT_NUM = 20;
  // Interval to check empty queue.
  private static final long CHECKPOINT_INTERVAL_MS = 10;

  @SuppressWarnings("unchecked")
  static <E> Class<? extends BlockingQueue<E>> convertQueueClass(
      Class<?> queueClass, Class<E> elementClass) {
    return (Class<? extends BlockingQueue<E>>)queueClass;
  }

  @SuppressWarnings("unchecked")
  static Class<? extends RpcScheduler> convertSchedulerClass(
      Class<?> schedulerClass) {
    return (Class<? extends RpcScheduler>)schedulerClass;
  }

  private volatile boolean clientBackOffEnabled;
  private volatile boolean serverFailOverEnabled;

  // Atomic refs point to active callQueue
  // We have two so we can better control swapping
  private final AtomicReference<BlockingQueue<E>> putRef;
  private final AtomicReference<BlockingQueue<E>> takeRef;

  private RpcScheduler scheduler;

  public CallQueueManager(Class<? extends BlockingQueue<E>> backingClass,
                          Class<? extends RpcScheduler> schedulerClass,
      boolean clientBackOffEnabled, int maxQueueSize, String namespace,
      Configuration conf) {
    int priorityLevels = parseNumLevels(namespace, conf);
    this.scheduler = createScheduler(schedulerClass, priorityLevels,
        namespace, conf);
    int[] capacityWeights = parseCapacityWeights(priorityLevels,
        namespace, conf);
    this.serverFailOverEnabled = getServerFailOverEnable(namespace, conf);
    BlockingQueue<E> bq = createCallQueueInstance(backingClass,
        priorityLevels, maxQueueSize, namespace, capacityWeights, conf);
    this.clientBackOffEnabled = clientBackOffEnabled;
    this.putRef = new AtomicReference<BlockingQueue<E>>(bq);
    this.takeRef = new AtomicReference<BlockingQueue<E>>(bq);
    LOG.info("Using callQueue: {}, queueCapacity: {}, " +
        "scheduler: {}, ipcBackoff: {}, ipcFailOver: {}.",
        backingClass, maxQueueSize, schedulerClass, clientBackOffEnabled, serverFailOverEnabled);
  }

  @VisibleForTesting // only!
  CallQueueManager(BlockingQueue<E> queue, RpcScheduler scheduler,
      boolean clientBackOffEnabled, boolean serverFailOverEnabled) {
    this.putRef = new AtomicReference<BlockingQueue<E>>(queue);
    this.takeRef = new AtomicReference<BlockingQueue<E>>(queue);
    this.scheduler = scheduler;
    this.clientBackOffEnabled = clientBackOffEnabled;
    this.serverFailOverEnabled = serverFailOverEnabled;
  }

  /**
   * Return boolean value configured by property 'ipc.<port>.callqueue.overflow.trigger.failover'
   * if it is present. If the config is not present, default config
   * (without port) is used to derive class i.e 'ipc.callqueue.overflow.trigger.failover',
   * and derived value is returned if configured. Otherwise, default value
   * {@link CommonConfigurationKeys#IPC_CALLQUEUE_SERVER_FAILOVER_ENABLE_DEFAULT} is returned.
   *
   * @param namespace Namespace "ipc" + "." + Server's listener port.
   * @param conf Configuration properties.
   * @return Value returned based on configuration.
   */
  private boolean getServerFailOverEnable(String namespace, Configuration conf) {
    String propertyKey = namespace + "." +
        CommonConfigurationKeys.IPC_CALLQUEUE_SERVER_FAILOVER_ENABLE;

    if (conf.get(propertyKey) != null) {
      return conf.getBoolean(propertyKey,
          CommonConfigurationKeys.IPC_CALLQUEUE_SERVER_FAILOVER_ENABLE_DEFAULT);
    }

    String[] nsPort = namespace.split("\\.");
    if (nsPort.length == 2) {
      // Only if ns is split with ".", we can separate namespace and port.
      // In the absence of "ipc.<port>.callqueue.overflow.trigger.failover" property,
      // we look up "ipc.callqueue.overflow.trigger.failover" property.
      return conf.getBoolean(nsPort[0] + "."
          + IPC_CALLQUEUE_SERVER_FAILOVER_ENABLE, IPC_CALLQUEUE_SERVER_FAILOVER_ENABLE_DEFAULT);
    }

    // Otherwise return default value.
    LOG.info("{} not specified set default value is {}",
        IPC_CALLQUEUE_SERVER_FAILOVER_ENABLE, IPC_CALLQUEUE_SERVER_FAILOVER_ENABLE_DEFAULT);
    return CommonConfigurationKeys.IPC_CALLQUEUE_SERVER_FAILOVER_ENABLE_DEFAULT;
  }

  private static <T extends RpcScheduler> T createScheduler(
      Class<T> theClass, int priorityLevels, String ns, Configuration conf) {
    // Used for custom, configurable scheduler
    try {
      Constructor<T> ctor = theClass.getDeclaredConstructor(int.class,
          String.class, Configuration.class);
      return ctor.newInstance(priorityLevels, ns, conf);
    } catch (RuntimeException e) {
      throw e;
    } catch (InvocationTargetException e) {
      throw new RuntimeException(theClass.getName()
          + " could not be constructed.", e.getCause());
    } catch (Exception e) {
    }

    try {
      Constructor<T> ctor = theClass.getDeclaredConstructor(int.class);
      return ctor.newInstance(priorityLevels);
    } catch (RuntimeException e) {
      throw e;
    } catch (InvocationTargetException e) {
      throw new RuntimeException(theClass.getName()
          + " could not be constructed.", e.getCause());
    } catch (Exception e) {
    }

    // Last attempt
    try {
      Constructor<T> ctor = theClass.getDeclaredConstructor();
      return ctor.newInstance();
    } catch (RuntimeException e) {
      throw e;
    } catch (InvocationTargetException e) {
      throw new RuntimeException(theClass.getName()
          + " could not be constructed.", e.getCause());
    } catch (Exception e) {
    }

    // Nothing worked
    throw new RuntimeException(theClass.getName() +
        " could not be constructed.");
  }

  private <T extends BlockingQueue<E>> T createCallQueueInstance(
      Class<T> theClass, int priorityLevels, int maxLen, String ns,
      int[] capacityWeights, Configuration conf) {

    // Used for custom, configurable callqueues
    try {
      Constructor<T> ctor = theClass.getDeclaredConstructor(int.class,
          int.class, String.class, int[].class, boolean.class, Configuration.class);
      return ctor.newInstance(priorityLevels, maxLen, ns, capacityWeights,
          this.serverFailOverEnabled, conf);
    } catch (RuntimeException e) {
      throw e;
    } catch (InvocationTargetException e) {
      throw new RuntimeException(theClass.getName()
          + " could not be constructed.", e.getCause());
    } catch (Exception e) {
    }

    // Used for LinkedBlockingQueue, ArrayBlockingQueue, etc
    try {
      Constructor<T> ctor = theClass.getDeclaredConstructor(int.class);
      return ctor.newInstance(maxLen);
    } catch (RuntimeException e) {
      throw e;
    } catch (InvocationTargetException e) {
      throw new RuntimeException(theClass.getName()
          + " could not be constructed.", e.getCause());
    } catch (Exception e) {
    }

    // Last attempt
    try {
      Constructor<T> ctor = theClass.getDeclaredConstructor();
      return ctor.newInstance();
    } catch (RuntimeException e) {
      throw e;
    } catch (InvocationTargetException e) {
      throw new RuntimeException(theClass.getName()
          + " could not be constructed.", e.getCause());
    } catch (Exception e) {
    }

    // Nothing worked
    throw new RuntimeException(theClass.getName() +
      " could not be constructed.");
  }

  boolean isClientBackoffEnabled() {
    return clientBackOffEnabled;
  }

  @VisibleForTesting
  public boolean isServerFailOverEnabled() {
    return serverFailOverEnabled;
  }

  @VisibleForTesting
  public boolean isServerFailOverEnabledByQueue() {
    BlockingQueue<E> bq = putRef.get();
    if (bq instanceof FairCallQueue) {
      return ((FairCallQueue<E>) bq).isServerFailOverEnabled();
    }
    return false;
  }

  // Based on policy to determine back off current call
  boolean shouldBackOff(Schedulable e) {
    return scheduler.shouldBackOff(e);
  }

  void addResponseTime(String name, Schedulable e, ProcessingDetails details) {
    scheduler.addResponseTime(name, e, details);
  }

  // This should be only called once per call and cached in the call object
  int getPriorityLevel(Schedulable e) {
    return scheduler.getPriorityLevel(e);
  }

  int getPriorityLevel(UserGroupInformation user) {
    if (scheduler instanceof DecayRpcScheduler) {
      return ((DecayRpcScheduler)scheduler).getPriorityLevel(user);
    }
    return 0;
  }

  void setPriorityLevel(UserGroupInformation user, int priority) {
    if (scheduler instanceof DecayRpcScheduler) {
      ((DecayRpcScheduler)scheduler).setPriorityLevel(user, priority);
    }
  }

  void setClientBackoffEnabled(boolean value) {
    clientBackOffEnabled = value;
  }

  /**
   * Insert e into the backing queue or block until we can.  If client
   * backoff is enabled this method behaves like add which throws if
   * the queue overflows.
   * If we block and the queue changes on us, we will insert while the
   * queue is drained.
   */
  @Override
  public void put(E e) throws InterruptedException {
    if (!isClientBackoffEnabled()) {
      putRef.get().put(e);
    } else if (shouldBackOff(e)) {
      throwBackoff();
    } else {
      // No need to re-check backoff criteria since they were just checked
      addInternal(e, false);
    }
  }

  @Override
  public boolean add(E e) {
    return addInternal(e, true);
  }

  @VisibleForTesting
  boolean addInternal(E e, boolean checkBackoff) {
    if (checkBackoff && isClientBackoffEnabled() && shouldBackOff(e)) {
      throwBackoff();
    }
    try {
      return putRef.get().add(e);
    } catch (CallQueueOverflowException ex) {
      // queue provided a custom exception that may control if the client
      // should be disconnected.
      throw ex;
    } catch (IllegalStateException ise) {
      throwBackoff();
    }
    return true;
  }

  // ideally this behavior should be controllable too.
  private void throwBackoff() throws IllegalStateException {
    throw serverFailOverEnabled ?
        CallQueueOverflowException.FAILOVER :
        CallQueueOverflowException.DISCONNECT;
  }

  /**
   * Insert e into the backing queue.
   * Return true if e is queued.
   * Return false if the queue is full.
   */
  @Override
  public boolean offer(E e) {
    return putRef.get().offer(e);
  }

  @Override
  public boolean offer(E e, long timeout, TimeUnit unit)
      throws InterruptedException {
    return putRef.get().offer(e, timeout, unit);
  }

  @Override
  public E peek() {
    return takeRef.get().peek();
  }

  @Override
  public E poll() {
    return takeRef.get().poll();
  }

  @Override
  public E poll(long timeout, TimeUnit unit) throws InterruptedException {
    return takeRef.get().poll(timeout, unit);
  }

  /**
   * Retrieve an E from the backing queue or block until we can.
   * Guaranteed to return an element from the current queue.
   */
  @Override
  public E take() throws InterruptedException {
    E e = null;

    while (e == null) {
      e = takeRef.get().poll(1000L, TimeUnit.MILLISECONDS);
    }

    return e;
  }

  @Override
  public int size() {
    return takeRef.get().size();
  }

  @Override
  public int remainingCapacity() {
    return takeRef.get().remainingCapacity();
  }

  /**
   * Read the number of levels from the configuration.
   * This will affect the FairCallQueue's overall capacity.
   * @throws IllegalArgumentException on invalid queue count
   */
  @SuppressWarnings("deprecation")
  private static int parseNumLevels(String ns, Configuration conf) {
    // Fair call queue levels (IPC_CALLQUEUE_PRIORITY_LEVELS_KEY)
    // takes priority over the scheduler level key
    // (IPC_SCHEDULER_PRIORITY_LEVELS_KEY)
    int retval = conf.getInt(ns + "." +
        FairCallQueue.IPC_CALLQUEUE_PRIORITY_LEVELS_KEY, 0);
    if (retval == 0) { // No FCQ priority level configured
      retval = conf.getInt(ns + "." +
          CommonConfigurationKeys.IPC_SCHEDULER_PRIORITY_LEVELS_KEY,
          CommonConfigurationKeys.IPC_SCHEDULER_PRIORITY_LEVELS_DEFAULT_KEY);
    } else {
      LOG.warn(ns + "." + FairCallQueue.IPC_CALLQUEUE_PRIORITY_LEVELS_KEY +
          " is deprecated. Please use " + ns + "." +
          CommonConfigurationKeys.IPC_SCHEDULER_PRIORITY_LEVELS_KEY + ".");
    }
    if(retval < 1) {
      throw new IllegalArgumentException("numLevels must be at least 1");
    }
    return retval;
  }

  /**
   * Read the weights of capacity in callqueue and pass the value to
   * callqueue constructions.
   */
  private static int[] parseCapacityWeights(
      int priorityLevels, String ns, Configuration conf) {
    int[] weights = conf.getInts(ns + "." +
      CommonConfigurationKeys.IPC_CALLQUEUE_CAPACITY_WEIGHTS_KEY);
    if (weights.length == 0) {
      weights = getDefaultQueueCapacityWeights(priorityLevels);
    } else if (weights.length != priorityLevels) {
      throw new IllegalArgumentException(
          CommonConfigurationKeys.IPC_CALLQUEUE_CAPACITY_WEIGHTS_KEY + " must "
              + "specify " + priorityLevels + " capacity weights: one for each "
              + "priority level");
    } else {
      // only allow positive numbers
      for (int w : weights) {
        if (w <= 0) {
          throw new IllegalArgumentException(
              CommonConfigurationKeys.IPC_CALLQUEUE_CAPACITY_WEIGHTS_KEY +
                  " only takes positive weights. " + w + " capacity weight " +
                  "found");
        }
      }
    }
    return weights;
  }

  /**
   * By default, queue capacity is the same for all priority levels.
   *
   * @param priorityLevels number of levels
   * @return default weights
   */
  public static int[] getDefaultQueueCapacityWeights(int priorityLevels) {
    int[] weights = new int[priorityLevels];
    Arrays.fill(weights, 1);
    return weights;
  }

  /**
   * Replaces active queue with the newly requested one and transfers
   * all calls to the newQ before returning.
   *
   * @param schedulerClass input schedulerClass.
   * @param queueClassToUse input queueClassToUse.
   * @param maxSize input maxSize.
   * @param ns input ns.
   * @param conf input configuration.
   */
  public synchronized void swapQueue(
      Class<? extends RpcScheduler> schedulerClass,
      Class<? extends BlockingQueue<E>> queueClassToUse, int maxSize,
      String ns, Configuration conf) {
    int priorityLevels = parseNumLevels(ns, conf);
    this.scheduler.stop();
    RpcScheduler newScheduler = createScheduler(schedulerClass, priorityLevels,
        ns, conf);
    int[] capacityWeights = parseCapacityWeights(priorityLevels, ns, conf);

    // Update serverFailOverEnabled.
    this.serverFailOverEnabled = getServerFailOverEnable(ns, conf);
    BlockingQueue<E> newQ = createCallQueueInstance(queueClassToUse,
        priorityLevels, maxSize, ns, capacityWeights, conf);

    // Our current queue becomes the old queue
    BlockingQueue<E> oldQ = putRef.get();

    // Swap putRef first: allow blocked puts() to be unblocked
    putRef.set(newQ);

    // Wait for handlers to drain the oldQ
    while (!queueIsReallyEmpty(oldQ)) {}

    // Swap takeRef to handle new calls
    takeRef.set(newQ);

    this.scheduler = newScheduler;

    LOG.info("Old Queue: " + stringRepr(oldQ) + ", " +
      "Replacement: " + stringRepr(newQ));
  }

  /**
   * Checks if queue is empty by checking at CHECKPOINT_NUM points with
   * CHECKPOINT_INTERVAL_MS interval.
   * This doesn't mean the queue might not fill up at some point later, but
   * it should decrease the probability that we lose a call this way.
   */
  private boolean queueIsReallyEmpty(BlockingQueue<?> q) {
    for (int i = 0; i < CHECKPOINT_NUM; i++) {
      try {
        Thread.sleep(CHECKPOINT_INTERVAL_MS);
      } catch (InterruptedException ie) {
        return false;
      }
      if (!q.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  private String stringRepr(Object o) {
    return o.getClass().getName() + '@' + Integer.toHexString(o.hashCode());
  }

  @Override
  public int drainTo(Collection<? super E> c) {
    return takeRef.get().drainTo(c);
  }

  @Override
  public int drainTo(Collection<? super E> c, int maxElements) {
    return takeRef.get().drainTo(c, maxElements);
  }

  @Override
  public Iterator<E> iterator() {
    return takeRef.get().iterator();
  }

  // exception that mimics the standard ISE thrown by blocking queues but
  // embeds a rpc server exception for the client to retry and indicate
  // if the client should be disconnected.
  @SuppressWarnings("serial")
  static class CallQueueOverflowException extends IllegalStateException {
    private static String TOO_BUSY = "Server too busy";
    static final CallQueueOverflowException KEEPALIVE =
        new CallQueueOverflowException(
            new RetriableException(TOO_BUSY),
            RpcStatusProto.ERROR);
    static final CallQueueOverflowException DISCONNECT =
        new CallQueueOverflowException(
            new RetriableException(TOO_BUSY + " - disconnecting"),
            RpcStatusProto.FATAL);
    static final CallQueueOverflowException FAILOVER =
        new CallQueueOverflowException(
            new StandbyException(TOO_BUSY + " - disconnect and failover"),
            RpcStatusProto.FATAL);
    CallQueueOverflowException(final IOException ioe,
        final RpcStatusProto status) {
      super("Queue full", new RpcServerException(ioe.getMessage(), ioe){
        @Override
        public RpcStatusProto getRpcStatusProto() {
          return status;
        }
      });
    }
    @Override
    public IOException getCause() {
      return (IOException)super.getCause();
    }
  }
}
