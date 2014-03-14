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

import java.lang.reflect.Constructor;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Abstracts queue operations for different blocking queues.
 */
public class CallQueueManager<E> {
  public static final Log LOG = LogFactory.getLog(CallQueueManager.class);

  @SuppressWarnings("unchecked")
  static <E> Class<? extends BlockingQueue<E>> convertQueueClass(
      Class<?> queneClass, Class<E> elementClass) {
    return (Class<? extends BlockingQueue<E>>)queneClass;
  }
  
  // Atomic refs point to active callQueue
  // We have two so we can better control swapping
  private final AtomicReference<BlockingQueue<E>> putRef;
  private final AtomicReference<BlockingQueue<E>> takeRef;

  public CallQueueManager(Class<? extends BlockingQueue<E>> backingClass,
      int maxQueueSize, String namespace, Configuration conf) {
    BlockingQueue<E> bq = createCallQueueInstance(backingClass,
      maxQueueSize, namespace, conf);
    this.putRef = new AtomicReference<BlockingQueue<E>>(bq);
    this.takeRef = new AtomicReference<BlockingQueue<E>>(bq);
    LOG.info("Using callQueue " + backingClass);
  }

  private <T extends BlockingQueue<E>> T createCallQueueInstance(
      Class<T> theClass, int maxLen, String ns, Configuration conf) {

    // Used for custom, configurable callqueues
    try {
      Constructor<T> ctor = theClass.getDeclaredConstructor(int.class, String.class,
        Configuration.class);
      return ctor.newInstance(maxLen, ns, conf);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
    }

    // Used for LinkedBlockingQueue, ArrayBlockingQueue, etc
    try {
      Constructor<T> ctor = theClass.getDeclaredConstructor(int.class);
      return ctor.newInstance(maxLen);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
    }

    // Last attempt
    try {
      Constructor<T> ctor = theClass.getDeclaredConstructor();
      return ctor.newInstance();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
    }

    // Nothing worked
    throw new RuntimeException(theClass.getName() +
      " could not be constructed.");
  }

  /**
   * Insert e into the backing queue or block until we can.
   * If we block and the queue changes on us, we will insert while the
   * queue is drained.
   */
  public void put(E e) throws InterruptedException {
    putRef.get().put(e);
  }

  /**
   * Retrieve an E from the backing queue or block until we can.
   * Guaranteed to return an element from the current queue.
   */
  public E take() throws InterruptedException {
    E e = null;

    while (e == null) {
      e = takeRef.get().poll(1000L, TimeUnit.MILLISECONDS);
    }

    return e;
  }

  public int size() {
    return takeRef.get().size();
  }

  /**
   * Replaces active queue with the newly requested one and transfers
   * all calls to the newQ before returning.
   */
  public synchronized void swapQueue(
      Class<? extends BlockingQueue<E>> queueClassToUse, int maxSize,
      String ns, Configuration conf) {
    BlockingQueue<E> newQ = createCallQueueInstance(queueClassToUse, maxSize,
      ns, conf);

    // Our current queue becomes the old queue
    BlockingQueue<E> oldQ = putRef.get();

    // Swap putRef first: allow blocked puts() to be unblocked
    putRef.set(newQ);

    // Wait for handlers to drain the oldQ
    while (!queueIsReallyEmpty(oldQ)) {}

    // Swap takeRef to handle new calls
    takeRef.set(newQ);

    LOG.info("Old Queue: " + stringRepr(oldQ) + ", " +
      "Replacement: " + stringRepr(newQ));
  }

  /**
   * Checks if queue is empty by checking at two points in time.
   * This doesn't mean the queue might not fill up at some point later, but
   * it should decrease the probability that we lose a call this way.
   */
  private boolean queueIsReallyEmpty(BlockingQueue<?> q) {
    boolean wasEmpty = q.isEmpty();
    try {
      Thread.sleep(10);
    } catch (InterruptedException ie) {
      return false;
    }
    return q.isEmpty() && wasEmpty;
  }

  private String stringRepr(Object o) {
    return o.getClass().getName() + '@' + Integer.toHexString(o.hashCode());
  }
}
