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
package org.apache.hadoop.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.MoreExecutors;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * This ExecutorService blocks the submission of new tasks when its queue is
 * already full by using a semaphore. Task submissions require permits, task
 * completions release permits.
 * <p>
 * This is inspired by <a href="https://github.com/apache/incubator-s4/blob/master/subprojects/s4-comm/src/main/java/org/apache/s4/comm/staging/BlockingThreadPoolExecutorService.java">
 * this s4 threadpool</a>
 */
@InterfaceAudience.Private
public final class BlockingThreadPoolExecutorService
    extends SemaphoredDelegatingExecutor {

  private static final Logger LOG = LoggerFactory
      .getLogger(BlockingThreadPoolExecutorService.class);

  private static final AtomicInteger POOLNUMBER = new AtomicInteger(1);

  private final ThreadPoolExecutor eventProcessingExecutor;

  /**
   * Returns a {@link java.util.concurrent.ThreadFactory} that names each
   * created thread uniquely,
   * with a common prefix.
   *
   * @param prefix The prefix of every created Thread's name
   * @return a {@link java.util.concurrent.ThreadFactory} that names threads
   */
  static ThreadFactory getNamedThreadFactory(final String prefix) {
    SecurityManager s = System.getSecurityManager();
    final ThreadGroup threadGroup = (s != null) ? s.getThreadGroup() :
        Thread.currentThread().getThreadGroup();

    return new ThreadFactory() {
      private final AtomicInteger threadNumber = new AtomicInteger(1);
      private final int poolNum = POOLNUMBER.getAndIncrement();
      private final ThreadGroup group = threadGroup;

      @Override
      public Thread newThread(Runnable r) {
        final String name =
            prefix + "-pool" + poolNum + "-t" + threadNumber.getAndIncrement();
        return new Thread(group, r, name);
      }
    };
  }

  /**
   * Get a named {@link ThreadFactory} that just builds daemon threads.
   *
   * @param prefix name prefix for all threads created from the factory
   * @return a thread factory that creates named, daemon threads with
   * the supplied exception handler and normal priority
   */
  public static ThreadFactory newDaemonThreadFactory(final String prefix) {
    final ThreadFactory namedFactory = getNamedThreadFactory(prefix);
    return new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread t = namedFactory.newThread(r);
        if (!t.isDaemon()) {
          t.setDaemon(true);
        }
        if (t.getPriority() != Thread.NORM_PRIORITY) {
          t.setPriority(Thread.NORM_PRIORITY);
        }
        return t;
      }

    };
  }

  private BlockingThreadPoolExecutorService(int permitCount,
      ThreadPoolExecutor eventProcessingExecutor) {
    super(MoreExecutors.listeningDecorator(eventProcessingExecutor),
        permitCount, false);
    this.eventProcessingExecutor = eventProcessingExecutor;
  }

  /**
   * A thread pool that that blocks clients submitting additional tasks if
   * there are already {@code activeTasks} running threads and {@code
   * waitingTasks} tasks waiting in its queue.
   *
   * @param activeTasks maximum number of active tasks
   * @param waitingTasks maximum number of waiting tasks
   * @param keepAliveTime time until threads are cleaned up in {@code unit}
   * @param unit time unit
   * @param prefixName prefix of name for threads
   */
  public static BlockingThreadPoolExecutorService newInstance(
      int activeTasks,
      int waitingTasks,
      long keepAliveTime, TimeUnit unit,
      String prefixName) {

    /* Although we generally only expect up to waitingTasks tasks in the
    queue, we need to be able to buffer all tasks in case dequeueing is
    slower than enqueueing. */
    final BlockingQueue<Runnable> workQueue =
        new LinkedBlockingQueue<>(waitingTasks + activeTasks);
    ThreadPoolExecutor eventProcessingExecutor =
        new ThreadPoolExecutor(activeTasks, activeTasks, keepAliveTime, unit,
            workQueue, newDaemonThreadFactory(prefixName),
            new RejectedExecutionHandler() {
              @Override
              public void rejectedExecution(Runnable r,
                  ThreadPoolExecutor executor) {
                // This is not expected to happen.
                LOG.error("Could not submit task to executor {}",
                    executor.toString());
              }
            });
    eventProcessingExecutor.allowCoreThreadTimeOut(true);
    return new BlockingThreadPoolExecutorService(waitingTasks + activeTasks,
        eventProcessingExecutor);
  }

  /**
   * Get the actual number of active threads.
   * @return the active thread count
   */
  int getActiveCount() {
    return eventProcessingExecutor.getActiveCount();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "BlockingThreadPoolExecutorService{");
    sb.append(super.toString());
    sb.append(", activeCount=").append(getActiveCount());
    sb.append('}');
    return sb.toString();
  }
}
