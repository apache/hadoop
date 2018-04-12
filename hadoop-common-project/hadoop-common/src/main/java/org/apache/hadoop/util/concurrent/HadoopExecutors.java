/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.util.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;

/** Factory methods for ExecutorService, ScheduledExecutorService instances.
 * These executor service instances provide additional functionality (e.g
 * logging uncaught exceptions). */
public final class HadoopExecutors {
  public static ExecutorService newCachedThreadPool(ThreadFactory
      threadFactory) {
    return new HadoopThreadPoolExecutor(0, Integer.MAX_VALUE,
        60L, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(),
        threadFactory);
  }

  public static ExecutorService newFixedThreadPool(int nThreads,
      ThreadFactory threadFactory) {
    return new HadoopThreadPoolExecutor(nThreads, nThreads,
        0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(),
        threadFactory);
  }

  public static ExecutorService newFixedThreadPool(int nThreads) {
    return new HadoopThreadPoolExecutor(nThreads, nThreads,
        0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>());
  }

  //Executors.newSingleThreadExecutor has special semantics - for the
  // moment we'll delegate to it rather than implement the semantics here.
  public static ExecutorService newSingleThreadExecutor() {
    return Executors.newSingleThreadExecutor();
  }

  //Executors.newSingleThreadExecutor has special semantics - for the
  // moment we'll delegate to it rather than implement the semantics here.
  public static ExecutorService newSingleThreadExecutor(ThreadFactory
      threadFactory) {
    return Executors.newSingleThreadExecutor(threadFactory);
  }

  public static ScheduledExecutorService newScheduledThreadPool(
      int corePoolSize) {
    return new HadoopScheduledThreadPoolExecutor(corePoolSize);
  }

  public static ScheduledExecutorService newScheduledThreadPool(
      int corePoolSize, ThreadFactory threadFactory) {
    return new HadoopScheduledThreadPoolExecutor(corePoolSize, threadFactory);
  }

  //Executors.newSingleThreadScheduledExecutor has special semantics - for the
  // moment we'll delegate to it rather than implement the semantics here
  public static ScheduledExecutorService newSingleThreadScheduledExecutor() {
    return Executors.newSingleThreadScheduledExecutor();
  }

  //Executors.newSingleThreadScheduledExecutor has special semantics - for the
  // moment we'll delegate to it rather than implement the semantics here
  public static ScheduledExecutorService newSingleThreadScheduledExecutor(
      ThreadFactory threadFactory) {
    return Executors.newSingleThreadScheduledExecutor(threadFactory);
  }

  /**
   * Helper routine to shutdown a executorService.
   *
   * @param executorService - executorService
   * @param logger          - Logger
   * @param timeout         - Timeout
   * @param unit            - TimeUnits, generally seconds.
   */
  public static void shutdown(ExecutorService executorService, Logger logger,
      long timeout, TimeUnit unit) {
    try {
      if (executorService != null) {
        executorService.shutdown();
        try {
          if (!executorService.awaitTermination(timeout, unit)) {
            executorService.shutdownNow();
          }

          if (!executorService.awaitTermination(timeout, unit)) {
            logger.error("Unable to shutdown properly.");
          }
        } catch (InterruptedException e) {
          logger.error("Error attempting to shutdown.", e);
          executorService.shutdownNow();
        }
      }
    } catch (Exception e) {
      logger.error("Error during shutdown: ", e);
      throw e;
    }
  }

  //disable instantiation
  private HadoopExecutors() { }
}