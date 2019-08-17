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
   * Helper routine to shutdown a {@link ExecutorService}. Will wait up to a
   * certain timeout for the ExecutorService to gracefully shutdown. If the
   * ExecutorService did not shutdown and there are still tasks unfinished after
   * the timeout period, the ExecutorService will be notified to forcibly shut
   * down. Another timeout period will be waited before giving up. So, at most,
   * a shutdown will be allowed to wait up to twice the timeout value before
   * giving up.
   *
   * @param executorService ExecutorService to shutdown
   * @param logger Logger
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   */
  public static void shutdown(ExecutorService executorService, Logger logger,
      long timeout, TimeUnit unit) {

    if (executorService == null) {
      return;
    }

    try {
      executorService.shutdown();

      logger.info(
          "Gracefully shutting down executor service. Waiting max {} {}",
          timeout, unit);
      if (!executorService.awaitTermination(timeout, unit)) {
        logger.info(
            "Executor service has not shutdown yet. Forcing. "
                + "Will wait up to an additional {} {} for shutdown",
            timeout, unit);
        executorService.shutdownNow();
      }
      if (executorService.awaitTermination(timeout, unit)) {
        logger.info("Succesfully shutdown executor service");
      } else {
        logger.error("Unable to shutdown executor service after timeout {} {}",
            (2 * timeout), unit);
      }
    } catch (InterruptedException e) {
      logger.error("Interrupted while attempting to shutdown", e);
      executorService.shutdownNow();
    } catch (Exception e) {
      logger.warn("Exception closing executor service {}", e.getMessage());
      logger.debug("Exception closing executor service", e);
      throw e;
    }
  }

  //disable instantiation
  private HadoopExecutors() { }
}