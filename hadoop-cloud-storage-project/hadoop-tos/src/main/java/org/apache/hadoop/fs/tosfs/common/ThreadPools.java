/*
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

package org.apache.hadoop.fs.tosfs.common;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Copied from Apache Iceberg, please see.
 * <a href="https://github.com/apache/iceberg/blob/master/core/src/main/java/org/apache/iceberg/util/ThreadPools.java">
 * ThreadPools</a>
 */
public final class ThreadPools {

  private static final Logger LOG = LoggerFactory.getLogger(ThreadPools.class);

  private ThreadPools() {
  }

  public static final String WORKER_THREAD_POOL_SIZE_PROP = "tos.worker.num-threads";

  public static final int WORKER_THREAD_POOL_SIZE =
      poolSize(Math.max(2, Runtime.getRuntime().availableProcessors()));

  private static final ExecutorService WORKER_POOL = newWorkerPool("tos-default-worker-pool");

  public static ExecutorService defaultWorkerPool() {
    return WORKER_POOL;
  }

  public static ExecutorService newWorkerPool(String namePrefix) {
    return newWorkerPool(namePrefix, WORKER_THREAD_POOL_SIZE);
  }

  public static ExecutorService newWorkerPool(String namePrefix, int poolSize) {
    return Executors.newFixedThreadPool(poolSize, newDaemonThreadFactory(namePrefix));
  }

  public static ScheduledExecutorService newScheduleWorkerPool(String namePrefix, int poolSize) {
    return Executors.newScheduledThreadPool(poolSize, newDaemonThreadFactory(namePrefix));
  }

  /**
   * Helper routine to shutdown a {@link ExecutorService}. Will wait up to a
   * certain timeout for the ExecutorService to gracefully shutdown. If the
   * ExecutorService did not shutdown and there are still tasks unfinished after
   * the timeout period, the ExecutorService will be notified to forcibly shut
   * down. Another timeout period will be waited before giving up. So, at most,
   * a shutdown will be allowed to wait up to twice the timeout value before
   * giving up.
   * <p>
   * This method is copied from
   * {@link HadoopExecutors#shutdown(ExecutorService, Logger, long, TimeUnit)}.
   *
   * @param executorService ExecutorService to shutdown
   * @param timeout         the maximum time to wait
   * @param unit            the time unit of the timeout argument
   */
  public static void shutdown(ExecutorService executorService, long timeout, TimeUnit unit) {
    if (executorService == null) {
      return;
    }

    try {
      executorService.shutdown();
      LOG.debug("Gracefully shutting down executor service. Waiting max {} {}", timeout, unit);

      if (!executorService.awaitTermination(timeout, unit)) {
        LOG.debug("Executor service has not shutdown yet. Forcing. Will wait up to an additional"
                + " {} {} for shutdown", timeout, unit);
        executorService.shutdownNow();
      }

      if (executorService.awaitTermination(timeout, unit)) {
        LOG.debug("Succesfully shutdown executor service");
      } else {
        LOG.error("Unable to shutdown executor service after timeout {} {}", (2 * timeout), unit);
      }
    } catch (InterruptedException e) {
      LOG.error("Interrupted while attempting to shutdown", e);
      executorService.shutdownNow();
    } catch (Exception e) {
      LOG.warn("Exception closing executor service {}", e.getMessage());
      LOG.debug("Exception closing executor service", e);
      throw e;
    }
  }

  private static int poolSize(int defaultSize) {
    String value = System.getProperty(WORKER_THREAD_POOL_SIZE_PROP);
    if (value != null) {
      try {
        return Integer.parseUnsignedInt(value);
      } catch (NumberFormatException e) {
        // will return the default
      }
    }
    return defaultSize;
  }

  public static ThreadFactory newDaemonThreadFactory(String namePrefix) {
    return new ThreadFactoryBuilder().setDaemon(true).setNameFormat(namePrefix + "-%d")
        .setUncaughtExceptionHandler(
            (t, e) -> LOG.error("Thread {} encounter uncaught exception", t, e)).build();
  }

  public static Thread newDaemonThread(String name, Runnable runnable,
      UncaughtExceptionHandler handler) {
    Thread t = new Thread(runnable);
    t.setName(name);
    t.setDaemon(true);
    if (handler != null) {
      t.setUncaughtExceptionHandler(handler);
    }
    return t;
  }
}
