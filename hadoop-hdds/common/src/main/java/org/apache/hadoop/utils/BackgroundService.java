/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An abstract class for a background service in ozone.
 * A background service schedules multiple child tasks in parallel
 * in a certain period. In each interval, it waits until all the tasks
 * finish execution and then schedule next interval.
 */
public abstract class BackgroundService {

  @VisibleForTesting
  public static final Logger LOG =
      LoggerFactory.getLogger(BackgroundService.class);

  // Executor to launch child tasks
  private final ScheduledExecutorService exec;
  private final ThreadGroup threadGroup;
  private final ThreadFactory threadFactory;
  private final String serviceName;
  private final long interval;
  private final long serviceTimeout;
  private final TimeUnit unit;
  private final PeriodicalTask service;

  public BackgroundService(String serviceName, long interval,
      TimeUnit unit, int threadPoolSize, long serviceTimeout) {
    this.interval = interval;
    this.unit = unit;
    this.serviceName = serviceName;
    this.serviceTimeout = serviceTimeout;
    threadGroup = new ThreadGroup(serviceName);
    ThreadFactory tf = r -> new Thread(threadGroup, r);
    threadFactory = new ThreadFactoryBuilder()
        .setThreadFactory(tf)
        .setDaemon(true)
        .setNameFormat(serviceName + "#%d")
        .build();
    exec = Executors.newScheduledThreadPool(threadPoolSize, threadFactory);
    service = new PeriodicalTask();
  }

  protected ExecutorService getExecutorService() {
    return this.exec;
  }

  @VisibleForTesting
  public int getThreadCount() {
    return threadGroup.activeCount();
  }

  @VisibleForTesting
  public void triggerBackgroundTaskForTesting() {
    service.run();
  }

  // start service
  public void start() {
    exec.scheduleWithFixedDelay(service, 0, interval, unit);
  }

  public abstract BackgroundTaskQueue getTasks();

  /**
   * Run one or more background tasks concurrently.
   * Wait until all tasks to return the result.
   */
  public class PeriodicalTask implements Runnable {
    @Override
    public synchronized void run() {
      LOG.debug("Running background service : {}", serviceName);
      BackgroundTaskQueue tasks = getTasks();
      if (tasks.isEmpty()) {
        // No task found, or some problems to init tasks
        // return and retry in next interval.
        return;
      }

      LOG.debug("Number of background tasks to execute : {}", tasks.size());
      CompletionService<BackgroundTaskResult> taskCompletionService =
          new ExecutorCompletionService<>(exec);

      List<Future<BackgroundTaskResult>> results = Lists.newArrayList();
      while (tasks.size() > 0) {
        BackgroundTask task = tasks.poll();
        Future<BackgroundTaskResult> result =
            taskCompletionService.submit(task);
        results.add(result);
      }

      results.parallelStream().forEach(taskResultFuture -> {
        try {
          // Collect task results
          BackgroundTaskResult result = serviceTimeout > 0
              ? taskResultFuture.get(serviceTimeout, unit)
              : taskResultFuture.get();
          if (LOG.isDebugEnabled()) {
            LOG.debug("task execution result size {}", result.getSize());
          }
        } catch (InterruptedException | ExecutionException e) {
          LOG.warn(
              "Background task fails to execute, "
                  + "retrying in next interval", e);
        } catch (TimeoutException e) {
          LOG.warn("Background task executes timed out, "
              + "retrying in next interval", e);
        }
      });
    }
  }

  // shutdown and make sure all threads are properly released.
  public void shutdown() {
    LOG.info("Shutting down service {}", this.serviceName);
    exec.shutdown();
    try {
      if (!exec.awaitTermination(60, TimeUnit.SECONDS)) {
        exec.shutdownNow();
      }
    } catch (InterruptedException e) {
      exec.shutdownNow();
    }
    if (threadGroup.activeCount() == 0 && !threadGroup.isDestroyed()) {
      threadGroup.destroy();
    }
  }
}
