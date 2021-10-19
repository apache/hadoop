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
package org.apache.hadoop.hdfs.server.federation.router;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Service to periodically execute a runnable.
 */
public abstract class PeriodicService extends AbstractService {

  private static final Logger LOG =
      LoggerFactory.getLogger(PeriodicService.class);

  /** Default interval in milliseconds for the periodic service. */
  private static final long DEFAULT_INTERVAL_MS = TimeUnit.MINUTES.toMillis(1);


  /** Interval for running the periodic service in milliseconds. */
  private long intervalMs;
  /** Name of the service. */
  private final String serviceName;

  /** Scheduler for the periodic service. */
  private final ScheduledExecutorService scheduler;

  /** If the service is running. */
  private volatile boolean isRunning = false;

  /** How many times we run. */
  private long runCount;
  /** How many errors we got. */
  private long errorCount;
  /** When was the last time we executed this service successfully. */
  private long lastRun;

  /**
   * Create a new periodic update service.
   *
   * @param name Name of the service.
   */
  public PeriodicService(String name) {
    this(name, DEFAULT_INTERVAL_MS);
  }

  /**
   * Create a new periodic update service.
   *
   * @param name Name of the service.
   * @param interval Interval for the periodic service in milliseconds.
   */
  public PeriodicService(String name, long interval) {
    super(name);
    this.serviceName = name;
    this.intervalMs = interval;

    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat(this.getName() + "-%d")
        .build();
    this.scheduler = Executors.newScheduledThreadPool(1, threadFactory);
  }

  /**
   * Set the interval for the periodic service.
   *
   * @param interval Interval in milliseconds.
   */
  protected void setIntervalMs(long interval) {
    if (getServiceState() == STATE.STARTED) {
      throw new ServiceStateException("Periodic service already started");
    } else {
      this.intervalMs = interval;
    }
  }

  /**
   * Get the interval for the periodic service.
   *
   * @return Interval in milliseconds.
   */
  protected long getIntervalMs() {
    return this.intervalMs;
  }

  /**
   * Get how many times we failed to run the periodic service.
   *
   * @return Times we failed to run the periodic service.
   */
  protected long getErrorCount() {
    return this.errorCount;
  }

  /**
   * Get how many times we run the periodic service.
   *
   * @return Times we run the periodic service.
   */
  protected long getRunCount() {
    return this.runCount;
  }

  /**
   * Get the last time the periodic service was executed.
   *
   * @return Last time the periodic service was executed.
   */
  protected long getLastUpdate() {
    return this.lastRun;
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    LOG.info("Starting periodic service {}", this.serviceName);
    startPeriodic();
  }

  @Override
  protected void serviceStop() throws Exception {
    stopPeriodic();
    LOG.info("Stopping periodic service {}", this.serviceName);
    super.serviceStop();
  }

  /**
   * Stop the periodic task.
   */
  protected synchronized void stopPeriodic() {
    if (this.isRunning) {
      LOG.info("{} is shutting down", this.serviceName);
      this.isRunning = false;
      this.scheduler.shutdownNow();
    }
  }

  /**
   * Start the periodic execution.
   */
  protected synchronized void startPeriodic() {
    stopPeriodic();

    // Create the runnable service
    Runnable updateRunnable = new Runnable() {
      @Override
      public void run() {
        LOG.debug("Running {} update task", serviceName);
        try {
          if (!isRunning) {
            return;
          }
          periodicInvoke();
          runCount++;
          lastRun = Time.now();
        } catch (Exception ex) {
          errorCount++;
          LOG.warn(serviceName + " service threw an exception", ex);
        }
      }
    };

    // Start the execution of the periodic service
    this.isRunning = true;
    this.scheduler.scheduleWithFixedDelay(
        updateRunnable, 0, this.intervalMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Method that the service will run periodically.
   */
  protected abstract void periodicInvoke();
}