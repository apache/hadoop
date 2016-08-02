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

package org.apache.slider.server.appmaster.actions;

import com.google.common.base.Preconditions;
import org.apache.slider.server.appmaster.SliderAppMaster;
import org.apache.slider.server.appmaster.state.AppState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This action executes then reschedules an inner action; a limit
 * can specify the number of times to run
 */

public class RenewingAction<A extends AsyncAction> extends AsyncAction {
  private static final Logger log =
      LoggerFactory.getLogger(RenewingAction.class);
  private final A action;
  private long interval;
  private TimeUnit timeUnit;
  public final AtomicInteger executionCount = new AtomicInteger();
  private final ReentrantReadWriteLock intervalLock = new ReentrantReadWriteLock();
  private final Lock intervalReadLock = intervalLock.readLock();
  private final Lock intervalWriteLock = intervalLock.writeLock();
  public final int limit;


  /**
   * Rescheduling action
   * @param action action to execute
   * @param initialDelay initial delay
   * @param interval interval for later delays
   * @param timeUnit time unit for all times
   * @param limit limit on the no. of executions. If 0 or less: no limit
   */
  public RenewingAction(A action,
      long initialDelay,
      long interval,
      TimeUnit timeUnit,
      int limit) {
    super("renewing " + action.name, initialDelay, timeUnit, action.getAttrs());
    Preconditions.checkArgument(interval > 0, "invalid interval: " + interval);
    this.action = action;
    this.interval = interval;
    this.timeUnit = timeUnit;
    this.limit = limit;
  }

  /**
   * Execute the inner action then reschedule ourselves
   * @param appMaster
   * @param queueService
   * @param appState
   * @throws Exception
   */
  @Override
  public void execute(SliderAppMaster appMaster,
      QueueAccess queueService,
      AppState appState)
      throws Exception {
    long exCount = executionCount.incrementAndGet();
    log.debug("{}: Executing inner action count # {}", this, exCount);
    action.execute(appMaster, queueService, appState);
    boolean reschedule = true;
    if (limit > 0) {
      reschedule = limit > exCount;
    }
    if (reschedule) {
      this.setNanos(convertAndOffset(getInterval(), getTimeUnit()));
      log.debug("{}: rescheduling, new offset {} mS ", this,
          getDelay(TimeUnit.MILLISECONDS));
      queueService.schedule(this);
    }
  }

  /**
   * Get the action
   * @return
   */
  public A getAction() {
    return action;
  }

  public long getInterval() {
    intervalReadLock.lock();
    try {
      return interval;
    } finally {
      intervalReadLock.unlock();
    }
  }

  public void updateInterval(long delay, TimeUnit timeUnit) {
    intervalWriteLock.lock();
    try {
      interval = delay;
      this.timeUnit = timeUnit;
    } finally {
      intervalWriteLock.unlock();
    }
  }

  public TimeUnit getTimeUnit() {
    intervalReadLock.lock();
    try {
      return timeUnit;
    } finally {
      intervalReadLock.unlock();
    }
  }

  public int getExecutionCount() {
    return executionCount.get();
  }

  public int getLimit() {
    return limit;
  }
}
