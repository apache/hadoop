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

package org.apache.hadoop.yarn.event.multidispatcher;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.metrics.DispatcherEventMetrics;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;

/**
 * Dispatches {@link Event}s in a parallel thread.
 * The {@link this#getEventHandler()} method can be used to post an event to the dispatcher.
 * The posted event will be added to the event queue what is polled by many thread,
 * based on the config values in the {@link MultiDispatcherConfig}.
 * The posted events can be parallel executed,
 * if the result of the {@link Event#getLockKey()} is different between 2 event.
 * If the method return with null, then a global semaphore will be used for these events.
 * The method usually returns with the applicationId based on the concept
 * parallel apps should not affect each others.
 * The locking logic is implemented in {@link MultiDispatcherLocks}.
 * The Dispatcher provides metric data using the {@link DispatcherEventMetrics}
 */
public class MultiDispatcher extends AbstractService implements Dispatcher {

  private final Logger LOG;
  private final String dispatcherName;
  private final DispatcherEventMetrics metrics;
  private final MultiDispatcherLocks locks;
  private final MultiDispatcherLibrary library;
  private final Clock clock = new MonotonicClock();

  private MultiDispatcherConfig config;
  private BlockingQueue<Runnable> eventQueue;
  private ThreadPoolExecutor threadPoolExecutor;
  private ScheduledThreadPoolExecutor monitorExecutor;

  public MultiDispatcher(String dispatcherName) {
    super("Dispatcher");
    this.dispatcherName = dispatcherName.replaceAll(" ", "-").toLowerCase();
    this.LOG = LoggerFactory.getLogger(MultiDispatcher.class.getCanonicalName() + "." + this.dispatcherName);
    this.metrics = new DispatcherEventMetrics(this.dispatcherName);
    this.locks = new MultiDispatcherLocks(this.LOG);
    this.library = new MultiDispatcherLibrary();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception{
    super.serviceInit(conf);
    this.config = new MultiDispatcherConfig(getConfig(), dispatcherName);
    this.eventQueue = new LinkedBlockingQueue<>(config.getQueueSize());
    createWorkerPool();
    createMonitorThread();
    DefaultMetricsSystem.instance().register(
        "Event metrics for " +  dispatcherName,
        "Event metrics for " +  dispatcherName,
        metrics
    );
  }

  @Override
  protected void serviceStop() throws Exception {
    if (monitorExecutor != null) {
      monitorExecutor.shutdownNow();
    }
    threadPoolExecutor.shutdown();
    threadPoolExecutor.awaitTermination(config.getGracefulStopSeconds(), TimeUnit.SECONDS);
    int terminatedSize = threadPoolExecutor.shutdownNow().size();
    if (0 < terminatedSize) {
      LOG.error("{} tasks not finished in time, so they were terminated", terminatedSize);
    }
  }

  @Override
  public EventHandler getEventHandler() {
    return event -> {
      if (isInState(STATE.STOPPED)) {
        LOG.warn("Discard event {} because stopped state", event);
      } else {
        EventHandler handler = library.getEventHandler(event);
        threadPoolExecutor.execute(createRunnable(event, handler));
        metrics.addEvent(event.getType());
      }
    };
  }

  @Override
  public void register(Class<? extends Enum> eventType, EventHandler handler) {
    library.register(eventType, handler);
    metrics.init(eventType);
  }

  private Runnable createRunnable(Event event, EventHandler handler) {
    return () -> {
      LOG.debug("{} handle {}, queue size: {}",
          Thread.currentThread().getName(), event.getClass().getSimpleName(), eventQueue.size());
      locks.lock(event);
      long start = clock.getTime();
      try {
        handler.handle(event);
      } finally {
        locks.unLock(event);
        metrics.updateRate(event.getType(), clock.getTime() - start);
        metrics.removeEvent(event.getType());
      }
    };
  }

  private void createWorkerPool() {
    this.threadPoolExecutor = new ThreadPoolExecutor(
        config.getDefaultPoolSize(),
        config.getMaxPoolSize(),
        config.getKeepAliveSeconds(),
        TimeUnit.SECONDS,
        eventQueue,
        new BasicThreadFactory.Builder()
            .namingPattern(this.dispatcherName + "-worker-%d")
            .build()
    );
  }

  private void createMonitorThread() {
    int interval = config.getMonitorSeconds();
    if (interval < 1) {
      return;
    }
    this.monitorExecutor = new ScheduledThreadPoolExecutor(
        1,
        new BasicThreadFactory.Builder()
            .namingPattern(this.dispatcherName + "-monitor-%d")
            .build());
    monitorExecutor.scheduleAtFixedRate(() -> {
      int size = eventQueue.size();
      if (0 < size) {
        LOG.info("Event queue size is {}", size);
        LOG.debug("Metrics: {}", metrics);
      }
    },10, interval, TimeUnit.SECONDS);
  }
}
