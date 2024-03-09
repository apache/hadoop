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

import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
import org.apache.hadoop.yarn.metrics.DispatcherEventMetricsImpl;
import org.apache.hadoop.yarn.metrics.DispatcherEventMetricsNoOps;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;

/**
 * Dispatches {@link Event}s in a parallel thread.
 * The {@link Dispatcher#getEventHandler()} method can be used to post an event to the dispatcher.
 * The posted event will be separated based on the hashcode of the {@link Event#getLockKey()}.
 * If the getLockKey() method returns with null,
 * then the first executor thread will be used as default worker
 * The {@link MultiDispatcherConfig} contains the information,
 * how many thread will be used, for parallel execution.
 * The {@link MultiDispatcherExecutor} contains the worker threads, which handle the events.
 * The {@link MultiDispatcherLibrary} contains the information,
 * how to pair event types with {@link EventHandler}s
 * The Dispatcher provides metric data using the {@link DispatcherEventMetricsImpl}.
 */
public class MultiDispatcher extends AbstractService implements Dispatcher {

  private final Logger log;
  private final String dispatcherName;
  private final MultiDispatcherLibrary library;
  private final Clock clock = new MonotonicClock();

  private MultiDispatcherExecutor workerExecutor;
  private ScheduledThreadPoolExecutor monitorExecutor;
  private DispatcherEventMetrics metrics;

  public MultiDispatcher(String dispatcherName) {
    super("Dispatcher");
    this.dispatcherName = dispatcherName.replaceAll(" ", "-").toLowerCase();
    this.log = LoggerFactory.getLogger(MultiDispatcher.class.getCanonicalName() + "." + this.dispatcherName);
    this.library = new MultiDispatcherLibrary();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception{
    super.serviceInit(conf);
    MultiDispatcherConfig config = new MultiDispatcherConfig(getConfig(), dispatcherName);
    workerExecutor = new MultiDispatcherExecutor(log, config, dispatcherName);
    workerExecutor.start();
    createMonitorThread(config);
    if (config.getMetricsEnabled()) {
      metrics = new DispatcherEventMetricsImpl(dispatcherName);
      DefaultMetricsSystem.instance().register(
          "Event metrics for " +  dispatcherName,
          "Event metrics for " +  dispatcherName,
          metrics
      );
    } else {
      metrics = new DispatcherEventMetricsNoOps(log);
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    if (monitorExecutor != null) {
      monitorExecutor.shutdown();
    }
    workerExecutor.stop();
  }

  @Override
  public EventHandler getEventHandler() {
    return event -> {
      if (isInState(STATE.STOPPED)) {
        log.warn("Discard event {} because stopped state", event);
      } else {
        EventHandler handler = library.getEventHandler(event);
        Runnable runnable = createRunnable(event, handler);
        workerExecutor.execute(event, runnable);
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
      long start = clock.getTime();
      try {
        handler.handle(event);
      } finally {
        metrics.updateRate(event.getType(), clock.getTime() - start);
        metrics.removeEvent(event.getType());
      }
    };
  }

  private void createMonitorThread(MultiDispatcherConfig config) {
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
      List<String> notEmptyQueues = workerExecutor.getQueuesSize().entrySet().stream()
          .filter(e -> 0 < e.getValue())
          .map(e -> String.format("%s has queue size %d", e.getKey(), e.getValue()))
          .sorted()
          .collect(Collectors.toList());
      if (!notEmptyQueues.isEmpty()) {
        log.info("Event queue sizes: {}", notEmptyQueues);
      }
      log.debug("Metrics: {}", metrics);
    },10, interval, TimeUnit.SECONDS);
  }
}
