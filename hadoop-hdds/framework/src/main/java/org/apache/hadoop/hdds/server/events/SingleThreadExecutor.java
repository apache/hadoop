/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.server.events;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

/**
 * Simple EventExecutor to call all the event handler one-by-one.
 *
 * @param <T>
 */
@Metrics(context = "EventQueue")
public class SingleThreadExecutor<T> implements EventExecutor<T> {

  public static final String THREAD_NAME_PREFIX = "EventQueue";

  private static final Logger LOG =
      LoggerFactory.getLogger(SingleThreadExecutor.class);

  private final String name;

  private final ThreadPoolExecutor executor;

  @Metric
  private MutableCounterLong queued;

  @Metric
  private MutableCounterLong done;

  @Metric
  private MutableCounterLong failed;

  /**
   * Create SingleThreadExecutor.
   *
   * @param name Unique name used in monitoring and metrics.
   */
  public SingleThreadExecutor(String name) {
    this.name = name;
    DefaultMetricsSystem.instance()
        .register("EventQueue" + name, "Event Executor metrics ", this);

    LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
    executor =
        new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, workQueue,
            runnable -> {
              Thread thread = new Thread(runnable);
              thread.setName(THREAD_NAME_PREFIX + "-" + name);
              return thread;
            });

  }

  @Override
  public void onMessage(EventHandler<T> handler, T message, EventPublisher
      publisher) {
    queued.incr();
    executor.execute(() -> {
      try {
        handler.onMessage(message, publisher);
        done.incr();
      } catch (Exception ex) {
        LOG.error("Error on execution message {}", message, ex);
        failed.incr();
      }
    });
  }

  @Override
  public long failedEvents() {
    return failed.value();
  }

  @Override
  public long successfulEvents() {
    return done.value();
  }

  @Override
  public long queuedEvents() {
    return queued.value();
  }

  @Override
  public void close() {
    executor.shutdown();
  }

  @Override
  public String getName() {
    return name;
  }
}
