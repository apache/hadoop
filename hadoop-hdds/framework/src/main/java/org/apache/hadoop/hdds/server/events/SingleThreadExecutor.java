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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple EventExecutor to call all the event handler one-by-one.
 *
 * @param <T>
 */
public class SingleThreadExecutor<T> implements EventExecutor<T> {

  public static final String THREAD_NAME_PREFIX = "EventQueue";

  private static final Logger LOG =
      LoggerFactory.getLogger(SingleThreadExecutor.class);

  private final String name;

  private final ThreadPoolExecutor executor;

  private final AtomicLong queuedCount = new AtomicLong(0);

  private final AtomicLong successfulCount = new AtomicLong(0);

  private final AtomicLong failedCount = new AtomicLong(0);

  public SingleThreadExecutor(String name) {
    this.name = name;

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
    queuedCount.incrementAndGet();
    executor.execute(() -> {
      try {
        handler.onMessage(message, publisher);
        successfulCount.incrementAndGet();
      } catch (Exception ex) {
        LOG.error("Error on execution message {}", message, ex);
        failedCount.incrementAndGet();
      }
    });
  }

  @Override
  public long failedEvents() {
    return failedCount.get();
  }

  @Override
  public long successfulEvents() {
    return successfulCount.get();
  }

  @Override
  public long queuedEvents() {
    return queuedCount.get();
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
