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

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import org.slf4j.Logger;

import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;

/**
 * This class contains the thread which process the {@link MultiDispatcher}'s events.
 */
public class MultiDispatcherExecutor {

  private final Logger log;
  private final MultiDispatcherConfig config;
  private final MultiDispatcherExecutorThread[] threads;
  private final Clock clock = new MonotonicClock();

  public MultiDispatcherExecutor(
      Logger log,
      MultiDispatcherConfig config,
      String dispatcherName
  ) {
    this.log = log;
    this.config = config;
    this.threads = new MultiDispatcherExecutorThread[config.getDefaultPoolSize()];
    ThreadGroup group = new ThreadGroup(dispatcherName);
    for (int i = 0; i < threads.length; ++i) {
      threads[i] = new MultiDispatcherExecutorThread(group, i, config.getQueueSize());
    }
  }

  public void start() {
    for(Thread t : threads) {
      t.start();
    }
  }

  public void execute(Event event, Runnable runnable) {
    String lockKey = event.getLockKey();
    // abs of Integer.MIN_VALUE is Integer.MIN_VALUE
    int threadIndex = lockKey == null  || lockKey.hashCode() == Integer.MIN_VALUE ?
        0 : Math.abs(lockKey.hashCode() % threads.length);
    MultiDispatcherExecutorThread thread = threads[threadIndex];
    thread.add(runnable);
    log.trace("The {} with lock key {} will be handled by {}",
        event.getType(), lockKey, thread.getName());
  }

  public void stop() throws InterruptedException {
    long timeOut = clock.getTime() + config.getGracefulStopSeconds() * 1_000L;
    // if not all queue is empty
    if (Arrays.stream(threads).anyMatch(t -> 0 < t.queueSize())
        // and not timeout yet
      && clock.getTime() < timeOut) {
      log.debug("Not all event queue is empty, waiting to drain ...");
      Thread.sleep(1_000);
    }
    for (MultiDispatcherExecutorThread thread : threads) {
      thread.interrupt();
    }
  }

  public Map<String, Long> getQueuesSize() {
    return Arrays.stream(threads).collect(Collectors.toMap(
        MultiDispatcherExecutorThread::getName,
        MultiDispatcherExecutorThread::queueSize
    ));
  }

  private final class MultiDispatcherExecutorThread extends Thread {
    private final BlockingQueue<Runnable> queue;

    MultiDispatcherExecutorThread(ThreadGroup group, int index, int queueSize) {
      super(group, String.format("%s-worker-%d", group.getName(), index));
      this.queue = new LinkedBlockingQueue<>(queueSize);
    }

    void add(Runnable runnable) {
      queue.add(runnable);
    }

    long queueSize() {
      return queue.size();
    }

    @Override
    public void run() {
      try {
        while (true) {
          queue.take().run();
        }
      } catch (InterruptedException e) {
        log.warn("{} get interrupted", getName());
      }
    }
  }
}
