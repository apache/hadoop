/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.executor;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.executor.EventHandler.EventType;
import org.apache.hadoop.hbase.executor.ExecutorService.Executor;
import org.apache.hadoop.hbase.executor.ExecutorService.ExecutorType;
import org.junit.Test;

public class TestExecutorService {
  @Test
  public void testExecutorService() throws Exception {
    int maxThreads = 5;
    int maxTries = 10;
    int sleepInterval = 10;

    // Start an executor service pool with max 5 threads
    ExecutorService executorService = new ExecutorService("unit_test");
    executorService.startExecutorService(
        ExecutorType.MASTER_SERVER_OPERATIONS, maxThreads);

    Executor executor =
      executorService.getExecutor(ExecutorType.MASTER_SERVER_OPERATIONS);

    AtomicBoolean lock = new AtomicBoolean(true);
    AtomicInteger counter = new AtomicInteger(0);

    for (int i = 0; i < maxThreads; i++) {
      executorService.submit(
        new TestEventHandler(EventType.M_SERVER_SHUTDOWN, lock, counter));
    }

    int tries = 0;
    while (counter.get() < maxThreads && tries < maxTries) {
      System.out.println("Waiting for all event handlers to start...");
      Thread.sleep(sleepInterval);
      tries++;
    }

    assertEquals(maxThreads, counter.get());

    synchronized (lock) {
      lock.set(false);
      lock.notifyAll();
    }

    while (counter.get() < (maxThreads * 2) && tries < maxTries) {
      System.out.println("Waiting for all event handlers to finish...");
      Thread.sleep(sleepInterval);
      tries++;
    }

    assertEquals(maxThreads*2, counter.get());

    // Add too many. Make sure they are queued.  Make sure we don't get
    // RejectedExecutionException.
    for (int i = 0; i < maxThreads; i++) {
      executorService.submit(
        new TestEventHandler(EventType.M_SERVER_SHUTDOWN, lock, counter));
    }

    Thread.sleep(executor.keepAliveTimeInMillis * 2);
  }

  public static class TestEventHandler extends EventHandler {
    private AtomicBoolean lock;
    private AtomicInteger counter;

    public TestEventHandler(EventType eventType, AtomicBoolean lock,
        AtomicInteger counter) {
      super(null, eventType);
      this.lock = lock;
      this.counter = counter;
    }

    @Override
    public void process() throws IOException {
      int num = counter.incrementAndGet();
      System.out.println("Running process #" + num + ", thread=" + Thread.currentThread());
      synchronized (lock) {
        while (lock.get()) {
          try {
            lock.wait();
          } catch (InterruptedException e) {
            // do nothing
          }
        }
      }
      counter.incrementAndGet();
    }
  }
}