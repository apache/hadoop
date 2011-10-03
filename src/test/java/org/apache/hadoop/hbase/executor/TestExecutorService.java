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

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.executor.EventHandler.EventType;
import org.apache.hadoop.hbase.executor.ExecutorService.Executor;
import org.apache.hadoop.hbase.executor.ExecutorService.ExecutorStatus;
import org.apache.hadoop.hbase.executor.ExecutorService.ExecutorType;
import org.junit.Test;
import static org.mockito.Mockito.*;

public class TestExecutorService {
  private static final Log LOG = LogFactory.getLog(TestExecutorService.class);

  @Test
  public void testExecutorService() throws Exception {
    int maxThreads = 5;
    int maxTries = 10;
    int sleepInterval = 10;

    Server mockedServer = mock(Server.class);
    when(mockedServer.getConfiguration()).thenReturn(HBaseConfiguration.create());

    // Start an executor service pool with max 5 threads
    ExecutorService executorService = new ExecutorService("unit_test");
    executorService.startExecutorService(
      ExecutorType.MASTER_SERVER_OPERATIONS, maxThreads);

    Executor executor =
      executorService.getExecutor(ExecutorType.MASTER_SERVER_OPERATIONS);
    ThreadPoolExecutor pool = executor.threadPoolExecutor;

    // Assert no threads yet
    assertEquals(0, pool.getPoolSize());

    AtomicBoolean lock = new AtomicBoolean(true);
    AtomicInteger counter = new AtomicInteger(0);

    // Submit maxThreads executors.
    for (int i = 0; i < maxThreads; i++) {
      executorService.submit(
        new TestEventHandler(mockedServer, EventType.M_SERVER_SHUTDOWN,
            lock, counter));
    }

    // The TestEventHandler will increment counter when it starts.
    int tries = 0;
    while (counter.get() < maxThreads && tries < maxTries) {
      LOG.info("Waiting for all event handlers to start...");
      Thread.sleep(sleepInterval);
      tries++;
    }

    // Assert that pool is at max threads.
    assertEquals(maxThreads, counter.get());
    assertEquals(maxThreads, pool.getPoolSize());

    ExecutorStatus status = executor.getStatus();
    assertTrue(status.queuedEvents.isEmpty());
    assertEquals(5, status.running.size());
    checkStatusDump(status);
    
    
    // Now interrupt the running Executor
    synchronized (lock) {
      lock.set(false);
      lock.notifyAll();
    }

    // Executor increments counter again on way out so.... test that happened.
    while (counter.get() < (maxThreads * 2) && tries < maxTries) {
      System.out.println("Waiting for all event handlers to finish...");
      Thread.sleep(sleepInterval);
      tries++;
    }

    assertEquals(maxThreads * 2, counter.get());
    assertEquals(maxThreads, pool.getPoolSize());

    // Add more than the number of threads items.
    // Make sure we don't get RejectedExecutionException.
    for (int i = 0; i < (2 * maxThreads); i++) {
      executorService.submit(
        new TestEventHandler(mockedServer, EventType.M_SERVER_SHUTDOWN,
            lock, counter));
    }
    // Now interrupt the running Executor
    synchronized (lock) {
      lock.set(false);
      lock.notifyAll();
    }

    // Make sure threads are still around even after their timetolive expires.
    Thread.sleep(executor.keepAliveTimeInMillis * 2);
    assertEquals(maxThreads, pool.getPoolSize());

    executorService.shutdown();

    assertEquals(0, executorService.getAllExecutorStatuses().size());

    // Test that submit doesn't throw NPEs
    executorService.submit(
      new TestEventHandler(mockedServer, EventType.M_SERVER_SHUTDOWN,
            lock, counter));
  }

  private void checkStatusDump(ExecutorStatus status) throws IOException {
    StringWriter sw = new StringWriter();
    status.dumpTo(sw, "");
    String dump = sw.toString();
    LOG.info("Got status dump:\n" + dump);
    
    assertTrue(dump.contains("Waiting on java.util.concurrent.atomic.AtomicBoolean"));
  }

  public static class TestEventHandler extends EventHandler {
    private AtomicBoolean lock;
    private AtomicInteger counter;

    public TestEventHandler(Server server, EventType eventType,
                            AtomicBoolean lock, AtomicInteger counter) {
      super(server, eventType);
      this.lock = lock;
      this.counter = counter;
    }

    @Override
    public void process() throws IOException {
      int num = counter.incrementAndGet();
      LOG.info("Running process #" + num + ", threadName=" +
        Thread.currentThread().getName());
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