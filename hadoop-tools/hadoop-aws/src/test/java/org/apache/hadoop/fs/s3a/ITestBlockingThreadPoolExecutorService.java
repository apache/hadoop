/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.util.SemaphoredDelegatingExecutor;
import org.apache.hadoop.util.StopWatch;

import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Basic test for S3A's blocking executor service.
 */
public class ITestBlockingThreadPoolExecutorService {

  private static final Logger LOG = LoggerFactory.getLogger(
      BlockingThreadPoolExecutorService.class);

  private static final int NUM_ACTIVE_TASKS = 4;
  private static final int NUM_WAITING_TASKS = 2;
  private static final int TASK_SLEEP_MSEC = 100;
  private static final int SHUTDOWN_WAIT_MSEC = 200;
  private static final int SHUTDOWN_WAIT_TRIES = 5;
  private static final int BLOCKING_THRESHOLD_MSEC = 50;

  private static final Integer SOME_VALUE = 1337;

  private static BlockingThreadPoolExecutorService tpe;

  @Rule
  public Timeout testTimeout = new Timeout(60 * 1000);

  @AfterClass
  public static void afterClass() throws Exception {
    ensureDestroyed();
  }

  /**
   * Basic test of running one trivial task.
   */
  @Test
  public void testSubmitCallable() throws Exception {
    ensureCreated();
    Future<Integer> f = tpe.submit(callableSleeper);
    Integer v = f.get();
    assertEquals(SOME_VALUE, v);
  }

  /**
   * More involved test, including detecting blocking when at capacity.
   */
  @Test
  public void testSubmitRunnable() throws Exception {
    ensureCreated();
    verifyQueueSize(tpe, NUM_ACTIVE_TASKS + NUM_WAITING_TASKS);
  }

  /**
   * Verify the size of the executor's queue, by verifying that the first
   * submission to block is {@code expectedQueueSize + 1}.
   * @param executorService executor service to test
   * @param expectedQueueSize size of queue
   */
  protected void verifyQueueSize(ExecutorService executorService,
      int expectedQueueSize) {
    CountDownLatch latch = new CountDownLatch(1);
    for (int i = 0; i < expectedQueueSize; i++) {
      executorService.submit(new LatchedSleeper(latch));
    }
    StopWatch stopWatch = new StopWatch().start();
    latch.countDown();
    executorService.submit(sleeper);
    assertDidBlock(stopWatch);
  }

  @Test
  public void testShutdown() throws Exception {
    // Cover create / destroy, regardless of when this test case runs
    ensureCreated();
    ensureDestroyed();

    // Cover create, execute, destroy, regardless of when test case runs
    ensureCreated();
    testSubmitRunnable();
    ensureDestroyed();
  }

  @Test
  public void testChainedQueue() throws Throwable {
    ensureCreated();
    int size = 2;
    ExecutorService wrapper = new SemaphoredDelegatingExecutor(tpe,
        size, true);
    verifyQueueSize(wrapper, size);
  }

  // Helper functions, etc.

  private void assertDidBlock(StopWatch sw) {
    try {
      if (sw.now(TimeUnit.MILLISECONDS) < BLOCKING_THRESHOLD_MSEC) {
        throw new RuntimeException("Blocking call returned too fast.");
      }
    } finally {
      sw.reset().start();
    }
  }

  private Runnable sleeper = new Runnable() {
    @Override
    public void run() {
      String name = Thread.currentThread().getName();
      try {
        Thread.sleep(TASK_SLEEP_MSEC);
      } catch (InterruptedException e) {
        LOG.info("Thread {} interrupted.", name);
        Thread.currentThread().interrupt();
      }
    }
  };

  private Callable<Integer> callableSleeper = new Callable<Integer>() {
    @Override
    public Integer call() throws Exception {
      sleeper.run();
      return SOME_VALUE;
    }
  };

  private class LatchedSleeper implements Runnable {
    private final CountDownLatch latch;

    LatchedSleeper(CountDownLatch latch) {
      this.latch = latch;
    }

    @Override
    public void run() {
      try {
        latch.await();
        Thread.sleep(TASK_SLEEP_MSEC);
      } catch (InterruptedException e) {
        LOG.info("Thread {} interrupted.", Thread.currentThread().getName());
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Helper function to create thread pool under test.
   */
  private static void ensureCreated() throws Exception {
    if (tpe == null) {
      LOG.debug("Creating thread pool");
      tpe = BlockingThreadPoolExecutorService.newInstance(
          NUM_ACTIVE_TASKS, NUM_WAITING_TASKS,
          1, TimeUnit.SECONDS, "btpetest");
    }
  }

  /**
   * Helper function to terminate thread pool under test, asserting that
   * shutdown -> terminate works as expected.
   */
  private static void ensureDestroyed() throws Exception {
    if (tpe == null) {
      return;
    }
    int shutdownTries = SHUTDOWN_WAIT_TRIES;

    tpe.shutdown();
    if (!tpe.isShutdown()) {
      throw new RuntimeException("Shutdown had no effect.");
    }

    while (!tpe.awaitTermination(SHUTDOWN_WAIT_MSEC,
        TimeUnit.MILLISECONDS)) {
      LOG.info("Waiting for thread pool shutdown.");
      if (shutdownTries-- <= 0) {
        LOG.error("Failed to terminate thread pool gracefully.");
        break;
      }
    }
    if (!tpe.isTerminated()) {
      tpe.shutdownNow();
      if (!tpe.awaitTermination(SHUTDOWN_WAIT_MSEC,
          TimeUnit.MILLISECONDS)) {
        throw new RuntimeException(
            "Failed to terminate thread pool in timely manner.");
      }
    }
    tpe = null;
  }
}
