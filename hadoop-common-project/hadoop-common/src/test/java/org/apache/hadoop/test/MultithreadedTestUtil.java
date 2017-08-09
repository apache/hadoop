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
package org.apache.hadoop.test;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility to easily test threaded/synchronized code.
 * Utility works by letting you add threads that do some work to a
 * test context object, and then lets you kick them all off to stress test
 * your parallel code.
 *
 * Also propagates thread exceptions back to the runner, to let you verify.
 *
 * An example:
 *
 * <code>
 *  final AtomicInteger threadsRun = new AtomicInteger();
 *
 *  TestContext ctx = new TestContext();
 *  // Add 3 threads to test.
 *  for (int i = 0; i < 3; i++) {
 *    ctx.addThread(new TestingThread(ctx) {
 *      @Override
 *      public void doWork() throws Exception {
 *        threadsRun.incrementAndGet();
 *      }
 *    });
 *  }
 *  ctx.startThreads();
 *  // Set a timeout period for threads to complete.
 *  ctx.waitFor(30000);
 *  assertEquals(3, threadsRun.get());
 * </code>
 *
 * For repetitive actions, use the {@link MultithreadedTestUtil.RepeatingThread}
 * instead.
 *
 * (More examples can be found in {@link TestMultithreadedTestUtil})
 */
public abstract class MultithreadedTestUtil {

  public static final Logger LOG =
      LoggerFactory.getLogger(MultithreadedTestUtil.class);

  /**
   * TestContext is used to setup the multithreaded test runner.
   * It lets you add threads, run them, wait upon or stop them.
   */
  public static class TestContext {
    private Throwable err = null;
    private boolean stopped = false;
    private Set<TestingThread> testThreads = new HashSet<TestingThread>();
    private Set<TestingThread> finishedThreads = new HashSet<TestingThread>();

    /**
     * Check if the context can run threads.
     * Can't if its been stopped and contains an error.
     * @return true if it can run, false if it can't.
     */
    public synchronized boolean shouldRun()  {
      return !stopped && err == null;
    }

    /**
     * Add a thread to the context for running.
     * Threads can be of type {@link MultithreadedTestUtil.TestingThread}
     * or {@link MultithreadedTestUtil.RepeatingTestThread}
     * or other custom derivatives of the former.
     * @param t the thread to add for running.
     */
    public void addThread(TestingThread t) {
      testThreads.add(t);
    }

    /**
     * Starts all test threads that have been added so far.
     */
    public void startThreads() {
      for (TestingThread t : testThreads) {
        t.start();
      }
    }

    /**
     * Waits for threads to finish or error out.
     * @param millis the number of milliseconds to wait
     * for threads to complete.
     * @throws Exception if one or more of the threads
     * have thrown up an error.
     */
    public synchronized void waitFor(long millis) throws Exception {
      long endTime = Time.now() + millis;
      while (shouldRun() &&
             finishedThreads.size() < testThreads.size()) {
        long left = endTime - Time.now();
        if (left <= 0) break;
        checkException();
        wait(left);
      }
      checkException();
    }

    /**
     * Checks for thread exceptions, and if they've occurred
     * throws them as RuntimeExceptions in a deferred manner.
     */
    public synchronized void checkException() throws Exception {
      if (err != null) {
        throw new RuntimeException("Deferred", err);
      }
    }

    /**
     * Called by {@link MultithreadedTestUtil.TestingThread}s to signal
     * a failed thread.
     * @param t the thread that failed.
     */
    public synchronized void threadFailed(Throwable t) {
      if (err == null) err = t;
      LOG.error("Failed!", err);
      notify();
    }

    /**
     * Called by {@link MultithreadedTestUtil.TestingThread}s to signal
     * a successful completion.
     * @param t the thread that finished.
     */
    public synchronized void threadDone(TestingThread t) {
      finishedThreads.add(t);
      notify();
    }

    /**
     * Returns after stopping all threads by joining them back.
     * @throws Exception in case a thread terminated with a failure.
     */
    public void stop() throws Exception {
      synchronized (this) {
        stopped = true;
      }
      for (TestingThread t : testThreads) {
        t.join();
      }
      checkException();
    }

    public Iterable<? extends Thread> getTestThreads() {
      return testThreads;
    }
  }

  /**
   * A thread that can be added to a test context, and properly
   * passes exceptions through.
   */
  public static abstract class TestingThread extends Thread {
    protected final TestContext ctx;
    protected boolean stopped;

    public TestingThread(TestContext ctx) {
      this.ctx = ctx;
    }

    @Override
    public void run() {
      try {
        doWork();
      } catch (Throwable t) {
        ctx.threadFailed(t);
      }
      ctx.threadDone(this);
    }

    /**
     * User method to add any code to test thread behavior of.
     * @throws Exception throw an exception if a failure has occurred.
     */
    public abstract void doWork() throws Exception;

    protected void stopTestThread() {
      this.stopped = true;
    }
  }

  /**
   * A test thread that performs a repeating operation.
   */
  public static abstract class RepeatingTestThread extends TestingThread {
    public RepeatingTestThread(TestContext ctx) {
      super(ctx);
    }

    /**
     * Repeats a given user action until the context is asked to stop
     * or meets an error.
     */
    @Override
    public final void doWork() throws Exception {
      while (ctx.shouldRun() && !stopped) {
        doAnAction();
      }
    }

    /**
     * User method for any code to test repeating behavior of (as threads).
     * @throws Exception throw an exception if a failure has occurred.
     */
    public abstract void doAnAction() throws Exception;
  }
}
