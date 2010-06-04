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
package org.apache.hadoop.hbase;

import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;

public abstract class MultithreadedTestUtil {

  public static class TestContext {
    private final Configuration conf;
    private Throwable err = null;
    private boolean stopped = false;
    private int threadDoneCount = 0;
    private Set<TestThread> testThreads = new HashSet<TestThread>();

    public TestContext(Configuration configuration) {
      this.conf = configuration;
    }

    protected Configuration getConf() {
      return conf;
    }

    public synchronized boolean shouldRun()  {
      return !stopped && err == null;
    }

    public void addThread(TestThread t) {
      testThreads.add(t);
    }

    public void startThreads() {
      for (TestThread t : testThreads) {
        t.start();
      }
    }

    public void waitFor(long millis) throws Exception {
      long endTime = System.currentTimeMillis() + millis;
      while (!stopped) {
        long left = endTime - System.currentTimeMillis();
        if (left <= 0) break;
        synchronized (this) {
          checkException();
          wait(left);
        }
      }
    }
    private synchronized void checkException() throws Exception {
      if (err != null) {
        throw new RuntimeException("Deferred", err);
      }
    }

    public synchronized void threadFailed(Throwable t) {
      if (err == null) err = t;
      notify();
    }

    public synchronized void threadDone() {
      threadDoneCount++;
    }

    public void stop() throws InterruptedException {
      synchronized (this) {
        stopped = true;
      }
      for (TestThread t : testThreads) {
        t.join();
      }
    }
  }

  public static abstract class TestThread extends Thread {
    final TestContext ctx;
    protected boolean stopped;

    public TestThread(TestContext ctx) {
      this.ctx = ctx;
    }

    public void run() {
      try {
        while (ctx.shouldRun() && !stopped) {
          doAnAction();
        }
      } catch (Throwable t) {
        ctx.threadFailed(t);
      }
      ctx.threadDone();
    }

    protected void stopTestThread() {
      this.stopped = true;
    }

    public abstract void doAnAction() throws Exception;
  }
}
