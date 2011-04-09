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
package org.apache.hadoop.hbase.master;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.Executors;

import org.apache.hadoop.hbase.Server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Base class used bulk assigning and unassigning regions.
 * Encapsulates a fixed size thread pool of executors to run assignment/unassignment.
 * Implement {@link #populatePool(java.util.concurrent.ExecutorService)} and
 * {@link #waitUntilDone(long)}.  The default implementation of
 * the {@link #getUncaughtExceptionHandler()} is to abort the hosting
 * Server.
 */
public abstract class BulkAssigner {
  final Server server;

  /**
   * @param server An instance of Server
   */
  public BulkAssigner(final Server server) {
    this.server = server;
  }

  /**
   * @return What to use for a thread prefix when executor runs.
   */
  protected String getThreadNamePrefix() {
    return this.server.getServerName() + "-" + this.getClass().getName(); 
  }

  protected UncaughtExceptionHandler getUncaughtExceptionHandler() {
    return new UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        // Abort if exception of any kind.
        server.abort("Uncaught exception in " + t.getName(), e);
      }
    };
  }

  protected int getThreadCount() {
    return this.server.getConfiguration().
      getInt("hbase.bulk.assignment.threadpool.size", 20);
  }

  protected long getTimeoutOnRIT() {
    return this.server.getConfiguration().
      getLong("hbase.bulk.assignment.waiton.empty.rit", 5 * 60 * 1000);
  }

  protected abstract void populatePool(final java.util.concurrent.ExecutorService pool);

  public boolean bulkAssign() throws InterruptedException {
    return bulkAssign(true);
  }

  /**
   * Run the bulk assign.
   * @param sync Whether to assign synchronously.
   * @throws InterruptedException
   * @return True if done.
   */
  public boolean bulkAssign(boolean sync) throws InterruptedException {
    boolean result = false;
    ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
    builder.setDaemon(true);
    builder.setNameFormat(getThreadNamePrefix() + "-%1$d");
    builder.setUncaughtExceptionHandler(getUncaughtExceptionHandler());
    int threadCount = getThreadCount();
    java.util.concurrent.ExecutorService pool =
      Executors.newFixedThreadPool(threadCount, builder.build());
    try {
      populatePool(pool);
      // How long to wait on empty regions-in-transition.  If we timeout, the
      // RIT monitor should do fixup.
      if (sync) result = waitUntilDone(getTimeoutOnRIT());
    } finally {
      // We're done with the pool.  It'll exit when its done all in queue.
      pool.shutdown();
    }
    return result;
  }

  /**
   * Wait until bulk assign is done.
   * @param timeout How long to wait.
   * @throws InterruptedException
   * @return True if the condition we were waiting on happened.
   */
  protected abstract boolean waitUntilDone(final long timeout)
  throws InterruptedException;
}