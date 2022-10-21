/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.impl.prefetch;

import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;

import org.apache.hadoop.util.concurrent.HadoopExecutors;

/**
 * A FuturePool implementation backed by a java.util.concurrent.ExecutorService.
 *
 * If a piece of work has started, it cannot (currently) be cancelled.
 *
 * This class is a simplified version of <code>com.twitter:util-core_2.11</code>
 * ExecutorServiceFuturePool designed to avoid depending on that Scala library.
 * One problem with using a Scala library is that many downstream projects
 * (eg Apache Spark) use Scala, and they might want to use a different version of Scala
 * from the version that Hadoop chooses to use.
 *
 */
public class ExecutorServiceFuturePool {

  private final ExecutorService executor;

  public ExecutorServiceFuturePool(ExecutorService executor) {
    this.executor = executor;
  }

  /**
   * @param f function to run in future on executor pool
   * @return future
   * @throws java.util.concurrent.RejectedExecutionException can be thrown
   * @throws NullPointerException if f param is null
   */
  public Future<Void> executeFunction(final Supplier<Void> f) {
    return executor.submit(f::get);
  }

  /**
   * @param r runnable to run in future on executor pool
   * @return future
   * @throws java.util.concurrent.RejectedExecutionException can be thrown
   * @throws NullPointerException if r param is null
   */
  @SuppressWarnings("unchecked")
  public Future<Void> executeRunnable(final Runnable r) {
    return (Future<Void>) executor.submit(r::run);
  }

  /**
   * Utility to shutdown the {@link ExecutorService} used by this class. Will wait up to a
   * certain timeout for the ExecutorService to gracefully shutdown.
   *
   * @param logger Logger
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   */
  public void shutdown(Logger logger, long timeout, TimeUnit unit) {
    HadoopExecutors.shutdown(executor, logger, timeout, unit);
  }

  public String toString() {
    return String.format(Locale.ROOT, "ExecutorServiceFuturePool(executor=%s)", executor);
  }
}
