/*
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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static java.util.Objects.requireNonNull;

/**
 * A task submitter which is closeable, and whose close() call
 * shuts down the pool. This can help manage
 * thread pool lifecycles.
 */
final class CloseableTaskSubmitter implements TaskPool.Submitter,
    Closeable {

  /** Executors. */
  private final ExecutorService pool;

  /**
   * Constructor.
   * @param pool non-null executor.
   */
  CloseableTaskSubmitter(final ExecutorService pool) {
    this.pool = requireNonNull(pool);
  }

  ExecutorService getPool() {
    return pool;
  }

  /**
   * Shut down the pool.
   */
  @Override
  public void close() {
    pool.shutdown();
  }

  @Override
  public Future<?> submit(final Runnable task) {
    return pool.submit(task);
  }
}
