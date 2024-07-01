/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.federation.router.async;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * The AsyncRun interface represents an asynchronous operation that can be
 * executed in the context of the Hadoop Distributed File System (HDFS)
 * Federation router. Implementations of this interface are responsible for
 * performing a task and completing a {@link CompletableFuture} with the
 * result of the operation.
 *
 * <p>
 * The {@code run} method of this interface is intended to be used in an
 * asynchronous workflow, where the operation may involve I/O tasks or
 * other long-running processes. By implementing this interface, classes
 * can define custom asynchronous behavior that can be chained with other
 * asynchronous operations using utility methods provided by the
 * {@link AsyncUtil} class.</p>
 *
 * <p>
 * For example, an implementation of AsyncRun could perform a non-blocking
 * read or write operation to HDFS, and upon completion, it could use
 * AsyncUtil methods to handle the result or propagate any exceptions that
 * occurred during the operation.</p>
 *
 * @param <R> the type of the result produced by the asynchronous operation
 * @see AsyncUtil
 */
@FunctionalInterface
public interface AsyncRun<R> extends Async<R> {

  /**
   * Executes the asynchronous operation represented by this AsyncRun instance.
   * This method is expected to perform the operation and, upon completion,
   * complete the current thread's {@link CompletableFuture} with the result.
   *
   * @throws IOException if an I/O error occurs during the execution of the operation
   */
  void run() throws IOException;

  /**
   * Provides an asynchronous version of the {@code run} method, which returns a
   * {@link CompletableFuture} representing the result of the operation.
   * This method is typically used in an asynchronous workflow to initiate the
   * operation without waiting for its completion.
   *
   * @return a CompletableFuture that completes with the result of the operation
   * @throws IOException if an I/O error occurs during the initiation of the operation
   */
  default CompletableFuture<R> async() throws IOException {
    run();
    CompletableFuture<R> completableFuture = getCurCompletableFuture();
    assert completableFuture != null;
    return completableFuture;
  }
}
