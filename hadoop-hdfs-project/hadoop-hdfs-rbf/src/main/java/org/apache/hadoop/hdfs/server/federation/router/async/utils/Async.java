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
package org.apache.hadoop.hdfs.server.federation.router.async.utils;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CompletionException;

/**
 * An interface for asynchronous operations, providing utility methods
 * and constants related to asynchronous computations.
 *
 * @param <R> The type of the result of the asynchronous operation
 */
public interface Async<R> {

  /**
   * A thread-local variable to store the {@link CompletableFuture} instance for the current thread.
   * <p>
   * <b>Note:</b> After executing an asynchronous method, the thread stores the CompletableFuture
   * of the asynchronous method in the thread's local variable
   */
  ThreadLocal<CompletableFuture<Object>> CUR_COMPLETABLE_FUTURE
      = new ThreadLocal<>();

  /**
   * Sets the {@link CompletableFuture} instance for the current thread.
   *
   * @param completableFuture The {@link CompletableFuture} instance to be set
   * @param <T> The type of the result in the CompletableFuture
   */
  default <T> void setCurCompletableFuture(CompletableFuture<T> completableFuture) {
    CUR_COMPLETABLE_FUTURE.set((CompletableFuture<Object>) completableFuture);
  }

  /**
   * Gets the {@link CompletableFuture} instance for the current thread.
   *
   * @return The {@link CompletableFuture} instance for the current thread,
   * or {@code null} if not set
   */
  default CompletableFuture<R> getCurCompletableFuture() {
    return (CompletableFuture<R>) CUR_COMPLETABLE_FUTURE.get();
  }

  /**
   * Blocks and retrieves the result of the {@link CompletableFuture} instance
   * for the current thread.
   *
   * @return The result of the CompletableFuture, or {@code null} if the thread was interrupted
   * @throws IOException If the completion exception to the CompletableFuture
   * is an IOException or a subclass of it
   */
  default R result() throws IOException {
    try {
      CompletableFuture<R> completableFuture =
          (CompletableFuture<R>) CUR_COMPLETABLE_FUTURE.get();
      assert completableFuture != null;
      return completableFuture.get();
    } catch (InterruptedException e) {
      return null;
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException)cause;
      }
      throw new IOException(e);
    }
  }

  /**
   * Extracts the real cause of an exception wrapped by CompletionException.
   *
   * @param e The incoming exception, which may be a CompletionException.
   * @return Returns the real cause of the original exception,
   * or the original exception if there is no cause.
   */
  static Throwable unWarpCompletionException(Throwable e) {
    if (e instanceof CompletionException) {
      if (e.getCause() != null) {
        return e.getCause();
      }
    }
    return e;
  }

  /**
   * Wraps the incoming exception in a new CompletionException.
   *
   * @param e The incoming exception, which may be any type of Throwable.
   * @return Returns a new CompletionException with the original exception as its cause.
   */
  static CompletionException warpCompletionException(Throwable e) {
    if (e instanceof CompletionException) {
      return (CompletionException) e;
    }
    return new CompletionException(e);
  }
}
