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
import java.util.concurrent.Executor;

import static org.apache.hadoop.hdfs.server.federation.router.async.utils.Async.warpCompletionException;

/**
 * The AsyncApplyFunction interface represents a function that
 * asynchronously accepts a value of type T and produces a result
 * of type R. This interface extends {@link ApplyFunction} and is
 * designed to be used with asynchronous computation frameworks,
 * such as Java's {@link java.util.concurrent.CompletableFuture}.
 *
 * <p>An implementation of this interface is expected to perform an
 * asynchronous operation and return a result, which is typically
 * represented as a {@code CompletableFuture<R>}. This allows for
 * non-blocking execution of tasks and is particularly useful for
 * I/O operations or any operation that may take a significant amount
 * of time to complete.</p>
 *
 * <p>AsyncApplyFunction is used to implement the following semantics:</p>
 * <pre>
 * {@code
 *    T res = doAsync1(input);
 *    // Can use AsyncApplyFunction
 *    R result = doAsync2(res);
 * }
 * </pre>
 *
 * @param <T> the type of the input to the function
 * @param <R> the type of the result of the function
 * @see ApplyFunction
 * @see java.util.concurrent.CompletableFuture
 */
@FunctionalInterface
public interface AsyncApplyFunction<T, R> extends ApplyFunction<T, R> {

  /**
   * Asynchronously applies this function to the given argument.
   *
   * <p>This method is intended to initiate the function application
   * without waiting for the result. It is typically used when the
   * result of the operation is not required immediately or when the
   * operation is part of a larger asynchronous workflow.</p>
   *
   * @param t the function argument
   * @throws IOException if an I/O error occurs during the application
   *                     of the function
   */
  void applyAsync(T t) throws IOException;

  /**
   * Synchronously applies this function to the given argument and
   * returns the result.
   *
   * <p>This method waits for the asynchronous operation to complete
   * and returns its result. It is useful when the result is needed
   * immediately and the calling code cannot proceed without it.</p>
   *
   * @param t the function argument
   * @return the result of applying the function to the argument
   * @throws IOException if an I/O error occurs during the application
   *                     of the function
   */
  @Override
  default R apply(T t) throws IOException {
    applyAsync(t);
    return result();
  }

  /**
   * Initiates the asynchronous application of this function to the given result.
   * <p>
   * This method calls applyAsync to start the asynchronous operation and then retrieves
   * the current thread's CompletableFuture using getCurCompletableFuture.
   * It returns this CompletableFuture, which will be completed with the result of the
   * asynchronous operation once it is finished.
   * <p>
   * This method is useful for chaining with other asynchronous operations, as it allows the
   * current operation to be part of a larger asynchronous workflow.
   *
   * @param t the function argument
   * @return a CompletableFuture that will be completed with the result of the
   *         asynchronous operation
   * @throws IOException if an I/O error occurs during the initiation of the asynchronous operation
   */
  default CompletableFuture<R> async(T t) throws IOException {
    applyAsync(t);
    CompletableFuture<R> completableFuture = getCurCompletableFuture();
    assert completableFuture != null;
    return completableFuture;
  }

  /**
   * Asynchronously applies this function to the result of the given
   * CompletableFuture.
   *
   * <p>This method chains the function application to the completion
   * of the input future. It returns a new CompletableFuture that
   * completes with the function's result when the input future
   * completes.</p>
   *
   * @param in the input future
   * @return a new CompletableFuture that holds the result of the
   *         function application
   */
  @Override
  default CompletableFuture<R> apply(CompletableFuture<T> in) {
    return in.thenCompose(t -> {
      try {
        return async(t);
      } catch (IOException e) {
        throw warpCompletionException(e);
      }
    });
  }

  /**
   * Asynchronously applies this function to the result of the given
   * CompletableFuture, using the specified executor for the
   * asynchronous computation.
   *
   * <p>This method allows for more control over the execution
   * context of the asynchronous operation, such as running the
   * operation in a separate thread or thread pool.</p>
   *
   * @param in the input future
   * @param executor the executor to use for the asynchronous
   *                 computation
   * @return a new CompletableFuture that holds the result of the
   *         function application
   */
  @Override
  default CompletableFuture<R> apply(CompletableFuture<T> in, Executor executor) {
    return in.thenComposeAsync(t -> {
      try {
        return async(t);
      } catch (IOException e) {
        throw warpCompletionException(e);
      }
    }, executor);
  }
}
