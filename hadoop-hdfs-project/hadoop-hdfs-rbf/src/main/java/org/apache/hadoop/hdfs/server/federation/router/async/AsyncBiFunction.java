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
 * The {@code AsyncBiFunction} interface represents a bi-function that
 * asynchronously accepts two arguments and produces a result. This interface
 * extends the {@link Async} interface and provides a method to apply the
 * function asynchronously.
 *
 * <p>Implementations of this interface are expected to perform an
 * asynchronous operation that takes two parameters of types T and P, and
 * returns a result of type R. The asynchronous operation is typically
 * represented as a {@link CompletableFuture} of the result type R.</p>
 *
 * <p>For example, an implementation of this interface might perform an
 * asynchronous computation using the two input parameters and return a
 * future representing the result of that computation.</p>
 *
 * @param <T> the type of the first argument to the function
 * @param <P> the type of the second argument to the function
 * @param <R> the type of the result of the function
 * @see Async
 */
@FunctionalInterface
public interface AsyncBiFunction<T, P, R> extends Async<R>{

  /**
   * Asynchronously applies this function to the given arguments.
   *
   * <p>This method is intended to initiate the function application
   * without waiting for the result. It should be used when the
   * operation can be performed in the background, and the result
   * is not required immediately.</p>
   *
   * @param t the first argument to the function
   * @param p the second argument to the function
   * @throws IOException if an I/O error occurs during the application of the function
   */
  void applyAsync(T t, P p) throws IOException;

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
   * @param t the first argument to the function
   * @param p the second argument to the function
   * @return a CompletableFuture that will be completed with the result of the
   *         asynchronous operation
   * @throws IOException if an I/O error occurs during the initiation of the asynchronous operation
   */
  default CompletableFuture<R> async(T t, P p) throws IOException {
    applyAsync(t, p);
    CompletableFuture<R> completableFuture = getCurCompletableFuture();
    assert completableFuture != null;
    return completableFuture;
  }
}
