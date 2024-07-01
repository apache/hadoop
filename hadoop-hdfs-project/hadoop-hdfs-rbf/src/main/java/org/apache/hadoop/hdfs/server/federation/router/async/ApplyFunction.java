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
import java.util.concurrent.Executor;

import static org.apache.hadoop.hdfs.server.federation.router.async.Async.warpCompletionException;

/**
 * Represents a function that accepts a value of type T and produces a result of type R.
 * This interface extends {@link Async} and provides methods to apply the function
 * asynchronously using {@link CompletableFuture}.
 *
 * <p>ApplyFunction is used to implement the following semantics:</p>
 * <pre>
 * {@code
 *    T res = doAsync(input);
 *    // Can use ApplyFunction
 *    R result = thenApply(res);
 * }
 * </pre>
 *
 * @param <T> the type of the input to the function
 * @param <R> the type of the result of the function
 */
@FunctionalInterface
public interface ApplyFunction<T, R> extends Async<R>{

  /**
   * Applies this function to the given argument.
   *
   * @param t the function argument
   * @return the function result
   * @throws IOException if an I/O error occurs
   */
  R apply(T t) throws IOException;

  /**
   * Applies this function asynchronously to the result of the given {@link CompletableFuture}.
   * The function is executed on the same thread as the completion of the given future.
   *
   * @param in the input future
   * @return a new future that holds the result of the function application
   */
  default CompletableFuture<R> apply(CompletableFuture<T> in) {
    return in.thenApply(t -> {
      try {
        return ApplyFunction.this.apply(t);
      } catch (IOException e) {
        throw warpCompletionException(e);
      }
    });
  }

  /**
   * Applies this function asynchronously to the result of the given {@link CompletableFuture},
   * using the specified executor for the asynchronous computation.
   *
   * @param in the input future
   * @param executor the executor to use for the asynchronous computation
   * @return a new future that holds the result of the function application
   */
  default CompletableFuture<R> apply(CompletableFuture<T> in, Executor executor) {
    return in.thenApplyAsync(t -> {
      try {
        return ApplyFunction.this.apply(t);
      } catch (IOException e) {
        throw warpCompletionException(e);
      }
    }, executor);
  }
}
