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
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static org.apache.hadoop.hdfs.server.federation.router.async.utils.Async.CUR_COMPLETABLE_FUTURE;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.Async.warpCompletionException;

/**
 * The AsyncUtil class provides a collection of utility methods to simplify
 * the implementation of asynchronous operations using Java's CompletableFuture.
 * It encapsulates common patterns such as applying functions, handling exceptions,
 * and executing tasks in a non-blocking manner. This class is designed to work
 * with Hadoop's asynchronous router operations in HDFS Federation.
 *
 * <p>The utility methods support a fluent-style API, allowing for the chaining of
 * asynchronous operations. For example, after an asynchronous operation completes,
 * a function can be applied to its result, and the process can continue with
 * the new result. This is particularly useful for complex workflows that require
 * multiple steps, where each step depends on the completion of the previous one.</p>
 *
 * <p>The class also provides methods to handle exceptions that may occur during a
 * synchronous operation. This ensures that error handling is integrated smoothly
 * into the workflow, allowing for robust and fault-tolerant applications.</p>
 *
 * @see CompletableFuture
 * @see ApplyFunction
 * @see AsyncApplyFunction
 * @see AsyncRun
 * @see AsyncForEachRun
 * @see CatchFunction
 * @see AsyncCatchFunction
 * @see FinallyFunction
 * @see AsyncBiFunction
 */
public final class AsyncUtil {
  private static final Boolean BOOLEAN_RESULT = false;
  private static final Long LONG_RESULT = -1L;
  private static final Integer INT_RESULT = -1;
  private static final Object NULL_RESULT = null;

  private AsyncUtil(){}

  /**
   * Provides a default value based on the type specified.
   *
   * @param clazz The {@link Class} object representing the type of the value
   *              to be returned.
   * @param <R>   The type of the value to be returned.
   * @return An object with a value determined by the type:
   *         <ul>
   *           <li>{@code false} if {@code clazz} is {@link Boolean},
   *           <li>-1 if {@code clazz} is {@link Long},
   *           <li>-1 if {@code clazz} is {@link Integer},
   *           <li>{@code null} for any other type.
   *         </ul>
   */
  public static <R> R asyncReturn(Class<R> clazz) {
    if (clazz == null) {
      return null;
    }
    if (clazz.equals(Boolean.class)
        || clazz.equals(boolean.class)) {
      return (R) BOOLEAN_RESULT;
    } else if (clazz.equals(Long.class)
        || clazz.equals(long.class)) {
      return (R) LONG_RESULT;
    } else if (clazz.equals(Integer.class)
        || clazz.equals(int.class)) {
      return (R) INT_RESULT;
    }
    return (R) NULL_RESULT;
  }

  /**
   * Synchronously returns the result of the current asynchronous operation.
   * This method is designed to be used in scenarios where the result of an
   * asynchronous operation is needed synchronously, and it is known that
   * the operation has completed.
   *
   * <p>The method retrieves the current thread's {@link CompletableFuture} and
   * attempts to get the result. If the future is not yet complete, this
   * method will block until the result is available. If the future completed
   * exceptionally, the cause of the exception is thrown as a runtime
   * exception wrapped in an {@link ExecutionException}.</p>
   *
   * <p>This method is typically used after an asynchronous operation has been
   * initiated and the caller needs to obtain the result in a synchronous
   * manner, for example, when bridging between asynchronous and synchronous
   * code paths.</p>
   *
   * @param <R> the type of the result to be returned
   * @param clazz the {@link Class} object representing the type of the value
   *              to be returned, used to cast the result to the correct type
   * @return the result of the asynchronous operation as an object of the
   *         specified class
   * @throws Exception if an error occurs during the synchronous retrieval of
   *                    the result, including the original exception if the
   *                    future completed exceptionally
   */
  public static <R> R syncReturn(Class<R> clazz)
      throws Exception {
    CompletableFuture<Object> completableFuture = CUR_COMPLETABLE_FUTURE.get();
    assert completableFuture != null;
    try {
      return (R) completableFuture.get();
    } catch (ExecutionException e) {
      throw (Exception)e.getCause();
    }
  }

  /**
   * Completes the current asynchronous operation with the specified value.
   * This method sets the result of the current thread's {@link CompletableFuture}
   * to the provided value, effectively completing the asynchronous operation.
   *
   * @param value The value to complete the future with.
   * @param <R>    The type of the value to be completed.
   */
  public static <R> void asyncComplete(R value) {
    CUR_COMPLETABLE_FUTURE.set(
        CompletableFuture.completedFuture(value));
  }

  /**
   * Completes the current asynchronous operation with the specified completableFuture.
   *
   * @param completableFuture The completableFuture to complete the future with.
   * @param <R>    The type of the value to be completed.
   */
  public static <R> void asyncCompleteWith(CompletableFuture<R> completableFuture) {
    CUR_COMPLETABLE_FUTURE.set((CompletableFuture<Object>) completableFuture);
  }

  /**
   * Completes the current asynchronous operation with an exception.
   * This method sets the result of the current thread's {@link CompletableFuture}
   * to an exceptional completion, using the provided {@link Throwable} as the cause.
   * This is typically used to handle errors in asynchronous operations.
   *
   * @param e The exception to complete the future exceptionally with.
   */
  public static void asyncThrowException(Throwable e) {
    CompletableFuture<Object> result = new CompletableFuture<>();
    result.completeExceptionally(warpCompletionException(e));
    CUR_COMPLETABLE_FUTURE.set(result);
  }

  /**
   * Applies an asynchronous function to the current {@link CompletableFuture}.
   * This method retrieves the current thread's {@link CompletableFuture} and applies
   * the provided {@link ApplyFunction} to it. It is used to chain asynchronous
   * operations, where the result of one operation is used as the input for the next.
   *
   * @param function The asynchronous function to apply, which takes a type T and
   *                 produces a type R.
   * @param <T>       The type of the input to the function.
   * @param <R>       The type of the result of the function.
   * @see CompletableFuture
   * @see ApplyFunction
   */
  public static <T, R> void asyncApply(ApplyFunction<T, R> function) {
    CompletableFuture<T> completableFuture =
        (CompletableFuture<T>) CUR_COMPLETABLE_FUTURE.get();
    assert completableFuture != null;
    CompletableFuture<R> result = function.apply(completableFuture);
    CUR_COMPLETABLE_FUTURE.set((CompletableFuture<Object>) result);
  }

  /**
   * Applies an asynchronous function to the current {@link CompletableFuture}
   * using the specified executor. This method retrieves the current thread's
   * {@link CompletableFuture} and applies the provided{@link ApplyFunction} to
   * it with the given executor service. It allows for more control over the
   * execution context, such as running the operation in a separate thread or
   * thread pool.
   *
   * <p>This is particularly useful when you need to perform blocking I/O operations
   * or other long-running tasks without blocking the main thread or
   * when you want to manage the thread resources more efficiently.</p>
   *
   * @param function The asynchronous function to apply, which takes a type T and
   *                 produces a type R.
   * @param executor The executor service used to run the asynchronous function.
   * @param <T> The type of the input to the function.
   * @param <R> The type of the result of the function.
   * @see CompletableFuture
   * @see ApplyFunction
   */
  public static <T, R> void asyncApplyUseExecutor(
      ApplyFunction<T, R> function, Executor executor) {
    CompletableFuture<T> completableFuture =
        (CompletableFuture<T>) CUR_COMPLETABLE_FUTURE.get();
    assert completableFuture != null;
    CompletableFuture<R> result = function.apply(completableFuture, executor);
    CUR_COMPLETABLE_FUTURE.set((CompletableFuture<Object>) result);
  }

  /**
   * Attempts to execute an asynchronous task defined by the provided
   * {@link AsyncRun} and associates it with the current thread's
   * {@link CompletableFuture}. This method is useful for trying operations
   * that may throw exceptions and handling them asynchronously.
   *
   * <p>The provided {@code asyncRun} is a functional interface that
   * encapsulates the logic to be executed asynchronously. It is executed in
   * the context of the current CompletableFuture, allowing for chaining further
   * asynchronous operations based on the result or exception of this try.</p>
   *
   * <p>If the operation completes successfully, the result is propagated to the
   * next operation in the chain. If an exception occurs, it can be caught and
   * handled using the {@link #asyncCatch(CatchFunction, Class)} method,
   * allowing for error recovery or alternative processing.</p>
   *
   * @param asyncRun The asynchronous task to be executed, defined by
   *                  an {@link AsyncRun} instance.
   * @param <R>       The type of the result produced by the asynchronous task.
   * @see AsyncRun
   * @see #asyncCatch(CatchFunction, Class)
   */
  public static <R> void asyncTry(AsyncRun<R> asyncRun) {
    try {
      CompletableFuture<R> result = asyncRun.async();
      CUR_COMPLETABLE_FUTURE.set((CompletableFuture<Object>) result);
    } catch (Throwable e) {
      asyncThrowException(e);
    }
  }

  /**
   * Handles exceptions to a specified type that may occur during
   * an asynchronous operation. This method is used to catch and deal
   * with exceptions in a non-blocking manner, allowing the application
   * to continue processing even when errors occur.
   *
   * <p>The provided {@code function} is a {@link CatchFunction} that
   * defines how to handle the caught exception. It takes the result of
   * the asynchronous operation (if any) and the caught exception, and
   * returns a new result or modified result to continue the asynchronous
   * processing.</p>
   *
   * <p>The {@code eClass} parameter specifies the type of exceptions to
   * catch. Only exceptions that are instances of this type (or its
   * subclasses) will be caught and handled by the provided function.</p>
   *
   * @param function The {@link CatchFunction} that defines how to
   *                  handle the caught exception.
   * @param eClass   The class of the exception type to catch.
   * @param <R>       The type of the result of the asynchronous operation.
   * @param <E>       The type of the exception to catch.
   * @see CatchFunction
   */
  public static <R, E extends Throwable> void asyncCatch(
      CatchFunction<R, E> function, Class<E> eClass) {
    CompletableFuture<R> completableFuture =
        (CompletableFuture<R>) CUR_COMPLETABLE_FUTURE.get();
    assert completableFuture != null;
    CompletableFuture<R> result = function.apply(completableFuture, eClass);
    CUR_COMPLETABLE_FUTURE.set((CompletableFuture<Object>) result);
  }

  /**
   * Executes a final action after an asynchronous operation
   * completes, regardless of whether the operation was successful
   * or resulted in an exception. This method provides a way to
   * perform cleanup or finalization tasks in an asynchronous
   * workflow.
   *
   * <p>The provided {@code function} is a {@link FinallyFunction}
   * that encapsulates the logic to be executed after the
   * asynchronous operation. It takes the result of the operation
   * and returns a new result, which can be used to continue the
   * asynchronous processing or to handle the final output of
   * the workflow.</p>
   *
   * <p>This method is particularly useful for releasing resources,
   * closing connections, or performing other cleanup actions that
   * need to occur after all other operations have completed.</p>
   *
   * @param function The {@link FinallyFunction} that defines
   *                  the final action to be executed.
   * @param <R>       The type of the result of the asynchronous
   *                   operation.
   * @see FinallyFunction
   */
  public static <R> void asyncFinally(FinallyFunction<R> function) {
    CompletableFuture<R> completableFuture =
        (CompletableFuture<R>) CUR_COMPLETABLE_FUTURE.get();
    assert completableFuture != null;
    CompletableFuture<R> result = function.apply(completableFuture);
    CUR_COMPLETABLE_FUTURE.set((CompletableFuture<Object>) result);
  }

  /**
   * Executes an asynchronous operation for each element in an Iterator, applying
   * a given async function to each element. This method is part of the asynchronous
   * utilities provided to facilitate non-blocking operations on collections of elements.
   *
   * <p>The provided {@code asyncDo} is an {@link AsyncBiFunction} that encapsulates
   * the logic to be executed asynchronously for each element. It is executed in
   * the context of the current CompletableFuture, allowing for chaining further
   * asynchronous operations based on the result or exception of each iteration.</p>
   *
   * <p>The method is particularly useful for performing asynchronous iterations
   * over collections where the processing of each element is independent.</p>
   *
   * @param forEach the Iterator over which to iterate and apply the async function
   * @param asyncDo the asynchronous function to apply to each element of the Iterator,
   *                 implemented as an {@link AsyncBiFunction}
   * @param <I> the type of the elements being iterated over
   * @param <R> the type of the result produced by the asynchronous task applied to each element
   * @see AsyncBiFunction
   * @see AsyncForEachRun
   */
  public static <I, R> void asyncForEach(
      Iterator<I> forEach, AsyncBiFunction<AsyncForEachRun<I, R>, I, R> asyncDo) {
    AsyncForEachRun<I, R> asyncForEachRun = new AsyncForEachRun<>();
    asyncForEachRun.forEach(forEach).asyncDo(asyncDo).run();
  }

  /**
   * Applies an asynchronous operation to each element of a collection
   * and aggregates the results. This method is designed to process a
   * collection of elements concurrently using asynchronous tasks, and
   * then combine the results into a single aggregated result.
   *
   * <p>The operation defined by {@code asyncDo} is applied to each
   * element of the collection. This operation is expected to return a
   * {@link CompletableFuture} representing the asynchronous task.
   * Once all tasks have been started, the method (async) waits for all of
   * them to complete and then uses the {@code then} function to
   * process and aggregate the results.</p>
   *
   * <p>The {@code then} function takes an array of {@link CompletableFuture}
   * instances, each representing the future result of an individual
   * asynchronous operation. It should return a new aggregated result
   * based on these futures. This allows for various forms of result
   * aggregation, such as collecting all results into a list,
   * reducing them to a single value, or performing any other custom
   * aggregation logic.</p>
   *
   * @param collection the collection of elements to process.
   * @param asyncDo    the asynchronous operation to apply to each
   *                   element. It must return a {@link CompletableFuture}
   *                   representing the operation.
   * @param then        a function that takes an array of futures
   *                   representing the results of the asynchronous
   *                   operations and returns an aggregated result.
   * @param <I>        the type of the elements in the collection.
   * @param <R>        the type of the intermediate result from the
   *                   asynchronous operations.
   * @param <P>        the type of the final aggregated result.
   * @see CompletableFuture
   */
  public static <I, R, P> void asyncCurrent(
      Collection<I> collection, AsyncApplyFunction<I, R> asyncDo,
      Function<CompletableFuture<R>[], P> then) {
    CompletableFuture<R>[] completableFutures =
        new CompletableFuture[collection.size()];
    int i = 0;
    for(I entry : collection) {
      CompletableFuture<R> future = null;
      try {
        future = asyncDo.async(entry);
      } catch (IOException e) {
        future = new CompletableFuture<>();
        future.completeExceptionally(warpCompletionException(e));
      }
      completableFutures[i++] = future;
    }
    CompletableFuture<P> result = CompletableFuture.allOf(completableFutures)
        .handle((unused, throwable) -> then.apply(completableFutures));
    CUR_COMPLETABLE_FUTURE.set((CompletableFuture<Object>) result);
  }

  /**
   * Get the CompletableFuture object stored in the current thread's local variable.
   *
   * @return The completableFuture object.
   */
  public static CompletableFuture<Object> getCompletableFuture() {
    return CUR_COMPLETABLE_FUTURE.get();
  }
}
