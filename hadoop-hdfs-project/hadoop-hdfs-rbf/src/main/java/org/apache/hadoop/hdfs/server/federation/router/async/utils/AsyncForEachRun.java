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
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.apache.hadoop.hdfs.server.federation.router.async.utils.Async.warpCompletionException;

/**
 * The AsyncForEachRun class is part of the asynchronous operation utilities
 * within the Hadoop Distributed File System (HDFS) Federation router.
 * It provides the functionality to perform asynchronous operations on each
 * element of an Iterator, applying a given async function.
 *
 * <p>This class is designed to work with other asynchronous interfaces and
 * utility classes to enable complex asynchronous workflows. It allows for
 * non-blocking execution of tasks, which can improve the performance and
 * responsiveness of HDFS operations.</p>
 *
 * <p>The class implements the AsyncRun interface, which means it can be used
 * in asynchronous task chains. It maintains an Iterator of elements to
 * process, an asyncDoOnce to apply to each element.</p>
 *
 * <p>The run method initiates the asynchronous operation, and the doOnce
 * method recursively applies the asyncDoOnce to each element and handles
 * the results. If the shouldBreak flag is set, the operation is completed
 * with the current result.</p>
 *
 * <p>AsyncForEachRun is used to implement the following semantics:</p>
 * <pre>
 * {@code
 * for (I element : elements) {
 *     R result = asyncDoOnce(element);
 * }
 * return result;
 * }
 * </pre>
 *
 * @param <I> the type of the elements being iterated over
 * @param <R> the type of the final result after applying the thenApply function
 * @see AsyncRun
 * @see AsyncBiFunction
 */
public class AsyncForEachRun<I, R> implements AsyncRun<R> {

  // Indicates whether the iteration should be broken immediately
  // after the next asynchronous operation is completed.
  private boolean shouldBreak = false;
  // The Iterator over the elements to process asynchronously.
  private Iterator<I> iterator;
  // The async function to apply to each element from the iterator.
  private AsyncBiFunction<AsyncForEachRun<I, R>, I, R> asyncDoOnce;

  /**
   * Initiates the asynchronous foreach operation by starting the iteration process
   * over the elements provided by the iterator. This method sets up the initial
   * call to doOnce(R) with a null result, which begins the recursive
   * application of the async function to each element of the iterator.
   *
   * <p>This method is an implementation of the {@link AsyncRun} interface's
   * {@code run} method, allowing it to be used in a chain of asynchronous
   * operations. It is responsible for starting the asynchronous processing and
   * handling the completion of the operation through the internal
   * {@link CompletableFuture}.</p>
   *
   * <p>If an exception occurs during the first call to {@code doOnce}, the
   * exception is caught and the internal CompletableFuture is completed
   * exceptionally with a {@link CompletionException} wrapping the original
   * IOException.</p>
   *
   * <p>After initiating the operation, the method sets the current thread's
   * {@link Async} {@link CompletableFuture} by calling
   * {@link #setCurCompletableFuture(CompletableFuture)} with the internal result
   * CompletableFuture. This allows other parts of the asynchronous workflow to
   * chain further operations or handle the final result once the foreach loop
   * completes.</p>
   *
   * @see AsyncRun
   * @see Async#setCurCompletableFuture(CompletableFuture)
   */
  @Override
  public void run() {
    if (iterator == null || !iterator.hasNext()) {
      setCurCompletableFuture(CompletableFuture.completedFuture(null));
      return;
    }
    CompletableFuture<R> result;
    try {
      result = doOnce(iterator.next());
    } catch (IOException ioe) {
      result = new CompletableFuture<>();
      result.completeExceptionally(warpCompletionException(ioe));
    }
    setCurCompletableFuture(result);
  }

  /**
   * Recursively applies the async function to the next element of the iterator
   * and handles the result. This method is called for each iteration of the
   * asynchronous foreach loop, applying the async function to each element
   * and chaining the results.
   *
   * <p>If the iterator has no more elements, the CompletableFuture held by this
   * class is completed with the last result. If an exception occurs during
   * the application of the async function, it is propagated to the
   * CompletableFuture, which completes exceptionally.</p>
   *
   * <p>This method is designed to be called by the {@link #run()} method and
   * handles the iteration logic, including breaking the loop if the
   * {@link #shouldBreak} flag is set to true.</p>
   *
   * @param element The current element from the async function application.
   * @throws IOException if an I/O error occurs during the application of the async function.
   */
  private CompletableFuture<R> doOnce(I element) throws IOException {
    CompletableFuture<R> completableFuture = asyncDoOnce.async(AsyncForEachRun.this, element);
    return completableFuture.thenCompose(res -> {
      if (shouldBreak || !iterator.hasNext()) {
        return completableFuture;
      }
      try {
        return doOnce(iterator.next());
      } catch (IOException e) {
        throw warpCompletionException(e);
      }
    });
  }

  /**
   * Triggers the termination of the current asynchronous iteration.
   *
   * <p>This method is used to break out of the asynchronous for-each loop
   * prematurely. It sets a flag that indicates the iteration should be
   * terminated at the earliest opportunity. This is particularly useful when
   * the processing logic determines that further iteration is unnecessary
   * or when a specific condition has been met.</p>
   *
   * <p>Once this method is called, the next time the loop is about to process
   * a new element, it will check the flag and cease operation, allowing the
   * application to move on to the next step or complete the task.</p>
   */
  public void breakNow() {
    shouldBreak = true;
  }

  /**
   * Sets the Iterator for the elements to be processed in the asynchronous operation.
   *
   * @param forEach The Iterator over the elements.
   * @return The current AsyncForEachRun instance for chaining.
   */
  public AsyncForEachRun<I, R> forEach(Iterator<I> forEach) {
    this.iterator = forEach;
    return this;
  }

  /**
   * Sets the async function to apply to each element from the iterator.
   *
   * @param asyncDo The async function.
   * @return The current AsyncForEachRun instance for chaining.
   */
  public AsyncForEachRun<I, R> asyncDo(AsyncBiFunction<AsyncForEachRun<I, R>, I, R> asyncDo) {
    this.asyncDoOnce = asyncDo;
    return this;
  }
}
