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
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * The AsyncForEachRun class is part of the asynchronous operation utilities
 * within the Hadoop Distributed File System (HDFS) Federation router.
 * It provides the functionality to perform asynchronous operations on each
 * element of an Iterator, applying a given async function and then applying
 * a final transformation function to the results.
 *
 * <p>This class is designed to work with other asynchronous interfaces and
 * utility classes to enable complex asynchronous workflows. It allows for
 * non-blocking execution of tasks, which can improve the performance and
 * responsiveness of HDFS operations.</p>
 *
 * <p>The class implements the AsyncRun interface, which means it can be used
 * in asynchronous task chains. It maintains an Iterator of elements to
 * process, an asyncFunction to apply to each element, and a final
 * transformation function (thenApply) to produce the final result.</p>
 *
 * <p>The run method initiates the asynchronous operation, and the doOnce
 * method recursively applies the asyncFunction to each element and handles
 * the results. If the satisfy flag is set, the operation is completed
 * with the current result.</p>
 *
 * <p>AsyncForEachRun is used to implement the following semantics:</p>
 * <pre>
 * {@code
 * for (I element : elements) {
 *     T res = asyncFunction(element);
 *     R result = thenApply(element, res);
 *     if (satisfyCondition(res, result)) {
 *         break;
 *     }
 * }
 * return result;
 * }
 * </pre>
 *
 * @param <I> the type of the elements being iterated over
 * @param <T> the type of the intermediate result from the asyncFunction
 * @param <R> the type of the final result after applying the thenApply function
 * @see AsyncRun
 * @see AsyncApplyFunction
 * @see BiFunction
 */
public class AsyncForEachRun<I, T, R> implements AsyncRun<R> {

  private boolean satisfy = false;
  private Iterator<I> iterator;
  private I now;
  private final CompletableFuture<R> result = new CompletableFuture<>();
  private AsyncApplyFunction<I, T> asyncFunction;
  private BiFunction<AsyncForEachRun<I, T, R>, T, R> thenApply;

  @Override
  public void run() {
    try {
      doOnce(null);
    } catch (IOException ioe) {
      result.completeExceptionally(ioe);
    }
    setCurCompletableFuture(result);
  }

  /**
   * Performs a single iteration of the asynchronous for-each operation.
   *
   * <p>This method is called to process each element of the iterator provided to
   * the {@link AsyncForEachRun} constructor. It applies the asynchronous function to
   * the current element, then applies the 'then' function to the result. If the
   * 'satisfy' condition is met, the iteration is halted, and the current result is
   * used to complete the future. This method is recursive, so it will continue to
   * call itself for the next elements until the iterator is exhausted or the satisfy
   * condition is true.</p>
   *
   * @param ret the initial or current result to be passed into the 'then' function
   * @throws IOException if an I/O error occurs while applying the asynchronous function
   * @see #forEach(Iterator)
   * @see #asyncDo(AsyncApplyFunction)
   * @see #then(BiFunction)
   */
  private void doOnce(R ret) throws IOException {
    if (!iterator.hasNext()) {
      result.complete(ret);
      return;
    }
    now = iterator.next();
    CompletableFuture<T> completableFuture = asyncFunction.async(now);
    completableFuture.thenApply(t -> {
      R r = null;
      try {
        r = thenApply.apply(AsyncForEachRun.this, t);
      } catch (IOException e) {
        result.completeExceptionally(new CompletionException(e));
        return null;
      }
      if (satisfy) {
        result.complete(r);
        return null;
      }
      try {
        doOnce(r);
      } catch (IOException e) {
        throw new CompletionException(e);
      }
      return null;
    }).exceptionally(e ->
        result.completeExceptionally(e.getCause()));
  }

  /**
   * Retrieves the current element being processed in the asynchronous for-each loop.
   *
   * <p>This method provides access to the element that is currently being
   * operated on within the asynchronous iteration. It can be useful for
   * inspection, logging, or other purposes that require knowledge of the
   * current state of the iteration.</p>
   *
   * @return the current element of type {@code I} being processed in the iterator.
   * @see #forEach(Iterator)
   * @see #run()
   */
  public I getNow() {
    return now;
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
   *
   * @see #getNow()
   * @see #run()
   */
  public void breakNow() {
    satisfy = true;
  }

  public AsyncForEachRun<I, T, R> forEach(Iterator<I> forEach) {
    this.iterator = forEach;
    return this;
  }

  public AsyncForEachRun<I, T, R> asyncDo(AsyncApplyFunction<I, T> asyncDo) {
    this.asyncFunction = asyncDo;
    return this;
  }

  public AsyncForEachRun<I, T, R> then(
      BiFunction<AsyncForEachRun<I, T, R>, T, R> then) {
    this.thenApply = then;
    return this;
  }
}

