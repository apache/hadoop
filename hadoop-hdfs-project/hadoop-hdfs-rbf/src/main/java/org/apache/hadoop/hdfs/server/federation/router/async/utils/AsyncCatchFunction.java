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

import static org.apache.hadoop.hdfs.server.federation.router.async.utils.Async.unWarpCompletionException;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.Async.warpCompletionException;

/**
 * The AsyncCatchFunction interface represents a function that handles exceptions
 * occurring within an asynchronous operation. It extends the CatchFunction
 * interface and adds the capability to perform asynchronous exception handling.
 *
 * <p>This interface is part of the asynchronous utilities provided by the Hadoop
 * Distributed File System (HDFS) Federation router. It is used in conjunction
 * with other asynchronous interfaces such as AsyncRun to build complex,
 * non-blocking operations.</p>
 *
 * <p>An implementation of this interface should define how to handle a caught
 * exception asynchronously. It takes two parameters: the result of the
 * asynchronous operation (if any) and the caught exception. The function can then
 * initiate an asynchronous process to handle the exception, which may involve
 * logging the error, performing a recovery operation, or any other custom logic.</p>
 *
 * <p>For example, the applyAsync method is intended to be called when an exception
 * is caught during an asynchronous operation. It should initiate the asynchronous
 * process to handle the exception without blocking the main execution thread.</p>
 *
 * <p>The default method apply is provided to allow synchronous operation in case
 * the asynchronous handling is not required. It simply calls applyAsync and waits
 * for the result.</p>
 *
 * <p>The default method async is provided to allow chaining with other asynchronous
 * operations. It calls applyAsync and returns a CompletableFuture that completes
 * with the result of the operation.</p>
 *
 * <p>AsyncCatchFunction is used to implement the following semantics:</p>
 * <pre>
 * {@code
 *    try{
 *      R res = doAsync1(input);
 *    } catch(E e) {
 *      // Can use AsyncCatchFunction
 *      R result = doAsync2(res, e);
 *    }
 * }
 * </pre>
 *
 * @param <R> the type of the result of the asynchronous operation
 * @param <E> the type of the exception to catch, extending Throwable
 * @see CatchFunction
 * @see AsyncRun
 */
@FunctionalInterface
public interface AsyncCatchFunction<R, E extends Throwable>
    extends CatchFunction<R, E> {

  /**
   * Asynchronously applies this function to the given exception.
   *
   * <p>This method is intended to be called when an exception is caught
   * during an asynchronous operation. It should initiate the asynchronous process
   * to handle the exception without blocking the main execution thread.
   * The actual handling of the exception, such as logging the error, performing
   * a recovery operation, or any other custom logic, should be implemented in this
   * method.</p>
   *
   * @param r the result of the asynchronous operation, if any; may be null
   * @param e the caught exception
   * @throws IOException if an I/O error occurs during the application of the function
   */
  void applyAsync(R r, E e) throws IOException;

  /**
   * Synchronously applies this function to the given result and exception.
   * <p>
   * This method first calls {@code applyAsync} to initiate the asynchronous handling
   * of the exception. Then, it waits for the asynchronous operation to complete
   * by calling {@code result}, which retrieves the result of the current
   * thread's {@link CompletableFuture}.
   * <p>
   *
   * @param r the result of the asynchronous operation, if any; may be null
   * @param e the caught exception
   * @return the result after applying the function
   * @throws IOException if an I/O error occurs during the application of the function
   */
  @Override
  default R apply(R r, E e) throws IOException {
    applyAsync(r, e);
    return result();
  }

  /**
   * Initiates the asynchronous application of this function to the given result and exception.
   * <p>
   * This method calls applyAsync to start the asynchronous operation and then retrieves
   * the current thread's CompletableFuture using getCurCompletableFuture.
   * It returns this CompletableFuture, which will be completed with the result of the
   * asynchronous operation once it is finished.
   * <p>
   * This method is useful for chaining with other asynchronous operations, as it allows the
   * current operation to be part of a larger asynchronous workflow.
   *
   * @param r the result of the asynchronous operation, if any; may be null
   * @param e the caught exception
   * @return a CompletableFuture that will be completed with the result of the
   *         asynchronous operation
   * @throws IOException if an I/O error occurs during the initiation of the asynchronous operation
   */
  default CompletableFuture<R> async(R r, E e) throws IOException {
    applyAsync(r, e);
    CompletableFuture<R> completableFuture = getCurCompletableFuture();
    assert completableFuture != null;
    return completableFuture;
  }

  /**
   * Applies the catch function to a {@link CompletableFuture}, handling exceptions of a
   * specified type.
   * <p>
   * This method is a default implementation that provides a way to integrate exception
   * handling into a chain of asynchronous operations. It takes a {@code CompletableFuture}
   * and a class object representing the exception type to catch. The method then completes
   * the future with the result of applying this catch function to the input future and the
   * specified exception type.
   * <p>
   * If the input future completes exceptionally with an instance of the specified exception
   * type, the catch function is applied to the exception. Otherwise, if the future
   * completes with a different type of exception or normally, the original result or
   * exception is propagated.
   *
   * @param in the input {@code CompletableFuture} to which the catch function is applied
   * @param eClazz the class object representing the exception type to catch
   * @return a new {@code CompletableFuture} that completes with the result of applying
   *         the catch function, or propagates the original exception if it does not match
   *         the specified type
   */
  @Override
  default CompletableFuture<R> apply(
      CompletableFuture<R> in, Class<E> eClazz) {
    return in.handle((r, e) -> {
      if (e == null) {
        return in;
      }
      Throwable readException = unWarpCompletionException(e);
      if (eClazz.isInstance(readException)) {
        try {
          return async(r, (E) readException);
        } catch (IOException ex) {
          throw warpCompletionException(ex);
        }
      }
      throw warpCompletionException(e);
    }).thenCompose(result -> result);
  }
}
