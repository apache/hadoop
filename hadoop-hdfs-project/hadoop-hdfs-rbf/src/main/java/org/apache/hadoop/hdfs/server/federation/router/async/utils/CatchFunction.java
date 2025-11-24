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
 * The {@code CatchFunction} interface represents a function that handles exceptions
 * occurring within an asynchronous operation. It provides a mechanism to catch and
 * process exceptions to a specific type, allowing for error recovery or alternative
 * processing paths within an asynchronous workflow.
 *
 * <p>This interface is part of the asynchronous utilities provided by the Hadoop
 * Distributed File System (HDFS) Federation router. It is used in conjunction with
 * other asynchronous interfaces such as {@link AsyncRun} and
 * {@link FinallyFunction} to build complex, non-blocking operations.</p>
 *
 * <p>An implementation of this interface should define how to handle a caught
 * exception. It takes two parameters: the result of the asynchronous operation (if
 * any) and the caught exception. The function can then return a new result or modify
 * the existing result to continue the asynchronous processing.</p>
 *
 * <p>CatchFunction is used to implement the following semantics:</p>
 * <pre>
 * {@code
 *    try{
 *      R res = doAsync(input);
 *    } catch(E e) {
 *      // Can use CatchFunction
 *      R result = thenApply(res, e);
 *    }
 * }
 * </pre>
 *
 * @param <R> the type of the result of the asynchronous operation
 * @param <E> the type of the exception to catch, extending {@link Throwable}
 * @see AsyncRun
 * @see FinallyFunction
 */
@FunctionalInterface
public interface CatchFunction<R, E extends Throwable>
    extends Async<R> {

  /**
   * Applies this catch function to the given result and exception.
   * <p>
   * This method is called to process an exception that occurred during an asynchronous
   * operation. The implementation of this method should define how to handle the
   * caught exception. It may involve logging the error, performing a recovery operation,
   * or any other custom error handling logic.
   * <p>
   * The method takes two parameters: the result of the asynchronous operation (if any),
   * and the caught exception. Depending on the implementation, the method may return a
   * new result, modify the existing result, or throw a new exception.
   *
   * @param r the result of the asynchronous operation, which may be null if the operation
   *           did not complete successfully
   * @param e the caught exception, which the function should handle
   * @return the result after applying the catch function, which may be a new result or a
   *         modified version of the input result
   * @throws IOException if an I/O error occurs during the application of the catch function
   */
  R apply(R r, E e) throws IOException;

  /**
   * Applies the catch function to a {@code CompletableFuture}, handling exceptions of a
   * specified type.
   * <p>
   * This default method provides a way to integrate exception handling into a chain of
   * asynchronous operations. It takes a {@code CompletableFuture} and a class object
   * representing the type of exception to catch. The method uses the handle method of the
   * {@code CompletableFuture} to apply the catch function.
   * <p>
   * If the input future completes exceptionally with an instance of the specified exception
   * type, the catch function is applied to the exception. If the future completes with a
   * different type of exception or normally, the original result or exception is propagated.
   *
   * @param in the input {@code CompletableFuture} to which the catch function is applied
   * @param eClazz the class object representing the exception type to catch
   * @return a new {@code CompletableFuture} that completes with the result of applying
   *         the catch function, or propagates the original exception if it does not match
   *         the specified type
   */
  default CompletableFuture<R> apply(
      CompletableFuture<R> in, Class<E> eClazz) {
    return in.handle((r, e) -> {
      if (e == null) {
        return r;
      }
      Throwable readException = unWarpCompletionException(e);
      if (eClazz.isInstance(readException)) {
        try {
          return CatchFunction.this.apply(r, (E) readException);
        } catch (IOException ioe) {
          throw warpCompletionException(ioe);
        }
      }
      throw warpCompletionException(e);
    });
  }
}
