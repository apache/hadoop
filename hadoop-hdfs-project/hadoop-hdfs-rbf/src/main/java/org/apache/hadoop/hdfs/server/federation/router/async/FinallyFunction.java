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

import static org.apache.hadoop.hdfs.server.federation.router.async.Async.warpCompletionException;

/**
 * The {@code FinallyFunction} interface represents a function that is used to perform
 * final actions after an asynchronous operation completes, regardless of whether the
 * operation was successful or resulted in an exception. This interface is part of
 * the asynchronous utilities provided by the Hadoop Distributed File System (HDFS)
 * Federation router.
 *
 * <p>A {@code FinallyFunction} is typically used for cleanup or finalization tasks,
 * such as releasing resources, closing connections, or performing other actions that
 * need to occur after all other operations have completed.</p>
 *
 * <p>An implementation of this interface should define what the final action is. It
 * takes the result of the asynchronous operation as an argument and returns a new
 * result, which can be the same as the input result or a modified version of it.</p>
 *
 * <p>FinallyFunction is used to implement the following semantics:</p>
 * <pre>
 * {@code
 *    try{
 *      R res = doAsync(input);
 *    } catch(...) {
 *      ...
 *    } finally {
 *      // Can use FinallyFunction
 *      R result = thenApply(res);
 *    }
 * }
 * </pre>
 *
 * @param <R> the type of the result of the asynchronous operation
 */
@FunctionalInterface
public interface FinallyFunction<R> {

  /**
   * Applies this final action function to the result of an asynchronous operation.
   *
   * @param r the result of the asynchronous operation, which may be null if the
   *           operation did not complete successfully
   * @return the result after applying the final action, which may be a new result or a
   *         modified version of the input result
   * @throws IOException if an I/O error occurs during the application of the final action
   */
  R apply(R r) throws IOException;

  /**
   * Applies this final action function to a {@code CompletableFuture}, which is expected
   * to be the result of an asynchronous operation.
   * <p>
   * This method is a convenience that simplifies the use of {@code FinallyFunction}
   * with asynchronous operations. It handles the completion of the future and applies
   * the {@code FinallyFunction} to the result.
   *
   * @param in the {@code CompletableFuture} representing the asynchronous operation
   * @return a new {@code CompletableFuture} that completes with the result of applying
   *         the final action function
   */
  default CompletableFuture<R> apply(CompletableFuture<R> in) {
    return in.handle((r, e) -> {
      try {
        R ret = apply(r);
        if (e != null) {
          throw warpCompletionException(e);
        } else {
          return ret;
        }
      } catch (IOException ioe) {
        throw warpCompletionException(ioe);
      }
    });
  }
}
