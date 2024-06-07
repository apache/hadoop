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
import java.util.concurrent.CompletionException;

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
 * @param <R> the type of the result of the asynchronous operation
 * @param <E> the type of the exception to catch, extending {@link Throwable}
 * @see AsyncRun
 * @see FinallyFunction
 */
@FunctionalInterface
public interface CatchFunction<R, E extends Throwable>
    extends Async<R>{
  R apply(R r, E e) throws IOException;

  default CompletableFuture<R> apply(
      CompletableFuture<R> in, Class<E> eClazz) {
    return in.handle((r, e) -> {
      if (e == null) {
        return r;
      }
      Throwable cause = e.getCause();
      assert cause != null;
      if (eClazz.isInstance(cause)) {
        try {
          return CatchFunction.this.apply(r, (E) cause);
        } catch (IOException ioe) {
          throw new CompletionException(ioe);
        }
      } else {
        throw new CompletionException(cause);
      }
    });
  }

}