/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.federation.router;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public final class RouterAsyncRpcUtil {
  public static final ThreadLocal<CompletableFuture<Object>> CUR_COMPLETABLE_FUTURE
      = new ThreadLocal<>();
  private static final Boolean BOOLEAN_RESULT = false;
  private static final Long LONG_RESULT = -1L;
  private static final Object NULL_RESULT = null;

  private RouterAsyncRpcUtil(){}

  public static CompletableFuture<Object> getCompletableFuture() {
    return CUR_COMPLETABLE_FUTURE.get();
  }

  public static void setCurCompletableFuture(
      CompletableFuture<Object> completableFuture) {
    CUR_COMPLETABLE_FUTURE.set(completableFuture);
  }

  public static Object getResult() throws IOException {
    try {
      CompletableFuture<Object> completableFuture = CUR_COMPLETABLE_FUTURE.get();
      Object o =  completableFuture.get();
      return o;
    } catch (InterruptedException e) {
    } catch (ExecutionException e) {
      IOException ioe = (IOException) e.getCause();
      throw ioe;
    }
    return null;
  }

  public static <T> T asyncReturn(Class<T> clazz) {
    if (clazz == null) {
      return null;
    }
    if (clazz.equals(Boolean.class)) {
      return (T) BOOLEAN_RESULT;
    } else if (clazz.equals(Long.class)) {
      return (T) LONG_RESULT;
    }
    return (T) NULL_RESULT;
  }

  public static <T, R> T asyncRequestThenApply(AsyncRequest<R> asyncRequest,
      Function<R, T> thenDo, Class<T> clazz) throws IOException {
    asyncRequest.res();
    CompletableFuture<R> completableFuture =
        (CompletableFuture<R>) CUR_COMPLETABLE_FUTURE.get();
    CompletableFuture<T> resCompletableFuture = completableFuture.thenApply(thenDo);
    setCurCompletableFuture((CompletableFuture<Object>) resCompletableFuture);
    return asyncReturn(clazz);
  }

  @FunctionalInterface
  interface AsyncRequest<R> {
    R res() throws IOException;
  }

}
