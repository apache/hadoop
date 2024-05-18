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
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;

public class AsyncRequestDoWhile<I, T, R> {

  private boolean satisfy = false;
  private Iterator<I> iterator;
  private final CompletableFuture<R> result = new CompletableFuture<>();
  private RouterAsyncRpcUtil.AsyncRequestUntil<I, T> asyncRequest;
  private BiFunction<I, T, R> thenApply;

  public final void breakNow() {
    satisfy = true;
  }

  public R asyncDoWhile(Class<R> clazz) throws IOException {
    processOnce(null);
    RouterAsyncRpcUtil.setCurCompletableFuture(
        (CompletableFuture<Object>) result);
    return RouterAsyncRpcUtil.asyncReturn(clazz);
  }

  private void processOnce(R ret) throws IOException {
    if (!iterator.hasNext()) {
      result.complete(ret);
      return;
    }
    I in = iterator.next();
    asyncRequest.res(in);
    CompletableFuture<T> completableFuture =
        (CompletableFuture<T>) RouterAsyncRpcUtil.getCompletableFuture();
    completableFuture.thenApply(t -> {
      R r = thenApply.apply(in, t);
      if (satisfy) {
        result.complete(r);
        return null;
      }
      try {
        processOnce(r);
      } catch (IOException e) {
        throw new CompletionException(e);
      }
      return null;
    }).exceptionally(throwable -> {
      breakNow();
      result.completeExceptionally(throwable);
      return null;
    });
  }

  public AsyncRequestDoWhile<I, T, R> whileFor(Iterator<I> it) {
    this.iterator = it;
    return this;
  }

  public AsyncRequestDoWhile<I, T, R> doAsync(
      RouterAsyncRpcUtil.AsyncRequestUntil<I, T> asyncReq) {
    this.asyncRequest = asyncReq;
    return this;
  }

  public AsyncRequestDoWhile<I, T, R> thenApply(BiFunction<I, T, R> then) {
    this.thenApply = then;
    return this;
  }
}
