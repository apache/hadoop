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
        result.completeExceptionally(e);
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

  public I getNow() {
    return now;
  }

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

