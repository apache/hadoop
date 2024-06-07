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

@FunctionalInterface
public interface AsyncCatchFunction<R, E extends Throwable>
    extends CatchFunction<R, E> {

  void applyAsync(R r, E e) throws IOException;

  @Override
  default R apply(R r, E e) throws IOException {
    applyAsync(r, e);
    return result();
  }

  default CompletableFuture<R> async(R r, E e) throws IOException {
    applyAsync(r, e);
    CompletableFuture<R> completableFuture = getCurCompletableFuture();
    assert completableFuture != null;
    return completableFuture;
  }

  @Override
  default CompletableFuture<R> apply(
      CompletableFuture<R> in, Class<E> eClazz) {
    CompletableFuture<R> result = new CompletableFuture<>();
    in.handle((r, e) -> {
      if (e == null) {
        result.complete(r);
        return r;
      }
      Throwable cause = e.getCause();

      if (eClazz.isInstance(cause)) {
        try {
          async(r, (E) cause).handle((r1, ex) -> {
            if (ex != null) {
              result.completeExceptionally(ex);
            } else {
              result.complete(r1);
            }
            return null;
          });
        } catch (IOException ioe) {
          result.completeExceptionally(new CompletionException(ioe));
        }
      } else {
        result.completeExceptionally(e);
      }
      return r;
    });
    return result;
  }
}
