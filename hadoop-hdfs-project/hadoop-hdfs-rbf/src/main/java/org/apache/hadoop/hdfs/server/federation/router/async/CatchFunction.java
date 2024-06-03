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