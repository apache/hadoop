/*
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

package org.apache.hadoop.util;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Lambda-expression utilities be they generic or specific to
 * Hadoop datatypes.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class LambdaUtils {

  private LambdaUtils() {
  }

  /**
   * Utility method to evaluate a callable and fill in the future
   * with the result or the exception raised.
   * Once this method returns, the future will have been evaluated to
   * either a return value or an exception.
   * @param <T> type of future
   * @param result future for the result.
   * @param call callable to invoke.
   * @return the future passed in
   */
  public static <T> CompletableFuture<T> eval(
      final CompletableFuture<T> result,
      final Callable<T> call) {
    try {
      result.complete(call.call());
    } catch (Throwable tx) {
      result.completeExceptionally(tx);
    }
    return result;
  }

}
