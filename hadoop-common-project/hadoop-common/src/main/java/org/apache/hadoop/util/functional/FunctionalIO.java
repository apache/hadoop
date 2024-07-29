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

package org.apache.hadoop.util.functional;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Functional utilities for IO operations.
 */
@InterfaceAudience.Private
public final class FunctionalIO {

  private FunctionalIO() {
  }

  /**
   * Invoke any operation, wrapping IOExceptions with
   * {@code UncheckedIOException}.
   * @param call callable
   * @param <T> type of result
   * @return result
   * @throws UncheckedIOException if an IOE was raised.
   */
  public static <T> T uncheckIOExceptions(CallableRaisingIOE<T> call) {
    return call.unchecked();
  }

  /**
   * Wrap a {@link CallableRaisingIOE} as a {@link Supplier}.
   * @param call call to wrap
   * @param <T> type of result
   * @return a supplier which invokes the call.
   */
  public static <T> Supplier<T> toUncheckedIOExceptionSupplier(CallableRaisingIOE<T> call) {
    return call::unchecked;
  }

  /**
   * Invoke the supplier, catching any {@code UncheckedIOException} raised,
   * extracting the inner IOException and rethrowing it.
   * @param call call to invoke
   * @param <T> type of result
   * @return result
   * @throws IOException if the call raised an IOException wrapped by an UncheckedIOException.
   */
  public static <T> T extractIOExceptions(Supplier<T> call) throws IOException {
    try {
      return call.get();
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }


  /**
   * Convert a {@link FunctionRaisingIOE} as a {@link Supplier}.
   * @param fun function to wrap
   * @param <T> type of input
   * @param <R> type of return value.
   * @return a new function which invokes the inner function and wraps
   * exceptions.
   */
  public static <T, R> Function<T, R> toUncheckedFunction(FunctionRaisingIOE<T, R> fun) {
    return fun::unchecked;
  }


}
