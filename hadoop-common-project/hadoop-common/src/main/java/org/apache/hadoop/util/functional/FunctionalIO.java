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
import java.util.function.Supplier;

/**
 * Functional utilities for IO operations.
 */
public final class FunctionalIO {

  /**
   * Invoke any operation, wrapping IOExceptions with
   * {@code UncheckedIOException}.
   * @param call callable
   * @return result
   * @param <T> type of result
   * @throws UncheckedIOException if an IOE was raised.
   */
  public static <T> T uncheckIOExceptions(CallableRaisingIOE<T> call) {
    try {
      return call.apply();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Wrap a {@link CallableRaisingIOE} as a {@link Supplier}.
   * This is similar to {@link CommonCallableSupplier}, except that
   * only IOExceptions are caught and wrapped; all other exceptions are
   * propagated unchanged.
   *
   * @param <T> type of result
   */
  private static final class UncheckedIOExceptionSupplier<T> implements Supplier<T> {
    private final CallableRaisingIOE<T> call;

    private UncheckedIOExceptionSupplier(CallableRaisingIOE<T> call) {
      this.call = call;
    }

    @Override
    public T get() {
      return uncheckIOExceptions(call);
    }
  }

  /**
   * Wrap a {@link CallableRaisingIOE} as a {@link Supplier}.
   * @param call call to wrap
   * @param <T> type of result
   * @return a supplier which invokes the call.
   */
  public static <T> Supplier<T> toUncheckedIOExceptionSupplier(CallableRaisingIOE<T> call) {
    return new UncheckedIOExceptionSupplier<>(call);
  }

  /**
   * Invoke the supplier, catching any {@code UncheckedIOException} raised,
   * extracting the inner IOException and rethrowing it.
   * @param call call to invoke
   * @return result
   * @param <T> type of result
   * @throws IOException if the call raised an IOException wrapped by an UncheckedIOException.
   */
  public static <T> T extractIOExceptions(Supplier<T> call) throws IOException {
    try {
      return call.get();
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }

}
