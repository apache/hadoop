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

package org.apache.hadoop.fs.s3a.impl;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.impl.WrappedIOException;
import org.apache.hadoop.util.DurationInfo;

import static org.apache.hadoop.fs.impl.FutureIOSupport.raiseInnerCause;

/**
 * A bridge from Callable to Supplier; catching exceptions
 * raised by the callable and wrapping them as appropriate.
 * @param <T> return type.
 */
public final class CallableSupplier<T> implements Supplier {

  private static final Logger LOG =
      LoggerFactory.getLogger(CallableSupplier.class);

  private final Callable<T> call;

  /**
   * Create.
   * @param call call to invoke.
   */
  public CallableSupplier(final Callable<T> call) {
    this.call = call;
  }

  @Override
  public Object get() {
    try {
      return call.call();
    } catch (RuntimeException e) {
      throw e;
    } catch (IOException e) {
      throw new WrappedIOException(e);
    } catch (Exception e) {
      throw new WrappedIOException(new IOException(e));
    }
  }

  /**
   * Submit a callable into a completable future.
   * RTEs are rethrown.
   * Non RTEs are caught and wrapped; IOExceptions to
   * {@link WrappedIOException} instances.
   * @param executor executor.
   * @param call call to invoke
   * @param <T> type
   * @return the future to wait for
   */
  @SuppressWarnings("unchecked")
  public static <T> CompletableFuture<T> submit(
      final Executor executor,
      final Callable<T> call) {
    return CompletableFuture.supplyAsync(
        new CallableSupplier<T>(call), executor);
  }

  /**
   * Wait for a list of futures to complete. If the list is empty,
   * return immediately.
   * @param futures list of futures.
   * @throws IOException if one of the called futures raised an IOE.
   * @throws RuntimeException if one of the futures raised one.
   */
  public static <T> void waitForCompletion(
      final List<CompletableFuture<T>> futures)
      throws IOException {
    if (futures.isEmpty()) {
      return;
    }
    // await completion
    waitForCompletion(CompletableFuture.allOf(
        futures.toArray(new CompletableFuture[0])));
  }

  /**
   * Wait for a single of future to complete, extracting IOEs afterwards.
   * @param future future to wait for.
   * @throws IOException if one of the called futures raised an IOE.
   * @throws RuntimeException if one of the futures raised one.
   */
  public static <T> void waitForCompletion(
      final CompletableFuture<T> future)
      throws IOException {
    try (DurationInfo ignore =
            new DurationInfo(LOG, false, "Waiting for task completion")) {
      future.join();
    } catch (CancellationException e) {
      throw new IOException(e);
    } catch (CompletionException e) {
      raiseInnerCause(e);
    }
  }

}
