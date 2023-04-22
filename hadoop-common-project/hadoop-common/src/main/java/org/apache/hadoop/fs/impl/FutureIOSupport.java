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

package org.apache.hadoop.fs.impl;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSBuilder;
import org.apache.hadoop.util.functional.CallableRaisingIOE;
import org.apache.hadoop.util.functional.FutureIO;

/**
 * Support for future IO and the FS Builder subclasses.
 * All methods in this class have been superceded by those in
 * {@link FutureIO}.
 * The methods here are retained but all marked as deprecated.
 * This is to ensure that any external
 * filesystem implementations can still use these methods
 * without linkage problems surfacing.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@Deprecated
public final class FutureIOSupport {

  private FutureIOSupport() {
  }

  /**
   * Given a future, evaluate it. Raised exceptions are
   * extracted and handled.
   * See {@link FutureIO#awaitFuture(Future, long, TimeUnit)}.
   * @param future future to evaluate
   * @param <T> type of the result.
   * @return the result, if all went well.
   * @throws InterruptedIOException future was interrupted
   * @throws IOException if something went wrong
   * @throws RuntimeException any nested RTE thrown
   */
  @Deprecated
  public static <T> T awaitFuture(final Future<T> future)
      throws InterruptedIOException, IOException, RuntimeException {
    return FutureIO.awaitFuture(future);
  }


  /**
   * Given a future, evaluate it. Raised exceptions are
   * extracted and handled.
   * See {@link FutureIO#awaitFuture(Future, long, TimeUnit)}.
   * @param future future to evaluate
   * @param <T> type of the result.
   * @param timeout timeout.
   * @param unit unit.
   * @return the result, if all went well.
   * @throws InterruptedIOException future was interrupted
   * @throws IOException if something went wrong
   * @throws RuntimeException any nested RTE thrown
   * @throws TimeoutException the future timed out.
   */
  @Deprecated
  public static <T> T awaitFuture(final Future<T> future,
      final long timeout,
      final TimeUnit unit)
      throws InterruptedIOException, IOException, RuntimeException,
      TimeoutException {
    return FutureIO.awaitFuture(future, timeout, unit);
  }

  /**
   * From the inner cause of an execution exception, extract the inner cause
   * if it is an IOE or RTE.
   * See {@link FutureIO#raiseInnerCause(ExecutionException)}.
   * @param e exception.
   * @param <T> type of return value.
   * @return nothing, ever.
   * @throws IOException either the inner IOException, or a wrapper around
   * any non-Runtime-Exception
   * @throws RuntimeException if that is the inner cause.
   */
  @Deprecated
  public static <T> T raiseInnerCause(final ExecutionException e)
      throws IOException {
    return FutureIO.raiseInnerCause(e);
  }

  /**
   * Extract the cause of a completion failure and rethrow it if an IOE
   * or RTE.
   * See {@link FutureIO#raiseInnerCause(CompletionException)}.
   * @param e exception.
   * @param <T> type of return value.
   * @return nothing, ever.
   * @throws IOException either the inner IOException, or a wrapper around
   * any non-Runtime-Exception
   * @throws RuntimeException if that is the inner cause.
   */
  @Deprecated
  public static <T> T raiseInnerCause(final CompletionException e)
      throws IOException {
    return FutureIO.raiseInnerCause(e);
  }

  /**
   * Propagate options to any builder.
   * {@link FutureIO#propagateOptions(FSBuilder, Configuration, String, String)}
   * @param builder builder to modify
   * @param conf configuration to read
   * @param optionalPrefix prefix for optional settings
   * @param mandatoryPrefix prefix for mandatory settings
   * @param <T> type of result
   * @param <U> type of builder
   * @return the builder passed in.
   */
  @Deprecated
  public static <T, U extends FSBuilder<T, U>>
        FSBuilder<T, U> propagateOptions(
      final FSBuilder<T, U> builder,
      final Configuration conf,
      final String optionalPrefix,
      final String mandatoryPrefix) {
    return FutureIO.propagateOptions(builder,
        conf, optionalPrefix, mandatoryPrefix);
  }

  /**
   * Propagate options to any builder.
   * {@link FutureIO#propagateOptions(FSBuilder, Configuration, String, boolean)}
   * @param builder builder to modify
   * @param conf configuration to read
   * @param prefix prefix to scan/strip
   * @param mandatory are the options to be mandatory or optional?
   */
  @Deprecated
  public static void propagateOptions(
      final FSBuilder<?, ?> builder,
      final Configuration conf,
      final String prefix,
      final boolean mandatory) {
    FutureIO.propagateOptions(builder, conf, prefix, mandatory);
  }

  /**
   * Evaluate a CallableRaisingIOE in the current thread,
   * converting IOEs to RTEs and propagating.
   * See {@link FutureIO#eval(CallableRaisingIOE)}.
   *
   * @param callable callable to invoke
   * @param <T> Return type.
   * @return the evaluated result.
   * @throws UnsupportedOperationException fail fast if unsupported
   * @throws IllegalArgumentException invalid argument
   */
  public static <T> CompletableFuture<T> eval(
      CallableRaisingIOE<T> callable) {
    return FutureIO.eval(callable);
  }
}
