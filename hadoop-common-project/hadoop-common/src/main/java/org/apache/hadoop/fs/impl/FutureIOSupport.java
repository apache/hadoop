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
import java.util.Map;
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
 * If methods in here are needed for applications, promote
 * to {@link FutureIO} for public use -with the original
 * method relaying to it. This is to ensure that external
 * filesystem implementations can safely use these methods
 * without linkage problems surfacing.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class FutureIOSupport {

  private FutureIOSupport() {
  }

  /**
   * Given a future, evaluate it. Raised exceptions are
   * extracted and handled.
   * @param future future to evaluate
   * @param <T> type of the result.
   * @return the result, if all went well.
   * @throws InterruptedIOException future was interrupted
   * @throws IOException if something went wrong
   * @throws RuntimeException any nested RTE thrown
   */
  public static <T> T  awaitFuture(final Future<T> future)
      throws InterruptedIOException, IOException, RuntimeException {
    return FutureIO.awaitFuture(future);
  }


  /**
   * Given a future, evaluate it. Raised exceptions are
   * extracted and handled.
   * @param future future to evaluate
   * @param <T> type of the result.
   * @return the result, if all went well.
   * @throws InterruptedIOException future was interrupted
   * @throws IOException if something went wrong
   * @throws RuntimeException any nested RTE thrown
   * @throws TimeoutException the future timed out.
   */
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
   * This will always raise an exception, either the inner IOException,
   * an inner RuntimeException, or a new IOException wrapping the raised
   * exception.
   *
   * @param e exception.
   * @param <T> type of return value.
   * @return nothing, ever.
   * @throws IOException either the inner IOException, or a wrapper around
   * any non-Runtime-Exception
   * @throws RuntimeException if that is the inner cause.
   */
  public static <T> T raiseInnerCause(final ExecutionException e)
      throws IOException {
    return FutureIO.raiseInnerCause(e);
  }

  /**
   * Extract the cause of a completion failure and rethrow it if an IOE
   * or RTE.
   * @param e exception.
   * @param <T> type of return value.
   * @return nothing, ever.
   * @throws IOException either the inner IOException, or a wrapper around
   * any non-Runtime-Exception
   * @throws RuntimeException if that is the inner cause.
   */
  public static <T> T raiseInnerCause(final CompletionException e)
      throws IOException {
    return FutureIO.raiseInnerCause(e);
  }

  /**
   * Propagate options to any builder, converting everything with the
   * prefix to an option where, if there were 2+ dot-separated elements,
   * it is converted to a schema.
   * <pre>
   *   fs.example.s3a.option => s3a:option
   *   fs.example.fs.io.policy => s3a.io.policy
   *   fs.example.something => something
   * </pre>
   * @param builder builder to modify
   * @param conf configuration to read
   * @param optionalPrefix prefix for optional settings
   * @param mandatoryPrefix prefix for mandatory settings
   * @param <T> type of result
   * @param <U> type of builder
   * @return the builder passed in.
   */
  public static <T, U extends FSBuilder<T, U>>
        FSBuilder<T, U> propagateOptions(
      final FSBuilder<T, U> builder,
      final Configuration conf,
      final String optionalPrefix,
      final String mandatoryPrefix) {
    propagateOptions(builder, conf,
        optionalPrefix, false);
    propagateOptions(builder, conf,
        mandatoryPrefix, true);
    return builder;
  }

  /**
   * Propagate options to any builder, converting everything with the
   * prefix to an option where, if there were 2+ dot-separated elements,
   * it is converted to a schema.
   * <pre>
   *   fs.example.s3a.option => s3a:option
   *   fs.example.fs.io.policy => s3a.io.policy
   *   fs.example.something => something
   * </pre>
   * @param builder builder to modify
   * @param conf configuration to read
   * @param prefix prefix to scan/strip
   * @param mandatory are the options to be mandatory or optional?
   */
  public static void propagateOptions(
      final FSBuilder<?, ?> builder,
      final Configuration conf,
      final String prefix,
      final boolean mandatory) {

    final String p = prefix.endsWith(".") ? prefix : (prefix + ".");
    final Map<String, String> propsWithPrefix = conf.getPropsWithPrefix(p);
    for (Map.Entry<String, String> entry : propsWithPrefix.entrySet()) {
      // change the schema off each entry
      String key = entry.getKey();
      String val = entry.getValue();
      if (mandatory) {
        builder.must(key, val);
      } else {
        builder.opt(key, val);
      }
    }
  }

  /**
   * Evaluate a CallableRaisingIOE in the current thread,
   * converting IOEs to RTEs and propagating.
   * @param callable callable to invoke
   * @param <T> Return type.
   * @return the evaluated result.
   * @throws UnsupportedOperationException fail fast if unsupported
   * @throws IllegalArgumentException invalid argument
   */
  public static <T> CompletableFuture<T> eval(
      CallableRaisingIOE<T> callable) {
    CompletableFuture<T> result = new CompletableFuture<>();
    try {
      result.complete(callable.apply());
    } catch (UnsupportedOperationException | IllegalArgumentException tx) {
      // fail fast here
      throw tx;
    } catch (Throwable tx) {
      // fail lazily here to ensure callers expect all File IO operations to
      // surface later
      result.completeExceptionally(tx);
    }
    return result;
  }
}
