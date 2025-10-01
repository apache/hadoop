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
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSBuilder;
import org.apache.hadoop.util.Time;

/**
 * Future IO Helper methods.
 * <p>
 * Contains methods promoted from
 * {@link org.apache.hadoop.fs.impl.FutureIOSupport} because they
 * are a key part of integrating async IO in application code.
 * </p>
 * <p>
 * One key feature is that the {@link #awaitFuture(Future)} and
 * {@link #awaitFuture(Future, long, TimeUnit)} calls will
 * extract and rethrow exceptions raised in the future's execution,
 * including extracting the inner IOException of any
 * {@code UncheckedIOException} raised in the future.
 * This makes it somewhat easier to execute IOException-raising
 * code inside futures.
 * <p>
 * Important: any  {@code CancellationException} raised by the future
 * is rethrown unchanged. This has been the implicit behavior since
 * this code was first written, and is now explicitly documented.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class FutureIO {

  private static final Logger LOG =
      LoggerFactory.getLogger(FutureIO.class);

  private FutureIO() {
  }

  /**
   * Given a future, evaluate it.
   * <p>
   * Any exception generated in the future is
   * extracted and rethrown.
   * </p>
   * If this thread is interrupted while waiting for the future to complete,
   * an {@code InterruptedIOException} is raised.
   * However, if the future is cancelled, a {@code CancellationException}
   * is raised in the {code Future.get()} call. This is
   * passed up as is -so allowing the caller to distinguish between
   * thread interruption (such as when speculative task execution is aborted)
   * and future cancellation.
   * @param future future to evaluate
   * @param <T> type of the result.
   * @return the result, if all went well.
   * @throws InterruptedIOException waiting for future completion was interrupted
   * @throws CancellationException if the future itself was cancelled
   * @throws IOException if something went wrong
   * @throws RuntimeException any nested RTE thrown
   */
  public static <T> T awaitFuture(final Future<T> future)
      throws InterruptedIOException, IOException, CancellationException, RuntimeException {
    try {
      return future.get();
    } catch (CancellationException e) {
      LOG.debug("Future {} was cancelled", future, e);
      throw e;
    } catch (InterruptedException e) {
      throw (InterruptedIOException) new InterruptedIOException(e.toString())
          .initCause(e);
    } catch (ExecutionException e) {
      return raiseInnerCause(e);
    }
  }

  /**
   * Given a future, evaluate it.
   * <p>
   * Any exception generated in the future is
   * extracted and rethrown.
   * </p>
   * @param future future to evaluate
   * @param timeout timeout to wait.
   * @param unit time unit.
   * @param <T> type of the result.
   * @return the result, if all went well.
   * @throws InterruptedIOException waiting for future completion was interrupted
   * @throws CancellationException if the future itself was cancelled
   * @throws IOException if something went wrong
   * @throws RuntimeException any nested RTE thrown
   * @throws TimeoutException the future timed out.
   */
  public static <T> T awaitFuture(final Future<T> future,
      final long timeout,
      final TimeUnit unit)
      throws InterruptedIOException, IOException, CancellationException, RuntimeException,
             TimeoutException {
    try {
      return future.get(timeout, unit);
    } catch (CancellationException e) {
      LOG.debug("Future {} was cancelled", future, e);
      throw e;
    } catch (InterruptedException e) {
      throw (InterruptedIOException) new InterruptedIOException(e.toString())
          .initCause(e);
    } catch (ExecutionException e) {
      return raiseInnerCause(e);
    }
  }

  /**
   * Evaluates a collection of futures and returns their results as a list.
   * <p>
   * This method blocks until all futures in the collection have completed.
   * If any future throws an exception during its execution, this method
   * extracts and rethrows that exception.
   * </p>
   * @param collection collection of futures to be evaluated
   * @param <T> type of the result.
   * @return the list of future's result, if all went well.
   * @throws InterruptedIOException waiting for future completion was interrupted
   * @throws CancellationException if the future itself was cancelled
   * @throws IOException if something went wrong
   * @throws RuntimeException any nested RTE thrown
   */
  public static <T> List<T> awaitAllFutures(final Collection<Future<T>> collection)
      throws InterruptedIOException, IOException, CancellationException, RuntimeException {
    List<T> results = new ArrayList<>();
    for (Future<T> future : collection) {
      results.add(awaitFuture(future));
    }
    return results;
  }

  /**
   * Evaluates a collection of futures and returns their results as a list,
   * but only waits up to the specified timeout for each future to complete.
   * <p>
   * This method blocks until all futures in the collection have completed or
   * the timeout expires, whichever happens first. If any future throws an
   * exception during its execution, this method extracts and rethrows that exception.
   * @param collection collection of futures to be evaluated
   * @param duration timeout duration
   * @param <T> type of the result.
   * @return the list of future's result, if all went well.
   * @throws InterruptedIOException waiting for future completion was interrupted
   * @throws CancellationException if the future itself was cancelled
   * @throws IOException if something went wrong
   * @throws RuntimeException any nested RTE thrown
   * @throws TimeoutException the future timed out.
   */
  public static <T> List<T> awaitAllFutures(final Collection<Future<T>> collection,
      final Duration duration)
      throws InterruptedIOException, IOException, CancellationException, RuntimeException,
             TimeoutException {
    List<T> results = new ArrayList<>();
    for (Future<T> future : collection) {
      results.add(awaitFuture(future, duration.toMillis(), TimeUnit.MILLISECONDS));
    }
    return results;
  }

  /**
   * Cancels a collection of futures and awaits the specified duration for their completion.
   * <p>
   * This method blocks until all futures in the collection have completed or
   * the timeout expires, whichever happens first.
   * All exceptions thrown by the futures are ignored. as is any TimeoutException.
   * @param collection collection of futures to be evaluated
   * @param interruptIfRunning should the cancel interrupt any active futures?
   * @param duration total timeout duration
   * @param <T> type of the result.
   * @return all futures which completed successfully.
   */
  public static <T> List<T> cancelAllFuturesAndAwaitCompletion(
      final Collection<Future<T>> collection,
      final boolean interruptIfRunning,
      final Duration duration) {

    for (Future<T> future : collection) {
      future.cancel(interruptIfRunning);
    }
    // timeout is relative to the start of the operation
    long timeout = duration.toMillis();
    List<T> results = new ArrayList<>();
    for (Future<T> future : collection) {
      long start = Time.now();
      try {
        results.add(awaitFuture(future, timeout, TimeUnit.MILLISECONDS));
      } catch (CancellationException | IOException | TimeoutException e) {
        // swallow
        LOG.debug("Ignoring exception of cancelled future", e);
      }
      // measure wait time and reduce timeout accordingly
      long waited = Time.now() - start;
      timeout -= waited;
      if (timeout < 0) {
        // very brief timeout always
        timeout = 0;
      }
    }
    return results;
  }

  /**
   * From the inner cause of an execution exception, extract the inner cause
   * if it is an IOE or RTE.
   * This will always raise an exception, either the inner IOException,
   * an inner RuntimeException, or a new IOException wrapping the raised
   * exception.
   * @param e exception.
   * @param <T> type of return value.
   * @return nothing, ever.
   * @throws IOException either the inner IOException, or a wrapper around
   * any non-Runtime-Exception
   * @throws RuntimeException if that is the inner cause.
   */
  public static <T> T raiseInnerCause(final ExecutionException e)
      throws IOException {
    throw unwrapInnerException(e);
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
    throw unwrapInnerException(e);
  }

  /**
   * From the inner cause of an execution exception, extract the inner cause
   * to an IOException, raising RuntimeExceptions and Errors immediately.
   * <ol>
   *   <li> If it is an IOE: Return.</li>
   *   <li> If it is a {@link UncheckedIOException}: return the cause</li>
   *   <li> Completion/Execution Exceptions: extract and repeat</li>
   *   <li> If it is an RTE or Error: throw.</li>
   *   <li> Any other type: wrap in an IOE</li>
   * </ol>
   *
   * Recursively handles wrapped Execution and Completion Exceptions in
   * case something very complicated has happened.
   * @param e exception.
   * @return an IOException extracted or built from the cause.
   * @throws RuntimeException if that is the inner cause.
   * @throws Error if that is the inner cause.
   */
  @SuppressWarnings("ChainOfInstanceofChecks")
  public static IOException unwrapInnerException(final Throwable e) {
    Throwable cause = e.getCause();
    if (cause instanceof IOException) {
      return (IOException) cause;
    } else if (cause instanceof UncheckedIOException) {
      // this is always an IOException
      return ((UncheckedIOException) cause).getCause();
    } else if (cause instanceof CompletionException) {
      return unwrapInnerException(cause);
    } else if (cause instanceof ExecutionException) {
      return unwrapInnerException(cause);
    } else if (cause instanceof RuntimeException) {
      throw (RuntimeException) cause;
    } else if (cause instanceof Error) {
      throw (Error) cause;
    } else if (cause != null) {
      // other type: wrap with a new IOE
      return new IOException(cause);
    } else {
      // this only happens if there was no cause.
      return new IOException(e);
    }
  }

  /**
   * Propagate options to any builder, converting everything with the
   * prefix to an option where, if there were 2+ dot-separated elements,
   * it is converted to a schema.
   * See {@link #propagateOptions(FSBuilder, Configuration, String, boolean)}.
   * @param builder builder to modify
   * @param conf configuration to read
   * @param optionalPrefix prefix for optional settings
   * @param mandatoryPrefix prefix for mandatory settings
   * @param <T> type of result
   * @param <U> type of builder
   * @return the builder passed in.
   */
  public static <T, U extends FSBuilder<T, U>> FSBuilder<T, U> propagateOptions(
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
   *   fs.example.s3a.option becomes "s3a.option"
   *   fs.example.fs.io.policy becomes "fs.io.policy"
   *   fs.example.something becomes "something"
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
