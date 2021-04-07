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
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

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
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class FutureIO {

  private FutureIO() {
  }

  /**
   * Given a future, evaluate it.
   * <p>
   * Any exception generated in the future is
   * extracted and rethrown.
   * </p>
   * @param future future to evaluate
   * @param <T> type of the result.
   * @return the result, if all went well.
   * @throws InterruptedIOException future was interrupted
   * @throws IOException if something went wrong
   * @throws RuntimeException any nested RTE thrown
   */
  public static <T> T awaitFuture(final Future<T> future)
      throws InterruptedIOException, IOException, RuntimeException {
    try {
      return future.get();
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
    try {
      return future.get(timeout, unit);
    } catch (InterruptedException e) {
      throw (InterruptedIOException) new InterruptedIOException(e.toString())
          .initCause(e);
    } catch (ExecutionException e) {
      return raiseInnerCause(e);
    }
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

}
