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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSBuilder;

/**
 * Support for future IO and the FS Builder subclasses.
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
  public static <T> T awaitFuture(final Future<T> future)
      throws InterruptedIOException, IOException, RuntimeException {
    try {
      return future.get();
    } catch (InterruptedException e) {
      throw (InterruptedIOException)new InterruptedIOException(e.toString())
          .initCause(e);
    } catch (ExecutionException e) {
      return raiseInnerCause(e);
    }
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

    try {
      return future.get(timeout, unit);
    } catch (InterruptedException e) {
      throw (InterruptedIOException)new InterruptedIOException(e.toString())
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
    Throwable cause = e.getCause();
    if (cause instanceof IOException) {
      throw (IOException) cause;
    } else if (cause instanceof WrappedIOException){
      throw ((WrappedIOException) cause).getCause();
    } else if (cause instanceof RuntimeException){
      throw (RuntimeException) cause;
    } else if (cause != null) {
      // other type: wrap with a new IOE
      throw new IOException(cause);
    } else {
      // this only happens if somebody deliberately raises
      // an ExecutionException
      throw new IOException(e);
    }
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
}
