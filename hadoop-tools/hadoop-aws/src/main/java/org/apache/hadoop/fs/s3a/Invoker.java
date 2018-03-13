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

package org.apache.hadoop.fs.s3a;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Optional;
import javax.annotation.Nullable;

import com.amazonaws.AmazonClientException;
import com.amazonaws.SdkBaseException;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.retry.RetryPolicy;

/**
 * Class to provide lambda expression invocation of AWS operations.
 *
 * The core retry logic is in
 * {@link #retryUntranslated(String, boolean, Retried, Operation)};
 * the other {@code retry() and retryUntranslated()} calls are wrappers.
 *
 * The static {@link #once(String, String, Operation)} and
 * {@link #once(String, String, VoidOperation)} calls take an operation and
 * return it with AWS exceptions translated to IOEs of some form.
 *
 * The retry logic on a failure is defined by the retry policy passed in
 * the constructor; the standard retry policy is {@link S3ARetryPolicy},
 * though others may be used.
 *
 * The constructor also takes two {@link Retried} callbacks.
 * The {@code caughtCallback} is called whenever an exception (IOE or AWS)
 * is caught, before the retry processing looks at it.
 * The {@code retryCallback} is invoked after a retry is scheduled
 * but before the sleep.
 * These callbacks can be used for reporting and incrementing statistics.
 *
 * The static {@link #quietly(String, String, VoidOperation)} and
 * {@link #quietlyEval(String, String, Operation)} calls exist to take any
 * operation and quietly catch and log at debug. The return value of
 * {@link #quietlyEval(String, String, Operation)} is a java 8 optional,
 * which can then be used in java8-expressions.
 */
public class Invoker {
  private static final Logger LOG = LoggerFactory.getLogger(Invoker.class);

  /**
   * Retry policy to use.
   */
  private final RetryPolicy retryPolicy;

  /**
   * Default retry handler.
   */
  private final Retried retryCallback;

  /**
   * Instantiate.
   * @param retryPolicy retry policy for all operations.
   * @param retryCallback standard retry policy
   */
  public Invoker(
      RetryPolicy retryPolicy,
      Retried retryCallback) {
    this.retryPolicy = retryPolicy;
    this.retryCallback = retryCallback;
  }

  public RetryPolicy getRetryPolicy() {
    return retryPolicy;
  }

  public Retried getRetryCallback() {
    return retryCallback;
  }

  /**
   * Execute a function, translating any exception into an IOException.
   * @param action action to execute (used in error messages)
   * @param path path of work (used in error messages)
   * @param operation operation to execute
   * @param <T> type of return value
   * @return the result of the function call
   * @throws IOException any IOE raised, or translated exception
   */
  @Retries.OnceTranslated
  public static <T> T once(String action, String path, Operation<T> operation)
      throws IOException {
    try {
      return operation.execute();
    } catch (AmazonClientException e) {
      throw S3AUtils.translateException(action, path, e);
    }
  }

  /**
   * Execute an operation with no result.
   * @param action action to execute (used in error messages)
   * @param path path of work (used in error messages)
   * @param operation operation to execute
   * @throws IOException any IOE raised, or translated exception
   */
  @Retries.OnceTranslated
  public static void once(String action, String path, VoidOperation operation)
      throws IOException {
    once(action, path,
        () -> {
          operation.execute();
          return null;
        });
  }

  /**
   * Execute an operation and ignore all raised IOExceptions; log at INFO.
   * @param log log to log at info.
   * @param action action to include in log
   * @param path optional path to include in log
   * @param operation operation to execute
   * @param <T> type of operation
   */
  public static <T> void ignoreIOExceptions(
      Logger log,
      String action,
      String path,
      Operation<T> operation) {
    try {
      once(action, path, operation);
    } catch (IOException e) {
      log.info("{}: {}", toDescription(action, path), e.toString(), e);
    }
  }

  /**
   * Execute an operation and ignore all raised IOExceptions; log at INFO.
   * @param log log to log at info.
   * @param action action to include in log
   * @param path optional path to include in log
   * @param operation operation to execute
   */
  public static void ignoreIOExceptions(
      Logger log,
      String action,
      String path,
      VoidOperation operation) {
    ignoreIOExceptions(log, action, path,
        () -> {
          operation.execute();
          return null;
        });
  }

  /**
   * Execute a void operation with retry processing.
   * @param action action to execute (used in error messages)
   * @param path path of work (used in error messages)
   * @param idempotent does the operation have semantics
   * which mean that it can be retried even if was already executed?
   * @param retrying callback on retries
   * @param operation operation to execute
   * @throws IOException any IOE raised, or translated exception
   */
  @Retries.RetryTranslated
  public void retry(String action,
      String path,
      boolean idempotent,
      Retried retrying,
      VoidOperation operation)
      throws IOException {
    retry(action, path, idempotent, retrying,
        () -> {
          operation.execute();
          return null;
        });
  }

  /**
   * Execute a void operation with  the default retry callback invoked.
   * @param action action to execute (used in error messages)
   * @param path path of work (used in error messages)
   * @param idempotent does the operation have semantics
   * which mean that it can be retried even if was already executed?
   * @param operation operation to execute
   * @throws IOException any IOE raised, or translated exception
   */
  @Retries.RetryTranslated
  public void retry(String action,
      String path,
      boolean idempotent,
      VoidOperation operation)
      throws IOException {
    retry(action, path, idempotent, retryCallback, operation);
  }

  /**
   * Execute a function with the default retry callback invoked.
   * @param action action to execute (used in error messages)
   * @param path path of work (used in error messages)
   * @param idempotent does the operation have semantics
   * which mean that it can be retried even if was already executed?
   * @param operation operation to execute
   * @param <T> type of return value
   * @return the result of the call
   * @throws IOException any IOE raised, or translated exception
   */
  @Retries.RetryTranslated
  public <T> T retry(String action,
      @Nullable String path,
      boolean idempotent,
      Operation<T> operation)
      throws IOException {

    return retry(action, path, idempotent, retryCallback, operation);
  }

  /**
   * Execute a function with retry processing.
   * Uses {@link #once(String, String, Operation)} as the inner
   * invocation mechanism before retry logic is performed.
   * @param <T> type of return value
   * @param action action to execute (used in error messages)
   * @param path path of work (used in error messages)
   * @param idempotent does the operation have semantics
   * which mean that it can be retried even if was already executed?
   * @param retrying callback on retries
   * @param operation operation to execute
   * @return the result of the call
   * @throws IOException any IOE raised, or translated exception
   */
  @Retries.RetryTranslated
  public <T> T retry(
      String action,
      @Nullable String path,
      boolean idempotent,
      Retried retrying,
      Operation<T> operation)
      throws IOException {
    return retryUntranslated(
        toDescription(action, path),
        idempotent,
        retrying,
        () -> once(action, path, operation));
  }

  /**
   * Execute a function with retry processing and no translation.
   * and the default retry callback.
   * @param text description for the catching callback
   * @param idempotent does the operation have semantics
   * which mean that it can be retried even if was already executed?
   * @param operation operation to execute
   * @param <T> type of return value
   * @return the result of the call
   * @throws IOException any IOE raised
   * @throws RuntimeException any Runtime exception raised
   */
  @Retries.RetryRaw
  public <T> T retryUntranslated(
      String text,
      boolean idempotent,
      Operation<T> operation) throws IOException {
    return retryUntranslated(text, idempotent,
        retryCallback, operation);
  }

  /**
   * Execute a function with retry processing: AWS SDK Exceptions
   * are <i>not</i> translated.
   * This is method which the others eventually invoke.
   * @param <T> type of return value
   * @param text text to include in messages
   * @param idempotent does the operation have semantics
   * which mean that it can be retried even if was already executed?
   * @param retrying callback on retries
   * @param operation operation to execute
   * @return the result of the call
   * @throws IOException any IOE raised
   * @throws SdkBaseException any AWS exception raised
   * @throws RuntimeException : these are never caught and retries.
   */
  @Retries.RetryRaw
  public <T> T retryUntranslated(
      String text,
      boolean idempotent,
      Retried retrying,
      Operation<T> operation) throws IOException {

    Preconditions.checkArgument(retrying != null, "null retrying argument");
    int retryCount = 0;
    Exception caught;
    RetryPolicy.RetryAction retryAction;
    boolean shouldRetry;
    do {
      try {
        if (retryCount > 0) {
          LOG.debug("retry #{}", retryCount);
        }
        // execute the operation, returning if successful
        return operation.execute();
      } catch (IOException | SdkBaseException e) {
        caught = e;
      }
      // you only get here if the operation didn't complete
      // normally, hence caught != null

      // translate the exception into an IOE for the retry logic
      IOException translated;
      if (caught instanceof IOException) {
        translated = (IOException) caught;
      } else {
        translated = S3AUtils.translateException(text, "",
            (SdkBaseException)caught);
      }

      try {
        // decide action base on operation, invocation count, etc
        retryAction = retryPolicy.shouldRetry(translated, retryCount, 0,
            idempotent);
        // is it a retry operation?
        shouldRetry = retryAction.action.equals(
            RetryPolicy.RetryAction.RETRY.action);
        if (shouldRetry) {
          // notify the callback
          retrying.onFailure(text, translated, retryCount, idempotent);
          // then sleep for the policy delay
          Thread.sleep(retryAction.delayMillis);
        }
        // increment the retry count
        retryCount++;
      } catch (InterruptedException e) {
        // sleep was interrupted
        // change the exception
        caught = new InterruptedIOException("Interrupted");
        caught.initCause(e);
        // no retry
        shouldRetry = false;
        // and re-interrupt the thread
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        // The retry policy raised an exception
        // log that something happened
        LOG.warn("{}: exception in retry processing", text, e);
        // and fail the execution with the last execution exception.
        shouldRetry = false;
      }
    } while (shouldRetry);

    if (caught instanceof IOException) {
      throw (IOException) caught;
    } else {
      throw (SdkBaseException) caught;
    }
  }


  /**
   * Execute an operation; any exception raised is simply caught and
   * logged at debug.
   * @param action action to execute
   * @param path path (for exception construction)
   * @param operation operation
   */
  public static void quietly(String action,
      String path,
      VoidOperation operation) {
    try {
      once(action, path, operation);
    } catch (Exception e) {
      LOG.debug("Action {} failed", action, e);
    }
  }

  /**
   * Execute an operation; any exception raised is caught and
   * logged at debug.
   * The result is only non-empty if the operation succeeded
   * @param <T> type to return
   * @param action action to execute
   * @param path path (for exception construction)
   * @param operation operation
   * @return the result of a successful operation
   */
  public static <T> Optional<T> quietlyEval(String action,
      String path,
      Operation<T> operation) {
    try {
      return Optional.of(once(action, path, operation));
    } catch (Exception e) {
      LOG.debug("Action {} failed", action, e);
      return Optional.empty();
    }
  }

  /**
   * Take an action and path and produce a string for logging.
   * @param action action
   * @param path path (may be null or empty)
   * @return string for logs
   */
  private static String toDescription(String action, @Nullable String path) {
    return action +
        (StringUtils.isNotEmpty(path) ? (" on " + path) : "");
  }

  /**
   * Arbitrary operation throwing an IOException.
   * @param <T> return type
   */
  @FunctionalInterface
  public interface Operation<T> {
    T execute() throws IOException;
  }

  /**
   * Void operation which may raise an IOException.
   */
  @FunctionalInterface
  public interface VoidOperation {
    void execute() throws IOException;
  }

  /**
   * Callback for retry and notification operations.
   * Even if the interface is throwing up "raw" exceptions, this handler
   * gets the translated one.
   */
  @FunctionalInterface
  public interface Retried {
    /**
     * Retry event in progress (before any sleep).
     * @param text text passed in to the retry() Call.
     * @param exception the caught (and possibly translated) exception.
     * @param retries number of retries so far
     * @param idempotent is the request idempotent.
     */
    void onFailure(
        String text,
        IOException exception,
        int retries,
        boolean idempotent);
  }

  /**
   * No op for a retrying callback.
   */
  public static final Retried NO_OP = new Retried() {
    @Override
    public void onFailure(String text,
        IOException exception,
        int retries,
        boolean idempotent) {
    }
  };

  /**
   * Log summary at info, full stack at debug.
   */
  public static final Retried LOG_EVENT = new Retried() {
    @Override
    public void onFailure(String text,
        IOException exception,
        int retries,
        boolean idempotent) {
      LOG.debug("{}: " + exception, text);
      if (retries == 1) {
        // stack on first attempt, to keep noise down
        LOG.debug("{}: " + exception, text, exception);
      }
    }
  };
}
