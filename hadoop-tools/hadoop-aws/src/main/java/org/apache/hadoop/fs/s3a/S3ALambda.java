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

import com.amazonaws.AmazonClientException;
import com.amazonaws.SdkBaseException;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.retry.RetryPolicy;

/**
 * Class to provide lambda expression access to AWS operations.
 */
public class S3ALambda {
  private static final Logger LOG = LoggerFactory.getLogger(S3ALambda.class);

  /**
   * Retry policy to use
   */
  private final RetryPolicy retryPolicy;

  /**
   * Default retry handler.
   */
  private final S3ALambda.Retrying onRetry;

  private final Catching onCatch;

  /**
   * Instantiate.
   * @param retryPolicy retry policy for all operations.
   * @param onRetry standard retry policy
   * @param onCatch catch notification callback
   */
  public S3ALambda(
      RetryPolicy retryPolicy,
      Retrying onRetry,
      Catching onCatch) {
    this.retryPolicy = retryPolicy;
    this.onRetry = onRetry;
    this.onCatch = onCatch;
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
  public <T> T once(String action, String path, Operation<T> operation)
      throws IOException {
    try {
      return operation.execute();
    } catch (AmazonClientException e) {
      throw S3AUtils.translateException(action, path, e);
    }
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
  public <T> T once(String action, Path path, Operation<T> operation)
      throws IOException {
    return once(action, path.toString(), operation);
  }

  /**
   * Execute an operation with no result.
   * @param action action to execute (used in error messages)
   * @param path path of work (used in error messages)
   * @param operation operation to execute
   * @throws IOException any IOE raised, or translated exception
   */
  public void once(String action, String path, VoidOperation operation)
      throws IOException {
    once(action, path,
        () -> {
          operation.execute();
          return null;
        });
  }

  /**
   * Execute an operation with no result.
   * @param action action to execute (used in error messages)
   * @param path path of work (used in error messages)
   * @param operation operation to execute
   * @throws IOException any IOE raised, or translated exception
   */
  public void once(String action, Path path, VoidOperation operation)
      throws IOException {
    once(action, path,
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
   * @param operation operation to execute
   * @param retrying callback on retries
   * @throws IOException any IOE raised, or translated exception
   */
  public void retry(String action,
      String path,
      boolean idempotent,
      VoidOperation operation,
      Retrying retrying)
      throws IOException {
    retry(action, path, idempotent,
        retrying, () -> {
          operation.execute();
          return null;
        }
    );
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
  public void retry(String action,
      String path,
      boolean idempotent,
      VoidOperation operation)
      throws IOException {
    retry(action, path, idempotent, operation, onRetry);
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
  public <T> T retry(String action,
      String path,
      boolean idempotent,
      Operation<T> operation)
      throws IOException {

    return retry(action, path, idempotent, onRetry, operation);
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
  public <T> T retry(
      String action,
      String path,
      boolean idempotent,
      Retrying retrying,
      Operation<T> operation)
      throws IOException {
    return retryUntranslated(
        toDescription(action, path),
        idempotent,
        retrying,
        onCatch,
        () -> once(action, path, operation));
  }

  /**
   * Execute a function with retry processing and no translation.
   * and the default retry and catching callback.
   * @param text description for the catching callback
   * @param idempotent does the operation have semantics
   * which mean that it can be retried even if was already executed?
   * @param operation operation to execute
   * @param <T> type of return value
   * @return the result of the call
   * @throws IOException any IOE raised
   * @throws RuntimeException any Runtime exception raised
   */
  public <T> T retryUntranslated(
      String text,
      boolean idempotent,
      Operation<T> operation) throws IOException {
    return retryUntranslated(text, idempotent, onRetry, onCatch, operation);
  }

  /**
   * Execute a function with retry processing: AWS SDK Exceptions
   * are <i>not</i> translated.
   * This is method which the others eventually invoke.
   * @param <T> type of return value
   * @param idempotent does the operation have semantics
   * which mean that it can be retried even if was already executed?
   * @param retrying callback on retries
   * @param operation operation to execute
   * @return the result of the call
   * @throws IOException any IOE raised
   * @throws SdkBaseException any AWS exception raised
   */
  public <T> T retryUntranslated(
      String text,
      boolean idempotent,
      Retrying retrying,
      Catching catching,
      Operation<T> operation) throws IOException {

    Preconditions.checkArgument(retrying != null, "null retrying argument");
    int retryCount = 0;
    Exception caught;
    RetryPolicy.RetryAction retryAction;
    boolean shouldRetry;
    do {
      try {
        // execute the operation, returning if successful
        return operation.execute();
      } catch (IOException | SdkBaseException e) {
        caught = e;
      }
      // you only get here if the operation didn't complete
      // normally, hence caught != null
      catching.onCaught(text, caught);

      int attempts = retryCount + 1;
      try {
        // decide action base on operation, invocation count, etc
        retryAction = retryPolicy.shouldRetry(caught, retryCount, 0,
            idempotent);
        // is it a retry operation?
        shouldRetry = retryAction.action.equals(
            RetryPolicy.RetryAction.RETRY.action);
        if (shouldRetry) {
          // notify the callback
          retrying.onRetry(caught, retryCount, idempotent);
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

    // if the code gets here, then all retries have been exhausted
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
  public void quietly(String action,
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
   * @param action action to execute
   * @param path path (for exception construction)
   * @param operation operation
   */
  public <T> Optional<T> quietlyEval(String action,
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
   * Take an action and path and produce a string for logging
   * @param action action
   * @param path path (may be null or empty)
   * @return string for logs
   */
  private String toDescription(String action, String path) {
    return action +
        (StringUtils.isNotEmpty(path) ? (" on " + path) : "");
  }

  /**
   * Take an action and path and produce a string for logging
   * @param action action
   * @param path path (may be null)
   * @return string for logs
   */
  private String toDescription(String action, Path path) {
    return toDescription(action,
        path != null ? path.toString() : "");
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
   * Callback for retry operations.
   */
  @FunctionalInterface
  public interface Retrying {
    void onRetry(Exception e, int retries, boolean idempotent);
  }

  /**
   * Callback for exception caught, caller actions expected to be limited
   * to zero-side-effect catch and log.
   */
  @FunctionalInterface
  public interface Catching {
    void onCaught(String text, Exception e);
  }

  /**
   * No op for a retrying callback.
   */
  public static final Retrying NO_OP = (e, retries, idempotent) -> { };

  /**
   * And the no-op for catching things
   */
  public static final Catching CATCH_NO_OP = (t, e) -> {};

  /**
   * Log summary at info, full stack at debug.
   */
  public static final Catching CATCH_LOG = (t, e) -> {
    LOG.info("{}: " + e, t);
    LOG.debug("{}: " + e, t, e);
  };
}
