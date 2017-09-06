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

import com.amazonaws.AmazonClientException;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.retry.RetryPolicy;

/**
 * Class to provide lambda expression access to AWS operations, including
 * any retry policy.
 */
public class S3ALambda {
  private static final Logger LOG =
      LoggerFactory.getLogger(S3ALambda.class);

  private final RetryPolicy retryPolicy;

  /**
   * Instantiate.
   * @param retryPolicy retry policy for all operations.
   */
  public S3ALambda(RetryPolicy retryPolicy) {
    this.retryPolicy = retryPolicy;
  }

  /**
   * Execute a function.
   * @param action action to execute (used in error messages)
   * @param path path of work (used in error messages)
   * @param operation operation to execute
   * @param <T> type of return value
   * @return the result of the function call
   * @throws IOException any IOE raised, or translated exception
   */
  public <T> T execute(String action, String path, Operation<T> operation)
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
  public void execute(String action, String path, VoidOperation operation)
      throws IOException {
    execute(action, path,
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
        () -> {
          operation.execute();
          return null;
        },
        retrying);
 }

 /**
   * Execute a void operation with retry processing.
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
    retry(action, path, idempotent, operation, NO_OP);
 }

  /**
   * Execute a function with retry processing.
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

    return retry(action, path, idempotent, operation, NO_OP);
  }

    /**
     * Execute a function with retry processing.
     * @param action action to execute (used in error messages)
     * @param path path of work (used in error messages)
     * @param idempotent does the operation have semantics
     * which mean that it can be retried even if was already executed?
     * @param operation operation to execute
     * @param retrying callback on retries
     * @param <T> type of return value
     * @return the result of the call
     * @throws IOException any IOE raised, or translated exception
     */
  public <T> T retry(String action,
      String path,
      boolean idempotent,
      Operation<T> operation,
      Retrying retrying)
      throws IOException {

    Preconditions.checkArgument(retrying != null, "null retrying argument");
    int retryCount = 0;
    IOException caught;
    RetryPolicy.RetryAction retryAction;
    boolean shouldRetry;
    do {
      try {
        // execute the operation, returning if successful
        return execute(action, path, operation);
      } catch (IOException e) {
        caught = e;
      }
      int attempts = retryCount + 1;
      // log summary string at warn
      LOG.warn("Attempt {} of {}{}: {}", action,
          attempts,
          (StringUtils.isNoneEmpty(path) ? (" on " + path) : ""),
          caught.toString());
      // full stack at debug
      LOG.debug("Attempt {} of action={}; path={}",
          attempts, action, path, caught);
      // here the exception must have been caught
      try {
        retryAction = retryPolicy.shouldRetry(caught, retryCount, 0,
                idempotent);
        shouldRetry = retryAction.action.equals(
            RetryPolicy.RetryAction.RETRY.action);
        if (shouldRetry) {
          LOG.debug("Retrying {} after exception {}", operation,
              caught.toString());
          retrying.onRetry(caught, retryCount, idempotent);
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
        LOG.warn("Exception in retry processing", e);
        // and fail the execution with the last execution exception.
        shouldRetry = false;
      }
    } while (shouldRetry);

    // if the code gets here, then all retries have been exhausted
    // or something
    throw caught;
  }

  /**
   * Execute an operation; any Exception raised is simply caught and
   * logged.
   * @param action action to execute
   * @param path path (for exception construction)
   * @param operation operation
   */
  public void quietly(String action,
      String path,
      VoidOperation operation) {
    try {
      execute(action, path, operation);
    } catch (Exception e) {
      LOG.debug("Action {} failed", action, e);
    }
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
   * No op for a retrying callback.
   */
  public static final Retrying NO_OP = (e, retries, idempotent) -> { };
}
