/**
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


package org.apache.hadoop.tools.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.util.ThreadUtil;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * This class represents commands that be retried on failure, in a configurable
 * manner.
 */
public abstract class RetriableCommand {

  private static Log LOG = LogFactory.getLog(RetriableCommand.class);

  private static final long DELAY_MILLISECONDS = 500;
  private static final int  MAX_RETRIES        = 3;

  private RetryPolicy retryPolicy = RetryPolicies.
      exponentialBackoffRetry(MAX_RETRIES, DELAY_MILLISECONDS, TimeUnit.MILLISECONDS);
  protected String description;

  /**
   * Constructor.
   * @param description The human-readable description of the command.
   */
  public RetriableCommand(String description) {
    this.description = description;
  }

  /**
   * Constructor.
   * @param description The human-readable description of the command.
   * @param retryPolicy The RetryHandler to be used to compute retries.
   */
  public RetriableCommand(String description, RetryPolicy retryPolicy) {
    this(description);
    setRetryPolicy(retryPolicy);
  }

  /**
   * Implement this interface-method define the command-logic that will be
   * retried on failure (i.e. with Exception).
   * @param arguments Argument-list to the command.
   * @return Generic "Object".
   * @throws Exception Throws Exception on complete failure.
   */
  protected abstract Object doExecute(Object... arguments) throws Exception;

  /**
   * The execute() method invokes doExecute() until either:
   *  1. doExecute() succeeds, or
   *  2. the command may no longer be retried (e.g. runs out of retry-attempts).
   * @param arguments The list of arguments for the command.
   * @return Generic "Object" from doExecute(), on success.
   * @throws Exception
   */
  public Object execute(Object... arguments) throws Exception {
    Exception latestException;
    int counter = 0;
    while (true) {
      try {
        return doExecute(arguments);
      } catch(Exception exception) {
        LOG.error("Failure in Retriable command: " + description, exception);
        latestException = exception;
      }
      counter++;
      RetryAction action = retryPolicy.shouldRetry(latestException, counter, 0, true);
      if (action.action == RetryPolicy.RetryAction.RetryDecision.RETRY) {
        ThreadUtil.sleepAtLeastIgnoreInterrupts(action.delayMillis);
      } else {
        break;
      }
    }

    throw new IOException("Couldn't run retriable-command: " + description,
                          latestException);
  }

  /**
   * Fluent-interface to change the RetryHandler.
   * @param retryHandler The new RetryHandler instance to be used.
   * @return Self.
   */
  public RetriableCommand setRetryPolicy(RetryPolicy retryHandler) {
    this.retryPolicy = retryHandler;
    return this;
  }
}
