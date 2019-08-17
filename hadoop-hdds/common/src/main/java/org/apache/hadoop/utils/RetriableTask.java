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
package org.apache.hadoop.utils;

import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * {@code Callable} implementation that retries a delegate task according to
 * the specified {@code RetryPolicy}.  Sleeps between retries in the caller
 * thread.
 *
 * @param <V> the result type of method {@code call}
 */
public class RetriableTask<V> implements Callable<V> {

  private static final Logger LOG =
      LoggerFactory.getLogger(RetriableTask.class);

  private final String name;
  private final Callable<V> task;
  private final RetryPolicy retryPolicy;

  public RetriableTask(RetryPolicy retryPolicy, String name, Callable<V> task) {
    this.retryPolicy = retryPolicy;
    this.name = name;
    this.task = task;
  }

  @Override
  public V call() throws Exception {
    int attempts = 0;
    Exception cause;
    while (true) {
      try {
        return task.call();
      } catch (Exception e) {
        cause = e;
        RetryPolicy.RetryAction action = retryPolicy.shouldRetry(e, ++attempts,
             0, true);
        if (action.action == RetryPolicy.RetryAction.RetryDecision.RETRY) {
          LOG.info("Execution of task {} failed, will be retried in {} ms",
              name, action.delayMillis);
          ThreadUtil.sleepAtLeastIgnoreInterrupts(action.delayMillis);
        } else {
          break;
        }
      }
    }

    String msg = String.format(
        "Execution of task %s failed permanently after %d attempts",
        name, attempts);
    LOG.warn(msg, cause);
    throw new IOException(msg, cause);
  }

}
