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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ContainerRetryContext;
import org.apache.hadoop.yarn.api.records.ContainerRetryPolicy;
import org.apache.hadoop.yarn.util.Clock;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * <p>Sliding window retry policy for relaunching a
 * <code>Container</code> in Yarn.</p>
 */
@InterfaceStability.Unstable
public class SlidingWindowRetryPolicy {

  private Clock clock;

  public SlidingWindowRetryPolicy(Clock clock)  {
    this.clock = Preconditions.checkNotNull(clock);
  }

  public boolean shouldRetry(RetryContext retryContext,
      int errorCode) {
    ContainerRetryContext containerRC = retryContext
        .containerRetryContext;
    Preconditions.checkNotNull(containerRC, "container retry context null");
    ContainerRetryPolicy retryPolicy = containerRC.getRetryPolicy();
    if (retryPolicy == ContainerRetryPolicy.RETRY_ON_ALL_ERRORS
        || (retryPolicy == ContainerRetryPolicy.RETRY_ON_SPECIFIC_ERROR_CODES
        && containerRC.getErrorCodes() != null
        && containerRC.getErrorCodes().contains(errorCode))) {
      if (containerRC.getMaxRetries() == ContainerRetryContext.RETRY_FOREVER) {
        return true;
      }
      int pendingRetries = calculatePendingRetries(retryContext);
      updateRetryContext(retryContext, pendingRetries);
      return pendingRetries > 0;
    }
    return false;
  }

  /**
   * Calculates the pending number of retries.
   * <p>
   * When failuresValidityInterval is > 0, it also removes time entries from
   * <code>restartTimes</code> which are outside the validity interval.
   *
   * @return the pending retries.
   */
  private int calculatePendingRetries(RetryContext retryContext) {
    ContainerRetryContext containerRC =
        retryContext.containerRetryContext;
    if (containerRC.getFailuresValidityInterval() > 0) {
      Iterator<Long> iterator = retryContext.getRestartTimes().iterator();
      long currentTime = clock.getTime();
      while (iterator.hasNext()) {
        long restartTime = iterator.next();
        if (currentTime - restartTime
            > containerRC.getFailuresValidityInterval()) {
          iterator.remove();
        } else {
          break;
        }
      }
      return containerRC.getMaxRetries() -
          retryContext.getRestartTimes().size();
    } else {
      return retryContext.getRemainingRetries();
    }
  }

  /**
   * Updates remaining retries and the restart time when
   * required in the retryContext.
   */
  private void updateRetryContext(RetryContext retryContext,
      int pendingRetries) {
    retryContext.setRemainingRetries(pendingRetries - 1);
    if (retryContext.containerRetryContext.getFailuresValidityInterval()
        > 0) {
      retryContext.getRestartTimes().add(clock.getTime());
    }
  }

  /**
   * Sets the clock.
   * @param clock clock
   */
  public void setClock(Clock clock) {
    this.clock = Preconditions.checkNotNull(clock);
  }

  /**
   * Sliding window container retry context.
   * <p>
   * Besides {@link ContainerRetryContext}, it also provide details such as:
   * <ul>
   * <li>
   * <em>remainingRetries</em>: specifies the number of pending retries. It is
   * initially set to <code>containerRetryContext.maxRetries</code>.
   * </li>
   * <li>
   * <em>restartTimes</em>: when
   * <code>containerRetryContext.failuresValidityInterval</code> is set,
   * then this records the times when the container is set to restart.
   * </li>
   * </ul>
   */
  static class RetryContext {

    private final ContainerRetryContext containerRetryContext;
    private List<Long> restartTimes = new ArrayList<>();
    private int remainingRetries;

    RetryContext(ContainerRetryContext containerRetryContext) {
      this.containerRetryContext = Preconditions
          .checkNotNull(containerRetryContext);
      this.remainingRetries = containerRetryContext.getMaxRetries();
    }

    ContainerRetryContext getContainerRetryContext() {
      return containerRetryContext;
    }

    int getRemainingRetries() {
      return remainingRetries;
    }

    void setRemainingRetries(int remainingRetries) {
      this.remainingRetries = remainingRetries;
    }

    List<Long> getRestartTimes() {
      return restartTimes;
    }

    void setRestartTimes(List<Long> restartTimes) {
      if (restartTimes != null) {
        this.restartTimes.clear();
        this.restartTimes.addAll(restartTimes);
      }
    }
  }
}
