/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.impl.prefetch;

import static org.apache.hadoop.fs.impl.prefetch.Validate.checkGreater;
import static org.apache.hadoop.fs.impl.prefetch.Validate.checkPositiveInteger;

/**
 * Provides retry related functionality.
 */
public class Retryer {

  /* Maximum amount of delay (in ms) before retry fails. */
  private int maxDelay;

  /* Per retry delay (in ms). */
  private int perRetryDelay;

  /**
   * The time interval (in ms) at which status update would be made.
   */
  private int statusUpdateInterval;

  /* Current delay. */
  private int delay;

  /**
   * Initializes a new instance of the {@code Retryer} class.
   *
   * @param perRetryDelay per retry delay (in ms).
   * @param maxDelay maximum amount of delay (in ms) before retry fails.
   * @param statusUpdateInterval time interval (in ms) at which status update would be made.
   *
   * @throws IllegalArgumentException if perRetryDelay is zero or negative.
   * @throws IllegalArgumentException if maxDelay is less than or equal to perRetryDelay.
   * @throws IllegalArgumentException if statusUpdateInterval is zero or negative.
   */
  public Retryer(int perRetryDelay, int maxDelay, int statusUpdateInterval) {
    checkPositiveInteger(perRetryDelay, "perRetryDelay");
    checkGreater(maxDelay, "maxDelay", perRetryDelay, "perRetryDelay");
    checkPositiveInteger(statusUpdateInterval, "statusUpdateInterval");

    this.perRetryDelay = perRetryDelay;
    this.maxDelay = maxDelay;
    this.statusUpdateInterval = statusUpdateInterval;
  }

  /**
   * Returns true if retrying should continue, false otherwise.
   *
   * @return true if the caller should retry, false otherwise.
   */
  public boolean continueRetry() {
    if (this.delay >= this.maxDelay) {
      return false;
    }

    try {
      Thread.sleep(this.perRetryDelay);
    } catch (InterruptedException e) {
      // Ignore the exception as required by the semantic of this class;
    }

    this.delay += this.perRetryDelay;
    return true;
  }

  /**
   * Returns true if status update interval has been reached.
   *
   * @return true if status update interval has been reached.
   */
  public boolean updateStatus() {
    return (this.delay > 0) && this.delay % this.statusUpdateInterval == 0;
  }
}
