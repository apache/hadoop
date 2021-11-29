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

package org.apache.hadoop.fs.common;

/**
 * Provides retry related functionality.
 */
public class Retryer {
  // Maximum amount of delay (in ms) before retry fails.
  private int maxDelay;

  // Per retry delay (in mx).
  private int perRetryDelay;

  // The time interval (in ms) at which status update would be made.
  private int statusUpdateInterval;

  // Current delay.
  private int delay;

  public Retryer(int perRetryDelay, int maxDelay, int statusUpdateInterval) {
    Validate.checkPositiveInteger(perRetryDelay, "perRetryDelay");
    Validate.checkGreater(maxDelay, "maxDelay", perRetryDelay, "perRetryDelay");
    Validate.checkPositiveInteger(statusUpdateInterval, "statusUpdateInterval");

    this.perRetryDelay = perRetryDelay;
    this.maxDelay = maxDelay;
    this.statusUpdateInterval = statusUpdateInterval;
  }

  /**
   * Returns true if retrying should continue, false otherwise.
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
   */
  public boolean updateStatus() {
    return (this.delay > 0) && this.delay % this.statusUpdateInterval == 0;
  }
}
