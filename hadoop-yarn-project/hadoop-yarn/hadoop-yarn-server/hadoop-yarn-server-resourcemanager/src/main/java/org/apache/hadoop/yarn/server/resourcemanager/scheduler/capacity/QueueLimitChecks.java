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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Per-queue user limit and application lifetime checks, shared by queue
 * initialization and configuration validation. Each check returns null when it
 * passes, or the error message the queue initialization fails with.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class QueueLimitChecks {

  private QueueLimitChecks() {
  }

  /**
   * Checks the effective user weights of a leaf queue against its user limit.
   * The exception type of a failure is {@link java.io.IOException}.
   * @param input the user weights of the leaf queue
   * @return null if every weight is valid, the error message otherwise
   */
  public static String checkUserWeights(UserWeightsInput input) {
    float queueUserLimit = Math.min(100.0f, input.getUserLimit());
    for (Map.Entry<String, Float> e : input.getWeights().entrySet()) {
      String userName = e.getKey();
      float weight = e.getValue();
      if (weight < 0.0F || weight > (100.0F / queueUserLimit)) {
        return "Weight (" + weight + ") for user \"" + userName
            + "\" must be between 0 and" + " 100 / " + queueUserLimit + " (= " +
            100.0f / queueUserLimit + ", the number of concurrent active users in "
            + input.getQueuePath() + ")";
      }
    }
    return null;
  }

  /**
   * Checks that the default application lifetime of a queue does not exceed
   * its maximum application lifetime. The exception type of a failure is
   * {@link org.apache.hadoop.yarn.exceptions.YarnRuntimeException}.
   * @param input the lifetimes of the queue
   * @return null if the lifetimes are consistent, the error message otherwise
   */
  public static String checkDefaultAppLifetime(AppLifetimeInput input) {
    long maxLifetime = input.getMaximumLifetime();
    long defaultLifetime = input.getDefaultLifetime();
    if (maxLifetime > 0 && defaultLifetime > maxLifetime) {
      return "Default lifetime " + defaultLifetime
          + " can't exceed maximum lifetime " + maxLifetime;
    }
    return null;
  }

  /**
   * Input of {@link #checkUserWeights(UserWeightsInput)}.
   */
  public static final class UserWeightsInput {
    private final String queuePath;
    private final float userLimit;
    private final Map<String, Float> weights;

    /**
     * @param queuePath full path of the leaf queue, used in the message
     * @param userLimit configured user limit of the leaf queue; the check caps
     *                  it at 100 like the leaf queue does
     * @param weights effective user weights (own weights override the
     *                inherited ones); the iteration order decides which
     *                invalid user is reported
     */
    public UserWeightsInput(String queuePath, float userLimit,
        Map<String, Float> weights) {
      this.queuePath = queuePath;
      this.userLimit = userLimit;
      this.weights = Collections.unmodifiableMap(new LinkedHashMap<>(weights));
    }

    public String getQueuePath() {
      return queuePath;
    }

    public float getUserLimit() {
      return userLimit;
    }

    public Map<String, Float> getWeights() {
      return weights;
    }
  }

  /**
   * Input of {@link #checkDefaultAppLifetime(AppLifetimeInput)}.
   */
  public static final class AppLifetimeInput {
    private final long maximumLifetime;
    private final long defaultLifetime;

    /**
     * @param maximumLifetime effective maximum lifetime of the queue (own value
     *                        when set, otherwise the parent's); zero or a
     *                        negative value means unlimited
     * @param defaultLifetime default lifetime before it falls back to the
     *                        maximum lifetime: the value configured on the
     *                        queue when set, otherwise the inherited default
     *                        capped at the maximum, which never fails
     */
    public AppLifetimeInput(long maximumLifetime, long defaultLifetime) {
      this.maximumLifetime = maximumLifetime;
      this.defaultLifetime = defaultLifetime;
    }

    public long getMaximumLifetime() {
      return maximumLifetime;
    }

    public long getDefaultLifetime() {
      return defaultLifetime;
    }
  }
}
