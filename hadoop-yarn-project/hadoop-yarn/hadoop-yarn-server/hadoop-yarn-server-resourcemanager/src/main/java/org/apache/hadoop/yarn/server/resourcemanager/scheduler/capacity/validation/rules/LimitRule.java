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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueLimitChecks;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperty.Kind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.Resolved;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolvedQueueConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ValueSource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue.Severity;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationRule;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.DEFAULT_APPLICATION_LIFETIME;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.MAXIMUM_AM_RESOURCE_PERCENT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.MAXIMUM_APPLICATION_LIFETIME;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.USER_LIMIT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.USER_LIMIT_FACTOR;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.USER_WEIGHTS;

/**
 * User limit and application lifetime checks ({@link QueueLimitChecks}):
 * user weights parse and fit the user limit of a leaf, and an explicit
 * default application lifetime does not exceed the maximum. The ranges
 * that trunk does not check are reported as warnings.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class LimitRule implements ValidationRule {
  /** U02. */
  public static final String INVALID_USER_WEIGHT = "invalid-user-weight";
  /** U01. */
  public static final String USER_WEIGHT_OUT_OF_RANGE =
      "user-weight-out-of-range";
  /** U03. */
  public static final String DEFAULT_LIFETIME_EXCEEDS_MAXIMUM =
      "default-lifetime-exceeds-maximum";
  /** A user limit, user limit factor or AM resource share out of range. */
  public static final String VALUE_OUT_OF_RANGE = "value-out-of-range";

  @Override
  public String getId() {
    return "queue-limits";
  }

  @Override
  public void check(ValidationContext context, List<ValidationIssue> issues) {
    for (ResolvedQueueConfig queue : context.getTree().getQueues()) {
      String path = queue.getQueuePath().getFullPath();
      if (RuleSupport.reportFailure(issues, queue, USER_WEIGHTS, "",
          INVALID_USER_WEIGHT, Severity.ERROR)) {
        continue;
      }

      Resolved<Long> maximum = queue.get(MAXIMUM_APPLICATION_LIFETIME);
      Resolved<Long> defaultLifetime = queue.get(DEFAULT_APPLICATION_LIFETIME);
      if (!queue.getQueuePath().isRoot() && RuleSupport.ok(maximum)
          && RuleSupport.ok(defaultLifetime)
          && isConfigured(defaultLifetime)) {
        String error = QueueLimitChecks.checkDefaultAppLifetime(
            maximum.getValue(), defaultLifetime.getValue());
        if (error != null) {
          issues.add(ValidationIssue.error(path, RuleSupport.keyOf(queue,
              DEFAULT_APPLICATION_LIFETIME, "", defaultLifetime),
              DEFAULT_LIFETIME_EXCEEDS_MAXIMUM, error));
        }
      }

      if (queue.getKind() != Kind.LEAF && queue.getKind() != Kind.RESERVATION) {
        continue;
      }
      Float userLimit = userLimitForWeights(context, queue, issues);
      Resolved<Map<String, Float>> weights = queue.get(USER_WEIGHTS);
      if (userLimit != null && RuleSupport.ok(weights)) {
        String error = QueueLimitChecks.checkUserWeights(
            path, userLimit, weights.getValue());
        if (error != null) {
          issues.add(ValidationIssue.error(path,
              RuleSupport.key(queue, USER_WEIGHTS, ""),
              USER_WEIGHT_OUT_OF_RANGE, error));
        }
      }
      if (!queue.isDynamic()) {
        warnRanges(queue, issues);
      }
    }
  }

  /**
   * The user limit the user weights of a leaf are checked against. A
   * ReservationQueue is set up with the user limit of its own path, which the
   * limits of its plan replace only after the check; its resolved value is
   * the plan's.
   * @return the user limit, or null if it cannot be read
   */
  private static Float userLimitForWeights(ValidationContext context,
      ResolvedQueueConfig queue, List<ValidationIssue> issues) {
    if (queue.getKind() == Kind.RESERVATION) {
      try {
        return context.getConfiguration().getUserLimit(queue.getQueuePath());
      } catch (RuntimeException e) {
        issues.add(ValidationIssue.error(queue.getQueuePath().getFullPath(),
            RuleSupport.key(queue, USER_LIMIT, ""), ValueRule.INVALID_VALUE,
            e));
        return null;
      }
    }
    Resolved<Float> userLimit = queue.get(USER_LIMIT);
    return RuleSupport.ok(userLimit) ? userLimit.getValue() : null;
  }

  private static boolean isConfigured(Resolved<?> value) {
    return value.getSource() == ValueSource.QUEUE
        || value.getSource() == ValueSource.TEMPLATE_V1
        || value.getSource() == ValueSource.TEMPLATE_V2;
  }

  /** Ranges that trunk accepts without a check, warnings. */
  private static void warnRanges(ResolvedQueueConfig queue,
      List<ValidationIssue> issues) {
    String path = queue.getQueuePath().getFullPath();
    Resolved<Float> userLimit = queue.get(USER_LIMIT);
    if (RuleSupport.ok(userLimit) && isConfigured(userLimit)
        && (userLimit.getValue() < 0 || userLimit.getValue() > 100)) {
      issues.add(ValidationIssue.warning(path,
          RuleSupport.keyOf(queue, USER_LIMIT, "", userLimit),
          VALUE_OUT_OF_RANGE, "User limit " + userLimit.getValue()
              + " of queue " + path + " is outside [0, 100]."));
    }
    Resolved<Float> factor = queue.get(USER_LIMIT_FACTOR);
    if (RuleSupport.ok(factor) && isConfigured(factor)
        && factor.getValue() <= 0 && factor.getValue() != -1f) {
      issues.add(ValidationIssue.warning(path,
          RuleSupport.keyOf(queue, USER_LIMIT_FACTOR, "", factor),
          VALUE_OUT_OF_RANGE, "User limit factor " + factor.getValue()
              + " of queue " + path + " is neither positive nor -1."));
    }
    Resolved<Float> amShare = queue.get(MAXIMUM_AM_RESOURCE_PERCENT);
    if (RuleSupport.ok(amShare) && isConfigured(amShare)
        && (amShare.getValue() < 0 || amShare.getValue() > 1)) {
      issues.add(ValidationIssue.warning(path,
          RuleSupport.keyOf(queue, MAXIMUM_AM_RESOURCE_PERCENT, "", amShare),
          VALUE_OUT_OF_RANGE, "Maximum AM resource percent "
              + amShare.getValue() + " of queue " + path
              + " is outside [0, 1]."));
    }
  }
}
