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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperty;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.Resolved;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolvedQueueConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ValueSource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue.Severity;

/**
 * Helpers shared by the rules.
 */
final class RuleSupport {

  private RuleSupport() {
  }

  /**
   * @param error whether the check fails the configuration
   * @return ERROR or WARNING
   */
  static Severity severity(boolean error) {
    return error ? Severity.ERROR : Severity.WARNING;
  }

  /**
   * @param e a failure
   * @return the exception message, the trunk message of the failure
   */
  static String message(Throwable e) {
    return e.getMessage() != null ? e.getMessage() : e.toString();
  }

  /**
   * @param value a resolved value
   * @return true if the value is present and was resolved
   */
  static boolean ok(Resolved<?> value) {
    return value != null && !value.isFailed();
  }

  /**
   * Reports a resolved value that failed to parse.
   * @param issues the issue list
   * @param queue the queue
   * @param property the property
   * @param label the node label
   * @param ruleId the rule id of the issue
   * @param severity the severity of the issue
   * @return true if the value failed and an issue was added
   */
  static boolean reportFailure(List<ValidationIssue> issues,
      ResolvedQueueConfig queue, QueueProperty<?> property, String label,
      String ruleId, Severity severity) {
    Resolved<?> value = queue.get(property, label);
    if (value == null || !value.isFailed()) {
      return false;
    }
    if (isInheritedFailure(queue, property, label, value)) {
      // Reported for the ancestor the failure comes from
      return true;
    }
    try {
      value.getValue();
      return false;
    } catch (RuntimeException e) {
      issues.add(new ValidationIssue(queue.getQueuePath().getFullPath(),
          keyOf(queue, property, label, value), ruleId, severity, message(e),
          e));
    }
    return true;
  }

  /**
   * Whether a failed value is the failure of an ancestor, which inheriting
   * properties pass on unchanged.
   */
  static boolean isInheritedFailure(ResolvedQueueConfig queue,
      QueueProperty<?> property, String label, Resolved<?> value) {
    String detail = value.getSourceDetail();
    String queuePrefix = CapacitySchedulerConfiguration.PREFIX
        + queue.getQueuePath().getFullPath() + ".";
    if (value.getSource() == ValueSource.TEMPLATE_V1
        || value.getSource() == ValueSource.TEMPLATE_V2) {
      return false;
    }
    return detail != null && detail.startsWith(
        CapacitySchedulerConfiguration.PREFIX + "root")
        && !detail.startsWith(queuePrefix)
        && !detail.equals(property.getKey(queue.getQueuePath(), label));
  }

  /**
   * The key a resolved value came from: the queue key, a template key or a
   * scheduler-wide key.
   */
  static String keyOf(ResolvedQueueConfig queue, QueueProperty<?> property,
      String label, Resolved<?> value) {
    String detail = value == null ? null : value.getSourceDetail();
    if (detail != null
        && detail.startsWith(CapacitySchedulerConfiguration.PREFIX)) {
      return detail;
    }
    return property.getKey(queue.getQueuePath(), label);
  }

  /**
   * @param queue a queue
   * @param property a property
   * @param label a node label
   * @return the full key of the property for the queue
   */
  static String key(ResolvedQueueConfig queue, QueueProperty<?> property,
      String label) {
    return property.getKey(queue.getQueuePath(), label);
  }

  /**
   * @param queue a queue
   * @param suffix a key suffix
   * @return the full key of the suffix under the queue prefix
   */
  static String key(ResolvedQueueConfig queue, String suffix) {
    return CapacitySchedulerConfiguration.PREFIX
        + queue.getQueuePath().getFullPath() + "." + suffix;
  }
}
