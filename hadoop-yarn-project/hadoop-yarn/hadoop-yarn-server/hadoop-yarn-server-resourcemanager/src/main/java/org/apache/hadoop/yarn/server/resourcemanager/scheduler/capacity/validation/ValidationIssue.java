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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation;

import java.util.Objects;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * One finding of Capacity Scheduler configuration validation.
 * <p>
 * Severity contract: {@link Severity#ERROR} only where the current scheduler
 * rejects the configuration at the same phase, {@link Severity#WARNING}
 * otherwise.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ValidationIssue {

  /** Issue severity. */
  public enum Severity {
    ERROR,
    WARNING
  }

  private final String queuePath;
  private final String propertyKey;
  private final String ruleId;
  private final Severity severity;
  private final String message;
  // The failure a rule caught, rethrown by the legacy validation; it is not
  // part of the issue's value or of the REST response
  private final transient RuntimeException failure;

  /**
   * @param queuePath full path of the queue the issue belongs to, or null
   *                  for scheduler-wide issues
   * @param propertyKey full configuration key the issue belongs to, or null
   * @param ruleId stable kebab-case rule id
   * @param severity issue severity
   * @param message human readable message
   */
  public ValidationIssue(String queuePath, String propertyKey, String ruleId,
      Severity severity, String message) {
    this(queuePath, propertyKey, ruleId, severity, message, null);
  }

  /**
   * @param queuePath full path of the queue the issue belongs to, or null
   *                  for scheduler-wide issues
   * @param propertyKey full configuration key the issue belongs to, or null
   * @param ruleId stable kebab-case rule id
   * @param severity issue severity
   * @param message human readable message
   * @param failure the exception the checked value or check failed with,
   *                or null
   */
  public ValidationIssue(String queuePath, String propertyKey, String ruleId,
      Severity severity, String message, RuntimeException failure) {
    this.queuePath = queuePath;
    this.propertyKey = propertyKey;
    this.ruleId = Objects.requireNonNull(ruleId, "ruleId");
    this.severity = Objects.requireNonNull(severity, "severity");
    this.message = Objects.requireNonNull(message, "message");
    this.failure = failure;
  }

  public static ValidationIssue error(String queuePath, String propertyKey,
      String ruleId, String message) {
    return new ValidationIssue(queuePath, propertyKey, ruleId,
        Severity.ERROR, message);
  }

  /**
   * An error for a value or check that failed with an exception.
   * @param queuePath full queue path, or null
   * @param propertyKey full configuration key, or null
   * @param ruleId stable kebab-case rule id
   * @param failure the exception, whose message is the issue message
   * @return the issue
   */
  public static ValidationIssue error(String queuePath, String propertyKey,
      String ruleId, RuntimeException failure) {
    return new ValidationIssue(queuePath, propertyKey, ruleId,
        Severity.ERROR, failure.getMessage() != null ? failure.getMessage()
            : failure.toString(), failure);
  }

  public static ValidationIssue warning(String queuePath, String propertyKey,
      String ruleId, String message) {
    return new ValidationIssue(queuePath, propertyKey, ruleId,
        Severity.WARNING, message);
  }

  /** @return full queue path, or null for scheduler-wide issues */
  public String getQueuePath() {
    return queuePath;
  }

  /** @return full configuration key, or null */
  public String getPropertyKey() {
    return propertyKey;
  }

  public String getRuleId() {
    return ruleId;
  }

  public Severity getSeverity() {
    return severity;
  }

  public String getMessage() {
    return message;
  }

  /**
   * @return the exception the checked value or check failed with, or null;
   *         not part of the value of the issue
   */
  public RuntimeException getFailure() {
    return failure;
  }

  public boolean isError() {
    return severity == Severity.ERROR;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ValidationIssue)) {
      return false;
    }
    ValidationIssue that = (ValidationIssue) o;
    return Objects.equals(queuePath, that.queuePath)
        && Objects.equals(propertyKey, that.propertyKey)
        && ruleId.equals(that.ruleId)
        && severity == that.severity
        && message.equals(that.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(queuePath, propertyKey, ruleId, severity, message);
  }

  @Override
  public String toString() {
    return severity + " " + ruleId
        + (queuePath == null ? "" : " queue=" + queuePath)
        + (propertyKey == null ? "" : " key=" + propertyKey)
        + ": " + message;
  }
}
