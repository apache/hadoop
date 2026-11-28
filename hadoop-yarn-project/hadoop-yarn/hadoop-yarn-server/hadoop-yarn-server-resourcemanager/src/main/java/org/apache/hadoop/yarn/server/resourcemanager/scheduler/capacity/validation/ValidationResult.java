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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Immutable outcome of a validation run: the issues found and, when it was
 * requested, the resolved values of a set of queues (explain).
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ValidationResult {

  private static final ValidationResult VALID =
      new ValidationResult(Collections.<ValidationIssue>emptyList());

  private final List<ValidationIssue> issues;
  private final Map<String, List<ExplainEntry>> explain;

  /**
   * @param issues issues in report order
   */
  public ValidationResult(List<ValidationIssue> issues) {
    this(issues, null);
  }

  /**
   * @param issues issues in report order
   * @param explain queue path to resolved entries in report order, or null
   *                when explain was not requested
   */
  public ValidationResult(List<ValidationIssue> issues,
      Map<String, List<ExplainEntry>> explain) {
    this.issues = Collections.unmodifiableList(new ArrayList<>(issues));
    if (explain == null) {
      this.explain = null;
    } else {
      Map<String, List<ExplainEntry>> copy = new LinkedHashMap<>();
      for (Map.Entry<String, List<ExplainEntry>> e : explain.entrySet()) {
        copy.put(e.getKey(),
            Collections.unmodifiableList(new ArrayList<>(e.getValue())));
      }
      this.explain = Collections.unmodifiableMap(copy);
    }
  }

  /** @return a valid result without issues and without explain */
  public static ValidationResult valid() {
    return VALID;
  }

  /**
   * @param explainEntries queue path to resolved entries, not null
   * @return a copy of this result carrying the given explain map
   */
  public ValidationResult withExplain(
      Map<String, List<ExplainEntry>> explainEntries) {
    return new ValidationResult(issues,
        Objects.requireNonNull(explainEntries, "explainEntries"));
  }

  public List<ValidationIssue> getIssues() {
    return issues;
  }

  /** @return true when no issue has severity ERROR */
  public boolean isValid() {
    for (ValidationIssue issue : issues) {
      if (issue.isError()) {
        return false;
      }
    }
    return true;
  }

  /** @return true when this result carries an explain map */
  public boolean hasExplain() {
    return explain != null;
  }

  /**
   * @return queue path to resolved entries in report order; empty when
   *         explain was not requested
   */
  public Map<String, List<ExplainEntry>> getExplain() {
    return explain == null
        ? Collections.<String, List<ExplainEntry>>emptyMap() : explain;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ValidationResult)) {
      return false;
    }
    ValidationResult that = (ValidationResult) o;
    return issues.equals(that.issues) && Objects.equals(explain, that.explain);
  }

  @Override
  public int hashCode() {
    return Objects.hash(issues, explain);
  }

  @Override
  public String toString() {
    return "ValidationResult{valid=" + isValid() + ", issues=" + issues
        + (explain == null ? "" : ", explain=" + explain) + "}";
  }

  /**
   * One resolved property of a queue: the full key, the value, the
   * {@code ValueSource} name it was resolved from and the key or parent path
   * it came from.
   */
  public static final class ExplainEntry {
    private final String key;
    private final String value;
    private final String source;
    private final String sourceDetail;

    public ExplainEntry(String key, String value, String source,
        String sourceDetail) {
      this.key = Objects.requireNonNull(key, "key");
      this.value = value;
      this.source = source;
      this.sourceDetail = sourceDetail;
    }

    public String getKey() {
      return key;
    }

    public String getValue() {
      return value;
    }

    public String getSource() {
      return source;
    }

    public String getSourceDetail() {
      return sourceDetail;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ExplainEntry)) {
        return false;
      }
      ExplainEntry that = (ExplainEntry) o;
      return key.equals(that.key) && Objects.equals(value, that.value)
          && Objects.equals(source, that.source)
          && Objects.equals(sourceDetail, that.sourceDetail);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, value, source, sourceDetail);
    }

    @Override
    public String toString() {
      return key + "=" + value + " (" + source
          + (sourceDetail == null ? "" : " " + sourceDetail) + ")";
    }
  }
}
