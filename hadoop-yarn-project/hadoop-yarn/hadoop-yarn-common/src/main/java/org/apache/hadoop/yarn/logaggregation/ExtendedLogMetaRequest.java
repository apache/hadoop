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

package org.apache.hadoop.yarn.logaggregation;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

/**
 * Represents a query of log metadata with extended filtering capabilities.
 */
public class ExtendedLogMetaRequest {
  private final String user;
  private final String appId;
  private final String containerId;
  private final MatchExpression nodeId;
  private final MatchExpression fileName;
  private final ComparisonCollection fileSize;
  private final ComparisonCollection modificationTime;

  public ExtendedLogMetaRequest(
      String user, String appId, String containerId, MatchExpression nodeId,
      MatchExpression fileName, ComparisonCollection fileSize,
      ComparisonCollection modificationTime) {
    this.user = user;
    this.appId = appId;
    this.containerId = containerId;
    this.nodeId = nodeId;
    this.fileName = fileName;
    this.fileSize = fileSize;
    this.modificationTime = modificationTime;
  }

  public String getUser() {
    return user;
  }

  public String getAppId() {
    return appId;
  }

  public String getContainerId() {
    return containerId;
  }

  public MatchExpression getNodeId() {
    return nodeId;
  }

  public MatchExpression getFileName() {
    return fileName;
  }

  public ComparisonCollection getFileSize() {
    return fileSize;
  }

  public ComparisonCollection getModificationTime() {
    return modificationTime;
  }

  public static class ExtendedLogMetaRequestBuilder {
    private String user;
    private String appId;
    private String containerId;
    private MatchExpression nodeId = new MatchExpression(null);
    private MatchExpression fileName = new MatchExpression(null);
    private ComparisonCollection fileSize = new ComparisonCollection(null);
    private ComparisonCollection modificationTime =
        new ComparisonCollection(null);

    public ExtendedLogMetaRequestBuilder setUser(String userName) {
      this.user = userName;
      return this;
    }

    public ExtendedLogMetaRequestBuilder setAppId(String applicationId) {
      this.appId = applicationId;
      return this;
    }

    public ExtendedLogMetaRequestBuilder setContainerId(String container) {
      this.containerId = container;
      return this;
    }

    public ExtendedLogMetaRequestBuilder setNodeId(String node) {
      try {
        this.nodeId = new MatchExpression(node);
      } catch (PatternSyntaxException e) {
        throw new IllegalArgumentException("Node Id expression is invalid", e);
      }
      return this;
    }

    public ExtendedLogMetaRequestBuilder setFileName(String file) {
      try {
        this.fileName = new MatchExpression(file);
      } catch (PatternSyntaxException e) {
        throw new IllegalArgumentException("Filename expression is invalid", e);
      }
      return this;
    }

    public ExtendedLogMetaRequestBuilder setFileSize(Set<String> fileSizes) {
      this.fileSize = new ComparisonCollection(fileSizes);
      return this;
    }

    public ExtendedLogMetaRequestBuilder setModificationTime(
        Set<String> modificationTimes) {
      this.modificationTime = new ComparisonCollection(modificationTimes);
      return this;
    }

    public boolean isUserSet() {
      return user != null;
    }

    public ExtendedLogMetaRequest build() {
      return new ExtendedLogMetaRequest(user, appId, containerId, nodeId,
          fileName, fileSize, modificationTime);
    }
  }

  /**
   * A collection of {@code ComparisonExpression}.
   */
  public static class ComparisonCollection {
    private List<ComparisonExpression> comparisonExpressions;

    public ComparisonCollection(Set<String> expressions) {
      if (expressions == null) {
        this.comparisonExpressions = Collections.emptyList();
      } else {
        List<String> equalExpressions = expressions.stream().filter(
            e -> !e.startsWith(ComparisonExpression.GREATER_OPERATOR) &&
                !e.startsWith(ComparisonExpression.LESSER_OPERATOR))
            .collect(Collectors.toList());
        if (equalExpressions.size() > 1) {
          throw new IllegalArgumentException(
              "Can not process more, than one exact match. Matches: "
                  + String.join(" ", equalExpressions));
        }

        this.comparisonExpressions = expressions.stream()
            .map(ComparisonExpression::new).collect(Collectors.toList());

      }

    }

    public boolean match(Long value) {
      return match(value, true);
    }

    public boolean match(String value) {
      if (value == null) {
        return true;
      }

      return match(Long.valueOf(value), true);
    }

    /**
     * Checks, if the given value matches all the {@code ComparisonExpression}.
     * This implies an AND logic between the expressions.
     * @param value given value to match against
     * @param defaultValue default value to return when no expression is defined
     * @return whether all expressions were matched
     */
    public boolean match(Long value, boolean defaultValue) {
      if (comparisonExpressions.isEmpty()) {
        return defaultValue;
      }

      return comparisonExpressions.stream()
          .allMatch(expr -> expr.match(value));
    }

  }

  /**
   * Wraps a comparison logic based on a stringified expression.
   * The format of the expression is:
   * &gt;value = is greater than value
   * &lt;value = is lower than value
   * value = is equal to value
   */
  public static class ComparisonExpression {
    public static final String GREATER_OPERATOR = ">";
    public static final String LESSER_OPERATOR = "<";

    private String expression;
    private Predicate<Long> comparisonFn;
    private Long convertedValue;

    public ComparisonExpression(String expression) {
      if (expression == null) {
        return;
      }

      if (expression.startsWith(GREATER_OPERATOR)) {
        convertedValue = Long.parseLong(expression.substring(1));
        comparisonFn = a -> a > convertedValue;
      } else if (expression.startsWith(LESSER_OPERATOR)) {
        convertedValue = Long.parseLong(expression.substring(1));
        comparisonFn = a -> a < convertedValue;
      } else {
        convertedValue = Long.parseLong(expression);
        comparisonFn = a -> a.equals(convertedValue);
      }

      this.expression = expression;
    }

    public boolean match(String value) {
      return match(Long.valueOf(value), true);
    }

    public boolean match(Long value) {
      return match(value, true);
    }

    /**
     * Test the given value with the defined comparison functions based on
     * stringified expression.
     * @param value value to test with
     * @param defaultValue value to return when no expression was defined
     * @return comparison test result or the given default value
     */
    public boolean match(Long value, boolean defaultValue) {
      if (expression == null) {
        return defaultValue;
      } else {
        return comparisonFn.test(value);
      }
    }

    @Override
    public String toString() {
      return convertedValue != null ? String.valueOf(convertedValue) : "";
    }
  }

  /**
   * Wraps a regex matcher.
   */
  public static class MatchExpression {
    private Pattern expression;

    public MatchExpression(String expression) {
      this.expression = expression != null ? Pattern.compile(expression) : null;
    }

    /**
     * Matches the value on the expression.
     * @param value value to be matched against
     * @return result of the match or true, if no expression was defined
     */
    public boolean match(String value) {
      return expression == null || expression.matcher(value).matches();
    }

    @Override
    public String toString() {
      return expression != null ? expression.pattern() : "";
    }
  }

}
