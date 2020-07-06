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

package org.apache.hadoop.fs.s3a.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.assertj.core.api.Assumptions;

import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.Statistic;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Support for declarative assertions about operation cost.
 */
public class OperationCostValidator {

  private final Map<String, S3ATestUtils.MetricDiff> metricDiffs
      = new TreeMap<>();

  public OperationCostValidator(Builder builder) {
    builder.metrics.forEach(stat ->
        metricDiffs.put(stat.getSymbol(),
            new S3ATestUtils.MetricDiff(builder.fs, stat))
    );
    builder.metrics.clear();
  }


  /**
   * Reset all the metrics being tracked.
   */
  public void resetMetricDiffs() {
    metricDiffs.values().forEach(S3ATestUtils.MetricDiff::reset);
  }

  /**
   * Get the diff of a statistic.
   * @param stat statistic to look up
   * @return the value
   * @throws NullPointerException if there is no match
   */
  public S3ATestUtils.MetricDiff get(Statistic stat) {
    S3ATestUtils.MetricDiff diff =
        requireNonNull(metricDiffs.get(stat.getSymbol()),
            () -> "No metric tracking for " + stat);
    return diff;
  }


  /**
   * Execute a closure and verify the metrics.
   * <p></p>
   * If no probes are active, the operation will
   * raise an Assumption exception for the test to be skipped.
   * @param eval closure to evaluate
   * @param expected varargs list of expected diffs
   * @param <T> return type.
   * @return the result of the evaluation
   */
  public <T> T verify(
      Callable<T> eval,
      ExpectedProbe... expected) throws Exception {
    resetMetricDiffs();
    // verify that 1+ probe is enabled
    assumeProbesEnabled(expected);
    // if we get here, then yes.
    // evaluate it
    T r = eval.call();
    // build the text for errors
    String text = r != null ? r.toString() : "operation returning null";
    for (ExpectedProbe ed : expected) {
      ed.verify(this, text);
    }
    return r;
  }

  /**
   * Scan all probes for being enabled.
   * <p></p>
   * If none of them are enabled, the evaluation will be skipped.
   * @param expected list of expected probes
   */
  private void assumeProbesEnabled(ExpectedProbe[] expected) {
    boolean enabled = false;
    for (ExpectedProbe ed : expected) {
      enabled |= ed.isEnabled();
    }
    Assumptions.assumeThat(enabled)
        .describedAs("metrics to probe for")
        .isTrue();
  }

  /**
   * Execute a closure, expecting an exception.
   * Verify the metrics after the exception has been caught and
   * validated.
   * @param clazz type of exception
   * @param text text to look for in exception (optional)
   * @param eval closure to evaluate
   * @param expected varargs list of expected diffs
   * @param <T> return type of closure
   * @param <E> exception type
   * @return the exception caught.
   * @throws Exception any other exception
   */
  public <T, E extends Throwable> E verifyIntercepting(
      Class<E> clazz,
      String text,
      Callable<T> eval,
      ExpectedProbe... expected) throws Exception {

    return verify(() ->
            intercept(clazz, text, eval),
        expected);
  }

  @Override
  public String toString() {
    return metricDiffs.values().stream()
        .map(S3ATestUtils.MetricDiff::toString)
        .collect(Collectors.joining(", "));
  }

  /**
   * Create a builder for the cost checker.
   *
   * @param fs filesystem.
   * @return builder.
   */
  public static Builder builder(S3AFileSystem fs) {
    return new Builder(fs);
  }

  /**
   * builder.
   */
  public static final class Builder {

    private final List<Statistic> metrics = new ArrayList<>();

    private final S3AFileSystem fs;


    public Builder(final S3AFileSystem fs) {
      this.fs = requireNonNull(fs);
    }


    public Builder withMetric(Statistic statistic) {
      return withMetric(statistic);
    }


    public Builder withMetrics(Statistic...stats) {
      metrics.addAll(Arrays.asList(stats));
      return this;
    }

    public OperationCostValidator build() {
      return new OperationCostValidator(this);
    }
  }

  public static ExpectedProbe probe(final boolean enabled,
      final Statistic statistic,
      final int expected) {
    return new ExpectedProbe(enabled, statistic, expected);
  }

  public static ExpectedProbe probe(
      final Statistic statistic,
      final int expected) {
    return new ExpectedProbe(statistic, expected);
  }

  /**
   * An expected probe to verify given criteria to trigger an eval.
   * Probes can be conditional, in which case they are only evaluated
   * when true.
   */
  public static final class ExpectedProbe {

    private final Statistic statistic;

    private final int expected;

    private final boolean enabled;

    /**
     * Create.
     * @param enabled criteria to trigger evaluation.
     * @param statistic statistic
     * @param expected expected value.
     */
    private ExpectedProbe(final boolean enabled,
        final Statistic statistic,
        final int expected) {
      this.statistic = statistic;
      this.expected = expected;
      this.enabled = enabled;
    }

    /**
     * Create a diff which is always evaluated
     * @param statistic statistic
     * @param expected expected value.
     */
    private ExpectedProbe(
        final Statistic statistic,
        final int expected) {
      this.statistic = statistic;
      this.expected = expected;
      this.enabled = true;
    }

    /**
     * Verify a diff if the FS instance is compatible.
     * @param message message to print; metric name is appended
     */
    private void verify(OperationCostValidator diffs, String message) {

      if (enabled) {
        S3ATestUtils.MetricDiff md = diffs.get(statistic);

        md.assertDiffEquals(message, expected);
      }
    }

    public Statistic getStatistic() {
      return statistic;
    }

    public int getExpected() {
      return expected;
    }

    public boolean isEnabled() {
      return enabled;
    }
  }

}
