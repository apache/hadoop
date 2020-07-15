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

package org.apache.hadoop.fs.s3a.test.costs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.assertj.core.api.Assumptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.s3a.ITestS3AFileOperationCost;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.Statistic;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Support for declarative assertions about operation cost.
 */
public class OperationCostValidator {

  private static final Logger LOG =
      LoggerFactory.getLogger(OperationCostValidator.class);

  /**
   * The empty probe.
   */
  public static final ExpectedProbe EMPTY_PROBE =
      new ProbeList(new ArrayList<>());

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
      ExpectedProbe... expectedA) throws Exception {
    List<ExpectedProbe> expected = Arrays.asList(expectedA);
    resetMetricDiffs();
    // verify that 1+ probe is enabled
    assumeProbesEnabled(expected);
    // if we get here, then yes.
    // evaluate it
    T r = eval.call();
    // build the text for errors
    String text =
        "operation returning "
            + (r != null ? r.toString() : "null");
    LOG.info("{}", text);
    LOG.info("state {}", this);
    LOG.info("probes {}", expected);
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
  private void assumeProbesEnabled(List<ExpectedProbe> expected) {
    boolean enabled = false;
    for (ExpectedProbe ed : expected) {
      enabled |= ed.isEnabled();
    }
    String pstr = expected.stream()
        .map(Object::toString)
        .collect(Collectors.joining(", "));
    Assumptions.assumeThat(enabled)
        .describedAs("metrics to probe for are not enabled in %s", pstr)
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

  /**
   * Create a probe of a statistic which is always enabled.
   * @param statistic statistic to check.
   * @param expected expected value.
   * @return a probe.
   */
  public static ExpectedProbe probe(
      final Statistic statistic,
      final int expected) {
    return probe(true, statistic, expected);
  }

  /**
   * Create a probe of a statistic which is conditionally enabled.
   * @param enabled is the probe enabled?
   * @param statistic statistic to check.
   * @param expected expected value.
   * @return a probe.
   */
  public static ExpectedProbe probe(
      final boolean enabled,
      final Statistic statistic,
      final int expected) {
    return enabled
        ? new ExpectSingleStatistic(statistic, expected)
        : EMPTY_PROBE;
  }

  /**
   * Create an aggregate probe from a vararges list of probes.
   * @param enabled should the probes be enabled?
   * @param plist probe list
   * @return a probe
   */
  public static ExpectedProbe probes(
      final boolean enabled,
      final ExpectedProbe...plist) {
    return enabled
        ? new ProbeList(Arrays.asList(plist))
        : EMPTY_PROBE;
  }

  /**
   * An expected probe to verify given criteria to trigger an eval.
   * <p></p>
   * Probes can be conditional, in which case they are only evaluated
   * when true.
   */
  public interface ExpectedProbe {

    /**
     * Verify a diff if the FS instance is compatible.
     * @param message message to print; metric name is appended
     */
    void verify(OperationCostValidator diffs, String message);

    boolean isEnabled();
  }

  /**
   * Simple probe is a single statistic.
   */
  public static final class ExpectSingleStatistic implements ExpectedProbe {

    private final Statistic statistic;

    private final int expected;

    /**
     * Create.
     * @param statistic statistic
     * @param expected expected value.
     */
    private ExpectSingleStatistic(final Statistic statistic,
        final int expected) {
      this.statistic = statistic;
      this.expected = expected;
    }

    /**
     * Verify a diff if the FS instance is compatible.
     * @param message message to print; metric name is appended
     */
    @Override
    public void verify(OperationCostValidator diffs, String message) {
      diffs.get(statistic).assertDiffEquals(message, expected);
    }

    public Statistic getStatistic() {
      return statistic;
    }

    public int getExpected() {
      return expected;
    }

    @Override
    public boolean isEnabled() {
      return true;
    }

    @Override
    public String toString() {
      String sb = "ExpectSingleStatistic{"
          + statistic
          + ", expected=" + expected
          + ", enabled=" + isEnabled()
          + '}';
      return sb;
    }
  }

  /**
   * A list of probes; the verify operation
   * verifies them all.
   */
  public static class ProbeList implements ExpectedProbe {

    /**
     * Probe list.
     */
    private final List<ExpectedProbe> probes;

    /**
     * Constructor.
     * @param probes probe list.
     */
    public ProbeList(final List<ExpectedProbe> probes) {
      this.probes = probes;
    }

    @Override
    public void verify(final OperationCostValidator diffs,
        final String message) {
      probes.forEach(p -> p.verify(diffs, message));
    }

    /**
     * Enabled if 1+ probe is enabled.
     * @return true if enabled.
     */
    @Override
    public boolean isEnabled() {
      boolean enabled = false;
      for (ExpectedProbe probe : probes) {
        enabled |= probe.isEnabled();
      }
      return enabled;
    }

    @Override
    public String toString() {
      String pstr = probes.stream()
          .map(Object::toString)
          .collect(Collectors.joining(", "));
      return "ProbeList{" + pstr + '}';
    }
  }
}
