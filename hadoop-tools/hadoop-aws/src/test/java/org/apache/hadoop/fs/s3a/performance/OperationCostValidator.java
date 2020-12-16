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

package org.apache.hadoop.fs.s3a.performance;

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

import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AInstrumentation;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.metrics2.lib.MutableCounter;
import org.apache.hadoop.metrics2.lib.MutableMetric;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_LIST_REQUEST;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_METADATA_REQUESTS;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Support for declarative assertions about operation cost.
 * <p></p>
 * Usage: A builder is used to declare the set of statistics
 * to be monitored in the filesystem.
 * <p></p>
 * A call to {@link #exec(Callable, ExpectedProbe...)}
 * executes the callable if 1+ probe is enabled; after
 * invocation the probes are validated.
 * The result of the callable is returned.
 * <p></p>
 * A call of {@link #intercepting(Class, String, Callable, ExpectedProbe...)}
 * Invokes the callable if 1+ probe is enabled, expects an exception
 * to be raised and then verifies metrics declared in the probes.
 * <p></p>
 * Probes are built up from the static method to create probes
 * for metrics:
 * <ul>
 *   <li>{@link #probe(boolean, Statistic, int)} </li>
 *   <li>{@link #probe(Statistic, int)} </li>
 *   <li>{@link #probes(boolean, ExpectedProbe...)} (Statistic, int)} </li>
 *   <li>{@link #always()}</li>
 * </ul>
 * If any probe evaluates to false, an assertion is raised.
 * <p></p>
 * When this happens: look in the logs!
 * The logs will contain the whole set of metrics, the probe details
 * and the result of the call.
 */
public final class OperationCostValidator {

  private static final Logger LOG =
      LoggerFactory.getLogger(OperationCostValidator.class);

  /**
   * The empty probe: declared as disabled.
   */
  private static final ExpectedProbe EMPTY_PROBE =
      new EmptyProbe("empty", false);

  /**
   * A probe which is always enabled.
   */
  private static final ExpectedProbe ALWAYS_PROBE =
      new EmptyProbe("always", true);

  /**
   * The map of metric diffs to track.
   */
  private final Map<String, S3ATestUtils.MetricDiff> metricDiffs
      = new TreeMap<>();

  /**
   * Instrumentation's IO Statistics.
   */
  private final IOStatisticsStore ioStatistics;

  /**
   * Build the instance.
   * @param builder builder containing all options.
   */
  private OperationCostValidator(Builder builder) {
    S3AFileSystem fs = builder.filesystem;
    S3AInstrumentation instrumentation = fs.getInstrumentation();
    for (Statistic stat : builder.metrics) {
      String symbol = stat.getSymbol();
      MutableMetric metric = instrumentation.lookupMetric(symbol);
      if (metric instanceof MutableCounter) {
        // only counters are used in the cost tracking;
        // other statistics are ignored.
        metricDiffs.put(symbol,
            new S3ATestUtils.MetricDiff(fs, stat));
      }
    }
    builder.metrics.clear();
    ioStatistics = instrumentation.getIOStatistics();
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
  public <T> T exec(
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
    LOG.info("state {}", this.toString());
    LOG.info("probes {}", expected);
    LOG.info("IOStatistics {}", ioStatistics);
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
  public <T, E extends Throwable> E intercepting(
      Class<E> clazz,
      String text,
      Callable<T> eval,
      ExpectedProbe... expected) throws Exception {

    return exec(() ->
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

    /**
     * Filesystem.
     */
    private final S3AFileSystem filesystem;

    /**
     * Metrics to create.
     */
    private final List<Statistic> metrics = new ArrayList<>();


    /**
     * Create with a required filesystem.
     * @param filesystem monitored filesystem
     */
    public Builder(final S3AFileSystem filesystem) {
      this.filesystem = requireNonNull(filesystem);
    }


    /**
     * Add a single metric.
     * @param statistic statistic to monitor.
     * @return this
     */
    public Builder withMetric(Statistic statistic) {
      metrics.add(statistic);
      return this;
    }

    /**
     * Add a varargs list of metrics.
     * @param stat statistics to monitor.
     * @return this.
     */
    public Builder withMetrics(Statistic...stats) {
      metrics.addAll(Arrays.asList(stats));
      return this;
    }

    /**
     * Instantiate.
     * @return the validator.
     */
    public OperationCostValidator build() {
      return new OperationCostValidator(this);
    }
  }

  /**
   * Get the "always" probe.
   * @return a probe which always triggers execution.
   */
  public static ExpectedProbe always() {
    return ALWAYS_PROBE;
  }

  /**
   * Create a probe of a statistic which is enabled whenever the expected
   * value is greater than zero.
   * @param statistic statistic to check.
   * @param expected expected value.
   * @return a probe.
   */
  public static ExpectedProbe probe(
      final Statistic statistic,
      final int expected) {
    return probe(expected >= 0, statistic, expected);
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
   * Expect the exact head and list requests of the operation
   * cost supplied.
   * @param enabled is the probe enabled?
   * @param cost expected cost.
   * @return a probe.
   */
  public static ExpectedProbe expect(
      boolean enabled, OperationCost cost) {
    return probes(enabled,
        probe(OBJECT_METADATA_REQUESTS, cost.head()),
        probe(OBJECT_LIST_REQUEST, cost.list()));
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

  /**
   * The empty probe always runs; it can be used to force
   * a verification to execute.
   */
  private static final class EmptyProbe implements ExpectedProbe {

    private final String name;

    private final boolean enabled;

    private EmptyProbe(final String name, boolean enabled) {
      this.name = name;
      this.enabled = enabled;
    }

    @Override
    public void verify(final OperationCostValidator diffs,
        final String message) {
    }

    @Override
    public boolean isEnabled() {
      return enabled;
    }

    @Override
    public String toString() {
      return name;
    }
  }
}
