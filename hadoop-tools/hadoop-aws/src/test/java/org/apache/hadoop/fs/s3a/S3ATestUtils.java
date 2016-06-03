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

package org.apache.hadoop.fs.s3a;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.junit.Assert;
import org.junit.internal.AssumptionViolatedException;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Callable;

import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.fs.s3a.S3ATestConstants.*;
import static org.apache.hadoop.fs.s3a.Constants.*;

/**
 * Utilities for the S3A tests.
 */
public class S3ATestUtils {

  /**
   * Create the test filesystem.
   *
   * If the test.fs.s3a.name property is not set, this will
   * trigger a JUnit failure.
   *
   * Multipart purging is enabled.
   * @param conf configuration
   * @return the FS
   * @throws IOException IO Problems
   * @throws AssumptionViolatedException if the FS is not named
   */
  public static S3AFileSystem createTestFileSystem(Configuration conf)
      throws IOException {
    String fsname = conf.getTrimmed(TEST_FS_S3A_NAME, "");


    boolean liveTest = !StringUtils.isEmpty(fsname);
    URI testURI = null;
    if (liveTest) {
      testURI = URI.create(fsname);
      liveTest = testURI.getScheme().equals(Constants.FS_S3A);
    }
    if (!liveTest) {
      // This doesn't work with our JUnit 3 style test cases, so instead we'll
      // make this whole class not run by default
      throw new AssumptionViolatedException(
          "No test filesystem in " + TEST_FS_S3A_NAME);
    }
    S3AFileSystem fs1 = new S3AFileSystem();
    //enable purging in tests
    conf.setBoolean(PURGE_EXISTING_MULTIPART, true);
    conf.setInt(PURGE_EXISTING_MULTIPART_AGE, 0);
    fs1.initialize(testURI, conf);
    return fs1;
  }

  /**
   * Create a file context for tests.
   *
   * If the test.fs.s3a.name property is not set, this will
   * trigger a JUnit failure.
   *
   * Multipart purging is enabled.
   * @param conf configuration
   * @return the FS
   * @throws IOException IO Problems
   * @throws AssumptionViolatedException if the FS is not named
   */
  public static FileContext createTestFileContext(Configuration conf)
      throws IOException {
    String fsname = conf.getTrimmed(TEST_FS_S3A_NAME, "");

    boolean liveTest = !StringUtils.isEmpty(fsname);
    URI testURI = null;
    if (liveTest) {
      testURI = URI.create(fsname);
      liveTest = testURI.getScheme().equals(Constants.FS_S3A);
    }
    if (!liveTest) {
      // This doesn't work with our JUnit 3 style test cases, so instead we'll
      // make this whole class not run by default
      throw new AssumptionViolatedException("No test filesystem in "
          + TEST_FS_S3A_NAME);
    }
    FileContext fc = FileContext.getFileContext(testURI,conf);
    return fc;
  }

  /**
   * Repeatedly attempt a callback until timeout or a {@link FailFastException}
   * is raised. This is modeled on ScalaTests {@code eventually(Closure)} code.
   * @param timeout timeout
   * @param callback callback to invoke
   * @throws FailFastException any fast-failure
   * @throws Exception the exception which caused the iterator to fail
   */
  public static void eventually(int timeout, Callable<Void> callback)
      throws Exception {
    Exception lastException;
    long endtime = System.currentTimeMillis() + timeout;
    do {
      try {
        callback.call();
        return;
      } catch (FailFastException e) {
        throw e;
      } catch (Exception e) {
        lastException = e;
      }
      Thread.sleep(500);
    } while (endtime > System.currentTimeMillis());
    throw lastException;
  }

  /**
   * The exception to raise so as to exit fast from
   * {@link #eventually(int, Callable)}.
   */
  public static class FailFastException extends Exception {
    public FailFastException() {
    }

    public FailFastException(String message) {
      super(message);
    }

    public FailFastException(String message, Throwable cause) {
      super(message, cause);
    }

    public FailFastException(Throwable cause) {
      super(cause);
    }
  }

  /**
   * Verify the class of an exception. If it is not as expected, rethrow it.
   * Comparison is on the exact class, not subclass-of inference as
   * offered by {@code instanceof}.
   * @param clazz the expected exception class
   * @param ex the exception caught
   * @return the exception, if it is of the expected class
   * @throws Exception the exception passed in.
   */
  public static Exception verifyExceptionClass(Class clazz,
      Exception ex)
      throws Exception {
    if (!(ex.getClass().equals(clazz))) {
      throw ex;
    }
    return ex;
  }

  /**
   * Turn off FS Caching: use if a filesystem with different options from
   * the default is required.
   * @param conf configuration to patch
   */
  public static void disableFilesystemCaching(Configuration conf) {
    conf.setBoolean("fs.s3a.impl.disable.cache", true);
  }

  /**
   * Skip a test if encryption tests are disabled.
   * @param configuration configuration to probe
   */
  public static void skipIfEncryptionTestsDisabled(
      Configuration configuration) {
    if (!configuration.getBoolean(KEY_ENCRYPTION_TESTS, true)) {
      skip("Skipping encryption tests");
    }
  }

  /**
   * Reset all metrics in a list.
   * @param metrics metrics to reset
   */
  public static void reset(S3ATestUtils.MetricDiff... metrics) {
    for (S3ATestUtils.MetricDiff metric : metrics) {
      metric.reset();
    }
  }

  /**
   * Print all metrics in a list.
   * @param log log to print the metrics to.
   * @param metrics metrics to process
   */
  public static void print(Logger log, S3ATestUtils.MetricDiff... metrics) {
    for (S3ATestUtils.MetricDiff metric : metrics) {
      log.info(metric.toString());
    }
  }

  /**
   * Print all metrics in a list, then reset them.
   * @param log log to print the metrics to.
   * @param metrics metrics to process
   */
  public static void printThenReset(Logger log,
      S3ATestUtils.MetricDiff... metrics) {
    print(log, metrics);
    reset(metrics);
  }

  /**
   * Helper class to do diffs of metrics.
   */
  public static final class MetricDiff {
    private final S3AFileSystem fs;
    private final Statistic statistic;
    private long startingValue;

    /**
     * Constructor.
     * Invokes {@link #reset()} so it is immediately capable of measuring the
     * difference in metric values.
     *
     * @param fs the filesystem to monitor
     * @param statistic the statistic to monitor.
     */
    public MetricDiff(S3AFileSystem fs, Statistic statistic) {
      this.fs = fs;
      this.statistic = statistic;
      reset();
    }

    /**
     * Reset the starting value to the current value.
     * Diffs will be against this new value.
     */
    public void reset() {
      startingValue = currentValue();
    }

    /**
     * Get the current value of the metric.
     * @return the latest value.
     */
    public long currentValue() {
      return fs.getInstrumentation().getCounterValue(statistic);
    }

    /**
     * Get the difference between the the current value and
     * {@link #startingValue}.
     * @return the difference.
     */
    public long diff() {
      return currentValue() - startingValue;
    }

    @Override
    public String toString() {
      long c = currentValue();
      final StringBuilder sb = new StringBuilder(statistic.getSymbol());
      sb.append(" starting=").append(startingValue);
      sb.append(" current=").append(c);
      sb.append(" diff=").append(c - startingValue);
      return sb.toString();
    }

    /**
     * Assert that the value of {@link #diff()} matches that expected.
     * @param expected expected value.
     */
    public void assertDiffEquals(long expected) {
      Assert.assertEquals("Count of " + this,
          expected, diff());
    }

    /**
     * Assert that the value of {@link #diff()} matches that of another
     * instance.
     * @param that the other metric diff instance.
     */
    public void assertDiffEquals(MetricDiff that) {
      Assert.assertEquals(this.toString() + " != " + that,
          this.diff(), that.diff());
    }

    /**
     * Comparator for assertions.
     * @param that other metric diff
     * @return true if the value is {@code ==} the other's
     */
    public boolean diffEquals(MetricDiff that) {
      return this.currentValue() == that.currentValue();
    }

    /**
     * Comparator for assertions.
     * @param that other metric diff
     * @return true if the value is {@code <} the other's
     */
    public boolean diffLessThan(MetricDiff that) {
      return this.currentValue() < that.currentValue();
    }

    /**
     * Comparator for assertions.
     * @param that other metric diff
     * @return true if the value is {@code <=} the other's
     */
    public boolean diffLessThanOrEquals(MetricDiff that) {
      return this.currentValue() <= that.currentValue();
    }

    /**
     * Get the statistic
     * @return the statistic
     */
    public Statistic getStatistic() {
      return statistic;
    }

    /**
     * Get the starting value; that set in the last {@link #reset()}.
     * @return the starting value for diffs.
     */
    public long getStartingValue() {
      return startingValue;
    }
  }
}
