/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.s3a.scale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AInputStream;
import org.apache.hadoop.fs.s3a.S3AInstrumentation;
import org.apache.hadoop.fs.s3a.S3ATestConstants;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;

/**
 * Base class for scale tests; here is where the common scale configuration
 * keys are defined.
 */
public class S3AScaleTestBase extends Assert implements S3ATestConstants {

  @Rule
  public final TestName methodName = new TestName();

  @Rule
  public Timeout testTimeout = createTestTimeout();

  @Before
  public void nameThread() {
    Thread.currentThread().setName("JUnit");
  }

  public static final int _1KB = 1024;
  public static final int _1MB = _1KB * _1KB;

  protected S3AFileSystem fs;

  protected static final Logger LOG =
      LoggerFactory.getLogger(S3AScaleTestBase.class);

  private Configuration conf;

  private boolean enabled;

  /**
   * Configuration generator. May be overridden to inject
   * some custom options.
   * @return a configuration with which to create FS instances
   */
  protected Configuration createConfiguration() {
    return new Configuration();
  }

  /**
   * Get the configuration used to set up the FS.
   * @return the configuration
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Setup. This triggers creation of the configuration.
   */
  @Before
  public void setUp() throws Exception {
    demandCreateConfiguration();
    LOG.debug("Scale test operation count = {}", getOperationCount());
    // multipart purges are disabled on the scale tests
    fs = createTestFileSystem(conf, false);
    // check for the test being enabled
    enabled = getTestPropertyBool(
        getConf(),
        KEY_SCALE_TESTS_ENABLED,
        DEFAULT_SCALE_TESTS_ENABLED);
    Assume.assumeTrue("Scale test disabled: to enable set property " +
        KEY_SCALE_TESTS_ENABLED, enabled);
  }

  /**
   * Create the configuration if it is not already set up.
   * @return the configuration.
   */
  private synchronized Configuration demandCreateConfiguration() {
    if (conf == null) {
      conf = createConfiguration();
    }
    return conf;
  }

  @After
  public void tearDown() throws Exception {
    ContractTestUtils.rm(fs, getTestPath(), true, true);
  }

  protected Path getTestPath() {
    String testUniqueForkId = System.getProperty("test.unique.fork.id");
    return testUniqueForkId == null ? new Path("/tests3a") :
        new Path("/" + testUniqueForkId, "tests3a");
  }

  protected long getOperationCount() {
    return getConf().getLong(KEY_OPERATION_COUNT, DEFAULT_OPERATION_COUNT);
  }

  /**
   * Create the timeout for tests. Some large tests may need a larger value.
   * @return the test timeout to use
   */
  protected Timeout createTestTimeout() {
    demandCreateConfiguration();
    return new Timeout(
        getTestTimeoutSeconds() * 1000);
  }

  /**
   * Get the test timeout in seconds.
   * @return the test timeout as set in system properties or the default.
   */
  protected static int getTestTimeoutSeconds() {
    return getTestPropertyInt(null,
        KEY_TEST_TIMEOUT,
        DEFAULT_TEST_TIMEOUT);
  }

  /**
   * Describe a test in the logs.
   * @param text text to print
   * @param args arguments to format in the printing
   */
  protected void describe(String text, Object... args) {
    LOG.info("\n\n{}: {}\n",
        methodName.getMethodName(),
        String.format(text, args));
  }

  /**
   * Get the input stream statistics of an input stream.
   * Raises an exception if the inner stream is not an S3A input stream
   * @param in wrapper
   * @return the statistics for the inner stream
   */
  protected S3AInstrumentation.InputStreamStatistics getInputStreamStatistics(
      FSDataInputStream in) {
    InputStream inner = in.getWrappedStream();
    if (inner instanceof S3AInputStream) {
      S3AInputStream s3a = (S3AInputStream) inner;
      return s3a.getS3AStreamStatistics();
    } else {
      Assert.fail("Not an S3AInputStream: " + inner);
      // never reached
      return null;
    }
  }

  /**
   * Get the gauge value of a statistic. Raises an assertion if
   * there is no such gauge.
   * @param statistic statistic to look up
   * @return the value.
   */
  public long gaugeValue(Statistic statistic) {
    S3AInstrumentation instrumentation = fs.getInstrumentation();
    MutableGaugeLong gauge = instrumentation.lookupGauge(statistic.getSymbol());
    assertNotNull("No gauge " + statistic
        + " in " + instrumentation.dump("", " = ", "\n", true), gauge);
    return gauge.value();
  }

  protected boolean isEnabled() {
    return enabled;
  }

  /**
   * Flag to indicate that this test is being used sequentially. This
   * is used by some of the scale tests to validate test time expectations.
   * @return true if the build indicates this test is being run in parallel.
   */
  protected boolean isParallelExecution() {
    return Boolean.getBoolean(S3ATestConstants.KEY_PARALLEL_TEST_EXECUTION);
  }
}
