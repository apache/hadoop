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
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AInputStream;
import org.apache.hadoop.fs.s3a.S3AInstrumentation;
import org.apache.hadoop.fs.s3a.S3ATestConstants;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;

/**
 * Base class for scale tests; here is where the common scale configuration
 * keys are defined.
 * <p>
 * Configuration setup is a bit more complex than in the parent classes,
 * as the test timeout is desired prior to the {@link #getTestTimeoutMillis()}
 * being called to set the test timeout rule; this happens before any of
 * the methods tagged with {@code @Before} are invoked.
 * <p>
 * The algorithm is:
 * <ol>
 *   <li>Create a configuration on demand, via
 *   {@link #demandCreateConfiguration()}</li>
 *   <li>Have that return the value of {@link #conf} or create a new one
 *   if that field is null (and set the field to the created value).</li>
 *   <li>Override the superclasses {@link #createConfiguration()}
 *   to return the demand created value; make that method final so that
 *   subclasses don't break things by overridding it.</li>
 *   <li>Add a new override point {@link #createScaleConfiguration()}
 *   to create the config, one which subclasses can (and do) override.</li>
 * </ol>
 * Bear in mind that this process also takes place during initialization
 * of the superclass; the overridden methods are being invoked before
 * their instances are fully configured. This is considered
 * <i>very bad form</i> in Java code (indeed, in C++ it is actually permitted;
 * the base class implementations get invoked instead).
 */
public class S3AScaleTestBase extends AbstractS3ATestBase {

  public static final int _1KB = 1024;
  public static final int _1MB = _1KB * _1KB;

  protected static final Logger LOG =
      LoggerFactory.getLogger(S3AScaleTestBase.class);

  private Configuration conf;

  private boolean enabled;


  private Path testPath;

  /**
   * Get the configuration used to set up the FS.
   * @return the configuration
   */
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    testPath = path("/tests3ascale");
    LOG.debug("Scale test operation count = {}", getOperationCount());
    enabled = getTestPropertyBool(
        getConf(),
        KEY_SCALE_TESTS_ENABLED,
        DEFAULT_SCALE_TESTS_ENABLED);
    assume("Scale test disabled: to enable set property " +
        KEY_SCALE_TESTS_ENABLED,
        isEnabled());
  }

  /**
   * Create the configuration if it is not already set up, calling
   * {@link #createScaleConfiguration()} to do so.
   * @return the configuration.
   */
  private synchronized Configuration demandCreateConfiguration() {
    if (conf == null) {
      conf = createScaleConfiguration();
    }
    return conf;
  }

  /**
   * Returns the config created with {@link #demandCreateConfiguration()}.
   * Subclasses must override {@link #createScaleConfiguration()}
   * in order to customize their configurations.
   * @return a configuration with which to create FS instances
   */
  protected final Configuration createConfiguration() {
    return demandCreateConfiguration();
  }

  /**
   * Override point: create a configuration.
   * @return a configuration with which to create FS instances
   */
  protected Configuration createScaleConfiguration() {
    return super.createConfiguration();
  }

  protected Path getTestPath() {
    return testPath;
  }

  protected long getOperationCount() {
    return getConf().getLong(KEY_OPERATION_COUNT, DEFAULT_OPERATION_COUNT);
  }

  /**
   * Get the test timeout in seconds.
   * @return the test timeout as set in system properties or the default.
   */
  protected int getTestTimeoutSeconds() {
    return getTestPropertyInt(demandCreateConfiguration(),
        KEY_TEST_TIMEOUT,
        SCALE_TEST_TIMEOUT_SECONDS);
  }

  @Override
  protected int getTestTimeoutMillis() {
    return getTestTimeoutSeconds() * 1000;
  }

  /**
   * Get the input stream statistics of an input stream.
   * Raises an exception if the inner stream is not an S3A input stream
   * @param in wrapper
   * @return the statistics for the inner stream
   */
  protected S3AInstrumentation.InputStreamStatistics getInputStreamStatistics(
      FSDataInputStream in) {
    return getS3AInputStream(in).getS3AStreamStatistics();
  }

  /**
   * Get the inner stream of an input stream.
   * Raises an exception if the inner stream is not an S3A input stream
   * @param in wrapper
   * @return the inner stream
   * @throws AssertionError if the inner stream is of the wrong type
   */
  protected S3AInputStream getS3AInputStream(
      FSDataInputStream in) {
    InputStream inner = in.getWrappedStream();
    if (inner instanceof S3AInputStream) {
      return (S3AInputStream) inner;
    } else {
      throw new AssertionError("Not an S3AInputStream: " + inner);
    }
  }

  /**
   * Get the gauge value of a statistic. Raises an assertion if
   * there is no such gauge.
   * @param statistic statistic to look up
   * @return the value.
   */
  public long gaugeValue(Statistic statistic) {
    S3AInstrumentation instrumentation = getFileSystem().getInstrumentation();
    MutableGaugeLong gauge = instrumentation.lookupGauge(statistic.getSymbol());
    assertNotNull("No gauge " + statistic
        + " in " + instrumentation.dump("", " = ", "\n", true), gauge);
    return gauge.value();
  }

  /**
   * Is the test enabled; this is controlled by the configuration
   * and the {@code -Dscale} maven option.
   * @return true if the scale tests are enabled.
   */
  protected final boolean isEnabled() {
    return enabled;
  }

  /**
   * Flag to indicate that this test is being executed in parallel.
   * This is used by some of the scale tests to validate test time expectations.
   * @return true if the build indicates this test is being run in parallel.
   */
  protected boolean isParallelExecution() {
    return Boolean.getBoolean(S3ATestConstants.KEY_PARALLEL_TEST_EXECUTION);
  }
}
