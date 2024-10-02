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

package org.apache.hadoop.fs.s3a.impl.logging;

import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.test.AbstractHadoopTestBase;
import org.apache.hadoop.test.GenericTestUtils;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.s3a.impl.logging.LogControllerFactory.createController;
import static org.apache.hadoop.fs.s3a.impl.logging.LogControllerFactory.createLog4JController;
import static org.apache.hadoop.test.GenericTestUtils.LogCapturer.captureLogs;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Unit test for log controller factory.
 */
public class TestLogControllerFactory extends AbstractHadoopTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestLogControllerFactory.class);

  /**
   * Classname of this class.
   */
  public static final String CLASSNAME =
      "org.apache.hadoop.fs.s3a.impl.logging.TestLogControllerFactory";

  public static final String DEBUG = "Debug Message";

  public static final String INFO = "Info Message";

  public static final String WARN = "Warn Message";

  public static final String ERROR = "Error Message";

  public static final String FATAL = "Fatal Message";

  /**
   * Log4J controller for this test case's log.
   */
  private LogControl controller;

  /**
   * Log capturer; stops capturing in teardown.
   */
  private GenericTestUtils.LogCapturer capturer;

  /**
   * Setup: create the contract then init it.
   */
  @Before
  public void setup() {
    controller = requireNonNull(createLog4JController());
    capturer = captureLogs(LOG);

  }

  /**
   * Teardown.
   */
  @After
  public void teardown() {
    if (capturer != null) {
      capturer.stopCapturing();
    }
  }

  /**
   * A class that is of the wrong type downgrades to null.
   */
  @Test
  public void testInstantationWrongClass() throws Throwable {
    Assertions.assertThat(createController(CLASSNAME))
        .describedAs("controller of wrong type")
        .isNull();
  }


  /**
   * A class that is of the wrong type downgrades to null.
   */
  @Test
  public void testInstantationNoClass() throws Throwable {
    Assertions.assertThat(createController("not.a.class"))
        .describedAs("missing class")
        .isNull();
  }

  /**
   * If the controller's implementation of
   * {@link LogControl#setLevel(String, LogControl.LogLevel)} raises an exception,
   * this is caught and downgraded to a "false" return code.
   */
  @Test
  public void testExceptionsDowngraded() throws Throwable {
    final LogControl failing = createController(LevelFailingLogController.class.getName());

    // inner method raises an exception
    intercept(NoSuchMethodException.class, "Simulated", () ->
        failing.setLevel(CLASSNAME, LogControl.LogLevel.DEBUG));

    // outer one doesn't
    Assertions.assertThat(failing.setLogLevel(CLASSNAME, LogControl.LogLevel.DEBUG))
        .describedAs("Invocation of setLogLevel()")
        .isFalse();
  }


  @Test
  public void testLogAllLevels() throws Throwable {
    assertLogsAtLevel(LogControl.LogLevel.ALL,
        DEBUG, INFO, WARN, ERROR);
  }

  @Test
  public void testLogAtInfo() throws Throwable {
    assertLogsAtLevel(LogControl.LogLevel.INFO,
        INFO, WARN, ERROR)
        .doesNotContain(DEBUG);
  }

  @Test
  public void testLogAtWarn() throws Throwable {
    assertLogsAtLevel(LogControl.LogLevel.WARN, WARN, ERROR)
        .doesNotContain(DEBUG, INFO);
  }

  @Test
  public void testLogAtError() throws Throwable {
    assertLogsAtLevel(LogControl.LogLevel.ERROR, ERROR)
        .doesNotContain(DEBUG, INFO, WARN);
  }

  @Test
  public void testLogAtNone() throws Throwable {
    assertLogsAtLevel(LogControl.LogLevel.OFF, "")
        .doesNotContain(DEBUG, INFO, WARN, ERROR);
  }

  /**
   * generate output at a given logging level, print messages at different levels
   * then assert what the final values are.
   * @param level log level
   * @param contains expected contained strings
   * @return the ongoing assertion.
   */
  private AbstractStringAssert<?> assertLogsAtLevel(
      final LogControl.LogLevel level, CharSequence... contains) {
    capturer.clearOutput();
    setLogLevel(level);
    logMessages();
    return Assertions.assertThat(capturer.getOutput())
        .describedAs("captured output")
        .contains(contains);
  }

  /**
   * Set the local log level.
   * @param level level to set to.
   */
  private void setLogLevel(final LogControl.LogLevel level) {
    Assertions.assertThat(controller.setLogLevel(CLASSNAME, level))
        .describedAs("Set log level %s", level)
        .isTrue();
  }

  /**
   * Log at all levels from debug to fatal.
   */
  private static void logMessages() {
    LOG.debug(DEBUG);
    LOG.info(INFO);
    LOG.warn(WARN);
    LOG.error(ERROR);
    LOG.error(FATAL);
  }

  @VisibleForTesting
  static class LevelFailingLogController extends LogControl {

    @Override
    protected boolean setLevel(final String log, final LogLevel level)
        throws Exception {
      throw new NoSuchMethodException("Simulated");
    }
  }


}
