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

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.s3a.DefaultS3ClientFactory.SDK_LOG_LEVEL;
import static org.apache.hadoop.fs.s3a.DefaultS3ClientFactory.SDK_LOG_NAME;
import static org.apache.hadoop.fs.s3a.DefaultS3ClientFactory.maybeTurnOnSdkLogging;
import static org.apache.hadoop.fs.s3a.impl.logging.LogControllerFactory.LOG4J1CONTROLLER;
import static org.apache.hadoop.fs.s3a.impl.logging.LogControllerFactory.enableLogging;

/**
 * Test the log controller by verifying that the log can be controlled.
 */
public class TestLogController extends AbstractHadoopTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestLogController.class);

  private static final String NAME = TestLogController.class.getName();

  /**
   * Controller.
   */
  private LogControl controller;
  
  /**
   * Special internal log which is used to drive whether or not
   * internal logs are enabled.
   */
  private static final Logger LOG_SDK =
      LoggerFactory.getLogger(SDK_LOG_NAME);

  /**
   * The Shaded HTTP client log.
   */
  private static final String HTTP_CLIENT_LOG_NAME =
      "software.amazon.awssdk.thirdparty.org.apache.http";

  private static final Logger HTTP_CLIENT_LOG =
      LoggerFactory.getLogger(HTTP_CLIENT_LOG_NAME);

  @Before
  public void setup() throws Exception {
    controller =
          LogControllerFactory.createController(LOG4J1CONTROLLER).get();
  }
  
  
  @Test
  public void testLogLevel() throws Throwable {
    
    // log at info
    setLogLevel(NAME, LogControl.LogLevel.INFO);

    // switch to debug
    Assertions.assertThat(enableLogging(LogControl.LogLevel.DEBUG, NAME))
        .describedAs("Log level of %s", NAME)
        .isTrue();
    
    // check
    assertLogLevel(NAME, LogControl.LogLevel.DEBUG);
    
  }

  /**
   * Set a log level; assert that it is set.
   * @param log log to set
   * @param level level to set
   */
  private void setLogLevel(final String log, final LogControl.LogLevel level) {
    controller.setLogLevel(log, level);
    assertLogLevel(log, level);
  }


  /**
   * Get the log level of a log.
   * @param name log name
   * @return the log level
   */
  private LogControl.LogLevel getLogLevel(final String name) {
    return controller.getLogLevel(name);
  }

  /**
   * Assert a log is at a given level.
   * @param name log name
   * @param expected expected value
   */
  private void assertLogLevel(final String name, final LogControl.LogLevel expected) {
    Assertions.assertThat(getLogLevel(name))
        .describedAs("Log level of %s", name)
        .isEqualTo(expected);
  }

  /**
   * Test the log enabling code in DefaultS3ClientFactory.
   */
  @Test
  public void testMaybeTurnOnSdkLogging() {
    // save the original log level
    final LogControl.LogLevel clientOriginal = getLogLevel(HTTP_CLIENT_LOG_NAME);
    // sdk log is forced to info
    final LogControl.LogLevel sdkOriginal = getLogLevel(SDK_LOG_NAME);
    try {
      setLogLevel(HTTP_CLIENT_LOG_NAME, LogControl.LogLevel.INFO);
      setLogLevel(SDK_LOG_NAME, LogControl.LogLevel.DEBUG);

      // turn on logging
      Assertions.assertThat(maybeTurnOnSdkLogging())
          .describedAs("maybeTurnOnSdkLogging()")
          .isTrue();
      // and the inner log is now at debug
      assertLogLevel(HTTP_CLIENT_LOG_NAME, SDK_LOG_LEVEL);
      HTTP_CLIENT_LOG.debug("HTTP client at debug");
      HTTP_CLIENT_LOG.trace("HTTP client at trace");
    } finally {
      // put things back
      setLogLevel(HTTP_CLIENT_LOG_NAME, clientOriginal);
      setLogLevel(SDK_LOG_NAME, sdkOriginal);
    }
  }
}
