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

package org.apache.hadoop.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;

public class TestGenericTestUtils extends GenericTestUtils {

  @Test
  public void testAssertExceptionContainsNullEx() throws Throwable {
    try {
      assertExceptionContains("", null);
    } catch (AssertionError e) {
      if (!e.toString().contains(E_NULL_THROWABLE)) {
        throw e;
      }
    }
  }

  @Test
  public void testAssertExceptionContainsNullString() throws Throwable {
    try {
      assertExceptionContains("", new BrokenException());
    } catch (AssertionError e) {
      if (!e.toString().contains(E_NULL_THROWABLE_STRING)) {
        throw e;
      }
    }
  }

  @Test
  public void testAssertExceptionContainsWrongText() throws Throwable {
    try {
      assertExceptionContains("Expected", new Exception("(actual)"));
    } catch (AssertionError e) {
      String s = e.toString();
      if (!s.contains(E_UNEXPECTED_EXCEPTION)
          || !s.contains("(actual)") ) {
        throw e;
      }
      if (e.getCause() == null) {
        throw new AssertionError("No nested cause in assertion", e);
      }
    }
  }

  @Test
  public void testAssertExceptionContainsWorking() throws Throwable {
    assertExceptionContains("Expected", new Exception("Expected"));
  }

  private static class BrokenException extends Exception {
    public BrokenException() {
    }

    @Override
    public String toString() {
      return null;
    }
  }

  @Test(timeout = 10000)
  public void testLogCapturer() {
    final Log log = LogFactory.getLog(TestGenericTestUtils.class);
    LogCapturer logCapturer = LogCapturer.captureLogs(log);
    final String infoMessage = "info message";
    // test get output message
    log.info(infoMessage);
    assertTrue(logCapturer.getOutput().endsWith(
        String.format(infoMessage + "%n")));
    // test clear output
    logCapturer.clearOutput();
    assertTrue(logCapturer.getOutput().isEmpty());
    // test stop capturing
    logCapturer.stopCapturing();
    log.info(infoMessage);
    assertTrue(logCapturer.getOutput().isEmpty());
  }

  @Test(timeout = 10000)
  public void testLogCapturerSlf4jLogger() {
    final Logger logger = LoggerFactory.getLogger(TestGenericTestUtils.class);
    LogCapturer logCapturer = LogCapturer.captureLogs(logger);
    final String infoMessage = "info message";
    // test get output message
    logger.info(infoMessage);
    assertTrue(logCapturer.getOutput().endsWith(
        String.format(infoMessage + "%n")));
    // test clear output
    logCapturer.clearOutput();
    assertTrue(logCapturer.getOutput().isEmpty());
    // test stop capturing
    logCapturer.stopCapturing();
    logger.info(infoMessage);
    assertTrue(logCapturer.getOutput().isEmpty());
  }

}
