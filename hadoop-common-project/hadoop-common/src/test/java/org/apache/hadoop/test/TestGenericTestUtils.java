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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.slf4j.event.Level;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;


public class TestGenericTestUtils extends GenericTestUtils {

  @Test
  public void testAssertExceptionContainsNullEx() throws Throwable {
    intercept(AssertionError.class, E_NULL_THROWABLE, () -> assertExceptionContains("", null));
  }

  @Test
  public void testAssertExceptionContainsNullString() throws Throwable {
    intercept(AssertionError.class, E_NULL_THROWABLE_STRING, () -> assertExceptionContains("", new BrokenException()));
  }

  @Test
  public void testAssertExceptionContainsWrongText() throws Throwable {
    AssertionError e = intercept(AssertionError.class, E_UNEXPECTED_EXCEPTION,
        () -> assertExceptionContains("Expected", new Exception("(actual)")));
    if (!e.toString().contains("(actual)")) {
      throw new AssertionError("no actual string in exception", e);
    }
    if (e.getCause() == null) {
      throw new AssertionError("No nested cause in assertion", e);
    }
  }

  @Test
  public void testAssertExceptionContainsWorking() throws Throwable {
    assertExceptionContains("Expected", new Exception("Expected"));
  }

  @Test
  public void testAssertExceptionMatchesNullEx() throws Throwable {
    intercept(AssertionError.class, E_NULL_THROWABLE, () -> assertExceptionMatches(null, null));
  }

  @Test
  public void testAssertExceptionMatchesNullString() throws Throwable {
    intercept(AssertionError.class, E_NULL_THROWABLE_STRING, () -> assertExceptionMatches(null, new BrokenException()));
  }

  @Test
  public void testAssertExceptionMatchesWrongText() throws Throwable {
    AssertionError e = intercept(AssertionError.class, E_UNEXPECTED_EXCEPTION,
        () -> assertExceptionMatches(Pattern.compile(".*Expected.*"), new Exception("(actual)")));
    if (!e.toString().contains("(actual)")) {
      throw new AssertionError("no actual string in exception", e);
    }
    if (e.getCause() == null) {
      throw new AssertionError("No nested cause in assertion", e);
    }
  }

  @Test
  public void testAssertExceptionMatchesWorking() throws Throwable {
    assertExceptionMatches(Pattern.compile(".*Expected.*"), new Exception("Expected"));
  }

  private static class BrokenException extends Exception {
    public BrokenException() {
    }

    @Override
    public String toString() {
      return null;
    }
  }

  @Test
  @Timeout(value = 10)
  public void testLogCapturer() {
    final Logger log = LoggerFactory.getLogger(TestGenericTestUtils.class);
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

  @Test
  @Timeout(value = 10)
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

  @Test
  public void testWaitingForConditionWithInvalidParams() throws Throwable {
    // test waitFor method with null supplier interface
    try {
      waitFor(null, 0, 0);
    } catch (NullPointerException e) {
      assertExceptionContains(GenericTestUtils.ERROR_MISSING_ARGUMENT, e);
    }

    Supplier<Boolean> simpleSupplier = new Supplier<Boolean>() {

      @Override
      public Boolean get() {
        return true;
      }
    };

    // test waitFor method with waitForMillis greater than checkEveryMillis
    waitFor(simpleSupplier, 5, 10);
    try {
      // test waitFor method with waitForMillis smaller than checkEveryMillis
      waitFor(simpleSupplier, 10, 5);
      fail(
          "Excepted a failure when the param value of"
          + " waitForMillis is smaller than checkEveryMillis.");
    } catch (IllegalArgumentException e) {
      assertExceptionContains(GenericTestUtils.ERROR_INVALID_ARGUMENT, e);
    }
  }

  @Test
  public void testToLevel() throws Throwable {
    assertEquals(Level.INFO, toLevel("INFO"));
    assertEquals(Level.DEBUG, toLevel("NonExistLevel"));
    assertEquals(Level.INFO, toLevel("INFO", Level.TRACE));
    assertEquals(Level.TRACE, toLevel("NonExistLevel", Level.TRACE));
  }
}
