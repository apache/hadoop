/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.common;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public final class ExceptionAsserts {
  private ExceptionAsserts() {}

  @FunctionalInterface
  public interface CodeThatMayThrow {
    void run() throws Exception;
  }

  /**
   * Asserts that the given code throws an exception of the given type
   * and that the exception message contains the given sub-message.
   *
   * Usage:
   *
   * ExceptionAsserts.assertThrows(
   *   IllegalArgumentException.class,
   *   "'nullArg' must not be null",
   *   () -> Preconditions.checkNotNull(null, "nullArg"));
   *
   * Note: JUnit 5 has similar functionality but it will be a long time before
   * we move to that framework because of significant differences and lack of
   * backward compatibility for some JUnit rules.
   */
  public static <E extends Exception> void assertThrows(
      Class<E> expectedExceptionClass,
      String partialMessage,
      CodeThatMayThrow code) {

    Exception thrownException = null;

    try {
      code.run();
    } catch (Exception e) {
      if (expectedExceptionClass.isAssignableFrom(e.getClass())) {

        thrownException = e;

        if (partialMessage != null) {
          String msg = e.getMessage();
          assertNotNull(
              String.format("Exception message is null, expected to contain: '%s'", partialMessage),
              msg);
          assertTrue(
              String.format("Exception message '%s' does not contain: '%s'", msg, partialMessage),
              msg.contains(partialMessage));
        }
      } else {
        fail(String.format(
                 "Expected exception of type %s but got %s",
                 expectedExceptionClass.getName(),
                 e.getClass().getName()));
      }
    }

    if (thrownException == null) {
      fail(String.format(
               "Expected exception of type %s but got none",
               expectedExceptionClass.getName()));
    }
  }

  public static <E extends Exception> void assertThrows(
      Class<E> expectedExceptionClass,
      CodeThatMayThrow code) {
    assertThrows(expectedExceptionClass, null, code);
  }
}
