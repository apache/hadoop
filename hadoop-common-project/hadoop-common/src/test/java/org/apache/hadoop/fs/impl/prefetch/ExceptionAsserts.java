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

package org.apache.hadoop.fs.impl.prefetch;

import org.apache.hadoop.test.LambdaTestUtils;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public final class ExceptionAsserts {

  private ExceptionAsserts() {
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
      LambdaTestUtils.VoidCallable code) throws Exception {

    intercept(expectedExceptionClass, partialMessage, code);

  }

  public static <E extends Exception> void assertThrows(
      Class<E> expectedExceptionClass,
      LambdaTestUtils.VoidCallable code) throws Exception {

    intercept(expectedExceptionClass, code);

  }
}
