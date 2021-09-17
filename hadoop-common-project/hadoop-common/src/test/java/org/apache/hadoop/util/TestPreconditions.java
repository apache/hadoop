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

package org.apache.hadoop.util;

import org.junit.Test;

import org.apache.hadoop.test.LambdaTestUtils;

public class TestPreconditions {
  private static final String NON_NULL_STRING = "NON_NULL_OBJECT";
  private static final String NON_INT_STRING = "NOT_INT";
  private static final String EXPECTED_ERROR_MSG = "Expected-Error-MSG";
  private static final String EXPECTED_ERROR_MSG_ARGS =
      EXPECTED_ERROR_MSG + " %s number %d";
  private static final String NULL_FORMATTER = null;

  private String errorMessage;

  @Test
  public void testCheckNotNullSuccess() {
    Preconditions.checkNotNull(NON_NULL_STRING);
    // null supplier
    Preconditions.checkNotNull(NON_NULL_STRING, null);
    // ill-formated string supplier
    Preconditions.checkNotNull(NON_NULL_STRING, ()-> String.format("%d",
        NON_INT_STRING));
    // null pattern to string formatter
    Preconditions.checkNotNull(NON_NULL_STRING, NULL_FORMATTER, null, 1);
    // null arguments to string formatter
    Preconditions.checkNotNull(NON_NULL_STRING, EXPECTED_ERROR_MSG_ARGS,
        null, null);
    // illegal format exception
    Preconditions.checkNotNull(NON_NULL_STRING, "message %d %d",
        NON_INT_STRING, 1);
    // insufficient arguments
    Preconditions.checkNotNull(NON_NULL_STRING, EXPECTED_ERROR_MSG_ARGS,
        NON_INT_STRING);
    // null format in string supplier
    Preconditions.checkNotNull(NON_NULL_STRING,
        () -> String.format(NULL_FORMATTER, NON_INT_STRING));
  }

  @Test
  public void testCheckNotNullFailure() throws Exception {
    // failure without message
    LambdaTestUtils.intercept(NullPointerException.class,
        Preconditions.getDefaultNullMSG(),
        () -> Preconditions.checkNotNull(null));

    // failure with message
    errorMessage = EXPECTED_ERROR_MSG;
    LambdaTestUtils.intercept(NullPointerException.class,
        errorMessage,
        () -> Preconditions.checkNotNull(null, errorMessage));

    // failure with Null message
    LambdaTestUtils.intercept(NullPointerException.class,
        null,
        () -> Preconditions.checkNotNull(null, errorMessage));

    // failure with message format
    errorMessage = EXPECTED_ERROR_MSG + " %s";
    String arg = "NPE";
    String expectedMSG = String.format(errorMessage, arg);
    LambdaTestUtils.intercept(NullPointerException.class,
        expectedMSG,
        () -> Preconditions.checkNotNull(null, errorMessage, arg));

    // failure with multiple arg message format
    errorMessage = EXPECTED_ERROR_MSG_ARGS;
    expectedMSG =
        String.format(errorMessage, arg, 1);
    LambdaTestUtils.intercept(NullPointerException.class,
        expectedMSG,
        () -> Preconditions.checkNotNull(null, errorMessage, arg, 1));

    // illegal format will be thrown if the case is not handled correctly
    LambdaTestUtils.intercept(NullPointerException.class,
        Preconditions.getDefaultNullMSG(),
        () -> Preconditions.checkNotNull(null,
            errorMessage, 1, NON_INT_STRING));

    // illegal format will be thrown for insufficient Insufficient Args
    LambdaTestUtils.intercept(NullPointerException.class,
        Preconditions.getDefaultNullMSG(),
        () -> Preconditions.checkNotNull(null, errorMessage, NON_NULL_STRING));

    // illegal format in supplier
    LambdaTestUtils.intercept(NullPointerException.class,
        Preconditions.getDefaultNullMSG(),
        () -> Preconditions.checkNotNull(null,
            () -> String.format(errorMessage, 1, NON_INT_STRING)));

    // insufficient arguments in string Supplier
    LambdaTestUtils.intercept(NullPointerException.class,
        Preconditions.getDefaultNullMSG(),
        () -> Preconditions.checkNotNull(null,
            () -> String.format(errorMessage, NON_NULL_STRING)));

    // null formatter
    LambdaTestUtils.intercept(NullPointerException.class,
        Preconditions.getDefaultNullMSG(),
        () -> Preconditions.checkNotNull(null,
            () -> String.format(NULL_FORMATTER, NON_NULL_STRING)));
  }
}
