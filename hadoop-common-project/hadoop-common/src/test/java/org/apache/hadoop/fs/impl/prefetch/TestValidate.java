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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import org.junit.Test;

import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.impl.prefetch.SampleDataForTests.EMPTY_BYTE_ARRAY;
import static org.apache.hadoop.fs.impl.prefetch.SampleDataForTests.EMPTY_INT_ARRAY;
import static org.apache.hadoop.fs.impl.prefetch.SampleDataForTests.EMPTY_LIST;
import static org.apache.hadoop.fs.impl.prefetch.SampleDataForTests.EMPTY_LONG_ARRAY;
import static org.apache.hadoop.fs.impl.prefetch.SampleDataForTests.EMPTY_SHORT_ARRAY;
import static org.apache.hadoop.fs.impl.prefetch.SampleDataForTests.NON_EMPTY_ARRAY;
import static org.apache.hadoop.fs.impl.prefetch.SampleDataForTests.NON_EMPTY_BYTE_ARRAY;
import static org.apache.hadoop.fs.impl.prefetch.SampleDataForTests.NON_EMPTY_INT_ARRAY;
import static org.apache.hadoop.fs.impl.prefetch.SampleDataForTests.NON_EMPTY_LONG_ARRAY;
import static org.apache.hadoop.fs.impl.prefetch.SampleDataForTests.NON_EMPTY_SHORT_ARRAY;
import static org.apache.hadoop.fs.impl.prefetch.SampleDataForTests.NULL_BYTE_ARRAY;
import static org.apache.hadoop.fs.impl.prefetch.SampleDataForTests.NULL_INT_ARRAY;
import static org.apache.hadoop.fs.impl.prefetch.SampleDataForTests.NULL_LIST;
import static org.apache.hadoop.fs.impl.prefetch.SampleDataForTests.NULL_LONG_ARRAY;
import static org.apache.hadoop.fs.impl.prefetch.SampleDataForTests.NULL_SHORT_ARRAY;
import static org.apache.hadoop.fs.impl.prefetch.SampleDataForTests.VALID_LIST;
import static org.apache.hadoop.fs.impl.prefetch.Validate.checkPositiveInteger;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class TestValidate extends AbstractHadoopTestBase {

  @Test
  public void testCheckNotNull() throws Exception {
    String nonNullArg = "nonNullArg";
    String nullArg = null;

    // Should not throw.
    Validate.checkNotNull(nonNullArg, "nonNullArg");

    // Verify it throws.

    intercept(IllegalArgumentException.class, "'nullArg' must not be null",
        () -> Validate.checkNotNull(nullArg, "nullArg"));

  }

  @Test
  public void testCheckPositiveInteger() throws Exception {
    int positiveArg = 1;
    int zero = 0;
    int negativeArg = -1;

    // Should not throw.
    checkPositiveInteger(positiveArg, "positiveArg");

    // Verify it throws.

    intercept(IllegalArgumentException.class,
        "'negativeArg' must be a positive integer",
        () -> checkPositiveInteger(negativeArg, "negativeArg"));

    intercept(IllegalArgumentException.class,
        "'zero' must be a positive integer",
        () -> checkPositiveInteger(zero, "zero"));

  }

  @Test
  public void testCheckNotNegative() throws Exception {
    int positiveArg = 1;
    int zero = 0;
    int negativeArg = -1;

    // Should not throw.
    Validate.checkNotNegative(zero, "zeroArg");
    Validate.checkNotNegative(positiveArg, "positiveArg");

    // Verify it throws.

    intercept(IllegalArgumentException.class,
        "'negativeArg' must not be negative",
        () -> Validate.checkNotNegative(negativeArg, "negativeArg"));

  }

  @Test
  public void testCheckRequired() throws Exception {
    // Should not throw.
    Validate.checkRequired(true, "arg");

    // Verify it throws.

    intercept(IllegalArgumentException.class, "'arg' is required",
        () -> Validate.checkRequired(false, "arg"));

  }

  @Test
  public void testCheckValid() throws Exception {
    // Should not throw.
    Validate.checkValid(true, "arg");

    // Verify it throws.
    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'arg' is invalid",
        () -> Validate.checkValid(false, "arg"));
  }

  @Test
  public void testCheckValidWithValues() throws Exception {
    String validValues = "foo, bar";

    // Should not throw.
    Validate.checkValid(true, "arg", validValues);

    // Verify it throws.

    intercept(IllegalArgumentException.class,
        "'arg' is invalid. Valid values are: foo, bar",
        () -> Validate.checkValid(false, "arg", validValues));

  }

  @Test
  public void testCheckNotNullAndNotEmpty() throws Exception {
    // Should not throw.
    Validate.checkNotNullAndNotEmpty(NON_EMPTY_ARRAY, "array");
    Validate.checkNotNullAndNotEmpty(NON_EMPTY_BYTE_ARRAY, "array");
    Validate.checkNotNullAndNotEmpty(NON_EMPTY_SHORT_ARRAY, "array");
    Validate.checkNotNullAndNotEmpty(NON_EMPTY_INT_ARRAY, "array");
    Validate.checkNotNullAndNotEmpty(NON_EMPTY_LONG_ARRAY, "array");

    // Verify it throws.

    intercept(IllegalArgumentException.class, "'string' must not be empty",
        () -> Validate.checkNotNullAndNotEmpty("", "string"));

    intercept(IllegalArgumentException.class, "'array' must not be null", () ->
        Validate.checkNotNullAndNotEmpty(SampleDataForTests.NULL_ARRAY,
            "array"));

    intercept(IllegalArgumentException.class,
        "'array' must have at least one element", () ->
            Validate.checkNotNullAndNotEmpty(SampleDataForTests.EMPTY_ARRAY,
                "array"));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'array' must not be null",
        () -> Validate.checkNotNullAndNotEmpty(NULL_BYTE_ARRAY, "array"));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'array' must have at least one element",
        () -> Validate.checkNotNullAndNotEmpty(EMPTY_BYTE_ARRAY, "array"));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'array' must not be null",
        () -> Validate.checkNotNullAndNotEmpty(NULL_SHORT_ARRAY, "array"));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'array' must have at least one element",
        () -> Validate.checkNotNullAndNotEmpty(EMPTY_SHORT_ARRAY, "array"));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'array' must not be null",
        () -> Validate.checkNotNullAndNotEmpty(NULL_INT_ARRAY, "array"));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'array' must have at least one element",
        () -> Validate.checkNotNullAndNotEmpty(EMPTY_INT_ARRAY, "array"));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'array' must not be null",
        () -> Validate.checkNotNullAndNotEmpty(NULL_LONG_ARRAY, "array"));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'array' must have at least one element",
        () -> Validate.checkNotNullAndNotEmpty(EMPTY_LONG_ARRAY, "array"));
  }

  @Test
  public void testCheckListNotNullAndNotEmpty() throws Exception {
    // Should not throw.
    Validate.checkNotNullAndNotEmpty(VALID_LIST, "list");

    // Verify it throws.
    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'list' must not be null",
        () -> Validate.checkNotNullAndNotEmpty(NULL_LIST, "list"));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'list' must have at least one element",
        () -> Validate.checkNotNullAndNotEmpty(EMPTY_LIST, "list"));
  }

  @Test
  public void testCheckNotNullAndNumberOfElements() throws Exception {
    // Should not throw.
    Validate.checkNotNullAndNumberOfElements(Arrays.asList(1, 2, 3), 3, "arg");

    // Verify it throws.

    intercept(IllegalArgumentException.class, "'arg' must not be null",
        () -> Validate.checkNotNullAndNumberOfElements(null, 3, "arg"));

    // Verify it throws.
    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "Number of elements in 'arg' must be exactly 3, 2 given.",
        () -> Validate.checkNotNullAndNumberOfElements(Arrays.asList(1, 2), 3,
            "arg")
    );
  }

  @Test
  public void testCheckValuesEqual() throws Exception {
    // Should not throw.
    Validate.checkValuesEqual(1, "arg1", 1, "arg2");

    // Verify it throws.

    intercept(IllegalArgumentException.class,
        "'arg1' (1) must equal 'arg2' (2)",
        () -> Validate.checkValuesEqual(1, "arg1", 2, "arg2"));

  }

  @Test
  public void testCheckIntegerMultiple() throws Exception {
    // Should not throw.
    Validate.checkIntegerMultiple(10, "arg1", 5, "arg2");

    // Verify it throws.

    intercept(IllegalArgumentException.class,
        "'arg1' (10) must be an integer multiple of 'arg2' (3)",
        () -> Validate.checkIntegerMultiple(10, "arg1", 3, "arg2"));

  }

  @Test
  public void testCheckGreater() throws Exception {
    // Should not throw.
    Validate.checkGreater(10, "arg1", 5, "arg2");

    // Verify it throws.

    intercept(IllegalArgumentException.class,
        "'arg1' (5) must be greater than 'arg2' (10)",
        () -> Validate.checkGreater(5, "arg1", 10, "arg2"));

  }

  @Test
  public void testCheckGreaterOrEqual() throws Exception {
    // Should not throw.
    Validate.checkGreaterOrEqual(10, "arg1", 5, "arg2");

    // Verify it throws.

    intercept(IllegalArgumentException.class,
        "'arg1' (5) must be greater than or equal to 'arg2' (10)",
        () -> Validate.checkGreaterOrEqual(5, "arg1", 10, "arg2"));

  }

  @Test
  public void testCheckWithinRange() throws Exception {
    // Should not throw.
    Validate.checkWithinRange(10, "arg", 5, 15);
    Validate.checkWithinRange(10.0, "arg", 5.0, 15.0);

    // Verify it throws.

    intercept(IllegalArgumentException.class,
        "'arg' (5) must be within the range [10, 20]",
        () -> Validate.checkWithinRange(5, "arg", 10, 20));

    intercept(IllegalArgumentException.class,
        "'arg' (5.0) must be within the range [10.0, 20.0]",
        () -> Validate.checkWithinRange(5.0, "arg", 10.0, 20.0));

  }

  @Test
  public void testCheckPathExists() throws Exception {
    Path tempFile = Files.createTempFile("foo", "bar");
    Path tempDir = tempFile.getParent();
    Path notFound = Paths.get("<not-found>");

    // Should not throw.
    Validate.checkPathExists(tempFile, "tempFile");
    Validate.checkPathExists(tempDir, "tempDir");

    // Verify it throws.

    intercept(IllegalArgumentException.class, "'nullArg' must not be null",
        () -> Validate.checkPathExists(null, "nullArg"));

    intercept(IllegalArgumentException.class,
        "Path notFound (<not-found>) does not exist",
        () -> Validate.checkPathExists(notFound, "notFound"));

    intercept(IllegalArgumentException.class, "must point to a directory",
        () -> Validate.checkPathExistsAsDir(tempFile, "tempFile"));

    intercept(IllegalArgumentException.class, "must point to a file",
        () -> Validate.checkPathExistsAsFile(tempDir, "tempDir"));

  }
}
