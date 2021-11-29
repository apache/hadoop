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

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class ValidateTest {
  @Test
  public void testCheckNotNull() {
    String nonNullArg = "nonNullArg";
    String nullArg = null;

    // Should not throw.
    Validate.checkNotNull(nonNullArg, "nonNullArg");

    // Verify it throws.
    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'nullArg' must not be null",
        () -> Validate.checkNotNull(nullArg, "nullArg"));
  }

  @Test
  public void testCheckPositiveInteger() {
    int positiveArg = 1;
    int zero = 0;
    int negativeArg = -1;

    // Should not throw.
    Validate.checkPositiveInteger(positiveArg, "positiveArg");

    // Verify it throws.
    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'negativeArg' must be a positive integer",
        () -> Validate.checkPositiveInteger(negativeArg, "negativeArg"));
    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'zero' must be a positive integer",
        () -> Validate.checkPositiveInteger(zero, "zero"));
  }

  @Test
  public void testCheckNotNegative() {
    int positiveArg = 1;
    int zero = 0;
    int negativeArg = -1;

    // Should not throw.
    Validate.checkNotNegative(zero, "zeroArg");
    Validate.checkNotNegative(positiveArg, "positiveArg");

    // Verify it throws.
    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'negativeArg' must not be negative",
        () -> Validate.checkNotNegative(negativeArg, "negativeArg"));
  }

  @Test
  public void testCheckRequired() {
    // Should not throw.
    Validate.checkRequired(true, "arg");

    // Verify it throws.
    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'arg' is required",
        () -> Validate.checkRequired(false, "arg"));
  }

  @Test
  public void testCheckValid() {
    // Should not throw.
    Validate.checkValid(true, "arg");

    // Verify it throws.
    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'arg' is invalid",
        () -> Validate.checkValid(false, "arg"));
  }

  @Test
  public void testCheckValidWithValues() {
    String validValues = "foo, bar";

    // Should not throw.
    Validate.checkValid(true, "arg", validValues);

    // Verify it throws.
    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'arg' is invalid. Valid values are: foo, bar",
        () -> Validate.checkValid(false, "arg", validValues));
  }

  @Test
  public void testCheckNotNullAndNotEmpty() {
    // Should not throw.
    Validate.checkNotNullAndNotEmpty(TestData.nonEmptyArray, "array");
    Validate.checkNotNullAndNotEmpty(TestData.nonEmptyByteArray, "array");
    Validate.checkNotNullAndNotEmpty(TestData.nonEmptyShortArray, "array");
    Validate.checkNotNullAndNotEmpty(TestData.nonEmptyIntArray, "array");
    Validate.checkNotNullAndNotEmpty(TestData.nonEmptyLongArray, "array");

    // Verify it throws.
    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'string' must not be empty",
        () -> Validate.checkNotNullAndNotEmpty("", "string"));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'array' must not be null",
        () -> Validate.checkNotNullAndNotEmpty(TestData.nullArray, "array"));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'array' must have at least one element",
        () -> Validate.checkNotNullAndNotEmpty(TestData.emptyArray, "array"));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'array' must not be null",
        () -> Validate.checkNotNullAndNotEmpty(TestData.nullByteArray, "array"));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'array' must have at least one element",
        () -> Validate.checkNotNullAndNotEmpty(TestData.emptyByteArray, "array"));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'array' must not be null",
        () -> Validate.checkNotNullAndNotEmpty(TestData.nullShortArray, "array"));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'array' must have at least one element",
        () -> Validate.checkNotNullAndNotEmpty(TestData.emptyShortArray, "array"));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'array' must not be null",
        () -> Validate.checkNotNullAndNotEmpty(TestData.nullIntArray, "array"));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'array' must have at least one element",
        () -> Validate.checkNotNullAndNotEmpty(TestData.emptyIntArray, "array"));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'array' must not be null",
        () -> Validate.checkNotNullAndNotEmpty(TestData.nullLongArray, "array"));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'array' must have at least one element",
        () -> Validate.checkNotNullAndNotEmpty(TestData.emptyLongArray, "array"));
  }

  @Test
  public void testCheckListNotNullAndNotEmpty() {
    // Should not throw.
    Validate.checkNotNullAndNotEmpty(TestData.validList, "list");

    // Verify it throws.
    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'list' must not be null",
        () -> Validate.checkNotNullAndNotEmpty(TestData.nullList, "list"));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'list' must have at least one element",
        () -> Validate.checkNotNullAndNotEmpty(TestData.emptyList, "list"));
  }

  @Test
  public void testCheckNotNullAndNumberOfElements() {
    // Should not throw.
    Validate.checkNotNullAndNumberOfElements(Arrays.asList(1, 2, 3), 3, "arg");

    // Verify it throws.
    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'arg' must not be null",
        () -> Validate.checkNotNullAndNumberOfElements(null, 3, "arg")
    );

    // Verify it throws.
    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "Number of elements in 'arg' must be exactly 3, 2 given.",
        () -> Validate.checkNotNullAndNumberOfElements(Arrays.asList(1, 2), 3, "arg")
    );
  }

  @Test
  public void testCheckValuesEqual() {
    // Should not throw.
    Validate.checkValuesEqual(1, "arg1", 1, "arg2");

    // Verify it throws.
    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'arg1' (1) must equal 'arg2' (2)",
        () -> Validate.checkValuesEqual(1, "arg1", 2, "arg2"));
  }

  @Test
  public void testCheckIntegerMultiple() {
    // Should not throw.
    Validate.checkIntegerMultiple(10, "arg1", 5, "arg2");

    // Verify it throws.
    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'arg1' (10) must be an integer multiple of 'arg2' (3)",
        () -> Validate.checkIntegerMultiple(10, "arg1", 3, "arg2"));
  }

  @Test
  public void testCheckGreater() {
    // Should not throw.
    Validate.checkGreater(10, "arg1", 5, "arg2");

    // Verify it throws.
    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'arg1' (5) must be greater than 'arg2' (10)",
        () -> Validate.checkGreater(5, "arg1", 10, "arg2"));
  }

  @Test
  public void testCheckGreaterOrEqual() {
    // Should not throw.
    Validate.checkGreaterOrEqual(10, "arg1", 5, "arg2");

    // Verify it throws.
    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'arg1' (5) must be greater than or equal to 'arg2' (10)",
        () -> Validate.checkGreaterOrEqual(5, "arg1", 10, "arg2"));
  }

  @Test
  public void testCheckWithinRange() {
    // Should not throw.
    Validate.checkWithinRange(10, "arg", 5, 15);
    Validate.checkWithinRange(10.0, "arg", 5.0, 15.0);

    // Verify it throws.
    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'arg' (5) must be within the range [10, 20]",
        () -> Validate.checkWithinRange(5, "arg", 10, 20));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'arg' (5.0) must be within the range [10.0, 20.0]",
        () -> Validate.checkWithinRange(5.0, "arg", 10.0, 20.0));
  }

  @Test
  public void testCheckPathExists() throws IOException {
    Path tempFile = Files.createTempFile("foo", "bar");
    Path tempDir  = tempFile.getParent();
    Path notFound = Paths.get("<not-found>");

    // Should not throw.
    Validate.checkPathExists(tempFile, "tempFile");
    Validate.checkPathExists(tempDir, "tempDir");

    // Verify it throws.
    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'nullArg' must not be null",
        () -> Validate.checkPathExists(null, "nullArg"));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "Path notFound (<not-found>) does not exist",
        () -> Validate.checkPathExists(notFound, "notFound"));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "must point to a directory",
        () -> Validate.checkPathExistsAsDir(tempFile, "tempFile"));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "must point to a file",
        () -> Validate.checkPathExistsAsFile(tempDir, "tempDir"));
  }
}
