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
import java.util.Collection;

import static org.apache.hadoop.util.Preconditions.checkArgument;

/**
 * A superset of Validate class in Apache commons lang3.
 * <p>
 * It provides consistent message strings for frequently encountered checks.
 * That simplifies callers because they have to supply only the name of the argument
 * that failed a check instead of having to supply the entire message.
 */
public final class Validate {

  private Validate() {
  }

  /**
   * Validates that the given reference argument is not null.
   * @param obj the argument reference to validate.
   * @param argName the name of the argument being validated.
   */
  public static void checkNotNull(Object obj, String argName) {
    checkArgument(obj != null, "'%s' must not be null.", argName);
  }

  /**
   * Validates that the given integer argument is not zero or negative.
   * @param value the argument value to validate
   * @param argName the name of the argument being validated.
   */
  public static void checkPositiveInteger(long value, String argName) {
    checkArgument(value > 0, "'%s' must be a positive integer.", argName);
  }

  /**
   * Validates that the given integer argument is not negative.
   * @param value the argument value to validate
   * @param argName the name of the argument being validated.
   */
  public static void checkNotNegative(long value, String argName) {
    checkArgument(value >= 0, "'%s' must not be negative.", argName);
  }

  /**
   * Validates that the expression (that checks a required field is present) is true.
   * @param isPresent indicates whether the given argument is present.
   * @param argName the name of the argument being validated.
   */
  public static void checkRequired(boolean isPresent, String argName) {
    checkArgument(isPresent, "'%s' is required.", argName);
  }

  /**
   * Validates that the expression (that checks a field is valid) is true.
   * @param isValid indicates whether the given argument is valid.
   * @param argName the name of the argument being validated.
   */
  public static void checkValid(boolean isValid, String argName) {
    checkArgument(isValid, "'%s' is invalid.", argName);
  }

  /**
   * Validates that the expression (that checks a field is valid) is true.
   * @param isValid indicates whether the given argument is valid.
   * @param argName the name of the argument being validated.
   * @param validValues the list of values that are allowed.
   */
  public static void checkValid(boolean isValid,
      String argName,
      String validValues) {
    checkArgument(isValid, "'%s' is invalid. Valid values are: %s.", argName,
        validValues);
  }

  /**
   * Validates that the given string is not null and has non-zero length.
   * @param arg the argument reference to validate.
   * @param argName the name of the argument being validated.
   */
  public static void checkNotNullAndNotEmpty(String arg, String argName) {
    checkNotNull(arg, argName);
    checkArgument(
        !arg.isEmpty(),
        "'%s' must not be empty.",
        argName);
  }

  /**
   * Validates that the given array is not null and has at least one element.
   * @param <T> the type of array's elements.
   * @param array the argument reference to validate.
   * @param argName the name of the argument being validated.
   */
  public static <T> void checkNotNullAndNotEmpty(T[] array, String argName) {
    checkNotNull(array, argName);
    checkNotEmpty(array.length, argName);
  }

  /**
   * Validates that the given array is not null and has at least one element.
   * @param array the argument reference to validate.
   * @param argName the name of the argument being validated.
   */
  public static void checkNotNullAndNotEmpty(byte[] array, String argName) {
    checkNotNull(array, argName);
    checkNotEmpty(array.length, argName);
  }

  /**
   * Validates that the given array is not null and has at least one element.
   * @param array the argument reference to validate.
   * @param argName the name of the argument being validated.
   */
  public static void checkNotNullAndNotEmpty(short[] array, String argName) {
    checkNotNull(array, argName);
    checkNotEmpty(array.length, argName);
  }

  /**
   * Validates that the given array is not null and has at least one element.
   * @param array the argument reference to validate.
   * @param argName the name of the argument being validated.
   */
  public static void checkNotNullAndNotEmpty(int[] array, String argName) {
    checkNotNull(array, argName);
    checkNotEmpty(array.length, argName);
  }

  /**
   * Validates that the given array is not null and has at least one element.
   * @param array the argument reference to validate.
   * @param argName the name of the argument being validated.
   */
  public static void checkNotNullAndNotEmpty(long[] array, String argName) {
    checkNotNull(array, argName);
    checkNotEmpty(array.length, argName);
  }

  /**
   * Validates that the given buffer is not null and has non-zero capacity.
   * @param <T> the type of iterable's elements.
   * @param iter the argument reference to validate.
   * @param argName the name of the argument being validated.
   */
  public static <T> void checkNotNullAndNotEmpty(Iterable<T> iter,
      String argName) {
    checkNotNull(iter, argName);
    int minNumElements = iter.iterator().hasNext() ? 1 : 0;
    checkNotEmpty(minNumElements, argName);
  }

  /**
   * Validates that the given set is not null and has an exact number of items.
   * @param <T> the type of collection's elements.
   * @param collection the argument reference to validate.
   * @param numElements the expected number of elements in the collection.
   * @param argName the name of the argument being validated.
   */
  public static <T> void checkNotNullAndNumberOfElements(
      Collection<T> collection, int numElements, String argName) {
    checkNotNull(collection, argName);
    checkArgument(
        collection.size() == numElements,
        "Number of elements in '%s' must be exactly %s, %s given.",
        argName,
        numElements,
        collection.size()
    );
  }

  /**
   * Validates that the given two values are equal.
   * @param value1 the first value to check.
   * @param value1Name the name of the first argument.
   * @param value2 the second value to check.
   * @param value2Name the name of the second argument.
   */
  public static void checkValuesEqual(
      long value1,
      String value1Name,
      long value2,
      String value2Name) {
    checkArgument(
        value1 == value2,
        "'%s' (%s) must equal '%s' (%s).",
        value1Name,
        value1,
        value2Name,
        value2);
  }

  /**
   * Validates that the first value is an integer multiple of the second value.
   * @param value1 the first value to check.
   * @param value1Name the name of the first argument.
   * @param value2 the second value to check.
   * @param value2Name the name of the second argument.
   */
  public static void checkIntegerMultiple(
      long value1,
      String value1Name,
      long value2,
      String value2Name) {
    checkArgument(
        (value1 % value2) == 0,
        "'%s' (%s) must be an integer multiple of '%s' (%s).",
        value1Name,
        value1,
        value2Name,
        value2);
  }

  /**
   * Validates that the first value is greater than the second value.
   * @param value1 the first value to check.
   * @param value1Name the name of the first argument.
   * @param value2 the second value to check.
   * @param value2Name the name of the second argument.
   */
  public static void checkGreater(
      long value1,
      String value1Name,
      long value2,
      String value2Name) {
    checkArgument(
        value1 > value2,
        "'%s' (%s) must be greater than '%s' (%s).",
        value1Name,
        value1,
        value2Name,
        value2);
  }

  /**
   * Validates that the first value is greater than or equal to the second value.
   * @param value1 the first value to check.
   * @param value1Name the name of the first argument.
   * @param value2 the second value to check.
   * @param value2Name the name of the second argument.
   */
  public static void checkGreaterOrEqual(
      long value1,
      String value1Name,
      long value2,
      String value2Name) {
    checkArgument(
        value1 >= value2,
        "'%s' (%s) must be greater than or equal to '%s' (%s).",
        value1Name,
        value1,
        value2Name,
        value2);
  }

  /**
   * Validates that the first value is less than or equal to the second value.
   * @param value1 the first value to check.
   * @param value1Name the name of the first argument.
   * @param value2 the second value to check.
   * @param value2Name the name of the second argument.
   */
  public static void checkLessOrEqual(
      long value1,
      String value1Name,
      long value2,
      String value2Name) {
    checkArgument(
        value1 <= value2,
        "'%s' (%s) must be less than or equal to '%s' (%s).",
        value1Name,
        value1,
        value2Name,
        value2);
  }

  /**
   * Validates that the given value is within the given range of values.
   * @param value the value to check.
   * @param valueName the name of the argument.
   * @param minValueInclusive inclusive lower limit for the value.
   * @param maxValueInclusive inclusive upper limit for the value.
   */
  public static void checkWithinRange(
      long value,
      String valueName,
      long minValueInclusive,
      long maxValueInclusive) {
    checkArgument(
        (value >= minValueInclusive) && (value <= maxValueInclusive),
        "'%s' (%s) must be within the range [%s, %s].",
        valueName,
        value,
        minValueInclusive,
        maxValueInclusive);
  }

  /**
   * Validates that the given value is within the given range of values.
   * @param value the value to check.
   * @param valueName the name of the argument.
   * @param minValueInclusive inclusive lower limit for the value.
   * @param maxValueInclusive inclusive upper limit for the value.
   */
  public static void checkWithinRange(
      double value,
      String valueName,
      double minValueInclusive,
      double maxValueInclusive) {
    checkArgument(
        (value >= minValueInclusive) && (value <= maxValueInclusive),
        "'%s' (%s) must be within the range [%s, %s].",
        valueName,
        value,
        minValueInclusive,
        maxValueInclusive);
  }

  /**
   * Validates that the given path exists.
   * @param path the path to check.
   * @param argName the name of the argument being validated.
   */
  public static void checkPathExists(Path path, String argName) {
    checkNotNull(path, argName);
    checkArgument(Files.exists(path), "Path %s (%s) does not exist.", argName,
        path);
  }

  /**
   * Validates that the given path exists and is a directory.
   * @param path the path to check.
   * @param argName the name of the argument being validated.
   */
  public static void checkPathExistsAsDir(Path path, String argName) {
    checkPathExists(path, argName);
    checkArgument(
        Files.isDirectory(path),
        "Path %s (%s) must point to a directory.",
        argName,
        path);
  }

  /**
   * Validates that the given path exists and is a file.
   * @param path the path to check.
   * @param argName the name of the argument being validated.
   */
  public static void checkPathExistsAsFile(Path path, String argName) {
    checkPathExists(path, argName);
    checkArgument(Files.isRegularFile(path),
        "Path %s (%s) must point to a file.", argName, path);
  }


  /**
   * Check state.
   * @param expression expression which must hold.
   * @param format format string
   * @param args arguments for the error string
   * @throws IllegalStateException if the state is not valid.
   */
  public static void checkState(boolean expression,
      String format,
      Object... args) {
    if (!expression) {
      throw new IllegalStateException(String.format(format, args));
    }
  }

  private static void checkNotEmpty(int arraySize, String argName) {
    checkArgument(
        arraySize > 0,
        "'%s' must have at least one element.",
        argName);
  }
}
