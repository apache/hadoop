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

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.IterableAssert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.util.ConfigurationHelper.ERROR_MULTIPLE_ELEMENTS_MATCHING_TO_LOWER_CASE_VALUE;
import static org.apache.hadoop.util.ConfigurationHelper.mapEnumNamesToValues;
import static org.apache.hadoop.util.ConfigurationHelper.parseEnumSet;
import static org.apache.hadoop.util.ConfigurationHelper.resolveEnum;

/**
 * Test for {@link ConfigurationHelper}.
 */
public class TestConfigurationHelper extends AbstractHadoopTestBase {

  /**
   * Simple Enums.
   * "i" is included for case tests, as it is special in turkey.
   */
  private enum SimpleEnum { a, b, c, i }

  /**
   * Upper case version of SimpleEnum.
   * "i" is included for case tests, as it is special in turkey.
   */
  private enum UppercaseEnum { A, B, C, I }


  /**
   * Special case: an enum with no values.
   */
  private enum EmptyEnum { }

  /**
   * Create assertion about the outcome of
   * {@link ConfigurationHelper#parseEnumSet(String, String, Class, boolean)}.
   * @param valueString value from Configuration
   * @param enumClass class of enum
   * @param ignoreUnknown should unknown values be ignored?
   * @param <E> enum type
   * @return an assertion on the outcome.
   * @throws IllegalArgumentException if one of the entries was unknown and ignoreUnknown is false,
   * or there are two entries in the enum which differ only by case.
   */
  private static <E extends Enum<E>> IterableAssert<E> assertEnumParse(
      final String valueString,
      final Class<E> enumClass,
      final boolean ignoreUnknown) {
    final Set<E> enumSet = parseEnumSet("key", valueString, enumClass, ignoreUnknown);
    final IterableAssert<E> assertion = Assertions.assertThat(enumSet);
    return assertion.describedAs("parsed enum set '%s'", valueString);
  }


  /**
   * Create a configuration with the key {@code key} set to a {@code value}.
   * @param value value for the key
   * @return a configuration with only key set.
   */
  private Configuration confWithKey(String value) {
    final Configuration conf = new Configuration(false);
    conf.set("key", value);
    return conf;
  }

  @Test
  public void testEnumParseAll() {
    assertEnumParse("*", SimpleEnum.class, false)
        .containsExactly(SimpleEnum.a, SimpleEnum.b, SimpleEnum.c, SimpleEnum.i);
  }

  @Test
  public void testEnumParse() {
    assertEnumParse("a, b,c", SimpleEnum.class, false)
        .containsExactly(SimpleEnum.a, SimpleEnum.b, SimpleEnum.c);
  }

  @Test
  public void testEnumCaseIndependence() {
    assertEnumParse("A, B, C, I", SimpleEnum.class, false)
        .containsExactly(SimpleEnum.a, SimpleEnum.b, SimpleEnum.c, SimpleEnum.i);
  }

  @Test
  public void testEmptyArguments() {
    assertEnumParse(" ", SimpleEnum.class, false)
        .isEmpty();
  }

  @Test
  public void testUnknownEnumNotIgnored() throws Throwable {
    intercept(IllegalArgumentException.class, "unrecognized", () ->
        parseEnumSet("key", "c, unrecognized", SimpleEnum.class, false));
  }

  @Test
  public void testUnknownEnumNotIgnoredThroughConf() throws Throwable {
    intercept(IllegalArgumentException.class, "unrecognized", () ->
        confWithKey("c, unrecognized")
            .getEnumSet("key", SimpleEnum.class, false));
  }

  @Test
  public void testUnknownEnumIgnored() {
    assertEnumParse("c, d", SimpleEnum.class, true)
        .containsExactly(SimpleEnum.c);
  }

  @Test
  public void testUnknownStarEnum() throws Throwable {
    intercept(IllegalArgumentException.class, "unrecognized", () ->
        parseEnumSet("key", "*, unrecognized", SimpleEnum.class, false));
  }

  @Test
  public void testUnknownStarEnumIgnored() {
    assertEnumParse("*, d", SimpleEnum.class, true)
        .containsExactly(SimpleEnum.a, SimpleEnum.b, SimpleEnum.c, SimpleEnum.i);
  }

  /**
   * Unsupported enum as the same case value is present.
   */
  private enum CaseConflictingEnum { a, A }

  @Test
  public void testCaseConflictingEnumNotSupported() throws Throwable {
    intercept(IllegalArgumentException.class,
        ERROR_MULTIPLE_ELEMENTS_MATCHING_TO_LOWER_CASE_VALUE,
        () ->
            parseEnumSet("key", "c, unrecognized",
                CaseConflictingEnum.class, false));
  }

  @Test
  public void testEmptyEnumMap() {
    Assertions.assertThat(mapEnumNamesToValues("", EmptyEnum.class))
        .isEmpty();
  }

  /**
   * A star enum for an empty enum must be empty.
   */
  @Test
  public void testEmptyStarEnum() {
    assertEnumParse("*", EmptyEnum.class, false)
        .isEmpty();
  }

  @Test
  public void testDuplicateValues() {
    assertEnumParse("a, a, c, b, c", SimpleEnum.class, true)
        .containsExactly(SimpleEnum.a, SimpleEnum.b, SimpleEnum.c);
  }

  @Test
  public void testResolveEnumGood() throws Throwable {
    assertEnumResolution("c", SimpleEnum.c);
  }

  @Test
  public void testResolveEnumTrimmed() throws Throwable {
    // strings are trimmed at each end
    assertEnumResolution("\n i \n ", SimpleEnum.i);
  }

  @Test
  public void testResolveEnumCaseConversion() throws Throwable {
    assertEnumResolution("C", SimpleEnum.c);
  }

  @Test
  public void testResolveEnumNoMatch() throws Throwable {
    assertEnumResolution("other", null);
  }

  @Test
  public void testResolveEnumEmpty() throws Throwable {
    assertEnumResolution("", null);
  }

  @Test
  public void testResolveEnumUpperCaseConversion() throws Throwable {
    assertUpperEnumResolution("C", UppercaseEnum.C);
  }

  @Test
  public void testResolveLowerToUpperCaseConversion() throws Throwable {
    assertUpperEnumResolution("i", UppercaseEnum.I);
  }

  /**
   * Assert that a string value in a configuration resolves to the expected
   * value.
   * @param value value to set
   * @param expected expected outcome, set to null for no resolution.
   */
  private void assertEnumResolution(final String value, final SimpleEnum expected) {
    Assertions.assertThat(resolveEnum(confWithKey(value),
            "key", SimpleEnum.class, (v) -> null))
        .describedAs("Resolution of %s", value)
        .isEqualTo(expected);
  }

  /**
   * Equivalent for Uppercase Enum.
   * @param value value to set
   * @param expected expected outcome, set to null for no resolution.
   */
  private void assertUpperEnumResolution(final String value, UppercaseEnum expected) {
    Assertions.assertThat(resolveEnum(confWithKey(value),
            "key", UppercaseEnum.class, (v) -> null))
        .describedAs("Resolution of %s", value)
        .isEqualTo(expected);
  }

}
