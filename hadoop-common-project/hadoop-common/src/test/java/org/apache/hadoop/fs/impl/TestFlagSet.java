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

package org.apache.hadoop.fs.impl;

import java.util.EnumSet;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static java.util.EnumSet.allOf;
import static java.util.EnumSet.noneOf;
import static org.apache.hadoop.fs.impl.FlagSet.buildFlagSet;
import static org.apache.hadoop.fs.impl.FlagSet.createFlagSet;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Unit tests for {@link FlagSet} class.
 */
public final class TestFlagSet extends AbstractHadoopTestBase {

  private static final String KEY = "key";

  public static final String CAPABILITY_B = KEY + ".b";

  public static final String CAPABILITY_C = KEY + ".c";

  public static final String CAPABILITY_A = KEY + ".a";

  private static final String KEYDOT = KEY + ".";

  /**
   * Flagset used in tests and assertions.
   */
  private FlagSet<SimpleEnum> flagSet =
      createFlagSet(SimpleEnum.class, KEYDOT, noneOf(SimpleEnum.class));

  /**
   * Simple Enums for the tests.
   */
  private enum SimpleEnum { a, b, c }

  /**
   * Enum with a single value.
   */
  private enum OtherEnum { a }

  /**
   * Test that an entry can be enabled and disabled.
   */
  @Test
  public void testEntryEnableDisable() {
    Assertions.assertThat(flagSet.flags()).isEmpty();
    assertDisabled(SimpleEnum.a);
    flagSet.enable(SimpleEnum.a);
    assertEnabled(SimpleEnum.a);
    flagSet.disable(SimpleEnum.a);
    assertDisabled(SimpleEnum.a);
  }

  /**
   * Test the setter.
   */
  @Test
  public void testSetMethod() {
    Assertions.assertThat(flagSet.flags()).isEmpty();
    flagSet.set(SimpleEnum.a, true);
    assertEnabled(SimpleEnum.a);
    flagSet.set(SimpleEnum.a, false);
    assertDisabled(SimpleEnum.a);
  }

  /**
   * Test mutability by making immutable and
   * expecting setters to fail.
   */
  @Test
  public void testMutability() throws Throwable {
    flagSet.set(SimpleEnum.a, true);
    flagSet.makeImmutable();
    intercept(IllegalStateException.class, () ->
        flagSet.disable(SimpleEnum.a));
    assertEnabled(SimpleEnum.a);
    intercept(IllegalStateException.class, () ->
        flagSet.set(SimpleEnum.a, false));
    assertEnabled(SimpleEnum.a);
    // now look at the setters
    intercept(IllegalStateException.class, () ->
        flagSet.enable(SimpleEnum.b));
    assertDisabled(SimpleEnum.b);
    intercept(IllegalStateException.class, () ->
        flagSet.set(SimpleEnum.b, true));
    assertDisabled(SimpleEnum.b);
  }

  /**
   * Test stringification.
   */
  @Test
  public void testToString() throws Throwable {
    // empty
    assertStringValue("{}");
    assertConfigurationStringMatches("");

    // single value
    flagSet.enable(SimpleEnum.a);
    assertStringValue("{a}");
    assertConfigurationStringMatches("a");

    // add a second value.
    flagSet.enable(SimpleEnum.b);
    assertStringValue("{a, b}");
  }

  /**
   * Assert that {@link FlagSet#toString()} matches the expected
   * value.
   * @param expected expected value
   */
  private void assertStringValue(final String expected) {
    Assertions.assertThat(flagSet.toString())
        .isEqualTo(expected);
  }

  /**
   * Assert the configuration string form matches that expected.
   */
  public void assertConfigurationStringMatches(final String expected) {
    Assertions.assertThat(flagSet.toConfigurationString())
        .describedAs("Configuration string of %s", flagSet)
        .isEqualTo(expected);
  }

  /**
   * Test parsing from a configuration file.
   * Multiple entries must be parsed, whitespace trimmed.
   */
  @Test
  public void testConfEntry() {
    flagSet = flagSetFromConfig("a\t,\nc ", true);
    assertFlagSetMatches(flagSet, SimpleEnum.a, SimpleEnum.c);
    assertHasCapability(CAPABILITY_A);
    assertHasCapability(CAPABILITY_C);
    assertLacksCapability(CAPABILITY_B);
    assertPathCapabilitiesMatch(flagSet, CAPABILITY_A, CAPABILITY_C);
  }

  /**
   * Create a flagset from a configuration string.
   * @param string configuration string.
   * @param ignoreUnknown should unknown values be ignored?
   * @return a flagset
   */
  private static FlagSet<SimpleEnum> flagSetFromConfig(final String string,
      final boolean ignoreUnknown) {
    final Configuration conf = mkConf(string);
    return buildFlagSet(SimpleEnum.class, conf, KEY, ignoreUnknown);
  }

  /**
   * Test parsing from a configuration file,
   * where an entry is unknown; the builder is set to ignoreUnknown.
   */
  @Test
  public void testConfEntryWithUnknownIgnored() {
    flagSet = flagSetFromConfig("a, unknown", true);
    assertFlagSetMatches(flagSet, SimpleEnum.a);
    assertHasCapability(CAPABILITY_A);
    assertLacksCapability(CAPABILITY_B);
    assertLacksCapability(CAPABILITY_C);
  }

  /**
   * Test parsing from a configuration file where
   * the same entry is duplicated.
   */
  @Test
  public void testDuplicateConfEntry() {
    flagSet = flagSetFromConfig("a,\ta,\na\"", true);
    assertFlagSetMatches(flagSet, SimpleEnum.a);
    assertHasCapability(CAPABILITY_A);
  }

  /**
   * Handle an unknown configuration value.
   */
  @Test
  public void testConfUnknownFailure() throws Throwable {
    intercept(IllegalArgumentException.class, () ->
        flagSetFromConfig("a, unknown", false));
  }

  /**
   * Create a configuration with {@link #KEY} set to the given value.
   * @param value value to set
   * @return the configuration.
   */
  private static Configuration mkConf(final String value) {
    final Configuration conf = new Configuration(false);
    conf.set(KEY, value);
    return conf;
  }

  /**
   * Assert that the flagset has a capability.
   * @param capability capability to probe for
   */
  private void assertHasCapability(final String capability) {
    Assertions.assertThat(flagSet.hasCapability(capability))
        .describedAs("Capability of %s on %s", capability, flagSet)
        .isTrue();
  }

  /**
   * Assert that the flagset lacks a capability.
   * @param capability capability to probe for
   */
  private void assertLacksCapability(final String capability) {
    Assertions.assertThat(flagSet.hasCapability(capability))
        .describedAs("Capability of %s on %s", capability, flagSet)
        .isFalse();
  }

  /**
   * Test the * binding.
   */
  @Test
  public void testStarEntry() {
    flagSet = flagSetFromConfig("*", false);
    assertFlags(SimpleEnum.a, SimpleEnum.b, SimpleEnum.c);
    assertHasCapability(CAPABILITY_A);
    assertHasCapability(CAPABILITY_B);
    Assertions.assertThat(flagSet.pathCapabilities())
        .describedAs("path capabilities of %s", flagSet)
        .containsExactlyInAnyOrder(CAPABILITY_A, CAPABILITY_B, CAPABILITY_C);
  }

  @Test
  public void testRoundTrip() {
    final FlagSet<SimpleEnum> s1 = createFlagSet(SimpleEnum.class,
        KEYDOT,
        allOf(SimpleEnum.class));
    final FlagSet<SimpleEnum> s2 = roundTrip(s1);
    Assertions.assertThat(s1.flags()).isEqualTo(s2.flags());
    assertFlagSetMatches(s2, SimpleEnum.a, SimpleEnum.b, SimpleEnum.c);
  }

  @Test
  public void testEmptyRoundTrip() {
    final FlagSet<SimpleEnum> s1 = createFlagSet(SimpleEnum.class, KEYDOT,
        noneOf(SimpleEnum.class));
    final FlagSet<SimpleEnum> s2 = roundTrip(s1);
    Assertions.assertThat(s1.flags())
        .isEqualTo(s2.flags());
    Assertions.assertThat(s2.isEmpty())
        .describedAs("empty flagset %s", s2)
        .isTrue();
    assertFlagSetMatches(flagSet);
    Assertions.assertThat(flagSet.pathCapabilities())
        .describedAs("path capabilities of %s", flagSet)
        .isEmpty();
  }

  @Test
  public void testSetIsClone() {
    final EnumSet<SimpleEnum> flags = noneOf(SimpleEnum.class);
    final FlagSet<SimpleEnum> s1 = createFlagSet(SimpleEnum.class, KEYDOT, flags);
    s1.enable(SimpleEnum.b);

    // set a source flag
    flags.add(SimpleEnum.a);

    // verify the derived flagset is unchanged
    assertFlagSetMatches(s1, SimpleEnum.b);
  }

  @Test
  public void testEquality() {
    final FlagSet<SimpleEnum> s1 = createFlagSet(SimpleEnum.class, KEYDOT, SimpleEnum.a);
    final FlagSet<SimpleEnum> s2 = createFlagSet(SimpleEnum.class, KEYDOT, SimpleEnum.a);
    // make one of them immutable
    s2.makeImmutable();
    Assertions.assertThat(s1)
        .describedAs("s1 == s2")
        .isEqualTo(s2);
    Assertions.assertThat(s1.hashCode())
        .describedAs("hashcode of s1 == hashcode of s2")
        .isEqualTo(s2.hashCode());
  }

  @Test
  public void testInequality() {
    final FlagSet<SimpleEnum> s1 =
        createFlagSet(SimpleEnum.class, KEYDOT, noneOf(SimpleEnum.class));
    final FlagSet<SimpleEnum> s2 =
        createFlagSet(SimpleEnum.class, KEYDOT, SimpleEnum.a, SimpleEnum.b);
    Assertions.assertThat(s1)
        .describedAs("s1 == s2")
        .isNotEqualTo(s2);
  }

  @Test
  public void testClassInequality() {
    final FlagSet<?> s1 =
        createFlagSet(SimpleEnum.class, KEYDOT, noneOf(SimpleEnum.class));
    final FlagSet<?> s2 =
        createFlagSet(OtherEnum.class, KEYDOT, OtherEnum.a);
    Assertions.assertThat(s1)
        .describedAs("s1 == s2")
        .isNotEqualTo(s2);
  }

  /**
   * The copy operation creates a new instance which is now mutable,
   * even if the original was immutable.
   */
  @Test
  public void testCopy() throws Throwable {
    FlagSet<SimpleEnum> s1 =
            createFlagSet(SimpleEnum.class, KEYDOT, SimpleEnum.a, SimpleEnum.b);
    s1.makeImmutable();
    FlagSet<SimpleEnum> s2 = s1.copy();
    Assertions.assertThat(s2)
        .describedAs("copy of %s", s1)
        .isNotSameAs(s1);
    Assertions.assertThat(!s2.isImmutable())
        .describedAs("set %s is immutable", s2)
        .isTrue();
    Assertions.assertThat(s1)
        .describedAs("s1 == s2")
        .isEqualTo(s2);
  }

  @Test
  public void testCreateNullEnumClass() throws Throwable {
    intercept(NullPointerException.class, () ->
        createFlagSet(null, KEYDOT, SimpleEnum.a));
  }

  @Test
  public void testCreateNullPrefix() throws Throwable {
    intercept(NullPointerException.class, () ->
        createFlagSet(SimpleEnum.class, null, SimpleEnum.a));
  }

  /**
   * Round trip a FlagSet.
   * @param flagset FlagSet to save to a configuration and retrieve.
   * @return a new FlagSet.
   */
  private FlagSet<SimpleEnum> roundTrip(FlagSet<SimpleEnum> flagset) {
    final Configuration conf = new Configuration(false);
    conf.set(KEY, flagset.toConfigurationString());
    return buildFlagSet(SimpleEnum.class, conf, KEY, false);
  }

  /**
   * Assert a flag is enabled in the {@link #flagSet} field.
   * @param flag flag to check
   */
  private void assertEnabled(final SimpleEnum flag) {
    Assertions.assertThat(flagSet.enabled(flag))
        .describedAs("status of flag %s in %s", flag, flagSet)
        .isTrue();
  }

  /**
   * Assert a flag is disabled in the {@link #flagSet} field.
   * @param flag flag to check
   */
  private void assertDisabled(final SimpleEnum flag) {
    Assertions.assertThat(flagSet.enabled(flag))
        .describedAs("status of flag %s in %s", flag, flagSet)
        .isFalse();
  }

  /**
   * Assert that a set of flags are enabled in the {@link #flagSet} field.
   * @param flags flags which must be set.
   */
  private void assertFlags(final SimpleEnum... flags) {
    for (SimpleEnum flag : flags) {
      assertEnabled(flag);
    }
  }

  /**
   * Assert that a FlagSet contains an exclusive set of values.
   * @param flags flags which must be set.
   */
  private void assertFlagSetMatches(
      FlagSet<SimpleEnum> fs,
      SimpleEnum... flags) {
    Assertions.assertThat(fs.flags())
        .describedAs("path capabilities of %s", fs)
        .containsExactly(flags);
  }

  /**
   * Assert that a flagset contains exactly the capabilities.
   * This is calculated by getting the list of active capabilities
   * and asserting on the list.
   * @param fs flagset
   * @param capabilities capabilities
   */
  private void assertPathCapabilitiesMatch(
      FlagSet<SimpleEnum> fs,
      String... capabilities) {
    Assertions.assertThat(fs.pathCapabilities())
        .describedAs("path capabilities of %s", fs)
        .containsExactlyInAnyOrder(capabilities);
  }
}
