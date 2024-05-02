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

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.util.ConfigurationHelper;
import org.apache.hadoop.util.Preconditions;

import static org.apache.hadoop.util.ConfigurationHelper.mapEnumNamesToValues;

/**
 * A set of flags, constructed from a configuration option or from a string,
 * with the semantics of
 * {@link ConfigurationHelper#parseEnumSet(String, String, Class, boolean)}
 * and implementing {@link StreamCapabilities}.
 * <p>
 * Thread safety: there is no synchronization on a mutable {@code FlagSet}.
 * Once declared immutable, flags cannot be changed, so they
 * becomes implicitly thread-safe.
 */
public final class FlagSet<E extends Enum<E>> implements StreamCapabilities {

  /**
   * Set of flags.
   */
  private final Set<E> flags;

  /**
   * Is the set immutable?
   */
  private final AtomicBoolean immutable = new AtomicBoolean(false);

  /**
   * Mapping of prefixed flag names to enum values.
   */
  private final Map<String, E> namesToValues;

  /**
   * Create a FlagSet.
   * @param enumClass class of enum
   * @param prefix prefix (with trailing ".") for path capabilities probe
   * @param flags flags. A copy of these are made.
   */
  private FlagSet(final Class<E> enumClass,
      final String prefix,
      @Nullable final EnumSet<E> flags) {

    this.flags = flags != null
        ? EnumSet.copyOf(flags)
        : EnumSet.noneOf(enumClass);
    this.namesToValues = mapEnumNamesToValues(prefix, enumClass);
  }

  /**
   * Get a copy of the flags.
   * <p>
   * This is immutable.
   * @return the flags.
   */
  public EnumSet<E> flags() {
    return EnumSet.copyOf(flags);
  }

  /**
   * Probe for the FlagSet being empty.
   * @return true if there are no flags set.
   */
  public boolean isEmpty() {
    return flags.isEmpty();
  }

  /**
   * Is a flag enabled?
   * @param flag flag to check
   * @return true if it is in the set of enabled flags.
   */
  public boolean enabled(final E flag) {
    return flags.contains(flag);
  }

  /**
   * Check for mutability before any mutating operation.
   * @throws IllegalStateException if the set is still mutable
   */
  private void checkMutable() {
    Preconditions.checkState(!immutable.get(),
        "FlagSet is immutable");
  }

  /**
   * Enable a flag.
   * @param flag flag to enable.
   */
  public void enable(final E flag) {
    checkMutable();
    flags.add(flag);
  }

  /**
   * Disable a flag.
   * @param flag flag to disable
   */
  public void disable(final E flag) {
    checkMutable();
    flags.remove(flag);
  }

  /**
   * Set a flag to the chosen value.
   * @param flag flag
   * @param state true to enable, false to disable.
   */
  public void set(final E flag, boolean state) {
    if (state) {
      enable(flag);
    } else {
      disable(flag);
    }
  }

  /**
   * Is a flag enabled?
   * @param capability string to query the stream support for.
   * @return true if the capability maps to an enum value and
   * that value is set.
   */
  @Override
  public boolean hasCapability(final String capability) {
    final E e = namesToValues.get(capability);
    return e != null && enabled(e);
  }

  /**
   * Make immutable; no-op if already set.
   */
  public void makeImmutable() {
    immutable.set(true);
  }

  @Override
  public String toString() {
    return "{" +
        (flags.stream()
            .map(Enum::name)
            .collect(Collectors.joining(", ")))
        + "}";
  }

  /**
   * Generate the list of capabilities.
   * @return a possibly empty list.
   */
  public List<String> pathCapabilities() {
    return namesToValues.keySet().stream()
        .filter(this::hasCapability)
        .collect(Collectors.toList());
  }


  /**
   * Equality is based on the set.
   * @param o other object
   * @return true iff the flags are equal.
   */
  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FlagSet<?> flagSet = (FlagSet<?>) o;
    return Objects.equals(flags, flagSet.flags);
  }

  /**
   * Hash code is based on the flags.
   * @return a hash code.
   */
  @Override
  public int hashCode() {
    return Objects.hashCode(flags);
  }

  /**
   * Convert to a string which can be then set in a configuration.
   * This is effectively a marshalled form of the flags.
   * @return a comma separated list of flag names.
   */
  public String toConfigurationString() {
    return flags.stream()
        .map(e -> e.name())
        .collect(Collectors.joining(", "));
  }

  /**
   * Create a FlagSet.
   * @param enumClass class of enum
   * @param prefix prefix (with trailing ".") for path capabilities probe
   * @param flags flags
   * @param <E> enum type
   * @return a mutable FlagSet
   */
  public static <E extends Enum<E>> FlagSet<E> createFlagSet(
      final Class<E> enumClass,
      final String prefix,
      final EnumSet<E> flags) {
    return new FlagSet<>(enumClass, prefix, flags);
  }

  /**
   * Create a FlagSet from a list of enum values.
   * @param enumClass class of enum
   * @param prefix prefix (with trailing ".") for path capabilities probe
   * @param enabled varags list of flags to enable.
   * @param <E> enum type
   * @return a mutable FlagSet
   */
  public static <E extends Enum<E>> FlagSet<E> createFlagSet(
      final Class<E> enumClass,
      final String prefix,
      final E... enabled) {
    final FlagSet<E> flagSet = new FlagSet<>(enumClass, prefix, null);
    Arrays.stream(enabled).forEach(flagSet::enable);
    return flagSet;
  }

  /**
   * Build a FlagSet from a comma separated list of values.
   * Case independent.
   * Special handling of "*" meaning: all values.
   * @param enumClass class of enum
   * @param conf configuration
   * @param key key to look for
   * @param ignoreUnknown should unknown values raise an exception?
   * @param <E> enumeration type
   * @return a mutable FlagSet
   * @throws IllegalArgumentException if one of the entries was unknown and ignoreUnknown is false,
   * or there are two entries in the enum which differ only by case.
   */
  public static <E extends Enum<E>> FlagSet<E> buildFlagSet(
      final Class<E> enumClass,
      final Configuration conf,
      final String key,
      final boolean ignoreUnknown) {
    final EnumSet<E> flags = conf.getEnumSet(key, enumClass, ignoreUnknown);
    return createFlagSet(enumClass, key + ".", flags);
  }

}
