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

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;

import static java.util.EnumSet.noneOf;
import static org.apache.hadoop.util.Preconditions.checkArgument;
import static org.apache.hadoop.util.StringUtils.getTrimmedStringCollection;

/**
 * Configuration Helper class to provide advanced configuration parsing.
 * Private; external code MUST use {@link Configuration} instead
 */
@InterfaceAudience.Private
public final class ConfigurationHelper {

  /**
   * Error string if there are multiple enum elements which only differ
   * by case: {@value}.
   */
  @VisibleForTesting
  static final String ERROR_MULTIPLE_ELEMENTS_MATCHING_TO_LOWER_CASE_VALUE =
      "has multiple elements matching to lower case value";

  private ConfigurationHelper() {
  }

  /**
   * Given a comma separated list of enum values,
   * trim the list, map to enum values in the message (case insensitive)
   * and return the set.
   * Special handling of "*" meaning: all values.
   * @param key Configuration object key -used in error messages.
   * @param valueString value from Configuration
   * @param enumClass class of enum
   * @param ignoreUnknown should unknown values be ignored?
   * @param <E> enum type
   * @return a mutable set of enum values parsed from the valueString, with any unknown
   * matches stripped if {@code ignoreUnknown} is true.
   * @throws IllegalArgumentException if one of the entries was unknown and ignoreUnknown is false,
   * or there are two entries in the enum which differ only by case.
   */
  @SuppressWarnings("unchecked")
  public static <E extends Enum<E>> EnumSet<E> parseEnumSet(final String key,
      final String valueString,
      final Class<E> enumClass,
      final boolean ignoreUnknown) throws IllegalArgumentException {

    // build a map of lower case string to enum values.
    final Map<String, E> mapping = mapEnumNamesToValues("", enumClass);

    // scan the input string and add all which match
    final EnumSet<E> enumSet = noneOf(enumClass);
    for (String element : getTrimmedStringCollection(valueString)) {
      final String item = element.toLowerCase(Locale.ROOT);
      if ("*".equals(item)) {
        enumSet.addAll(mapping.values());
        continue;
      }
      final E e = mapping.get(item);
      if (e != null) {
        enumSet.add(e);
      } else {
        // no match
        // unless configured to ignore unknown values, raise an exception
        checkArgument(ignoreUnknown, "%s: Unknown option value: %s in list %s."
                + " Valid options for enum class %s are: %s",
            key, element, valueString,
            enumClass.getName(),
            mapping.keySet().stream().collect(Collectors.joining(",")));
      }
    }
    return enumSet;
  }

  /**
   * Given an enum class, build a map of lower case names to values.
   * @param prefix prefix (with trailing ".") for path capabilities probe
   * @param enumClass class of enum
   * @param <E> enum type
   * @return a mutable map of lower case names to enum values
   * @throws IllegalArgumentException if there are two entries which differ only by case.
   */
  public static <E extends Enum<E>> Map<String, E> mapEnumNamesToValues(
      final String prefix,
      final Class<E> enumClass) {
    final E[] constants = enumClass.getEnumConstants();
    Map<String, E> mapping = new HashMap<>(constants.length);
    for (E constant : constants) {
      final String lc = constant.name().toLowerCase(Locale.ROOT);
      final E orig = mapping.put(prefix + lc, constant);
      checkArgument(orig == null,
          "Enum %s "
              + ERROR_MULTIPLE_ELEMENTS_MATCHING_TO_LOWER_CASE_VALUE
              + " %s",
          enumClass, lc);
    }
    return mapping;
  }

  /**
   * Look up an enum from the configuration option and map it to
   * a value in the supplied enum class.
   * If no value is supplied or there is no match for the supplied value,
   * the fallback function is invoked, passing in the trimmed and possibly
   * empty string of the value.
   * Extends {link {@link Configuration#getEnum(String, Enum)}}
   * by adding case independence and a lambda expression for fallback,
   * rather than a default value.
   * @param conf configuration
   * @param name property name
   * @param enumClass classname to resolve
   * @param fallback fallback supplier
   * @param <E> enumeration type.
   * @return an enum value
   * @throws IllegalArgumentException If mapping is illegal for the type provided
   */
  public static <E extends Enum<E>> E resolveEnum(
      Configuration conf,
      String name,
      Class<E> enumClass,
      Function<String, E> fallback) {

    final String val = conf.getTrimmed(name, "");

    // build a map of lower case string to enum values.
    final Map<String, E> mapping = mapEnumNamesToValues("", enumClass);
    final E mapped = mapping.get(val.toLowerCase(Locale.ROOT));
    if (mapped != null) {
      return mapped;
    } else {
      // fallback handles it
      return fallback.apply(val);
    }
  }
}
