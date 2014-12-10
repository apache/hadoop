/**
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

package org.apache.hadoop.lib.util;

import org.apache.hadoop.classification.InterfaceAudience;

import java.text.MessageFormat;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Utility methods to check preconditions.
 * <p>
 * Commonly used for method arguments preconditions.
 */
@InterfaceAudience.Private
public class Check {

  /**
   * Verifies a variable is not NULL.
   *
   * @param obj the variable to check.
   * @param name the name to use in the exception message.
   *
   * @return the variable.
   *
   * @throws IllegalArgumentException if the variable is NULL.
   */
  public static <T> T notNull(T obj, String name) {
    if (obj == null) {
      throw new IllegalArgumentException(name + " cannot be null");
    }
    return obj;
  }

  /**
   * Verifies a list does not have any NULL elements.
   *
   * @param list the list to check.
   * @param name the name to use in the exception message.
   *
   * @return the list.
   *
   * @throws IllegalArgumentException if the list has NULL elements.
   */
  public static <T> List<T> notNullElements(List<T> list, String name) {
    notNull(list, name);
    for (int i = 0; i < list.size(); i++) {
      notNull(list.get(i), MessageFormat.format("list [{0}] element [{1}]", name, i));
    }
    return list;
  }

  /**
   * Verifies a string is not NULL and not emtpy
   *
   * @param str the variable to check.
   * @param name the name to use in the exception message.
   *
   * @return the variable.
   *
   * @throws IllegalArgumentException if the variable is NULL or empty.
   */
  public static String notEmpty(String str, String name) {
    if (str == null) {
      throw new IllegalArgumentException(name + " cannot be null");
    }
    if (str.length() == 0) {
      throw new IllegalArgumentException(name + " cannot be empty");
    }
    return str;
  }

  /**
   * Verifies a string list is not NULL and not emtpy
   *
   * @param list the list to check.
   * @param name the name to use in the exception message.
   *
   * @return the variable.
   *
   * @throws IllegalArgumentException if the string list has NULL or empty
   * elements.
   */
  public static List<String> notEmptyElements(List<String> list, String name) {
    notNull(list, name);
    for (int i = 0; i < list.size(); i++) {
      notEmpty(list.get(i), MessageFormat.format("list [{0}] element [{1}]", name, i));
    }
    return list;
  }

  private static final String IDENTIFIER_PATTERN_STR = "[a-zA-z_][a-zA-Z0-9_\\-]*";

  private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("^" + IDENTIFIER_PATTERN_STR + "$");

  /**
   * Verifies a value is a valid identifier,
   * <code>[a-zA-z_][a-zA-Z0-9_\-]*</code>, up to a maximum length.
   *
   * @param value string to check if it is a valid identifier.
   * @param maxLen maximun length.
   * @param name the name to use in the exception message.
   *
   * @return the value.
   *
   * @throws IllegalArgumentException if the string is not a valid identifier.
   */
  public static String validIdentifier(String value, int maxLen, String name) {
    Check.notEmpty(value, name);
    if (value.length() > maxLen) {
      throw new IllegalArgumentException(
        MessageFormat.format("[{0}] = [{1}] exceeds max len [{2}]", name, value, maxLen));
    }
    if (!IDENTIFIER_PATTERN.matcher(value).find()) {
      throw new IllegalArgumentException(
        MessageFormat.format("[{0}] = [{1}] must be '{2}'", name, value, IDENTIFIER_PATTERN_STR));
    }
    return value;
  }

  /**
   * Verifies an integer is greater than zero.
   *
   * @param value integer value.
   * @param name the name to use in the exception message.
   *
   * @return the value.
   *
   * @throws IllegalArgumentException if the integer is zero or less.
   */
  public static int gt0(int value, String name) {
    return (int) gt0((long) value, name);
  }

  /**
   * Verifies an long is greater than zero.
   *
   * @param value long value.
   * @param name the name to use in the exception message.
   *
   * @return the value.
   *
   * @throws IllegalArgumentException if the long is zero or less.
   */
  public static long gt0(long value, String name) {
    if (value <= 0) {
      throw new IllegalArgumentException(
        MessageFormat.format("parameter [{0}] = [{1}] must be greater than zero", name, value));
    }
    return value;
  }

  /**
   * Verifies an integer is greater or equal to zero.
   *
   * @param value integer value.
   * @param name the name to use in the exception message.
   *
   * @return the value.
   *
   * @throws IllegalArgumentException if the integer is greater or equal to zero.
   */
  public static int ge0(int value, String name) {
    return (int) ge0((long) value, name);
  }

  /**
   * Verifies an long is greater or equal to zero.
   *
   * @param value integer value.
   * @param name the name to use in the exception message.
   *
   * @return the value.
   *
   * @throws IllegalArgumentException if the long is greater or equal to zero.
   */
  public static long ge0(long value, String name) {
    if (value < 0) {
      throw new IllegalArgumentException(MessageFormat.format(
        "parameter [{0}] = [{1}] must be greater than or equals zero", name, value));
    }
    return value;
  }

}
