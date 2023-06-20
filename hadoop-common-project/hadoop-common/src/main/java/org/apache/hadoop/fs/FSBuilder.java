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

package org.apache.hadoop.fs;

import javax.annotation.Nonnull;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * The base interface which various FileSystem FileContext Builder
 * interfaces can extend, and which underlying implementations
 * will then implement.
 * <p>
 * HADOOP-16202 expanded the opt() and must() arguments with
 * operator overloading, but HADOOP-18724 identified mapping problems:
 * passing a long value in to {@code opt()} could end up invoking
 * {@code opt(string, double)}, which could then trigger parse failures.
 * <p>
 * To fix this without forcing existing code to break/be recompiled.
 * <ol>
 *   <li>A new method to explicitly set a long value is added:
 *   {@link #optLong(String, long)}
 *   </li>
 *   <li>A new method to explicitly set a double value is added:
 *   {@link #optLong(String, long)}
 *   </li>
 *   <li>
 *     All of {@link #opt(String, long)}, {@link #opt(String, float)} and
 *     {@link #opt(String, double)} invoke {@link #optLong(String, long)}.
 *   </li>
 *   <li>
 *     The same changes have been applied to {@code must()} methods.
 *   </li>
 * </ol>
 *   The forwarding of existing double/float setters to the long setters ensure
 *   that existing code will link, but are guaranteed to always set a long value.
 *   If you need to write code which works correctly with all hadoop releases,
 *   covert the option to a string explicitly and then call {@link #opt(String, String)}
 *   or {@link #must(String, String)} as appropriate.
 *
 * @param <S> Return type on the {@link #build()} call.
 * @param <B> type of builder itself.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface FSBuilder<S, B extends FSBuilder<S, B>> {

  /**
   * Set optional Builder parameter.
   * @param key key.
   * @param value value.
   * @return generic type B.
   */
  B opt(@Nonnull String key, @Nonnull String value);

  /**
   * Set optional boolean parameter for the Builder.
   * @param key key.
   * @param value value.
   * @return generic type B.
   * @see #opt(String, String)
   */
  default B opt(@Nonnull String key, boolean value) {
    return opt(key, Boolean.toString(value));
  }

  /**
   * Set optional int parameter for the Builder.
   *
   * @param key key.
   * @param value value.
   * @return generic type B.
   * @see #opt(String, String)
   */
  default B opt(@Nonnull String key, int value) {
    return optLong(key, value);
  }

  /**
   * This parameter is converted to a long and passed
   * to {@link #optLong(String, long)} -all
   * decimal precision is lost.
   *
   * @param key key.
   * @param value value.
   * @return generic type B.
   * @see #opt(String, String)
   * @deprecated use {@link #optDouble(String, double)}
   */
  @Deprecated
  default B opt(@Nonnull String key, float value) {
    return optLong(key, (long) value);
  }

  /**
   * Set optional long parameter for the Builder.
   *
   * @param key key.
   * @param value value.
   * @return generic type B.
   * @deprecated use  {@link #optLong(String, long)} where possible.
   */
  default B opt(@Nonnull String key, long value) {
    return optLong(key, value);
  }

  /**
   * Pass an optional double parameter for the Builder.
   * This parameter is converted to a long and passed
   * to {@link #optLong(String, long)} -all
   * decimal precision is lost.
   * @param key key.
   * @param value value.
   * @return generic type B.
   * @see #opt(String, String)
   * @deprecated use {@link #optDouble(String, double)}
   */
  @Deprecated
  default B opt(@Nonnull String key, double value) {
    return optLong(key, (long) value);
  }

  /**
   * Set an array of string values as optional parameter for the Builder.
   *
   * @param key key.
   * @param values values.
   * @return generic type B.
   * @see #opt(String, String)
   */
  B opt(@Nonnull String key, @Nonnull String... values);

  /**
   * Set optional long parameter for the Builder.
   *
   * @param key key.
   * @param value value.
   * @return generic type B.
   * @see #opt(String, String)
   */
  default B optLong(@Nonnull String key, long value) {
    return opt(key, Long.toString(value));
  }

  /**
   * Set optional double parameter for the Builder.
   *
   * @param key key.
   * @param value value.
   * @return generic type B.
   * @see #opt(String, String)
   */
  default B optDouble(@Nonnull String key, double value) {
    return opt(key, Double.toString(value));
  }

  /**
   * Set mandatory option to the Builder.
   *
   * If the option is not supported or unavailable,
   * the client should expect {@link #build()} throws IllegalArgumentException.
   *
   * @param key key.
   * @param value value.
   * @return generic type B.
   */
  B must(@Nonnull String key, @Nonnull String value);

  /**
   * Set mandatory boolean option.
   *
   * @param key key.
   * @param value value.
   * @return generic type B.
   * @see #must(String, String)
   */
  default B must(@Nonnull String key, boolean value) {
    return must(key, Boolean.toString(value));
  }

  /**
   * Set mandatory int option.
   *
   * @param key key.
   * @param value value.
   * @return generic type B.
   * @see #must(String, String)
   */
  default B must(@Nonnull String key, int value) {
    return mustLong(key, value);
  }

  /**
   * This parameter is converted to a long and passed
   * to {@link #mustLong(String, long)} -all
   * decimal precision is lost.
   *
   * @param key key.
   * @param value value.
   * @return generic type B.
   * @deprecated use {@link #mustDouble(String, double)} to set floating point.
   */
  @Deprecated
  default B must(@Nonnull String key, float value) {
    return mustLong(key, (long) value);
  }

  /**
   * Set mandatory long option.
   *
   * @param key key.
   * @param value value.
   * @return generic type B.
   * @see #must(String, String)
   */
  @Deprecated
  default B must(@Nonnull String key, long value) {
    return mustLong(key, (long) value);
  }

  /**
   * Set mandatory long option, despite passing in a floating
   * point value.
   *
   * @param key key.
   * @param value value.
   * @return generic type B.
   * @see #must(String, String)
   */
  @Deprecated
  default B must(@Nonnull String key, double value) {
    return mustLong(key, (long) value);
  }

  /**
   * Set a string array as mandatory option.
   *
   * @param key key.
   * @param values values.
   * @return generic type B.
   * @see #must(String, String)
   */
  B must(@Nonnull String key, @Nonnull String... values);

  /**
   * Set mandatory long parameter for the Builder.
   *
   * @param key key.
   * @param value value.
   * @return generic type B.
   * @see #opt(String, String)
   */
  default B mustLong(@Nonnull String key, long value) {
    return must(key, Long.toString(value));
  }

  /**
   * Set mandatory double parameter for the Builder.
   *
   * @param key key.
   * @param value value.
   * @return generic type B.
   * @see #opt(String, String)
   */
  default B mustDouble(@Nonnull String key, double value) {
    return must(key, Double.toString(value));
  }

  /**
   * Instantiate the object which was being built.
   *
   * @throws IllegalArgumentException if the parameters are not valid.
   * @throws UnsupportedOperationException if the filesystem does not support
   * the specific operation.
   * @throws IOException on filesystem IO errors.
   * @return generic type S.
   */
  S build() throws IllegalArgumentException,
      UnsupportedOperationException, IOException;
}
