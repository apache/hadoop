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
package org.apache.hadoop.hdfs.util;

/**
 * Const Counters for an enum type.
 *
 * It's the const version of EnumCounters. Any modification ends with a
 * ConstEnumException.
 *
 * @see org.apache.hadoop.hdfs.util.EnumCounters
 */
public class ConstEnumCounters<E extends Enum<E>> extends EnumCounters<E> {

  /**
   * An exception class for modification on ConstEnumCounters.
   */
  public static final class ConstEnumException extends RuntimeException {
    private ConstEnumException(String msg) {
      super(msg);
    }
  }

  /**
   * Throwing this exception if any modification occurs.
   */
  private static final ConstEnumException CONST_ENUM_EXCEPTION =
      new ConstEnumException("modification on const.");

  public ConstEnumCounters(Class<E> enumClass, long defaultVal) {
    super(enumClass);
    forceReset(defaultVal);
  }

  @Override
  public final void negation() {
    throw CONST_ENUM_EXCEPTION;
  }

  @Override
  public final void set(final E e, final long value) {
    throw CONST_ENUM_EXCEPTION;
  }

  @Override
  public final void set(final EnumCounters<E> that) {
    throw CONST_ENUM_EXCEPTION;
  }

  @Override
  public final void reset() {
    throw CONST_ENUM_EXCEPTION;
  }

  @Override
  public final void add(final E e, final long value) {
    throw CONST_ENUM_EXCEPTION;
  }

  @Override
  public final void add(final EnumCounters<E> that) {
    throw CONST_ENUM_EXCEPTION;
  }

  @Override
  public final void subtract(final E e, final long value) {
    throw CONST_ENUM_EXCEPTION;
  }

  @Override
  public final void subtract(final EnumCounters<E> that) {
    throw CONST_ENUM_EXCEPTION;
  }

  @Override
  public final void reset(long val) {
    throw CONST_ENUM_EXCEPTION;
  }

  private void forceReset(long val) {
    super.reset(val);
  }
}
