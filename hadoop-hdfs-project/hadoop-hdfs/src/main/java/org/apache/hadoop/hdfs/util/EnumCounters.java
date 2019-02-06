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

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;

/**
 * Counters for an enum type.
 * 
 * For example, suppose there is an enum type
 * <pre>
 * enum Fruit { APPLE, ORANGE, GRAPE }
 * </pre>
 * An {@link EnumCounters} object can be created for counting the numbers of
 * APPLE, ORANGE and GRAPE.
 *
 * @param <E> the enum type
 */
public class EnumCounters<E extends Enum<E>> {
  /** The class of the enum. */
  private final Class<E> enumClass;
  /** An array of longs corresponding to the enum type. */
  private final long[] counters;

  /**
   * Construct counters for the given enum constants.
   * @param enumClass the enum class of the counters.
   */
  public EnumCounters(final Class<E> enumClass) {
    final E[] enumConstants = enumClass.getEnumConstants();
    Preconditions.checkNotNull(enumConstants);
    this.enumClass = enumClass;
    this.counters = new long[enumConstants.length];
  }

  public EnumCounters(final Class<E> enumClass, long defaultVal) {
    final E[] enumConstants = enumClass.getEnumConstants();
    Preconditions.checkNotNull(enumConstants);
    this.enumClass = enumClass;
    this.counters = new long[enumConstants.length];
    reset(defaultVal);
  }
  
  /** @return the value of counter e. */
  public final long get(final E e) {
    return counters[e.ordinal()];
  }

  /** @return the values of counter as a shadow copy of array*/
  public long[] asArray() {
    return ArrayUtils.clone(counters);
  }

  /** Negate all counters. */
  public final void negation() {
    for(int i = 0; i < counters.length; i++) {
      counters[i] = -counters[i];
    }
  }
  
  /** Set counter e to the given value. */
  public final void set(final E e, final long value) {
    counters[e.ordinal()] = value;
  }

  /** Set this counters to that counters. */
  public final void set(final EnumCounters<E> that) {
    for(int i = 0; i < counters.length; i++) {
      this.counters[i] = that.counters[i];
    }
  }

  /** Reset all counters to zero. */
  public final void reset() {
    reset(0L);
  }

  /** Add the given value to counter e. */
  public final void add(final E e, final long value) {
    counters[e.ordinal()] += value;
  }

  /** Add that counters to this counters. */
  public final void add(final EnumCounters<E> that) {
    for(int i = 0; i < counters.length; i++) {
      this.counters[i] += that.counters[i];
    }
  }

  /** Subtract the given value from counter e. */
  public final void subtract(final E e, final long value) {
    counters[e.ordinal()] -= value;
  }

  /** Subtract this counters from that counters. */
  public final void subtract(final EnumCounters<E> that) {
    for(int i = 0; i < counters.length; i++) {
      this.counters[i] -= that.counters[i];
    }
  }
  
  /** @return the sum of all counters. */
  public final long sum() {
    long sum = 0;
    for(int i = 0; i < counters.length; i++) {
      sum += counters[i];
    }
    return sum;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (obj == null || !(obj instanceof EnumCounters)) {
      return false;
    }
    final EnumCounters<?> that = (EnumCounters<?>)obj;
    return this.enumClass == that.enumClass
        && Arrays.equals(this.counters, that.counters);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(counters);
  }

  @Override
  public String toString() {
    final E[] enumConstants = enumClass.getEnumConstants();
    final StringBuilder b = new StringBuilder();
    for(int i = 0; i < counters.length; i++) {
      final String name = enumConstants[i].name();
      b.append(name).append("=").append(counters[i]).append(", ");
    }
    return b.substring(0, b.length() - 2);
  }

  public final void reset(long val) {
    for(int i = 0; i < counters.length; i++) {
      this.counters[i] = val;
    }
  }

  public boolean allLessOrEqual(long val) {
    for (long c : counters) {
      if (c > val) {
        return false;
      }
    }
    return true;
  }

  public boolean anyGreaterOrEqual(long val) {
    for (long c: counters) {
      if (c >= val) {
        return true;
      }
    }
    return false;
  }
}
