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

package org.apache.hadoop.fs.statistics;

import java.io.Serializable;
import java.util.Objects;

import com.google.common.base.Preconditions;

import static org.apache.commons.lang3.StringUtils.join;

/**
 * Entry of an IOStatistic.
 * <p></p>
 * it's a pair of (type, values[]) such that it is
 * trivially serializable and we can add new types
 * with multiple entries.
 * <p></p>
 * For example, the data for a mean value would be
 * (mean, sample count);
 * for a maximum it would just be the max value.
 * What is key is that each entry MUST provide all
 * the data needed to aggregate two entries together.
 * <p></p>
 * This isn't perfect, as we'd really want a union of types,
 * so doubles could be passed round too.
 * The length of the array can be queried with the {@link #length()}
 * method; the {@code _1(), _2()} and {@code _3()} methods return
 * the first, second and third entries in the array
 * respectively.
 * <p></p>
 * In this context, <i>arity</i> is the number of elements in the array.
 */
public final class IOStatisticEntry implements Serializable {

  /**
   * Counter type: {@value}.
   * <p></p>
   * Arity 1: (count)
   * <p></p>
   * aggregation {@code l._1() + r._1()}
   */
  public static final int IOSTATISTIC_COUNTER = 0;

  /**
   * Counter type: {@value}.
   * <p></p>
   * Arity 2: (mean, sample-count)
   * <p></p>
   * Aggregation:
   * {@code (l._1() * l._2() + r._1() * r._2()) / (l._2() + r._2())}
   */
  public static final int IOSTATISTIC_MEAN = 1;

  /**
   * Max value type: {@value}.
   * <p></p>
   * Arity 1: (max)
   * <p></p>
   * Aggregation {@code max(l._1(), r._1()}
   */
  public static final int IOSTATISTIC_MAX = 2;

  /**
   * Min value type: {@value}.
   * <p></p>
   * Arity 1: (min)
   * <p></p>
   * Aggregation {@code min(l._1(), r._1()}
   */
  public static final int IOSTATISTIC_MIN = 3;

  /**
   * Names of the originally defined types.
   */
  private static final String[] TYPE_NAMES = new String[]{
      "counter",
      "mean",
      "max",
      "min"
  };

  /**
   * Arity of the defined types.
   */
  private static final int[] TYPE_ARITY = new int[]{1, 2, 1, 1};

  private static final long serialVersionUID = 5925626116768440629L;

  /**
   * Type of statistic.
   */
  private int type;

  /**
   * value array.
   */
  private long[] values;

  /**
   * Instantiate from an array of values.
   * This includes a verification that the
   * length of the array matches that expected of the given
   * type (if it is known)
   * @param type type of entry
   * @param values values
   */
  public IOStatisticEntry(final int type, final long[] values) {
    Preconditions.checkArgument(type >= 0,
        "type out of range %d", type);
    this.type = type;
    this.values = Objects.requireNonNull(values);
    int len = values.length;
    Preconditions.checkArgument(len > 0,
        "entry value array is empty");
    if (type < TYPE_ARITY.length) {
      Preconditions.checkArgument(len == TYPE_ARITY[type],
          "arity value of %d does not match required value: %d",
          len, TYPE_ARITY[type]);
    }
  }

  public IOStatisticEntry() {
  }

  public int getType() {
    return type;
  }

  public long[] getValues() {
    return values;
  }

  public int length() {
    return values.length;
  }

  public void requireType(int r) {
    Preconditions.checkState(type == r,
        "Required type=%d actual type=%d",
        r, type);
  }

  public void requireLength(int r) {
    Preconditions.checkState(length() == r,
        "Required length=%d actual length=%d",
        r, length());
  }

  public long _1() {
    return values[0];
  }

  public long _2() {
    return values[1];
  }

  public long _3() {
    return values[2];
  }

  /**
   * Get the first value in the array, verifying
   * that the type is as expected.
   * @param type required type
   * @return the value
   */
  public long singleValue(int type) {
    requireLength(1);
    requireType(type);
    return _1();
  }

  /**
   * Type to string; if it is a known type this
   * will be a meaningful string.
   * @return a value for logging.
   */
  public String typeAsString() {
    return type < TYPE_NAMES.length
        ? TYPE_NAMES[type]
        : Integer.toString(type);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("(type=").append(typeAsString());
    sb.append(", (").append(join(values, ','));
    sb.append(')');
    return sb.toString();
  }

  /**
   * Entry of arity 1.
   * @param t type
   * @param v value
   * @return new entry
   */
  public static IOStatisticEntry entry(int t, long...v) {
    return new IOStatisticEntry(t, v);
  }

}
