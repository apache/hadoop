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
import java.util.Arrays;
import java.util.Objects;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
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
 * The length of the array can be queried with the {@link #arity()}
 * method; the {@code _1(), _2()} and {@code _3()} methods return
 * the first, second and third entries in the array
 * respectively.
 * <p></p>
 * In this context, <i>arity</i> is the number of elements in the array.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
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
   * Min value type: {@value}.
   * <p></p>
   * Arity 1: (min)
   * <p></p>
   * Aggregation {@code min(l._1(), r._1()}
   */
  public static final int IOSTATISTIC_MIN = 1;

  /**
   * Max value type: {@value}.
   * <p></p>
   * Arity 1: (max)
   * <p></p>
   * Aggregation {@code max(l._1(), r._1()}
   */
  public static final int IOSTATISTIC_MAX = 2;

  /**
   * Counter type: {@value}.
   * <p></p>
   * Arity 2: (mean, sample-count)
   * <p></p>
   * Aggregation:
   * {@code (l._1() * l._2() + r._1() * r._2()) / (l._2() + r._2())}
   */
  public static final int IOSTATISTIC_MEAN = 3;

  /**
   * Names of the known types.
   */
  private static final String[] TYPE_NAMES = {
      "counter",
      "min",
      "max",
      "mean"};

  /**
   * Arity of the known types.
   */
  private static final int[] TYPE_ARITY = {1, 1, 1, 2};

  /**
   * Names of the known types.
   */
  private static final String[] TYPE_FORMATS = {
      "counter",
      "min",
      "max",
      "mean"
  };

  /**
   * Serialization number.
   */
  private static final long serialVersionUID = 5925626116768440629L;

  /**
   * Type of statistic.
   */
  private int type;

  /**
   * data array.
   */
  private long[] data;

  /**
   * Instantiate from an array of long values.
   * This includes a verification that the
   * length of the array matches that expected of the given
   * type (if it is known)
   * @param type type of entry
   * @param data values
   */
  public IOStatisticEntry(final int type, final long[] data) {
    checkArgument(type >= 0,
        "type out of range %d", type);
    this.type = type;
    this.data = Objects.requireNonNull(data);
    int len = data.length;
    checkArgument(len > 0,
        "entry data array is empty");
    if (type < TYPE_ARITY.length) {
      checkArgument(len == TYPE_ARITY[type],
          "arity value of %s does not match required value: %s",
          len, TYPE_ARITY[type]);
    }
  }

  /**
   * Empty constructor.
   * This is for serialization rather than general use.
   * Until the type and values are set, the entry is not
   * valid.
   */
  public IOStatisticEntry() {
    type = -1;
    data = new long[0];
  }

  /**
   * Get the type of the entry.
   * @return the type
   */
  public int type() {
    return type;
  }


  /**
   * Get the length of the value array, i.e arity of the instance.
   * @return the length of the data.
   */
  public int arity() {
    return data.length;
  }

  /**
   * Get the array of values.
   * This is a mutable list, not a copy.
   * @return the current list of values.
   */
  public long[] getData() {
    return data;
  }


  /**
   * Require the type value of the instance to
   * be of the specified value.
   * @param r required tyoe.
   * @throws IllegalStateException if the requirement is not met.
   */
  public void requireType(int r) {
    checkState(type == r,
        "Required type=%d actual type=%d",
        r, type);
  }

  /**
   * Required the arity to match that expected.
   * @param r required arity.
   * @throws IllegalStateException if the requirement is not met.
   */
  public void requireArity(int r) {
    checkState(arity() == r,
        "Required length=%d actual length=%d",
        r, arity());
  }

  /**
   * Requre the type and arity of the two entries to match
   * @param that the other entry.
   * @throws IllegalStateException if the requirement is not met.
   */
  public void requireCompatible(IOStatisticEntry that) {
    that.requireTypeAndArity(type, arity());
  }

  /**
   * Require the specific type and arity.
   * @param t type
   * @param a arity
   */
  public void requireTypeAndArity(final int t, final int a) {
    requireType(t);
    requireArity(a);
  }

  /**
   * First entry in the tuple.
   * @return a tuple element value
   * @throws IndexOutOfBoundsException if the array is too short.
   */
  public long _1() {
    return data[0];
  }

  /**
   * Second entry in the tuple.
   * @return a tuple element value
   * @throws IndexOutOfBoundsException if the array is too short.
   */
  public long _2() {
    return data[1];
  }

  /**
   * Third entry in the tuple.
   * @return a tuple element value
   * @throws IndexOutOfBoundsException if the array is too short.
   */
  public long _3() {
    return data[2];
  }

  /**
   * Get the single tuple value, verifying
   * that the type is as expected.
   * @param requiredType required type
   * @return the value
   */
  public long scalar(int requiredType) {
    requireArity(1);
    requireType(requiredType);
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
    sb.append("(").append(typeAsString());
    sb.append(", (").append(join(data, ','));
    sb.append(')');
    return sb.toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) { return true; }
    if (o == null || getClass() != o.getClass()) { return false; }
    IOStatisticEntry that = (IOStatisticEntry) o;
    return type == that.type &&
        Arrays.equals(data, that.data);
  }

  /**
   * Entry with data varags.
   * @param t type
   * @param d data
   * @return new entry
   */
  public static IOStatisticEntry statsEntry(int t, long...d) {
    return new IOStatisticEntry(t, d);
  }

}
