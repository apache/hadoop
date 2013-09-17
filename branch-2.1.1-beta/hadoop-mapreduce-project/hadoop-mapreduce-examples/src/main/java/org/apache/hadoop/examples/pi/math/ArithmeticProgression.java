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
package org.apache.hadoop.examples.pi.math;

import org.apache.hadoop.examples.pi.Util;

/** An arithmetic progression */
public class ArithmeticProgression implements Comparable<ArithmeticProgression> {
  /** A symbol */
  public final char symbol;
  /** Starting value */
  public final long value;
  /** Difference between terms */
  public final long delta;
  /** Ending value */
  public final long limit;

  /** Constructor */
  public ArithmeticProgression(char symbol, long value, long delta, long limit) {
    if (delta == 0)
      throw new IllegalArgumentException("delta == 0");

    this.symbol = symbol;
    this.value = value;
    this.delta = delta;
    this.limit = limit;
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    else if (obj != null && obj instanceof ArithmeticProgression) {
      final ArithmeticProgression that = (ArithmeticProgression)obj;
      if (this.symbol != that.symbol)
        throw new IllegalArgumentException("this.symbol != that.symbol, this="
            + this + ", that=" + that);
      return this.value == that.value
          && this.delta == that.delta
          && this.limit == that.limit;
    }
    throw new IllegalArgumentException(obj == null? "obj == null":
      "obj.getClass()=" + obj.getClass());
  }

  /** Not supported */
  public int hashCode() {
    throw new UnsupportedOperationException();
  }

  /** {@inheritDoc} */
  @Override
  public int compareTo(ArithmeticProgression that) {
    if (this.symbol != that.symbol)
      throw new IllegalArgumentException("this.symbol != that.symbol, this="
          + this + ", that=" + that);
    if (this.delta != that.delta)
      throw new IllegalArgumentException("this.delta != that.delta, this="
          + this + ", that=" + that);
    final long d = this.limit - that.limit;
    return d > 0? 1: d == 0? 0: -1;
  }

  /** Does this contain that? */
  boolean contains(ArithmeticProgression that) {
    if (this.symbol != that.symbol)
      throw new IllegalArgumentException("this.symbol != that.symbol, this="
          + this + ", that=" + that);
    if (this.delta == that.delta) {
      if (this.value == that.value)
        return this.getSteps() >= that.getSteps();
      else if (this.delta < 0)
        return this.value > that.value && this.limit <= that.limit;
      else if (this.delta > 0)
        return this.value < that.value && this.limit >= that.limit;
    }
    return false;    
  }

  /** Skip some steps */
  long skip(long steps) {
    if (steps < 0)
      throw new IllegalArgumentException("steps < 0, steps=" + steps);
    return value + steps*delta; 
  }

  /** Get the number of steps */
  public long getSteps() {
    return (limit - value)/delta;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return symbol + ":value=" + value + ",delta=" + delta + ",limit=" + limit;
  }

  /** Convert a String to an ArithmeticProgression. */
  static ArithmeticProgression valueOf(final String s) {
    int i = 2;
    int j = s.indexOf(",delta=");
    final long value = Util.parseLongVariable("value", s.substring(2, j));
    i = j + 1;
    j = s.indexOf(",limit=");
    final long delta = Util.parseLongVariable("delta", s.substring(i, j));
    i = j + 1;
    final long limit = Util.parseLongVariable("limit", s.substring(i));
    return new ArithmeticProgression(s.charAt(0), value, delta, limit);
  }
}