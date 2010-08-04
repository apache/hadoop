/**
 * Copyright 2010 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.util;

import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;

/**
 * A generic, immutable class for pairs of objects both of type <code>T</code>.
 * @param <T>
 * @See {@link Pair} if Types differ.
 */
public class PairOfSameType<T> implements Iterable<T> {
  private final T first;
  private final T second;

  /**
   * Constructor
   * @param a operand
   * @param b operand
   */
  public PairOfSameType(T a, T b) {
    this.first = a;
    this.second = b;
  }

  /**
   * Return the first element stored in the pair.
   * @return T
   */
  public T getFirst() {
    return first;
  }

  /**
   * Return the second element stored in the pair.
   * @return T
   */
  public T getSecond() {
    return second;
  }

  private static boolean equals(Object x, Object y) {
     return (x == null && y == null) || (x != null && x.equals(y));
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean equals(Object other) {
    return other instanceof PairOfSameType &&
      equals(first, ((PairOfSameType)other).first) &&
      equals(second, ((PairOfSameType)other).second);
  }

  @Override
  public int hashCode() {
    if (first == null)
      return (second == null) ? 0 : second.hashCode() + 1;
    else if (second == null)
      return first.hashCode() + 2;
    else
      return first.hashCode() * 17 + second.hashCode();
  }

  @Override
  public String toString() {
    return "{" + getFirst() + "," + getSecond() + "}";
  }

  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      private int returned = 0;

      @Override
      public boolean hasNext() {
        return this.returned < 2;
      }

      @Override
      public T next() {
        if (++this.returned == 1) return getFirst();
        else if (this.returned == 2) return getSecond();
        else throw new IllegalAccessError("this.returned=" + this.returned);
      }

      @Override
      public void remove() {
        throw new NotImplementedException();
      }
    };
  }
}