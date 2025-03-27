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

package org.apache.hadoop.util.functional;

import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.classification.InterfaceStability;

/**
 * Tuple support.
 * This allows for tuples to be passed around as part of the public API without
 * committing to a third-party library tuple implementation.
 */
@InterfaceStability.Unstable
public final class Tuples {

  private Tuples() {
  }

  /**
   * Create a 2-tuple.
   * @param key element 1
   * @param value element 2
   * @return a tuple.
   * @param <K> element 1 type
   * @param <V> element 2 type
   */
  public static <K, V> Map.Entry<K, V> pair(final K key, final V value) {
    return new Tuple<>(key, value);
  }

  /**
   * Simple tuple class: uses the Map.Entry interface as other
   * implementations have done, so the API is available across
   * all java versions.
   * @param <K> key
   * @param <V> value
   */
  private static final class Tuple<K, V> implements Map.Entry<K, V> {

    private final K key;

    private final V value;

    private Tuple(final K key, final V value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public K getKey() {
      return key;
    }

    @Override
    public V getValue() {
      return value;
    }

    @Override
    public V setValue(final V value) {
      throw new UnsupportedOperationException("Tuple is immutable");
    }

    @Override
    public String toString() {
      return "(" + key + ", " + value + ')';
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Tuple<?, ?> tuple = (Tuple<?, ?>) o;
      return Objects.equals(key, tuple.key) && Objects.equals(value, tuple.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, value);
    }
  }
}
