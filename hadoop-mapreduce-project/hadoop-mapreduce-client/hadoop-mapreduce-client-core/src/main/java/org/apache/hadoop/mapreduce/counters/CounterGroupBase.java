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

package org.apache.hadoop.mapreduce.counters;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;

/**
 * The common counter group interface.
 *
 * @param <T> type of the counter for the group
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface CounterGroupBase<T extends Counter>
    extends Writable, Iterable<T> {

  /**
   * Get the internal name of the group
   * @return the internal name
   */
  String getName();

  /**
   * Get the display name of the group.
   * @return the human readable name
   */
  String getDisplayName();

  /**
   * Set the display name of the group
   * @param displayName of the group
   */
  void setDisplayName(String displayName);

  /** Add a counter to this group.
   * @param counter to add
   */
  void addCounter(T counter);

  /**
   * Add a counter to this group
   * @param name  of the counter
   * @param displayName of the counter
   * @param value of the counter
   * @return the counter
   */
  T addCounter(String name, String displayName, long value);

  /**
   * Find a counter in the group.
   * @param counterName the name of the counter
   * @param displayName the display name of the counter
   * @return the counter that was found or added
   */
  T findCounter(String counterName, String displayName);

  /**
   * Find a counter in the group
   * @param counterName the name of the counter
   * @param create create the counter if not found if true
   * @return the counter that was found or added or null if create is false
   */
  T findCounter(String counterName, boolean create);

  /**
   * Find a counter in the group.
   * @param counterName the name of the counter
   * @return the counter that was found or added
   */
  T findCounter(String counterName);

  /**
   * @return the number of counters in this group.
   */
  int size();

  /**
   * Increment all counters by a group of counters
   * @param rightGroup  the group to be added to this group
   */
  void incrAllCounters(CounterGroupBase<T> rightGroup);
  
  /**
   * Exposes the underlying group type if a facade.
   * @return the underlying object that this object is wrapping up.
   */
  CounterGroupBase<T> getUnderlyingGroup();
}
