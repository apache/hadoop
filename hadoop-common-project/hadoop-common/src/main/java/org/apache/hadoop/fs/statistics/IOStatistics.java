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

import java.util.Map;

import org.apache.hadoop.classification.InterfaceStability;

/**
 * IO Statistics.
 * <p>
 *   These are low-cost per-instance statistics provided by any IO components.
 * <p>
 * The statistics MUST BE for the specific instance of the source;
 * possibly including aggregate statistics from other objects
 * created by that stores.
 * For example, the statistics of a filesystem instance must be unique
 * to that instant and not shared with any other.
 * However, those statistics may also collect and aggregate statistics
 * generated in the use of input and output streams created by that
 * file system instance.
 *
 * <p>
 * The iterator is a possibly empty iterator over all monitored statistics.
 * <ol>
 *   <li>
 *     The attributes of an instance can be probed for with
 *     {@link #hasAttribute(Attributes)}.
 *   </li>
 *   <li>
 *     The set of statistic keys SHOULD remain unchanged, and MUST NOT
 *     ever remove keys.
 *   </li>
 *     The statistics MAY BE dynamic: every call to {@code iterator()}
 *     MAY return a current/recent set of statistics.
 *     This
 *   </li>
 *   <li>
 *     The values MAY change across invocations of {@code iterator()}.
 *   </li>
 *   <li>
 *     The update MAY be in the iterable() call, or MAY be in the actual
 *     Iterable.next() operation.
 *   </li>
 *   <li>
 *     The returned Map.Entry instances MUST return the same value on
 *     repeated getValue() calls.
 *   </li>
 *   <li>
 *     Queries of statistics SHOULD Be fast and Nonblocking to the extent
 *     that if invoked during a long operation, they will prioritize
 *     returning fast over most timely values.
 *   </li>
 *   <li>
 *     The statistics MAY lag; especially for statistics collected in separate
 *     operations (e.g stream IO statistics as provided by a filesystem
 *     instance).
 *   </li>
 *   <li>
 *     Thread safety: an instance of IOStatistics can be shared across threads;
 *     a call to @code iterator()} is thread safe.
 *     The actual Iterable returned MUST NOT be shared across threads.
 *   </li>
 *   <li>
 *     If the instance declares that it has the attribute {@link Attributes#Snapshotted},
 *     then it will take a snapshot of the attribute values in the call {@link #snapshot()}.
 *     These values MUST NOT change until a subsequent snapshot() operation.
 *   </li>
 *   <li>
 *     A snapshot MAY NOT be consistent, i.e. during the snapshot operation
 *     the underlying values may change.
 *   </li>
 *
 * </ol>
 */
@InterfaceStability.Unstable
public interface IOStatistics extends Iterable<Map.Entry<String, Long>> {

  /**
   * Get the value of a statistic.
   *
   * @return null if the statistic is not being tracked or is not a
   *                 long statistic. The value of the statistic, otherwise.
   */
  Long getLong(String key);

  /**
   * Return true if a statistic is being tracked.
   *
   * @return True only if the statistic is being tracked.
   */
  boolean isTracked(String key);

  /**
   * Probe for an attribute of this statistics set.
   * @return attributes.
   */
  default boolean hasAttribute(Attributes attr) {
    return false;
  }

  /**
   * Create a snapshot; no-op if not supported.
   * @return true if this call had any effect
   */
  default boolean snapshot() {
    return false;
  }

  /**
   * Possible attributes of the statistics.
   * This is very limited right now
   */
  enum Attributes {
    /** The statistics are dynamic: when you re-read a value it may change. */
    Dynamic,
    /**
     * The statistics are actually snapshots, updated when you call snapshot(),
     * or iterator();
     */
    Snapshotted
  }
}
