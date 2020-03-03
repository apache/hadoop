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

package org.apache.hadoop.fs.statistics.impl;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.statistics.IOStatistics;

/**
 * Snapshotted IO statistics; will update on a call to snapshot().
 */
class SnapshotIOStatistics implements IOStatistics {

  /**
   * Treemaps sort their insertions so the iterator is ordered.
   */
  private final Map<String, Long> entries
      = new TreeMap<>();

  /**
   * Snapshot source.
   */
  private final IOStatistics source;

  SnapshotIOStatistics(final IOStatistics source) {
    this.source = source;
    snapshot();
  }

  @Override
  public Long getLong(final String key) {
    return entries.get(key);
  }

  @Override
  public boolean isTracked(final String key) {
    return false;
  }

  @Override
  public Iterator<Map.Entry<String, Long>> iterator() {
    return entries.entrySet().iterator();
  }

  @Override
  public boolean hasAttribute(final Attributes attr) {
    return Attributes.Snapshotted == attr;
  }

  @Override
  public synchronized boolean snapshot() {
    entries.clear();
    for (Map.Entry<String, Long> sourceEntry : source) {
      entries.put(sourceEntry.getKey(), sourceEntry.getValue());
    }
    return true;
  }
}
