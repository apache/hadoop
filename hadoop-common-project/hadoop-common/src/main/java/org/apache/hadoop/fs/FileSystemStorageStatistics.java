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
package org.apache.hadoop.fs;

import java.util.EnumMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A basic StorageStatistics instance which simply returns data from
 * FileSystem#Statistics.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FileSystemStorageStatistics
    extends StorageStatistics
    implements Iterable<StorageStatistics.LongStatistic> {

  private class LongStatisticIterator implements Iterator<LongStatistic> {

    private final Iterator<Map.Entry<Statistic, AtomicLong>> iterator =
        opsCount.entrySet().iterator();

    @Override
    public boolean hasNext() {
      return this.iterator.hasNext();
    }

    @Override
    public LongStatistic next() {
      Map.Entry<Statistic, AtomicLong> entry = iterator.next();
      return new LongStatistic(
          entry.getKey().getSymbol(), entry.getValue().get());
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private final Map<Statistic, AtomicLong> opsCount =
      new EnumMap<>(Statistic.class);

  FileSystemStorageStatistics(String name) {
    super(name);
    for (Statistic opType : Statistic.VALUES) {
      this.opsCount.put(opType, new AtomicLong(0));
    }
  }

  /**
   * Increment a specific counter.
   * @param op operation
   * @param count increment value
   * @return the new value
   */
  public long incrementCounter(Statistic op, long count) {
    return opsCount.get(op).addAndGet(count);
  }

  @Override
  public String getScheme() {
    return getName();
  }

  @Override
  public Iterator<LongStatistic> getLongStatistics() {
    return new LongStatisticIterator();
  }

  @Override
  public Iterator<LongStatistic> iterator() {
    return getLongStatistics();
  }

  @Override
  public Long getLong(String key) {
    Statistic type = Statistic.fromSymbol(key);
    return type == null ? null : getLong(type);
  }

  public long getLong(Statistic op) {
    return opsCount.get(op).get();
  }

  /**
   * Return true if a statistic is being tracked.
   *
   * @return         True only if the statistic is being tracked.
   */
  @Override
  public boolean isTracked(String key) {
    return Statistic.fromSymbol(key) != null;
  }

  @Override
  public void reset() {
    for (AtomicLong value : opsCount.values()) {
      value.set(0);
    }
  }
}
