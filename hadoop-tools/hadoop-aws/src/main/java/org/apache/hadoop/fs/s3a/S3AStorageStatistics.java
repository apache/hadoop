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

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Storage statistics for S3A.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class S3AStorageStatistics extends StorageStatistics
    implements Iterable<StorageStatistics.LongStatistic> {
  private static final Logger LOG =
      LoggerFactory.getLogger(S3AStorageStatistics.class);

  public static final String NAME = "S3AStorageStatistics";
  private final Map<Statistic, AtomicLong> opsCount =
      new EnumMap<>(Statistic.class);

  public S3AStorageStatistics() {
    super(NAME);
    for (Statistic opType : Statistic.values()) {
      opsCount.put(opType, new AtomicLong(0));
    }
  }

  /**
   * Increment a specific counter.
   * @param op operation
   * @param count increment value
   * @return the new value
   */
  public long incrementCounter(Statistic op, long count) {
    long updated = opsCount.get(op).addAndGet(count);
    LOG.debug("{} += {}  ->  {}", op, count, updated);
    return updated;
  }

  private class LongIterator implements Iterator<LongStatistic> {
    private Iterator<Map.Entry<Statistic, AtomicLong>> iterator =
        Collections.unmodifiableSet(opsCount.entrySet()).iterator();

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public LongStatistic next() {
      if (!iterator.hasNext()) {
        throw new NoSuchElementException();
      }
      final Map.Entry<Statistic, AtomicLong> entry = iterator.next();
      return new LongStatistic(entry.getKey().getSymbol(),
          entry.getValue().get());
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public String getScheme() {
    return "s3a";
  }

  @Override
  public Iterator<LongStatistic> getLongStatistics() {
    return new LongIterator();
  }

  @Override
  public Iterator<LongStatistic> iterator() {
    return getLongStatistics();
  }

  @Override
  public Long getLong(String key) {
    final Statistic type = Statistic.fromSymbol(key);
    return type == null ? null : opsCount.get(type).get();
  }

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
