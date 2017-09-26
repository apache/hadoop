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

package org.apache.hadoop.metrics2.util;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;

import com.google.common.collect.Maps;

/**
 * A metrics cache for sinks that don't support sparse updates.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MetricsCache {
  static final Log LOG = LogFactory.getLog(MetricsCache.class);
  static final int MAX_RECS_PER_NAME_DEFAULT = 1000;

  private final Map<String, RecordCache> map = Maps.newHashMap();
  private final int maxRecsPerName;

  class RecordCache
      extends LinkedHashMap<Collection<MetricsTag>, Record> {
    private static final long serialVersionUID = 1L;
    private boolean gotOverflow = false;

    @Override
    protected boolean removeEldestEntry(Map.Entry<Collection<MetricsTag>,
                                                  Record> eldest) {
      boolean overflow = size() > maxRecsPerName;
      if (overflow && !gotOverflow) {
        LOG.warn("Metrics cache overflow at "+ size() +" for "+ eldest);
        gotOverflow = true;
      }
      return overflow;
    }
  }

  /**
   * Cached record
   */
  public static class Record {
    final Map<String, String> tags = Maps.newHashMap();
    final Map<String, AbstractMetric> metrics = Maps.newHashMap();

    /**
     * Lookup a tag value
     * @param key name of the tag
     * @return the tag value
     */
    public String getTag(String key) {
      return tags.get(key);
    }

    /**
     * Lookup a metric value
     * @param key name of the metric
     * @return the metric value
     */
    public Number getMetric(String key) {
      AbstractMetric metric = metrics.get(key);
      return metric != null ? metric.value() : null;
    }

    /**
     * Lookup a metric instance
     * @param key name of the metric
     * @return the metric instance
     */
    public AbstractMetric getMetricInstance(String key) {
      return metrics.get(key);
    }

    /**
     * @return the entry set of the tags of the record
     */
    public Set<Map.Entry<String, String>> tags() {
      return tags.entrySet();
    }

    /**
     * @deprecated use metricsEntrySet() instead
     * @return entry set of metrics
     */
    @Deprecated
    public Set<Map.Entry<String, Number>> metrics() {
      Map<String, Number> map = new LinkedHashMap<String, Number>(
          metrics.size());
      for (Map.Entry<String, AbstractMetric> mapEntry : metrics.entrySet()) {
        map.put(mapEntry.getKey(), mapEntry.getValue().value());
      }
      return map.entrySet();
    }

    /**
     * @return entry set of metrics
     */
    public Set<Map.Entry<String, AbstractMetric>> metricsEntrySet() {
      return metrics.entrySet();
    }

    @Override public String toString() {
      StringBuilder sb = new StringBuilder(32);
      sb.append(this.getClass().getSimpleName());
      sb.append("{tags=");
      sb.append(tags);
      sb.append(", metrics=");
      sb.append(metrics);
      return sb.append('}').toString();
    }
  }

  public MetricsCache() {
    this(MAX_RECS_PER_NAME_DEFAULT);
  }

  /**
   * Construct a metrics cache
   * @param maxRecsPerName  limit of the number records per record name
   */
  public MetricsCache(int maxRecsPerName) {
    this.maxRecsPerName = maxRecsPerName;
  }

  /**
   * Update the cache and return the current cached record
   * @param mr the update record
   * @param includingTags cache tag values (for later lookup by name) if true
   * @return the updated cache record
   */
  public Record update(MetricsRecord mr, boolean includingTags) {
    String name = mr.name();
    RecordCache recordCache = map.get(name);
    if (recordCache == null) {
      recordCache = new RecordCache();
      map.put(name, recordCache);
    }
    Collection<MetricsTag> tags = mr.tags();
    Record record = recordCache.get(tags);
    if (record == null) {
      record = new Record();
      recordCache.put(tags, record);
    }
    for (AbstractMetric m : mr.metrics()) {
      record.metrics.put(m.name(), m);
    }
    if (includingTags) {
      // mostly for some sinks that include tags as part of a dense schema
      for (MetricsTag t : mr.tags()) {
        record.tags.put(t.name(), t.value());
      }
    }
    return record;
  }

  /**
   * Update the cache and return the current cache record
   * @param mr the update record
   * @return the updated cache record
   */
  public Record update(MetricsRecord mr) {
    return update(mr, false);
  }

  /**
   * Get the cached record
   * @param name of the record
   * @param tags of the record
   * @return the cached record or null
   */
  public Record get(String name, Collection<MetricsTag> tags) {
    RecordCache rc = map.get(name);
    if (rc == null) return null;
    return rc.get(tags);
  }
}
