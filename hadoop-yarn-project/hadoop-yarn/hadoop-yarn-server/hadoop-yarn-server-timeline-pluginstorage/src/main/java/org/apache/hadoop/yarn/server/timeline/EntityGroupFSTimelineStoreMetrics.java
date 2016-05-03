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
package org.apache.hadoop.yarn.server.timeline;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableStat;

/**
 * This class tracks metrics for the EntityGroupFSTimelineStore. It tracks
 * the read and write metrics for timeline server v1.5. It serves as a
 * complement to {@link TimelineDataManagerMetrics}.
 */
@Metrics(about="Metrics for EntityGroupFSTimelineStore", context="yarn")
public class EntityGroupFSTimelineStoreMetrics {
  private static final String DEFAULT_VALUE_WITH_SCALE = "TimeMs";

  // General read related metrics
  @Metric("getEntity calls to summary storage")
  private MutableCounterLong getEntityToSummaryOps;

  @Metric("getEntity calls to detail storage")
  private MutableCounterLong getEntityToDetailOps;

  // Summary data related metrics
  @Metric(value = "summary log read ops and time",
      valueName = DEFAULT_VALUE_WITH_SCALE)
  private MutableStat summaryLogRead;

  @Metric("entities read into the summary storage")
  private MutableCounterLong entitiesReadToSummary;

  // Detail data cache related metrics
  @Metric("cache storage read that does not require a refresh")
  private MutableCounterLong noRefreshCacheRead;

  @Metric("cache storage refresh due to the cached storage is stale")
  private MutableCounterLong cacheStaleRefreshes;

  @Metric("cache storage evicts")
  private MutableCounterLong cacheEvicts;

  @Metric(value = "cache storage refresh ops and time",
      valueName = DEFAULT_VALUE_WITH_SCALE)
  private MutableStat cacheRefresh;

  // Log scanner and cleaner related metrics
  @Metric(value = "active log scan ops and time",
      valueName = DEFAULT_VALUE_WITH_SCALE)
  private MutableStat activeLogDirScan;

  @Metric(value = "log cleaner purging ops and time",
      valueName = DEFAULT_VALUE_WITH_SCALE)
  private MutableStat logClean;

  @Metric("log cleaner dirs purged")
  private MutableCounterLong logsDirsCleaned;

  private static EntityGroupFSTimelineStoreMetrics instance = null;

  EntityGroupFSTimelineStoreMetrics() {
  }

  public static synchronized EntityGroupFSTimelineStoreMetrics create() {
    if (instance == null) {
      MetricsSystem ms = DefaultMetricsSystem.instance();
      instance = ms.register(new EntityGroupFSTimelineStoreMetrics());
    }
    return instance;
  }

  // Setters
  // General read related
  public void incrGetEntityToSummaryOps() {
    getEntityToSummaryOps.incr();
  }

  public void incrGetEntityToDetailOps() {
    getEntityToDetailOps.incr();
  }

  // Summary data related
  public void addSummaryLogReadTime(long msec) {
    summaryLogRead.add(msec);
  }

  public void incrEntitiesReadToSummary(long delta) {
    entitiesReadToSummary.incr(delta);
  }

  // Cache related
  public void incrNoRefreshCacheRead() {
    noRefreshCacheRead.incr();
  }

  public void incrCacheStaleRefreshes() {
    cacheStaleRefreshes.incr();
  }

  public void incrCacheEvicts() {
    cacheEvicts.incr();
  }

  public void addCacheRefreshTime(long msec) {
    cacheRefresh.add(msec);
  }

  // Log scanner and cleaner related
  public void addActiveLogDirScanTime(long msec) {
    activeLogDirScan.add(msec);
  }

  public void addLogCleanTime(long msec) {
    logClean.add(msec);
  }

  public void incrLogsDirsCleaned() {
    logsDirsCleaned.incr();
  }

  // Getters
  MutableCounterLong getEntitiesReadToSummary() {
    return entitiesReadToSummary;
  }

  MutableCounterLong getLogsDirsCleaned() {
    return logsDirsCleaned;
  }

  MutableCounterLong getGetEntityToSummaryOps() {
    return getEntityToSummaryOps;
  }

  MutableCounterLong getGetEntityToDetailOps() {
    return getEntityToDetailOps;
  }

  MutableStat getCacheRefresh() {
    return cacheRefresh;
  }
}

