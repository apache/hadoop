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
package org.apache.hadoop.ipc.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.RetryCache;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

/**
 * This class is for maintaining the various RetryCache-related statistics
 * and publishing them through the metrics interfaces.
 */
@InterfaceAudience.Private
@Metrics(about="Aggregate RetryCache metrics", context="rpc")
public class RetryCacheMetrics {

  static final Log LOG = LogFactory.getLog(RetryCacheMetrics.class);
  final MetricsRegistry registry;
  final String name;

  RetryCacheMetrics(RetryCache retryCache) {
    name = "RetryCache."+ retryCache.getCacheName();
    registry = new MetricsRegistry(name);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Initialized "+ registry);
    }
  }

  public String getName() { return name; }

  public static RetryCacheMetrics create(RetryCache cache) {
    RetryCacheMetrics m = new RetryCacheMetrics(cache);
    return DefaultMetricsSystem.instance().register(m.name, null, m);
  }

  @Metric("Number of RetryCache hit") MutableCounterLong cacheHit;
  @Metric("Number of RetryCache cleared") MutableCounterLong cacheCleared;
  @Metric("Number of RetryCache updated") MutableCounterLong cacheUpdated;

  /**
   * One cache hit event
   */
  public void incrCacheHit() {
    cacheHit.incr();
  }

  /**
   * One cache cleared
   */
  public void incrCacheCleared() {
    cacheCleared.incr();
  }

  /**
   * One cache updated
   */
  public void incrCacheUpdated() {
    cacheUpdated.incr();
  }

  public long getCacheHit() {
    return cacheHit.value();
  }

  public long getCacheCleared() {
    return cacheCleared.value();
  }

  public long getCacheUpdated() {
    return cacheUpdated.value();
  }

}
