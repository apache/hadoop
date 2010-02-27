/*
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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.stargate.metrics;

import org.apache.hadoop.hbase.metrics.MetricsRate;

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.metrics.util.MetricsRegistry;

public class StargateMetrics implements Updater {
  private final MetricsRecord metricsRecord;
  private final MetricsRegistry registry = new MetricsRegistry();
  private final StargateStatistics stargateStatistics;

  private MetricsRate requests = new MetricsRate("requests", registry);

  public StargateMetrics() {
    MetricsContext context = MetricsUtil.getContext("stargate");
    metricsRecord = MetricsUtil.createRecord(context, "stargate");
    String name = Thread.currentThread().getName();
    metricsRecord.setTag("Master", name);
    context.registerUpdater(this);
    JvmMetrics.init("Stargate", name);
    // expose the MBean for metrics
    stargateStatistics = new StargateStatistics(registry);

  }

  public void shutdown() {
    if (stargateStatistics != null) {
      stargateStatistics.shutdown();
    }
  }

  /**
   * Since this object is a registered updater, this method will be called
   * periodically, e.g. every 5 seconds.
   * @param unused 
   */
  public void doUpdates(MetricsContext unused) {
    synchronized (this) {
      requests.pushMetric(metricsRecord);
    }
    this.metricsRecord.update();
  }
  
  public void resetAllMinMax() {
    // Nothing to do
  }

  /**
   * @return Count of requests.
   */
  public float getRequests() {
    return requests.getPreviousIntervalValue();
  }
  
  /**
   * @param inc How much to add to requests.
   */
  public void incrementRequests(final int inc) {
    requests.inc(inc);
  }

}
