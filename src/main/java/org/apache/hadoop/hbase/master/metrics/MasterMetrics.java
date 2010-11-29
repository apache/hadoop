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
package org.apache.hadoop.hbase.master.metrics;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.metrics.HBaseInfo;
import org.apache.hadoop.hbase.metrics.MetricsRate;
import org.apache.hadoop.hbase.metrics.PersistentMetricsTimeVaryingRate;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;


/**
 * This class is for maintaining the various master statistics
 * and publishing them through the metrics interfaces.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values.
 */
public class MasterMetrics implements Updater {
  private final Log LOG = LogFactory.getLog(this.getClass());
  private final MetricsRecord metricsRecord;
  private final MetricsRegistry registry = new MetricsRegistry();
  private final MasterStatistics masterStatistics;

  private long lastUpdate = System.currentTimeMillis();
  private long lastExtUpdate = System.currentTimeMillis();
  private long extendedPeriod = 0;
/*
   * Count of requests to the cluster since last call to metrics update
   */
  private final MetricsRate cluster_requests =
    new MetricsRate("cluster_requests", registry);

  /** Time it takes to finish HLog.splitLog() */
  final PersistentMetricsTimeVaryingRate splitTime =
    new PersistentMetricsTimeVaryingRate("splitTime", registry);

  /** Size of HLog files being split */
  final PersistentMetricsTimeVaryingRate splitSize =
    new PersistentMetricsTimeVaryingRate("splitSize", registry);

  public MasterMetrics(final String name) {
    MetricsContext context = MetricsUtil.getContext("hbase");
    metricsRecord = MetricsUtil.createRecord(context, "master");
    metricsRecord.setTag("Master", name);
    context.registerUpdater(this);
    JvmMetrics.init("Master", name);
    HBaseInfo.init();

    // expose the MBean for metrics
    masterStatistics = new MasterStatistics(this.registry);

    // get custom attributes
    try {
      Object m = 
        ContextFactory.getFactory().getAttribute("hbase.extendedperiod");
      if (m instanceof String) {
        this.extendedPeriod = Long.parseLong((String) m)*1000;
      }
    } catch (IOException ioe) {
      LOG.info("Couldn't load ContextFactory for Metrics config info");
    }

    LOG.info("Initialized");
  }

  public void shutdown() {
    if (masterStatistics != null)
      masterStatistics.shutdown();
  }

  /**
   * Since this object is a registered updater, this method will be called
   * periodically, e.g. every 5 seconds.
   * @param unused
   */
  public void doUpdates(MetricsContext unused) {
    synchronized (this) {
      this.lastUpdate = System.currentTimeMillis();

      // has the extended period for long-living stats elapsed?
      if (this.extendedPeriod > 0 &&
          this.lastUpdate - this.lastExtUpdate >= this.extendedPeriod) {
        this.lastExtUpdate = this.lastUpdate;
        this.splitTime.resetMinMaxAvg();
        this.splitSize.resetMinMaxAvg();
        this.resetAllMinMax();
      }

      this.cluster_requests.pushMetric(metricsRecord);
      this.splitTime.pushMetric(metricsRecord);
      this.splitSize.pushMetric(metricsRecord);
    }
    this.metricsRecord.update();
  }

  public void resetAllMinMax() {
    // Nothing to do
  }
  
  /**
   * Record a single instance of a split
   * @param time time that the split took
   * @param size length of original HLogs that were split
   */
  public synchronized void addSplit(long time, long size) {
    splitTime.inc(time);
    splitSize.inc(size);
  }

  /**
   * @return Count of requests.
   */
  public float getRequests() {
    return this.cluster_requests.getPreviousIntervalValue();
  }

  /**
   * @param inc How much to add to requests.
   */
  public void incrementRequests(final int inc) {
    this.cluster_requests.inc(inc);
  }
}
