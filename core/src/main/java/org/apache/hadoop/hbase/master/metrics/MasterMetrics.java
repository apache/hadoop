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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.metrics.MetricsRate;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
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
  /*
   * Count of requests to the cluster since last call to metrics update
   */
  private final MetricsRate cluster_requests =
    new MetricsRate("cluster_requests", registry);

  public MasterMetrics(final String name) {
    MetricsContext context = MetricsUtil.getContext("hbase");
    metricsRecord = MetricsUtil.createRecord(context, "master");
    metricsRecord.setTag("Master", name);
    context.registerUpdater(this);
    JvmMetrics.init("Master", name);

    // expose the MBean for metrics
    masterStatistics = new MasterStatistics(this.registry);

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
      this.cluster_requests.pushMetric(metricsRecord);
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
    return this.cluster_requests.getPreviousIntervalValue();
  }

  /**
   * @param inc How much to add to requests.
   */
  public void incrementRequests(final int inc) {
    this.cluster_requests.inc(inc);
  }
}