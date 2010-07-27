/**
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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.replication.regionserver;
import org.apache.hadoop.hbase.metrics.MetricsRate;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.metrics.util.MetricsIntValue;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;

/**
 * This class is for maintaining the various replication statistics
 * for a source and publishing them through the metrics interfaces.
 */
public class ReplicationSourceMetrics implements Updater {
  private final MetricsRecord metricsRecord;
  private MetricsRegistry registry = new MetricsRegistry();

  /** Rate of shipped operations by the source */
  public final MetricsRate shippedOpsRate =
      new MetricsRate("shippedOpsRate", registry);

  /** Rate of shipped batches by the source */
  public final MetricsRate shippedBatchesRate =
      new MetricsRate("shippedBatchesRate", registry);

  /** Rate of log entries (can be multiple Puts) read from the logs */
  public final MetricsRate logEditsReadRate =
      new MetricsRate("logEditsReadRate", registry);

  /** Rate of log entries filtered by the source */
  public final MetricsRate logEditsFilteredRate =
      new MetricsRate("logEditsFilteredRate", registry);

  /** Age of the last operation that was shipped by the source */
  private final MetricsLongValue ageOfLastShippedOp =
      new MetricsLongValue("ageOfLastShippedOp", registry);

  /**
   * Current size of the queue of logs to replicate,
   * excluding the one being processed at the moment
   */
  public final MetricsIntValue sizeOfLogQueue =
      new MetricsIntValue("sizeOfLogQueue", registry);

  /**
   * Constructor used to register the metrics
   * @param id Name of the source this class is monitoring
   */
  public ReplicationSourceMetrics(String id) {
    MetricsContext context = MetricsUtil.getContext("hbase");
    String name = Thread.currentThread().getName();
    metricsRecord = MetricsUtil.createRecord(context, "replication");
    metricsRecord.setTag("RegionServer", name);
    context.registerUpdater(this);
    // Add jvmmetrics.
    JvmMetrics.init("RegionServer", name);
    // export for JMX
    new ReplicationStatistics(this.registry, "ReplicationSource for " + id);
  }

  /**
   * Set the age of the last edit that was shipped
   * @param timestamp write time of the edit
   */
  public void setAgeOfLastShippedOp(long timestamp) {
    ageOfLastShippedOp.set(System.currentTimeMillis() - timestamp);
  }

  @Override
  public void doUpdates(MetricsContext metricsContext) {
    synchronized (this) {
      this.shippedOpsRate.pushMetric(this.metricsRecord);
      this.shippedBatchesRate.pushMetric(this.metricsRecord);
      this.logEditsReadRate.pushMetric(this.metricsRecord);
      this.logEditsFilteredRate.pushMetric(this.metricsRecord);
      this.ageOfLastShippedOp.pushMetric(this.metricsRecord);
      this.sizeOfLogQueue.pushMetric(this.metricsRecord);
    }
  }
}
