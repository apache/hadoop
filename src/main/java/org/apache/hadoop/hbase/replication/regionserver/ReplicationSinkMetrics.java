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
 * for a sink and publishing them through the metrics interfaces.
 */
public class ReplicationSinkMetrics implements Updater {
  private final MetricsRecord metricsRecord;
  private MetricsRegistry registry = new MetricsRegistry();
  private static ReplicationSinkMetrics instance;

  /** Rate of operations applied by the sink */
  public final MetricsRate appliedOpsRate =
      new MetricsRate("appliedOpsRate", registry);

  /** Rate of batches (of operations) applied by the sink */
  public final MetricsRate appliedBatchesRate =
      new MetricsRate("appliedBatchesRate", registry);

  /** Age of the last operation that was applied by the sink */
  private final MetricsLongValue ageOfLastAppliedOp =
      new MetricsLongValue("ageOfLastAppliedOp", registry);

  /**
   * Constructor used to register the metrics
   */
  public ReplicationSinkMetrics() {
    MetricsContext context = MetricsUtil.getContext("hbase");
    String name = Thread.currentThread().getName();
    metricsRecord = MetricsUtil.createRecord(context, "replication");
    metricsRecord.setTag("RegionServer", name);
    context.registerUpdater(this);
    // Add jvmmetrics.
    JvmMetrics.init("RegionServer", name);
    // export for JMX
    new ReplicationStatistics(this.registry, "ReplicationSink");
  }

  /**
   * Set the age of the last edit that was applied
   * @param timestamp write time of the edit
   */
  public void setAgeOfLastAppliedOp(long timestamp) {
    ageOfLastAppliedOp.set(System.currentTimeMillis() - timestamp);
  }
  @Override
  public void doUpdates(MetricsContext metricsContext) {
    synchronized (this) {
      this.appliedOpsRate.pushMetric(this.metricsRecord);
      this.appliedBatchesRate.pushMetric(this.metricsRecord);
      this.ageOfLastAppliedOp.pushMetric(this.metricsRecord);
    }
  }
}
