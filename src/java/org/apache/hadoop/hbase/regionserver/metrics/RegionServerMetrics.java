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
package org.apache.hadoop.hbase.regionserver.metrics;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.metrics.util.MetricsIntValue;


/** 
 * This class is for maintaining the various regionserver statistics
 * and publishing them through the metrics interfaces.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values.
 */
public class RegionServerMetrics implements Updater {
  private final Log LOG = LogFactory.getLog(this.getClass());
  private final MetricsRecord metricsRecord;
  private long lastUpdate = System.currentTimeMillis();
  private static final int MB = 1024*1024;
  
  /**
   * Count of regions carried by this regionserver
   */
  public final MetricsIntValue regions = new MetricsIntValue("regions");
  
  /*
   * Count of requests to the regionservers since last call to metrics update
   */
  private final MetricsIntValue requests = new MetricsIntValue("requests");

  /**
   * Count of stores open on the regionserver.
   */
  public final MetricsIntValue stores = new MetricsIntValue("stores");

  /**
   * Count of storefiles open on the regionserver.
   */
  public final MetricsIntValue storefiles = new MetricsIntValue("storefiles");

  /**
   * Sum of all the storefile index sizes in this regionserver in MB
   */
  public final MetricsIntValue storefileIndexSizeMB =
    new MetricsIntValue("storefileIndexSizeMB");

  /**
   * Sum of all the memcache sizes in this regionserver in MB
   */
  public final MetricsIntValue memcacheSizeMB =
    new MetricsIntValue("memcachSizeMB");

  public RegionServerMetrics() {
    MetricsContext context = MetricsUtil.getContext("hbase");
    metricsRecord = MetricsUtil.createRecord(context, "regionserver");
    String name = Thread.currentThread().getName();
    metricsRecord.setTag("RegionServer", name);
    context.registerUpdater(this);
    // Add jvmmetrics.
    JvmMetrics.init("RegionServer", name);
    LOG.info("Initialized");
  }
  
  public void shutdown() {
    // nought to do.
  }
    
  /**
   * Since this object is a registered updater, this method will be called
   * periodically, e.g. every 5 seconds.
   */
  public void doUpdates(MetricsContext unused) {
    synchronized (this) {
      this.stores.pushMetric(this.metricsRecord);
      this.storefiles.pushMetric(this.metricsRecord);
      this.storefileIndexSizeMB.pushMetric(this.metricsRecord);
      this.memcacheSizeMB.pushMetric(this.metricsRecord);
      this.regions.pushMetric(this.metricsRecord);
      synchronized(this.requests) {
        this.requests.pushMetric(this.metricsRecord);
        // Set requests down to zero again.
        this.requests.set(0);
      }
    }
    this.metricsRecord.update();
    this.lastUpdate = System.currentTimeMillis();
  }
  
  public void resetAllMinMax() {
    // Nothing to do
  }

  /**
   * @return Count of requests.
   */
  public int getRequests() {
    return this.requests.get();
  }
  
  /**
   * @param inc How much to add to requests.
   */
  public void incrementRequests(final int inc) {
    synchronized(this.requests) {
      this.requests.inc(inc);
    }
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    int seconds = (int)((System.currentTimeMillis() - this.lastUpdate)/1000);
    if (seconds == 0) {
      seconds = 1;
    }
    sb = Strings.appendKeyValue(sb, "request",
      Integer.valueOf(this.requests.get()/seconds));
    sb = Strings.appendKeyValue(sb, "regions",
      Integer.valueOf(this.regions.get()));
    sb = Strings.appendKeyValue(sb, "stores",
      Integer.valueOf(this.stores.get()));
    sb = Strings.appendKeyValue(sb, "storefiles",
      Integer.valueOf(this.storefiles.get()));
    sb = Strings.appendKeyValue(sb, "storefileIndexSize",
      Integer.valueOf(this.storefileIndexSizeMB.get()));
    sb = Strings.appendKeyValue(sb, "memcacheSize",
      Integer.valueOf(this.memcacheSizeMB.get()));
    // Duplicate from jvmmetrics because metrics are private there so
    // inaccessible.
    MemoryUsage memory =
      ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    sb = Strings.appendKeyValue(sb, "usedHeap",
      Long.valueOf(memory.getUsed()/MB));
    sb = Strings.appendKeyValue(sb, "maxHeap",
      Long.valueOf(memory.getMax()/MB));
    return sb.toString();
  }
}