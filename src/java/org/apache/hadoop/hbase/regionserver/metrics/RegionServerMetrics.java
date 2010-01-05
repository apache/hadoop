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
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.metrics.MetricsRate;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.metrics.util.MetricsIntValue;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;

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
  private MetricsRegistry registry = new MetricsRegistry();
  private final RegionServerStatistics statistics;
    
  public final MetricsTimeVaryingRate atomicIncrementTime =
      new MetricsTimeVaryingRate("atomicIncrementTime", registry);
  
  /**
   * Count of regions carried by this regionserver
   */
  public final MetricsIntValue regions =
    new MetricsIntValue("regions", registry);

  /**
   * Block cache size.
   */
  public final MetricsLongValue blockCacheSize = new MetricsLongValue("blockCacheSize", registry);

  /**
   * Block cache free size.
   */
  public final MetricsLongValue blockCacheFree = new MetricsLongValue("blockCacheFree", registry);

  /**
   * Block cache item count.
   */
  public final MetricsLongValue blockCacheCount = new MetricsLongValue("blockCacheCount", registry);

  /**
   * Block hit ratio.
   */
  public final MetricsIntValue blockCacheHitRatio = new MetricsIntValue("blockCacheHitRatio", registry);

  /*
   * Count of requests to the regionservers since last call to metrics update
   */
  private final MetricsRate requests = new MetricsRate("requests", registry);

  /**
   * Count of stores open on the regionserver.
   */
  public final MetricsIntValue stores = new MetricsIntValue("stores", registry);

  /**
   * Count of storefiles open on the regionserver.
   */
  public final MetricsIntValue storefiles = new MetricsIntValue("storefiles", registry);

  /**
   * Sum of all the storefile index sizes in this regionserver in MB
   */
  public final MetricsIntValue storefileIndexSizeMB =
    new MetricsIntValue("storefileIndexSizeMB", registry);

  /**
   * Sum of all the memstore sizes in this regionserver in MB
   */
  public final MetricsIntValue memstoreSizeMB =
    new MetricsIntValue("memstoreSizeMB", registry);

  /**
   * Size of the compaction queue.
   */
  public final MetricsIntValue compactionQueueSize = 
    new MetricsIntValue("compactionQueueSize", registry);
  
  /**
   * filesystem read latency
   */
  public final MetricsTimeVaryingRate fsReadLatency = 
    new MetricsTimeVaryingRate("fsReadLatency", registry);

  /**
   * filesystem write latency
   */
  public final MetricsTimeVaryingRate fsWriteLatency = 
    new MetricsTimeVaryingRate("fsWriteLatency", registry);

  /**
   * filesystem sync latency
   */
  public final MetricsTimeVaryingRate fsSyncLatency = 
    new MetricsTimeVaryingRate("fsSyncLatency", registry);

  public RegionServerMetrics() {
    MetricsContext context = MetricsUtil.getContext("hbase");
    metricsRecord = MetricsUtil.createRecord(context, "regionserver");
    String name = Thread.currentThread().getName();
    metricsRecord.setTag("RegionServer", name);
    context.registerUpdater(this);
    // Add jvmmetrics.
    JvmMetrics.init("RegionServer", name);

    // export for JMX
    statistics = new RegionServerStatistics(this.registry, name);

    LOG.info("Initialized");
  }

  public void shutdown() {
    if (statistics != null)
      statistics.shutdown();
  }

  /**
   * Since this object is a registered updater, this method will be called
   * periodically, e.g. every 5 seconds.
   * @param unused 
   */
  public void doUpdates(MetricsContext unused) {
    synchronized (this) {
      this.stores.pushMetric(this.metricsRecord);
      this.storefiles.pushMetric(this.metricsRecord);
      this.storefileIndexSizeMB.pushMetric(this.metricsRecord);
      this.memstoreSizeMB.pushMetric(this.metricsRecord);
      this.regions.pushMetric(this.metricsRecord);
      this.requests.pushMetric(this.metricsRecord);
      this.compactionQueueSize.pushMetric(this.metricsRecord);
      this.blockCacheSize.pushMetric(this.metricsRecord);
      this.blockCacheFree.pushMetric(this.metricsRecord);
      this.blockCacheCount.pushMetric(this.metricsRecord);
      this.blockCacheHitRatio.pushMetric(this.metricsRecord);

      // mix in HFile metrics
      this.fsReadLatency.inc((int)HFile.getReadOps(), HFile.getReadTime());
      this.fsWriteLatency.inc((int)HFile.getWriteOps(), HFile.getWriteTime());
      // mix in HLog metrics
      this.fsWriteLatency.inc((int)HLog.getWriteOps(), HLog.getWriteTime());
      this.fsSyncLatency.inc((int)HLog.getSyncOps(), HLog.getSyncTime());
      // push the result
      this.fsReadLatency.pushMetric(this.metricsRecord);
      this.fsWriteLatency.pushMetric(this.metricsRecord);
      this.fsSyncLatency.pushMetric(this.metricsRecord);
    }
    this.metricsRecord.update();
    this.lastUpdate = System.currentTimeMillis();
  }

  public void resetAllMinMax() {
    this.atomicIncrementTime.resetMinMax();
    this.fsReadLatency.resetMinMax();
    this.fsWriteLatency.resetMinMax();
  }

  /**
   * @return Count of requests.
   */
  public float getRequests() {
    return this.requests.getPreviousIntervalValue();
  }
  
  /**
   * @param inc How much to add to requests.
   */
  public void incrementRequests(final int inc) {
    this.requests.inc(inc);
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    int seconds = (int)((System.currentTimeMillis() - this.lastUpdate)/1000);
    if (seconds == 0) {
      seconds = 1;
    }
    sb = Strings.appendKeyValue(sb, "request",
      Float.valueOf(this.requests.getPreviousIntervalValue()));
    sb = Strings.appendKeyValue(sb, "regions",
      Integer.valueOf(this.regions.get()));
    sb = Strings.appendKeyValue(sb, "stores",
      Integer.valueOf(this.stores.get()));
    sb = Strings.appendKeyValue(sb, "storefiles",
      Integer.valueOf(this.storefiles.get()));
    sb = Strings.appendKeyValue(sb, "storefileIndexSize",
      Integer.valueOf(this.storefileIndexSizeMB.get()));
    sb = Strings.appendKeyValue(sb, "memstoreSize",
      Integer.valueOf(this.memstoreSizeMB.get()));
    sb = Strings.appendKeyValue(sb, "compactionQueueSize",
      Integer.valueOf(this.compactionQueueSize.get()));
    // Duplicate from jvmmetrics because metrics are private there so
    // inaccessible.
    MemoryUsage memory =
      ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    sb = Strings.appendKeyValue(sb, "usedHeap",
      Long.valueOf(memory.getUsed()/MB));
    sb = Strings.appendKeyValue(sb, "maxHeap",
      Long.valueOf(memory.getMax()/MB));
    sb = Strings.appendKeyValue(sb, this.blockCacheSize.getName(),
        Long.valueOf(this.blockCacheSize.get()));
    sb = Strings.appendKeyValue(sb, this.blockCacheFree.getName(),
        Long.valueOf(this.blockCacheFree.get()));
    sb = Strings.appendKeyValue(sb, this.blockCacheCount.getName(),
        Long.valueOf(this.blockCacheCount.get()));
    sb = Strings.appendKeyValue(sb, this.blockCacheHitRatio.getName(),
        Long.valueOf(this.blockCacheHitRatio.get()));
    return sb.toString();
  }
}