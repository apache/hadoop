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
package org.apache.hadoop.hbase.regionserver.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.metrics.HBaseInfo;
import org.apache.hadoop.hbase.metrics.MetricsRate;
import org.apache.hadoop.hbase.metrics.PersistentMetricsTimeVaryingRate;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.metrics.util.MetricsIntValue;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.List;

/**
 * This class is for maintaining the various regionserver statistics
 * and publishing them through the metrics interfaces.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values.
 */
public class RegionServerMetrics implements Updater {
  @SuppressWarnings({"FieldCanBeLocal"})
  private final Log LOG = LogFactory.getLog(this.getClass());
  private final MetricsRecord metricsRecord;
  private long lastUpdate = System.currentTimeMillis();
  private long lastExtUpdate = System.currentTimeMillis();
  private long extendedPeriod = 0;
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
   * Block cache hit count.
   */
  public final MetricsLongValue blockCacheHitCount = new MetricsLongValue("blockCacheHitCount", registry);

  /**
   * Block cache miss count.
   */
  public final MetricsLongValue blockCacheMissCount = new MetricsLongValue("blockCacheMissCount", registry);

  /**
   * Block cache evict count.
   */
  public final MetricsLongValue blockCacheEvictedCount = new MetricsLongValue("blockCacheEvictedCount", registry);

  /**
   * Block hit ratio.
   */
  public final MetricsIntValue blockCacheHitRatio = new MetricsIntValue("blockCacheHitRatio", registry);

  /**
   * Block hit caching ratio.  This only includes the requests to the block
   * cache where caching was turned on.  See HBASE-2253.
   */
  public final MetricsIntValue blockCacheHitCachingRatio = new MetricsIntValue("blockCacheHitCachingRatio", registry);

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
   * Count of read requests
   */
  public final MetricsLongValue readRequestsCount = new MetricsLongValue("readRequestsCount", registry);

  /**
   * Count of write requests
   */
  public final MetricsLongValue writeRequestsCount = new MetricsLongValue("writeRequestsCount", registry);

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
   * Size of the flush queue.
   */
  public final MetricsIntValue flushQueueSize =
    new MetricsIntValue("flushQueueSize", registry);

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

  /**
   * time each scheduled compaction takes
   */
  protected final PersistentMetricsTimeVaryingRate compactionTime =
    new PersistentMetricsTimeVaryingRate("compactionTime", registry);

  protected final PersistentMetricsTimeVaryingRate compactionSize =
    new PersistentMetricsTimeVaryingRate("compactionSize", registry);

  /**
   * time each scheduled flush takes
   */
  protected final PersistentMetricsTimeVaryingRate flushTime =
    new PersistentMetricsTimeVaryingRate("flushTime", registry);

  protected final PersistentMetricsTimeVaryingRate flushSize =
    new PersistentMetricsTimeVaryingRate("flushSize", registry);

  public RegionServerMetrics() {
    MetricsContext context = MetricsUtil.getContext("hbase");
    metricsRecord = MetricsUtil.createRecord(context, "regionserver");
    String name = Thread.currentThread().getName();
    metricsRecord.setTag("RegionServer", name);
    context.registerUpdater(this);
    // Add jvmmetrics.
    JvmMetrics.init("RegionServer", name);
    // Add Hbase Info metrics
    HBaseInfo.init();

    // export for JMX
    statistics = new RegionServerStatistics(this.registry, name);

    // get custom attributes
    try {
      Object m = ContextFactory.getFactory().getAttribute("hbase.extendedperiod");
      if (m instanceof String) {
        this.extendedPeriod = Long.parseLong((String) m)*1000;
      }
    } catch (IOException ioe) {
      LOG.info("Couldn't load ContextFactory for Metrics config info");
    }

    LOG.info("Initialized");
  }

  public void shutdown() {
    if (statistics != null)
      statistics.shutdown();
  }

  /**
   * Since this object is a registered updater, this method will be called
   * periodically, e.g. every 5 seconds.
   * @param caller the metrics context that this responsible for calling us
   */
  public void doUpdates(MetricsContext caller) {
    synchronized (this) {
      this.lastUpdate = System.currentTimeMillis();

      // has the extended period for long-living stats elapsed?
      if (this.extendedPeriod > 0 &&
          this.lastUpdate - this.lastExtUpdate >= this.extendedPeriod) {
        this.lastExtUpdate = this.lastUpdate;
        this.compactionTime.resetMinMaxAvg();
        this.compactionSize.resetMinMaxAvg();
        this.flushTime.resetMinMaxAvg();
        this.flushSize.resetMinMaxAvg();
        this.resetAllMinMax();
      }

      this.stores.pushMetric(this.metricsRecord);
      this.storefiles.pushMetric(this.metricsRecord);
      this.storefileIndexSizeMB.pushMetric(this.metricsRecord);
      this.memstoreSizeMB.pushMetric(this.metricsRecord);
      this.readRequestsCount.pushMetric(this.metricsRecord);
      this.writeRequestsCount.pushMetric(this.metricsRecord);
      this.regions.pushMetric(this.metricsRecord);
      this.requests.pushMetric(this.metricsRecord);
      this.compactionQueueSize.pushMetric(this.metricsRecord);
      this.flushQueueSize.pushMetric(this.metricsRecord);
      this.blockCacheSize.pushMetric(this.metricsRecord);
      this.blockCacheFree.pushMetric(this.metricsRecord);
      this.blockCacheCount.pushMetric(this.metricsRecord);
      this.blockCacheHitCount.pushMetric(this.metricsRecord);
      this.blockCacheMissCount.pushMetric(this.metricsRecord);
      this.blockCacheEvictedCount.pushMetric(this.metricsRecord);
      this.blockCacheHitRatio.pushMetric(this.metricsRecord);
      this.blockCacheHitCachingRatio.pushMetric(this.metricsRecord);

      // Mix in HFile and HLog metrics
      // Be careful. Here is code for MTVR from up in hadoop:
      // public synchronized void inc(final int numOps, final long time) {
      //   currentData.numOperations += numOps;
      //   currentData.time += time;
      //   long timePerOps = time/numOps;
      //    minMax.update(timePerOps);
      // }
      // Means you can't pass a numOps of zero or get a ArithmeticException / by zero.
      int ops = (int)HFile.getReadOps();
      if (ops != 0) this.fsReadLatency.inc(ops, HFile.getReadTime());
      ops = (int)HFile.getWriteOps();
      if (ops != 0) this.fsWriteLatency.inc(ops, HFile.getWriteTime());
      // mix in HLog metrics
      ops = (int)HLog.getWriteOps();
      if (ops != 0) this.fsWriteLatency.inc(ops, HLog.getWriteTime());
      ops = (int)HLog.getSyncOps();
      if (ops != 0) this.fsSyncLatency.inc(ops, HLog.getSyncTime());

      // push the result
      this.fsReadLatency.pushMetric(this.metricsRecord);
      this.fsWriteLatency.pushMetric(this.metricsRecord);
      this.fsSyncLatency.pushMetric(this.metricsRecord);
      this.compactionTime.pushMetric(this.metricsRecord);
      this.compactionSize.pushMetric(this.metricsRecord);
      this.flushTime.pushMetric(this.metricsRecord);
      this.flushSize.pushMetric(this.metricsRecord);
    }
    this.metricsRecord.update();
  }

  public void resetAllMinMax() {
    this.atomicIncrementTime.resetMinMax();
    this.fsReadLatency.resetMinMax();
    this.fsWriteLatency.resetMinMax();
    this.fsSyncLatency.resetMinMax();
  }

  /**
   * @return Count of requests.
   */
  public float getRequests() {
    return this.requests.getPreviousIntervalValue();
  }

  /**
   * @param compact history in <time, size>
   */
  public synchronized void addCompaction(final Pair<Long,Long> compact) {
     this.compactionTime.inc(compact.getFirst());
     this.compactionSize.inc(compact.getSecond());
  }

  /**
   * @param flushes history in <time, size>
   */
  public synchronized void addFlush(final List<Pair<Long,Long>> flushes) {
    for (Pair<Long,Long> f : flushes) {
      this.flushTime.inc(f.getFirst());
      this.flushSize.inc(f.getSecond());
    }
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
    sb = Strings.appendKeyValue(sb, "readRequestsCount",
        Long.valueOf(this.readRequestsCount.get()));
    sb = Strings.appendKeyValue(sb, "writeRequestsCount",
        Long.valueOf(this.writeRequestsCount.get()));
    sb = Strings.appendKeyValue(sb, "compactionQueueSize",
      Integer.valueOf(this.compactionQueueSize.get()));
    sb = Strings.appendKeyValue(sb, "flushQueueSize",
      Integer.valueOf(this.flushQueueSize.get()));
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
    sb = Strings.appendKeyValue(sb, this.blockCacheHitCount.getName(),
        Long.valueOf(this.blockCacheHitCount.get()));
    sb = Strings.appendKeyValue(sb, this.blockCacheMissCount.getName(),
        Long.valueOf(this.blockCacheMissCount.get()));
    sb = Strings.appendKeyValue(sb, this.blockCacheEvictedCount.getName(),
        Long.valueOf(this.blockCacheEvictedCount.get()));
    sb = Strings.appendKeyValue(sb, this.blockCacheHitRatio.getName(),
        Long.valueOf(this.blockCacheHitRatio.get()));
    sb = Strings.appendKeyValue(sb, this.blockCacheHitCachingRatio.getName(),
        Long.valueOf(this.blockCacheHitCachingRatio.get()));
    return sb.toString();
  }
}
