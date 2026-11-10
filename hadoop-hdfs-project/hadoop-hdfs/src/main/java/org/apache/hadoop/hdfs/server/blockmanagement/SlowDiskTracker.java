/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.blockmanagement;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.primitives.Doubles;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports.DiskOp;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class aggregates information from {@link SlowDiskReports} received via
 * heartbeats.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SlowDiskTracker {
  public static final Logger LOG =
      LoggerFactory.getLogger(SlowDiskTracker.class);

  /**
   * Time duration after which a report is considered stale. This is
   * set to DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY * 3 i.e.
   * maintained for at least two successive reports.
   */
  private long reportValidityMs;

  /**
   * Timer object for querying the current time. Separated out for
   * unit testing.
   */
  private final Timer timer;

  /**
   * ObjectWriter to convert JSON reports to String.
   */
  private static final ObjectWriter WRITER = new ObjectMapper().writer();

  /**
   * Number of disks to include in JSON report per operation. We will return
   * disks with the highest latency.
   */
  private final int maxDisksToReport;
  private static final String DATANODE_DISK_SEPARATOR = ":";
  private final long reportGenerationIntervalMs;
  private final long cacheRebuildIntervalMs;
  // Whether slow-disk read-path deprioritization is enabled. The read cache
  // (cachedSlowDisksForRead) is only consumed by the read path when this is
  // true, so rebuilding it is skipped when the feature is disabled to avoid
  // spawning an async thread on every heartbeat for nothing.
  private final boolean deprioritizeEnabled;

  private volatile long lastUpdateTime;
  private volatile long lastCacheRebuildTime;
  private AtomicBoolean isUpdateInProgress = new AtomicBoolean(false);
  private AtomicBoolean isCacheRebuildInProgress = new AtomicBoolean(false);

  /**
   * Information about disks that have been reported as being slow.
   * It is map of (Slow Disk ID) -> (DiskLatency). The DiskLatency contains
   * the disk ID, the latencies reported and the timestamp when the report
   * was received.
   */
  private final Map<String, DiskLatency> diskIDLatencyMap;

    /**
     * Cached slow disk map for efficient read path lookup.
     *
     * <p>Key format: {@code IP:PORT:StorageID}.
     *
     * <p>Uses a copy-on-write strategy: heartbeat processing only updates
     * {@code diskIDLatencyMap}; an async thread periodically rebuilds this cache
     * and atomically swaps the reference.
     */
  private volatile Map<String, Double> cachedSlowDisksForRead = Collections.emptyMap();

  /**
   * Map of slow disk -> diskOperations it has been reported slow in.
   */
  private volatile ArrayList<DiskLatency> slowDisksReport =
      Lists.newArrayList();
  private volatile ArrayList<DiskLatency> oldSlowDisksCheck;

  public SlowDiskTracker(Configuration conf, Timer timer) {
    this.timer = timer;
    this.lastUpdateTime = timer.monotonicNow();
    this.lastCacheRebuildTime = timer.monotonicNow();
    this.diskIDLatencyMap = new ConcurrentHashMap<>();
    this.reportGenerationIntervalMs = conf.getTimeDuration(
        DFSConfigKeys.DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY,
        DFSConfigKeys.DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.maxDisksToReport = conf.getInt(
        DFSConfigKeys.DFS_DATANODE_MAX_DISKS_TO_REPORT_KEY,
        DFSConfigKeys.DFS_DATANODE_MAX_DISKS_TO_REPORT_DEFAULT);
    this.reportValidityMs = reportGenerationIntervalMs * 3;
    this.cacheRebuildIntervalMs = conf.getTimeDuration(
            DFSConfigKeys.DFS_NAMENODE_SLOW_DISK_CACHE_REBUILD_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_SLOW_DISK_CACHE_REBUILD_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS);
    this.deprioritizeEnabled = conf.getBoolean(
            DFSConfigKeys.DFS_NAMENODE_DEPRIORITIZE_SLOW_DISK_DATANODE_FOR_READ_KEY,
            DFSConfigKeys.DFS_NAMENODE_DEPRIORITIZE_SLOW_DISK_DATANODE_FOR_READ_DEFAULT);

  }

  /**
   * Get all valid slow disks for read path lookup.
   *
   * @return cached slow disk map with key format "IP:PORT:StorageID"
   */
  public Map<String, Double> getAllValidSlowDisks() {
    return cachedSlowDisksForRead;
  }

  @VisibleForTesting
  public static String getSlowDiskIDForReport(String datanodeID,
      String slowDisk) {
    return datanodeID + DATANODE_DISK_SEPARATOR + slowDisk;
  }

  public void addSlowDiskReport(String dataNodeID,
      SlowDiskReports dnSlowDiskReport) {
    Map<String, Map<DiskOp, Double>> slowDisks =
        dnSlowDiskReport.getSlowDisks();

    long now = timer.monotonicNow();

    for (Map.Entry<String, Map<DiskOp, Double>> slowDiskEntry :
        slowDisks.entrySet()) {

      String diskID = getSlowDiskIDForReport(dataNodeID,
          slowDiskEntry.getKey());

      Map<DiskOp, Double> latencies = slowDiskEntry.getValue();

      DiskLatency diskLatency = new DiskLatency(diskID, latencies, now);
      diskIDLatencyMap.put(diskID, diskLatency);
    }

  }

  /**
   * Extraction mode for slow disk key formatting.
   */
  private enum KeyExtractMode {
    CACHE_KEY,
    LEGACY_KEY
  }

  /**
   * Extract a formatted key from a slow disk ID.
   *
   * <p>The slowDiskID format is "IP:PORT:volumeName|storageID".
   * CACHE_KEY mode returns "IP:PORT:StorageID" (null if parse fails).
   * LEGACY_KEY mode returns "IP:PORT:volumeName" (original if parse fails).
   *
   * <p>Parsing anchors on the '|' separator (unique in the format) then
   * scans backwards to the ':' that delimits IP:PORT from the disk info.
   * This avoids mis-parsing when the volume path contains ':' characters.</p>
   */
  private static String extractDiskKey(String slowDiskID, KeyExtractMode mode) {
    if (slowDiskID == null || slowDiskID.isEmpty()) {
      return mode == KeyExtractMode.CACHE_KEY ? null : slowDiskID;
    }

    int pipeIndex = slowDiskID.indexOf('|');
    if (pipeIndex < 0) {
      return mode == KeyExtractMode.CACHE_KEY ? null : slowDiskID;
    }

    // Find the ':' before volumeName by scanning backwards from '|'.
    // Format: "IP:PORT:volumeName|storageID"
    //                  ^-- this colon separates addr from disk info
    int colonBeforeVolume = slowDiskID.lastIndexOf(':', pipeIndex);
    if (colonBeforeVolume <= 0 || colonBeforeVolume >= pipeIndex) {
      return mode == KeyExtractMode.CACHE_KEY ? null : slowDiskID;
    }

    String datanodeAddr = slowDiskID.substring(0, colonBeforeVolume);

    if (mode == KeyExtractMode.CACHE_KEY) {
      if (pipeIndex >= slowDiskID.length() - 1) {
        return null;
      }
      String storageID = slowDiskID.substring(pipeIndex + 1);
      return datanodeAddr + ":" + storageID;
    } else {
      String volumeName = slowDiskID.substring(colonBeforeVolume + 1, pipeIndex);
      return datanodeAddr + ":" + volumeName;
    }
  }

  public void checkAndUpdateReportIfNecessary() {
    // Check if it is time for update
    long now = timer.monotonicNow();
    if (now - lastUpdateTime > reportGenerationIntervalMs) {
      updateSlowDiskReportAsync(now);
    }
    if (deprioritizeEnabled
        && now - lastCacheRebuildTime > cacheRebuildIntervalMs) {
      rebuildSlowDiskCacheAsync(now);
    }
  }

  @VisibleForTesting
  public void updateSlowDiskReportAsync(long now) {
    if (isUpdateInProgress.compareAndSet(false, true)) {
      lastUpdateTime = now;
      new Thread(new Runnable() {
        @Override
        public void run() {
          slowDisksReport = getSlowDisks(diskIDLatencyMap,
              maxDisksToReport, now);

          cleanUpOldReports(now);

          isUpdateInProgress.set(false);
        }
      }).start();
    }
  }

  /**
   * Asynchronously rebuild the slow disk cache.
   */
  private void rebuildSlowDiskCacheAsync(long now) {
    if (isCacheRebuildInProgress.compareAndSet(false, true)) {
      lastCacheRebuildTime = now;
      new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            rebuildSlowDiskCache();
          } finally {
            isCacheRebuildInProgress.set(false);
          }
        }
      }).start();
    }
  }

  /**
   * Rebuild the slow disk cache in full (called in an async thread).
   * Uses a fresh timestamp to avoid stale expiry decisions caused by
   * thread scheduling delays.
   */
  private void rebuildSlowDiskCache() {
    long now = timer.monotonicNow();
    Map<String, Double> newCache = new HashMap<>();

    for (Map.Entry<String, DiskLatency> entry : diskIDLatencyMap.entrySet()) {
      DiskLatency diskLatency = entry.getValue();

      if (now - diskLatency.timestamp >= reportValidityMs) {
        continue;
      }

      String cacheKey = extractDiskKey(entry.getKey(), KeyExtractMode.CACHE_KEY);
      if (cacheKey != null) {
        newCache.put(cacheKey, diskLatency.getReadLatency());
      }
    }

    cachedSlowDisksForRead = newCache;

    LOG.debug("Rebuilt slow disk cache: {} valid slow disks", newCache.size());
  }

  /**
   * This structure is a thin wrapper over disk latencies.
   */
  public static class DiskLatency {
    @JsonProperty("SlowDiskID")
    final private String slowDiskID;
    @JsonProperty("Latencies")
    final private Map<DiskOp, Double> latencyMap;
    @JsonIgnore
    private long timestamp;

    /**
     * Constructor needed by Jackson for Object mapping.
     */
    public DiskLatency(
        @JsonProperty("SlowDiskID") String slowDiskID,
        @JsonProperty("Latencies") Map<DiskOp, Double> latencyMap) {
      this.slowDiskID = slowDiskID;
      this.latencyMap = latencyMap;
    }

    public DiskLatency(String slowDiskID, Map<DiskOp, Double> latencyMap,
        long timestamp) {
      this.slowDiskID = slowDiskID;
      this.latencyMap = latencyMap;
      this.timestamp = timestamp;
    }

    String getSlowDiskID() {
      return this.slowDiskID;
    }

    double getMaxLatency() {
      double maxLatency = 0;
      for (double latency : latencyMap.values()) {
        if (latency > maxLatency) {
          maxLatency = latency;
        }
      }
      return maxLatency;
    }

    Double getLatency(DiskOp op) {
      return this.latencyMap.get(op);
    }

    /**
     * Return the READ latency if reported, otherwise fall back to max latency.
     * This is used by the read path cache so that sorting reflects the
     * operation the client actually cares about.
     */
    double getReadLatency() {
      Double readLatency = latencyMap.get(DiskOp.READ);
      return readLatency != null ? readLatency : getMaxLatency();
    }
  }

  /**
   * Retrieve a list of stop low disks i.e disks with the highest max latencies.
   * @param numDisks number of disks to return. This is to limit the size of
   *                 the generated JSON.
   */
  private ArrayList<DiskLatency> getSlowDisks(
      Map<String, DiskLatency> reports, int numDisks, long now) {
    if (reports.isEmpty()) {
      return new ArrayList(ImmutableList.of());
    }

    final PriorityQueue<DiskLatency> topNReports = new PriorityQueue<>(
        reports.size(),
        new Comparator<DiskLatency>() {
          @Override
          public int compare(DiskLatency o1, DiskLatency o2) {
            return Doubles.compare(
                o1.getMaxLatency(), o2.getMaxLatency());
          }
        });

    ArrayList<DiskLatency> oldSlowDiskIDs = Lists.newArrayList();

    for (Map.Entry<String, DiskLatency> entry : reports.entrySet()) {
      DiskLatency diskLatency = entry.getValue();
      if (now - diskLatency.timestamp < reportValidityMs) {
        if (topNReports.size() < numDisks) {
          topNReports.add(diskLatency);
        } else if (topNReports.peek().getMaxLatency() <
            diskLatency.getMaxLatency()) {
          topNReports.poll();
          topNReports.add(diskLatency);
        }
      } else {
        oldSlowDiskIDs.add(diskLatency);
      }
    }

    oldSlowDisksCheck = oldSlowDiskIDs;

    return Lists.newArrayList(topNReports);
  }

  /**
   * Retrieve all valid reports as a JSON string.
   * @return serialized representation of valid reports. null if
   *         serialization failed.
   */
  public String getSlowDiskReportAsJsonString() {
    try {
      if (slowDisksReport.isEmpty()) {
        return null;
      }
      // Transform slowDiskID to legacy format (IP:PORT:volumeName)
      // for backward compatibility with existing JSON consumers.
      ArrayList<DiskLatency> reportForJson = Lists.newArrayList();
      for (DiskLatency dl : slowDisksReport) {
        String legacyID = extractDiskKey(dl.getSlowDiskID(),
            KeyExtractMode.LEGACY_KEY);
        reportForJson.add(new DiskLatency(legacyID, dl.latencyMap));
      }
      return WRITER.writeValueAsString(reportForJson);
    } catch (JsonProcessingException e) {
      // Failed to serialize. Don't log the exception call stack.
      LOG.debug("Failed to serialize statistics" + e);
      return null;
    }
  }

  private void cleanUpOldReports(long now) {
    if (oldSlowDisksCheck != null) {
      for (DiskLatency oldDiskLatency : oldSlowDisksCheck) {
        diskIDLatencyMap.remove(oldDiskLatency.getSlowDiskID(), oldDiskLatency);
      }
    }
    // Replace oldSlowDiskIDsCheck with an empty ArrayList
    oldSlowDisksCheck = null;
  }

  @VisibleForTesting
  public ArrayList<DiskLatency> getSlowDisksReport() {
    return this.slowDisksReport;
  }

  @VisibleForTesting
  public long getReportValidityMs() {
    return reportValidityMs;
  }

  @VisibleForTesting
  public void setReportValidityMs(long reportValidityMs) {
    this.reportValidityMs = reportValidityMs;
  }
}
