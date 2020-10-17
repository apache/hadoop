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
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.thirdparty.com.google.common.primitives.Doubles;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports.DiskOp;
import org.apache.hadoop.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final int MAX_DISKS_TO_REPORT = 5;
  private static final String DATANODE_DISK_SEPARATOR = ":";
  private final long reportGenerationIntervalMs;

  private volatile long lastUpdateTime;
  private AtomicBoolean isUpdateInProgress = new AtomicBoolean(false);

  /**
   * Information about disks that have been reported as being slow.
   * It is map of (Slow Disk ID) -> (DiskLatency). The DiskLatency contains
   * the disk ID, the latencies reported and the timestamp when the report
   * was received.
   */
  private final Map<String, DiskLatency> diskIDLatencyMap;

  /**
   * Map of slow disk -> diskOperations it has been reported slow in.
   */
  private volatile ArrayList<DiskLatency> slowDisksReport =
      Lists.newArrayList();
  private volatile ArrayList<DiskLatency> oldSlowDisksCheck;

  public SlowDiskTracker(Configuration conf, Timer timer) {
    this.timer = timer;
    this.lastUpdateTime = timer.monotonicNow();
    this.diskIDLatencyMap = new ConcurrentHashMap<>();
    this.reportGenerationIntervalMs = conf.getTimeDuration(
        DFSConfigKeys.DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY,
        DFSConfigKeys.DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.reportValidityMs = reportGenerationIntervalMs * 3;
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

  public void checkAndUpdateReportIfNecessary() {
    // Check if it is time for update
    long now = timer.monotonicNow();
    if (now - lastUpdateTime > reportGenerationIntervalMs) {
      updateSlowDiskReportAsync(now);
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
              MAX_DISKS_TO_REPORT, now);

          cleanUpOldReports(now);

          isUpdateInProgress.set(false);
        }
      }).start();
    }
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
      return WRITER.writeValueAsString(slowDisksReport);
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
  ArrayList<DiskLatency> getSlowDisksReport() {
    return this.slowDisksReport;
  }

  @VisibleForTesting
  long getReportValidityMs() {
    return reportValidityMs;
  }

  @VisibleForTesting
  void setReportValidityMs(long reportValidityMs) {
    this.reportValidityMs = reportValidityMs;
  }
}
