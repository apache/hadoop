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
package org.apache.hadoop.hdfs.server.datanode.metrics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.DataNodeVolumeMetrics;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports.DiskOp;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SLOWDISK_LOW_THRESHOLD_MS_KEY;

/**
 * This class detects and maintains DataNode disk outliers and their
 * latencies for different ops (metadata, read, write).
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DataNodeDiskMetrics {

  public static final Logger LOG = LoggerFactory.getLogger(
      DataNodeDiskMetrics.class);

  private DataNode dn;
  private final long detectionInterval;
  private volatile boolean shouldRun;
  private OutlierDetector slowDiskDetector;
  private Daemon slowDiskDetectionDaemon;
  private volatile Map<String, Map<DiskOp, Double>>
      diskOutliersStats = Maps.newHashMap();

  // Adding for test purpose. When addSlowDiskForTesting() called from test
  // code, status should not be overridden by daemon thread.
  private boolean overrideStatus = true;

  /**
   * Minimum number of disks to run outlier detection.
   */
  private volatile long minOutlierDetectionDisks;
  /**
   * Threshold in milliseconds below which a disk is definitely not slow.
   */
  private volatile long lowThresholdMs;
  /**
   * The number of slow disks that needs to be excluded.
   */
  private volatile int maxSlowDisksToExclude;
  /**
   * List of slow disks that need to be excluded.
   */
  private List<String> slowDisksToExclude = new ArrayList<>();

  public DataNodeDiskMetrics(DataNode dn, long diskOutlierDetectionIntervalMs,
      Configuration conf) {
    this.dn = dn;
    this.detectionInterval = diskOutlierDetectionIntervalMs;
    minOutlierDetectionDisks =
        conf.getLong(DFSConfigKeys.DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_KEY,
            DFSConfigKeys.DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_DEFAULT);
    lowThresholdMs =
        conf.getLong(DFSConfigKeys.DFS_DATANODE_SLOWDISK_LOW_THRESHOLD_MS_KEY,
            DFSConfigKeys.DFS_DATANODE_SLOWDISK_LOW_THRESHOLD_MS_DEFAULT);
    maxSlowDisksToExclude =
        conf.getInt(DFSConfigKeys.DFS_DATANODE_MAX_SLOWDISKS_TO_EXCLUDE_KEY,
            DFSConfigKeys.DFS_DATANODE_MAX_SLOWDISKS_TO_EXCLUDE_DEFAULT);
    slowDiskDetector =
        new OutlierDetector(minOutlierDetectionDisks, lowThresholdMs);
    shouldRun = true;
    startDiskOutlierDetectionThread();
  }

  private void startDiskOutlierDetectionThread() {
    slowDiskDetectionDaemon = new Daemon(new Runnable() {
      @Override
      public void run() {
        while (shouldRun) {
          if (dn.getFSDataset() != null) {
            Map<String, Double> metadataOpStats = Maps.newHashMap();
            Map<String, Double> readIoStats = Maps.newHashMap();
            Map<String, Double> writeIoStats = Maps.newHashMap();
            FsDatasetSpi.FsVolumeReferences fsVolumeReferences = null;
            try {
              fsVolumeReferences = dn.getFSDataset().getFsVolumeReferences();
              Iterator<FsVolumeSpi> volumeIterator = fsVolumeReferences
                  .iterator();
              while (volumeIterator.hasNext()) {
                FsVolumeSpi volume = volumeIterator.next();
                DataNodeVolumeMetrics metrics = volume.getMetrics();
                String volumeName = volume.getBaseURI().getPath();

                metadataOpStats.put(volumeName,
                    metrics.getMetadataOperationMean());
                readIoStats.put(volumeName, metrics.getReadIoMean());
                writeIoStats.put(volumeName, metrics.getWriteIoMean());
              }
            } finally {
              if (fsVolumeReferences != null) {
                try {
                  fsVolumeReferences.close();
                } catch (IOException e) {
                  LOG.error("Error in releasing FS Volume references", e);
                }
              }
            }
            if (metadataOpStats.isEmpty() && readIoStats.isEmpty()
                && writeIoStats.isEmpty()) {
              LOG.debug("No disk stats available for detecting outliers.");
              continue;
            }

            detectAndUpdateDiskOutliers(metadataOpStats, readIoStats,
                writeIoStats);

            // Sort the slow disks by latency and extract the top n by maxSlowDisksToExclude.
            if (maxSlowDisksToExclude > 0) {
              ArrayList<DiskLatency> diskLatencies = new ArrayList<>();
              for (Map.Entry<String, Map<DiskOp, Double>> diskStats :
                  diskOutliersStats.entrySet()) {
                diskLatencies.add(new DiskLatency(diskStats.getKey(), diskStats.getValue()));
              }

              Collections.sort(diskLatencies, (o1, o2)
                  -> Double.compare(o2.getMaxLatency(), o1.getMaxLatency()));

              slowDisksToExclude = diskLatencies.stream().limit(maxSlowDisksToExclude)
                  .map(DiskLatency::getSlowDisk).collect(Collectors.toList());
            }
          }

          try {
            Thread.sleep(detectionInterval);
          } catch (InterruptedException e) {
            LOG.error("Disk Outlier Detection thread interrupted", e);
            Thread.currentThread().interrupt();
          }
        }
      }
    });
    slowDiskDetectionDaemon.start();
  }

  private void detectAndUpdateDiskOutliers(Map<String, Double> metadataOpStats,
      Map<String, Double> readIoStats, Map<String, Double> writeIoStats) {
    Map<String, Map<DiskOp, Double>> diskStats = Maps.newHashMap();

    // Get MetadataOp Outliers
    Map<String, Double> metadataOpOutliers = slowDiskDetector
        .getOutliers(metadataOpStats);
    for (Map.Entry<String, Double> entry : metadataOpOutliers.entrySet()) {
      addDiskStat(diskStats, entry.getKey(), DiskOp.METADATA, entry.getValue());
    }

    // Get ReadIo Outliers
    Map<String, Double> readIoOutliers = slowDiskDetector
        .getOutliers(readIoStats);
    for (Map.Entry<String, Double> entry : readIoOutliers.entrySet()) {
      addDiskStat(diskStats, entry.getKey(), DiskOp.READ, entry.getValue());
    }

    // Get WriteIo Outliers
    Map<String, Double> writeIoOutliers = slowDiskDetector
        .getOutliers(writeIoStats);
    for (Map.Entry<String, Double> entry : writeIoOutliers.entrySet()) {
      addDiskStat(diskStats, entry.getKey(), DiskOp.WRITE, entry.getValue());
    }
    if (overrideStatus) {
      diskOutliersStats = diskStats;
      LOG.debug("Updated disk outliers.");
    }
  }

  /**
   * This structure is a wrapper over disk latencies.
   */
  public static class DiskLatency {
    final private String slowDisk;
    final private Map<DiskOp, Double> latencyMap;

    public DiskLatency(
        String slowDiskID,
        Map<DiskOp, Double> latencyMap) {
      this.slowDisk = slowDiskID;
      this.latencyMap = latencyMap;
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

    public String getSlowDisk() {
      return slowDisk;
    }
  }

  private void addDiskStat(Map<String, Map<DiskOp, Double>> diskStats,
      String disk, DiskOp diskOp, double latency) {
    if (!diskStats.containsKey(disk)) {
      diskStats.put(disk, new HashMap<>());
    }
    diskStats.get(disk).put(diskOp, latency);
  }

  public Map<String, Map<DiskOp, Double>> getDiskOutliersStats() {
    return diskOutliersStats;
  }

  public void shutdownAndWait() {
    shouldRun = false;
    slowDiskDetectionDaemon.interrupt();
    try {
      slowDiskDetectionDaemon.join();
    } catch (InterruptedException e) {
      LOG.error("Disk Outlier Detection daemon did not shutdown", e);
    }
  }

  /**
   * Use only for testing.
   */
  @VisibleForTesting
  public void addSlowDiskForTesting(String slowDiskPath,
      Map<DiskOp, Double> latencies) {
    overrideStatus = false;
    if (latencies == null) {
      diskOutliersStats.put(slowDiskPath, ImmutableMap.of());
    } else {
      diskOutliersStats.put(slowDiskPath, latencies);
    }
  }

  public List<String> getSlowDisksToExclude() {
    return slowDisksToExclude;
  }

  public int getMaxSlowDisksToExclude() {
    return maxSlowDisksToExclude;
  }

  public void setMaxSlowDisksToExclude(int maxSlowDisksToExclude) {
    this.maxSlowDisksToExclude = maxSlowDisksToExclude;
  }

  public void setLowThresholdMs(long thresholdMs) {
    Preconditions.checkArgument(thresholdMs > 0,
        DFS_DATANODE_SLOWDISK_LOW_THRESHOLD_MS_KEY + " should be larger than 0");
    lowThresholdMs = thresholdMs;
    this.slowDiskDetector.setLowThresholdMs(thresholdMs);
  }

  public long getLowThresholdMs() {
    return lowThresholdMs;
  }

  public void setMinOutlierDetectionDisks(long minDisks) {
    Preconditions.checkArgument(minDisks > 0,
        DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_KEY + " should be larger than 0");
    minOutlierDetectionDisks = minDisks;
    this.slowDiskDetector.setMinNumResources(minDisks);
  }

  public long getMinOutlierDetectionDisks() {
    return minOutlierDetectionDisks;
  }

  @VisibleForTesting
  public OutlierDetector getSlowDiskDetector() {
    return this.slowDiskDetector;
  }
}
