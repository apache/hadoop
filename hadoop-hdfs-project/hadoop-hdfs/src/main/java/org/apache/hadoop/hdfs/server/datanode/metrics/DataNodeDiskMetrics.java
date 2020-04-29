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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.DataNodeVolumeMetrics;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports.DiskOp;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
  private final long MIN_OUTLIER_DETECTION_DISKS = 5;
  private final long SLOW_DISK_LOW_THRESHOLD_MS = 20;
  private final long detectionInterval;
  private volatile boolean shouldRun;
  private OutlierDetector slowDiskDetector;
  private Daemon slowDiskDetectionDaemon;
  private volatile Map<String, Map<DiskOp, Double>>
      diskOutliersStats = Maps.newHashMap();

  // Adding for test purpose. When addSlowDiskForTesting() called from test
  // code, status should not be overridden by daemon thread.
  private boolean overrideStatus = true;

  public DataNodeDiskMetrics(DataNode dn, long diskOutlierDetectionIntervalMs) {
    this.dn = dn;
    this.detectionInterval = diskOutlierDetectionIntervalMs;
    slowDiskDetector = new OutlierDetector(MIN_OUTLIER_DETECTION_DISKS,
        SLOW_DISK_LOW_THRESHOLD_MS);
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
}
