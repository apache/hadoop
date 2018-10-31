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
package org.apache.hadoop.hdfs.server.namenode.sps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfier.DatanodeMap;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Datanode cache Manager handles caching of {@link DatanodeStorageReport}.
 *
 * This class is instantiated by StoragePolicySatisifer. It maintains the array
 * of datanode storage reports. It has a configurable refresh interval and
 * periodically refresh the datanode cache by fetching latest
 * {@link Context#getLiveDatanodeStorageReport()} once it reaches refresh
 * interval.
 */
@InterfaceAudience.Private
public class DatanodeCacheManager {
  private static final Logger LOG = LoggerFactory
      .getLogger(DatanodeCacheManager.class);

  private final DatanodeMap datanodeMap;
  private NetworkTopology cluster;

  /**
   * Interval between scans in milliseconds.
   */
  private final long refreshIntervalMs;

  private long lastAccessedTime;

  public DatanodeCacheManager(Configuration conf) {
    refreshIntervalMs = conf.getLong(
        DFSConfigKeys.DFS_SPS_DATANODE_CACHE_REFRESH_INTERVAL_MS,
        DFSConfigKeys.DFS_SPS_DATANODE_CACHE_REFRESH_INTERVAL_MS_DEFAULT);

    LOG.info("DatanodeCacheManager refresh interval is {} milliseconds",
        refreshIntervalMs);
    datanodeMap = new DatanodeMap();
  }

  /**
   * Returns the live datanodes and its storage details, which has available
   * space (&gt; 0) to schedule block moves. This will return array of datanodes
   * from its local cache. It has a configurable refresh interval in millis and
   * periodically refresh the datanode cache by fetching latest
   * {@link Context#getLiveDatanodeStorageReport()} once it elapsed refresh
   * interval.
   *
   * @throws IOException
   */
  public DatanodeMap getLiveDatanodeStorageReport(
      Context spsContext) throws IOException {
    long now = Time.monotonicNow();
    long elapsedTimeMs = now - lastAccessedTime;
    boolean refreshNeeded = elapsedTimeMs >= refreshIntervalMs;
    lastAccessedTime = now;
    if (refreshNeeded) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("elapsedTimeMs > refreshIntervalMs : {} > {},"
            + " so refreshing cache", elapsedTimeMs, refreshIntervalMs);
      }
      datanodeMap.reset(); // clear all previously cached items.

      // Fetch live datanodes from namenode and prepare DatanodeMap.
      DatanodeStorageReport[] liveDns = spsContext
          .getLiveDatanodeStorageReport();
      for (DatanodeStorageReport storage : liveDns) {
        StorageReport[] storageReports = storage.getStorageReports();
        List<StorageType> storageTypes = new ArrayList<>();
        List<Long> remainingSizeList = new ArrayList<>();
        for (StorageReport t : storageReports) {
          if (t.getRemaining() > 0) {
            storageTypes.add(t.getStorage().getStorageType());
            remainingSizeList.add(t.getRemaining());
          }
        }
        datanodeMap.addTarget(storage.getDatanodeInfo(), storageTypes,
            remainingSizeList);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("LIVE datanodes: {}", datanodeMap);
      }
      // get network topology
      cluster = spsContext.getNetworkTopology(datanodeMap);
    }
    return datanodeMap;
  }

  NetworkTopology getCluster() {
    return cluster;
  }
}