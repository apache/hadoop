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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;

import java.nio.channels.ClosedChannelException;
import java.util.EnumMap;
import java.util.Map;

/**
 * MountVolumeInfo is a wrapper of
 * detailed volume information for MountVolumeMap.
 */
@InterfaceAudience.Private
class MountVolumeInfo {
  private final EnumMap<StorageType, FsVolumeImpl>
      storageTypeVolumeMap;
  private final EnumMap<StorageType, Double>
      capacityRatioMap;
  private double reservedForArchiveDefault;

  MountVolumeInfo(Configuration conf) {
    storageTypeVolumeMap = new EnumMap<>(StorageType.class);
    capacityRatioMap = new EnumMap<>(StorageType.class);
    reservedForArchiveDefault = conf.getDouble(
        DFSConfigKeys.DFS_DATANODE_RESERVE_FOR_ARCHIVE_DEFAULT_PERCENTAGE,
        DFSConfigKeys
            .DFS_DATANODE_RESERVE_FOR_ARCHIVE_DEFAULT_PERCENTAGE_DEFAULT);
    if (reservedForArchiveDefault > 1) {
      FsDatasetImpl.LOG.warn("Value of reserve-for-archival is > 100%." +
          " Setting it to 100%.");
      reservedForArchiveDefault = 1;
    }
    if (reservedForArchiveDefault < 0) {
      FsDatasetImpl.LOG.warn("Value of reserve-for-archival is < 0." +
          " Setting it to 0.0");
      reservedForArchiveDefault = 0;
    }
  }

  FsVolumeReference getVolumeRef(StorageType storageType) {
    try {
      FsVolumeImpl volumeImpl = storageTypeVolumeMap
          .getOrDefault(storageType, null);
      if (volumeImpl != null) {
        return volumeImpl.obtainReference();
      }
    } catch (ClosedChannelException e) {
      FsDatasetImpl.LOG.warn("Volume closed when getting volume" +
          " by storage type: " + storageType);
    }
    return null;
  }

  /**
   * Return configured capacity ratio.
   */
  double getCapacityRatio(StorageType storageType) {
    // If capacity ratio is set, return the val.
    if (capacityRatioMap.containsKey(storageType)) {
      return capacityRatioMap.get(storageType);
    }
    // If capacity ratio is set for counterpart,
    // use the rest of capacity of the mount for it.
    if (!capacityRatioMap.isEmpty()) {
      double leftOver = 1;
      for (Map.Entry<StorageType, Double> e : capacityRatioMap.entrySet()) {
        leftOver -= e.getValue();
      }
      return leftOver;
    }
    // Use reservedForArchiveDefault by default.
    if (storageTypeVolumeMap.containsKey(storageType)
        && storageTypeVolumeMap.size() > 1) {
      if (storageType == StorageType.ARCHIVE) {
        return reservedForArchiveDefault;
      } else if (storageType == StorageType.DISK) {
        return 1 - reservedForArchiveDefault;
      }
    }
    return 1;
  }

  /**
   * Add a volume to the mapping.
   * If there is already storage type exists on same mount, skip this volume.
   */
  boolean addVolume(FsVolumeImpl volume) {
    if (storageTypeVolumeMap.containsKey(volume.getStorageType())) {
      FsDatasetImpl.LOG.error("Found storage type already exist." +
          " Skipping for now. Please check disk configuration");
      return false;
    }
    storageTypeVolumeMap.put(volume.getStorageType(), volume);
    return true;
  }

  void removeVolume(FsVolumeImpl target) {
    storageTypeVolumeMap.remove(target.getStorageType());
    capacityRatioMap.remove(target.getStorageType());
  }

  /**
   * Set customize capacity ratio for a storage type.
   * Return false if the value is too big.
   */
  boolean setCapacityRatio(StorageType storageType,
      double capacityRatio) {
    double leftover = 1;
    for (Map.Entry<StorageType, Double> e : capacityRatioMap.entrySet()) {
      if (e.getKey() != storageType) {
        leftover -= e.getValue();
      }
    }
    if (leftover < capacityRatio) {
      return false;
    }
    capacityRatioMap.put(storageType, capacityRatio);
    return true;
  }

  int size() {
    return storageTypeVolumeMap.size();
  }
}
