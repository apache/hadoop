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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * MountVolumeMap contains information of the relationship
 * between underlying filesystem mount and datanode volumes.
 *
 * This is useful when configuring block tiering on same disk mount (HDFS-15548)
 * For now,
 * we don't configure multiple volumes with same storage type on a mount.
 */
@InterfaceAudience.Private
class MountVolumeMap {
  private ConcurrentMap<String, Map<StorageType, VolumeInfo>>
      mountVolumeMapping;
  private double reservedForArchive;

  MountVolumeMap(Configuration conf) {
    mountVolumeMapping = new ConcurrentHashMap<>();
    reservedForArchive = conf.getDouble(
        DFSConfigKeys.DFS_DATANODE_RESERVE_FOR_ARCHIVE_PERCENTAGE,
        DFSConfigKeys.DFS_DATANODE_RESERVE_FOR_ARCHIVE_PERCENTAGE_DEFAULT);
    if (reservedForArchive > 1) {
      FsDatasetImpl.LOG.warn("Value of reserve-for-archival is > 100%." +
          " Setting it to 100%.");
      reservedForArchive = 1;
    }
  }

  FsVolumeReference getVolumeRefByMountAndStorageType(String mount,
      StorageType storageType) {
    if (mountVolumeMapping != null
        && mountVolumeMapping.containsKey(mount)) {
      try {
        VolumeInfo volumeInfo = mountVolumeMapping
            .get(mount).getOrDefault(storageType, null);
        if (volumeInfo != null) {
          return volumeInfo.getFsVolume().obtainReference();
        }
      } catch (ClosedChannelException e) {
        FsDatasetImpl.LOG.warn("Volume closed when getting volume" +
            " by mount and storage type: "
            + mount + ", " + storageType);
      }
    }
    return null;
  }

  /**
   * Return configured capacity ratio. Otherwise return 1 as default
   */
  double getCapacityRatioByMountAndStorageType(String mount,
      StorageType storageType) {
    if (mountVolumeMapping != null
        && mountVolumeMapping.containsKey(mount)) {
      return mountVolumeMapping
          .get(mount).getOrDefault(storageType, null).getCapacityRatio();
    }
    return 1;
  }

  void addVolume(FsVolumeImpl volume) {
    String mount = volume.getMount();
    if (!mount.isEmpty()) {
      Map<StorageType, VolumeInfo> storageTypeMap =
          mountVolumeMapping
              .getOrDefault(mount, new ConcurrentHashMap<>());
      if (storageTypeMap.containsKey(volume.getStorageType())) {
        FsDatasetImpl.LOG.error("Found storage type already exist." +
            " Skipping for now. Please check disk configuration");
      } else {
        VolumeInfo volumeInfo = new VolumeInfo(volume, 1);
        if (volume.getStorageType() == StorageType.ARCHIVE) {
          volumeInfo.setCapacityRatio(reservedForArchive);
        } else if (volume.getStorageType() == StorageType.DISK) {
          volumeInfo.setCapacityRatio(1 - reservedForArchive);
        }
        storageTypeMap.put(volume.getStorageType(), volumeInfo);
        mountVolumeMapping.put(mount, storageTypeMap);
      }
    }
  }

  void removeVolume(FsVolumeImpl target) {
    String mount = target.getMount();
    if (!mount.isEmpty()) {
      Map storageTypeMap = mountVolumeMapping.get(mount);
      storageTypeMap.remove(target.getStorageType());
      if (storageTypeMap.isEmpty()) {
        mountVolumeMapping.remove(mount);
      }
    }
  }

  static class VolumeInfo {
    private final FsVolumeImpl fsVolume;
    private double capacityRatio;

    VolumeInfo(FsVolumeImpl fsVolume, double capacityRatio) {
      this.fsVolume = fsVolume;
      this.capacityRatio = capacityRatio;
    }

    FsVolumeImpl getFsVolume() {
      return fsVolume;
    }

    double getCapacityRatio() {
      return capacityRatio;
    }

    void setCapacityRatio(double capacityRatio) {
      this.capacityRatio = capacityRatio;
    }
  }
}
