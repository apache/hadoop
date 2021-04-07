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
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * MountVolumeMap contains information of the relationship
 * between underlying filesystem mount and datanode volumes.
 *
 * This is useful when configuring block tiering on same disk mount
 * (HDFS-15548). For now,
 * we don't configure multiple volumes with same storage type on one mount.
 */
@InterfaceAudience.Private
public class MountVolumeMap {
  private final ConcurrentMap<String, MountVolumeInfo>
      mountVolumeMapping;
  private final Configuration conf;

  MountVolumeMap(Configuration conf) {
    mountVolumeMapping = new ConcurrentHashMap<>();
    this.conf = conf;
  }

  FsVolumeReference getVolumeRefByMountAndStorageType(String mount,
      StorageType storageType) {
    if (mountVolumeMapping.containsKey(mount)) {
      return mountVolumeMapping
          .get(mount).getVolumeRef(storageType);
    }
    return null;
  }

  /**
   * Return capacity ratio.
   * If not exists, return 1 to use full capacity.
   */
  double getCapacityRatioByMountAndStorageType(String mount,
      StorageType storageType) {
    if (mountVolumeMapping.containsKey(mount)) {
      return mountVolumeMapping.get(mount).getCapacityRatio(storageType);
    }
    return 1;
  }

  void addVolume(FsVolumeImpl volume) {
    String mount = volume.getMount();
    if (!mount.isEmpty()) {
      MountVolumeInfo info;
      if (mountVolumeMapping.containsKey(mount)) {
        info = mountVolumeMapping.get(mount);
      } else {
        info = new MountVolumeInfo(conf);
        mountVolumeMapping.put(mount, info);
      }
      info.addVolume(volume);
    }
  }

  void removeVolume(FsVolumeImpl target) {
    String mount = target.getMount();
    if (!mount.isEmpty()) {
      MountVolumeInfo info = mountVolumeMapping.get(mount);
      info.removeVolume(target);
      if (info.size() == 0) {
        mountVolumeMapping.remove(mount);
      }
    }
  }

  void setCapacityRatio(FsVolumeImpl target, double capacityRatio)
      throws IOException {
    String mount = target.getMount();
    if (!mount.isEmpty()) {
      MountVolumeInfo info = mountVolumeMapping.get(mount);
      if (!info.setCapacityRatio(
          target.getStorageType(), capacityRatio)) {
        throw new IOException(
            "Not enough capacity ratio left on mount: "
                + mount + ", for " + target + ": capacity ratio: "
                + capacityRatio + ". Sum of the capacity"
                + " ratio of on same disk mount should be <= 1");
      }
    }
  }

  public boolean hasMount(String mount) {
    return mountVolumeMapping.containsKey(mount);
  }
}
