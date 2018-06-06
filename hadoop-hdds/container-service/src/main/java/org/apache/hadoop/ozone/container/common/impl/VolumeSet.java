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

package org.apache.hadoop.ozone.container.common.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.ozone.container.common.impl.VolumeInfo.VolumeState;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.util.InstrumentedLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * VolumeSet to manage volumes in a DataNode.
 */
public class VolumeSet {

  private static final Logger LOG = LoggerFactory.getLogger(VolumeSet.class);

  private Configuration conf;

  /**
   * {@link VolumeSet#volumeMap} maintains a map of all active volumes in the
   * DataNode. Each volume has one-to-one mapping with a volumeInfo object.
   */
  private Map<Path, VolumeInfo> volumeMap;
  /**
   * {@link VolumeSet#failedVolumeMap} maintains a map of volumes which have
   * failed. The keys in this map and {@link VolumeSet#volumeMap} are
   * mutually exclusive.
   */
  private Map<Path, VolumeInfo> failedVolumeMap;
  /**
   * {@link VolumeSet#volumeStateMap} maintains a list of active volumes per
   * StorageType.
   */
  private EnumMap<StorageType, List<VolumeInfo>> volumeStateMap;

  /**
   * Lock to synchronize changes to the VolumeSet. Any update to
   * {@link VolumeSet#volumeMap}, {@link VolumeSet#failedVolumeMap}, or
   * {@link VolumeSet#volumeStateMap} should be done after acquiring this lock.
   */
  private final AutoCloseableLock volumeSetLock;

  public VolumeSet(Configuration conf) throws DiskOutOfSpaceException {
    this.conf = conf;
    this.volumeSetLock = new AutoCloseableLock(
        new InstrumentedLock(getClass().getName(), LOG,
            new ReentrantLock(true),
            conf.getTimeDuration(
                DFSConfigKeys.DFS_LOCK_SUPPRESS_WARNING_INTERVAL_KEY,
                DFSConfigKeys.DFS_LOCK_SUPPRESS_WARNING_INTERVAL_DEFAULT,
                TimeUnit.MILLISECONDS),
            300));

    initializeVolumeSet();
  }

  // Add DN volumes configured through ConfigKeys to volumeMap.
  private void initializeVolumeSet() throws DiskOutOfSpaceException {
    volumeMap = new ConcurrentHashMap<>();
    failedVolumeMap = new ConcurrentHashMap<>();
    volumeStateMap = new EnumMap<>(StorageType.class);

    Collection<String> datanodeDirs = conf.getTrimmedStringCollection(
        HDDS_DATANODE_DIR_KEY);
    if (datanodeDirs.isEmpty()) {
      datanodeDirs = conf.getTrimmedStringCollection(DFS_DATANODE_DATA_DIR_KEY);
    }
    if (datanodeDirs.isEmpty()) {
      throw new IllegalArgumentException("No location configured in either "
          + HDDS_DATANODE_DIR_KEY + " or " + DFS_DATANODE_DATA_DIR_KEY);
    }

    for (StorageType storageType : StorageType.values()) {
      volumeStateMap.put(storageType, new ArrayList<VolumeInfo>());
    }

    for (String dir : datanodeDirs) {
      try {
        VolumeInfo volumeInfo = getVolumeInfo(dir);

        volumeMap.put(volumeInfo.getRootDir(), volumeInfo);
        volumeStateMap.get(volumeInfo.getStorageType()).add(volumeInfo);
      } catch (IOException e) {
        LOG.error("Failed to parse the storage location: " + dir, e);
      }
    }

    if (volumeMap.size() == 0) {
      throw new DiskOutOfSpaceException("No storage location configured");
    }
  }

  public void acquireLock() {
    volumeSetLock.acquire();
  }

  public void releaseLock() {
    volumeSetLock.release();
  }

  private VolumeInfo getVolumeInfo(String rootDir) throws IOException {
    StorageLocation location = StorageLocation.parse(rootDir);
    StorageType storageType = location.getStorageType();

    VolumeInfo.Builder volumeBuilder = new VolumeInfo.Builder(rootDir, conf);
    volumeBuilder.storageType(storageType);
    return volumeBuilder.build();
  }

  // Add a volume to VolumeSet
  public void addVolume(String dataDir) throws IOException {
    Path dirPath = new Path(dataDir);

    try (AutoCloseableLock lock = volumeSetLock.acquire()) {
      if (volumeMap.containsKey(dirPath)) {
        LOG.warn("Volume : {} already exists in VolumeMap", dataDir);
      } else {
        if (failedVolumeMap.containsKey(dirPath)) {
          failedVolumeMap.remove(dirPath);
        }

        VolumeInfo volumeInfo = getVolumeInfo(dirPath.toString());
        volumeMap.put(dirPath, volumeInfo);
        volumeStateMap.get(volumeInfo.getStorageType()).add(volumeInfo);

        LOG.debug("Added Volume : {} to VolumeSet", dataDir);
      }
    }
  }

  // Mark a volume as failed
  public void failVolume(String dataDir) {
    Path dirPath = new Path(dataDir);

    try (AutoCloseableLock lock = volumeSetLock.acquire()) {
      if (volumeMap.containsKey(dirPath)) {
        VolumeInfo volumeInfo = volumeMap.get(dirPath);
        volumeInfo.failVolume();

        volumeMap.remove(dirPath);
        volumeStateMap.get(volumeInfo.getStorageType()).remove(volumeInfo);
        failedVolumeMap.put(dirPath, volumeInfo);

        LOG.debug("Moving Volume : {} to failed Volumes", dataDir);
      } else if (failedVolumeMap.containsKey(dirPath)) {
        LOG.debug("Volume : {} is not active", dataDir);
      } else {
        LOG.warn("Volume : {} does not exist in VolumeSet", dataDir);
      }
    }
  }

  // Remove a volume from the VolumeSet completely.
  public void removeVolume(String dataDir) throws IOException {
    Path dirPath = new Path(dataDir);

    try (AutoCloseableLock lock = volumeSetLock.acquire()) {
      if (volumeMap.containsKey(dirPath)) {
        VolumeInfo volumeInfo = volumeMap.get(dirPath);
        volumeInfo.shutdown();

        volumeMap.remove(dirPath);
        volumeStateMap.get(volumeInfo.getStorageType()).remove(volumeInfo);

        LOG.debug("Removed Volume : {} from VolumeSet", dataDir);
      } else if (failedVolumeMap.containsKey(dirPath)) {
        VolumeInfo volumeInfo = failedVolumeMap.get(dirPath);
        volumeInfo.setState(VolumeState.NON_EXISTENT);

        failedVolumeMap.remove(dirPath);
        LOG.debug("Removed Volume : {} from failed VolumeSet", dataDir);
      } else {
        LOG.warn("Volume : {} does not exist in VolumeSet", dataDir);
      }
    }
  }

  public VolumeInfo chooseVolume(long containerSize,
      VolumeChoosingPolicy choosingPolicy) throws IOException {
    return choosingPolicy.chooseVolume(getVolumesList(), containerSize);
  }

  public void shutdown() {
    for (VolumeInfo volumeInfo : volumeMap.values()) {
      try {
        volumeInfo.shutdown();
      } catch (Exception e) {
        LOG.error("Failed to shutdown volume : " + volumeInfo.getRootDir(), e);
      }
    }
  }

  @VisibleForTesting
  public List<VolumeInfo> getVolumesList() {
    return ImmutableList.copyOf(volumeMap.values());
  }

  @VisibleForTesting
  public List<VolumeInfo> getFailedVolumesList() {
    return ImmutableList.copyOf(failedVolumeMap.values());
  }

  @VisibleForTesting
  public Map<Path, VolumeInfo> getVolumeMap() {
    return ImmutableMap.copyOf(volumeMap);
  }

  @VisibleForTesting
  public Map<StorageType, List<VolumeInfo>> getVolumeStateMap() {
    return ImmutableMap.copyOf(volumeStateMap);
  }
}