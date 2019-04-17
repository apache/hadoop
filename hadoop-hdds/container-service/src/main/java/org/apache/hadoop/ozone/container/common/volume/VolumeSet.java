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

package org.apache.hadoop.ozone.container.common.volume;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.ozone.common.InconsistentStorageStateException;
import org.apache.hadoop.ozone.container.common.impl.StorageLocationReport;
import org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume.VolumeState;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.Timer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import static org.apache.hadoop.util.RunJar.SHUTDOWN_HOOK_PRIORITY;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * VolumeSet to manage HDDS volumes in a DataNode.
 */
public class VolumeSet {

  private static final Logger LOG = LoggerFactory.getLogger(VolumeSet.class);

  private Configuration conf;

  /**
   * {@link VolumeSet#volumeMap} maintains a map of all active volumes in the
   * DataNode. Each volume has one-to-one mapping with a volumeInfo object.
   */
  private Map<String, HddsVolume> volumeMap;
  /**
   * {@link VolumeSet#failedVolumeMap} maintains a map of volumes which have
   * failed. The keys in this map and {@link VolumeSet#volumeMap} are
   * mutually exclusive.
   */
  private Map<String, HddsVolume> failedVolumeMap;

  /**
   * {@link VolumeSet#volumeStateMap} maintains a list of active volumes per
   * StorageType.
   */
  private EnumMap<StorageType, List<HddsVolume>> volumeStateMap;

  /**
   * An executor for periodic disk checks.
   */
  private final ScheduledExecutorService diskCheckerservice;
  private final ScheduledFuture<?> periodicDiskChecker;

  private static final long DISK_CHECK_INTERVAL_MINUTES = 15;

  /**
   * A Reentrant Read Write Lock to synchronize volume operations in VolumeSet.
   * Any update to {@link VolumeSet#volumeMap},
   * {@link VolumeSet#failedVolumeMap}, or {@link VolumeSet#volumeStateMap}
   * should be done after acquiring the write lock.
   */
  private final ReentrantReadWriteLock volumeSetRWLock;

  private final String datanodeUuid;
  private String clusterID;

  private Runnable shutdownHook;
  private final HddsVolumeChecker volumeChecker;

  public VolumeSet(String dnUuid, Configuration conf)
      throws IOException {
    this(dnUuid, null, conf);
  }

  public VolumeSet(String dnUuid, String clusterID, Configuration conf)
      throws IOException {
    this.datanodeUuid = dnUuid;
    this.clusterID = clusterID;
    this.conf = conf;
    this.volumeSetRWLock = new ReentrantReadWriteLock();
    this.volumeChecker = getVolumeChecker(conf);
    this.diskCheckerservice = Executors.newScheduledThreadPool(
        1, r -> new Thread(r, "Periodic HDDS volume checker"));
    this.periodicDiskChecker =
      diskCheckerservice.scheduleWithFixedDelay(() -> {
        try {
          checkAllVolumes();
        } catch (IOException e) {
          LOG.warn("Exception while checking disks", e);
        }
      }, DISK_CHECK_INTERVAL_MINUTES, DISK_CHECK_INTERVAL_MINUTES,
        TimeUnit.MINUTES);
    initializeVolumeSet();
  }

  @VisibleForTesting
  HddsVolumeChecker getVolumeChecker(Configuration configuration)
      throws DiskChecker.DiskErrorException {
    return new HddsVolumeChecker(configuration, new Timer());
  }

  /**
   * Add DN volumes configured through ConfigKeys to volumeMap.
   */
  private void initializeVolumeSet() throws IOException {
    volumeMap = new ConcurrentHashMap<>();
    failedVolumeMap = new ConcurrentHashMap<>();
    volumeStateMap = new EnumMap<>(StorageType.class);

    Collection<String> rawLocations = conf.getTrimmedStringCollection(
        HDDS_DATANODE_DIR_KEY);
    if (rawLocations.isEmpty()) {
      rawLocations = conf.getTrimmedStringCollection(DFS_DATANODE_DATA_DIR_KEY);
    }
    if (rawLocations.isEmpty()) {
      throw new IllegalArgumentException("No location configured in either "
          + HDDS_DATANODE_DIR_KEY + " or " + DFS_DATANODE_DATA_DIR_KEY);
    }

    for (StorageType storageType : StorageType.values()) {
      volumeStateMap.put(storageType, new ArrayList<>());
    }

    for (String locationString : rawLocations) {
      try {
        StorageLocation location = StorageLocation.parse(locationString);

        HddsVolume hddsVolume = createVolume(location.getUri().getPath(),
            location.getStorageType());

        checkAndSetClusterID(hddsVolume.getClusterID());

        volumeMap.put(hddsVolume.getHddsRootDir().getPath(), hddsVolume);
        volumeStateMap.get(hddsVolume.getStorageType()).add(hddsVolume);
        LOG.info("Added Volume : {} to VolumeSet",
            hddsVolume.getHddsRootDir().getPath());

        if (!hddsVolume.getHddsRootDir().mkdirs() &&
            !hddsVolume.getHddsRootDir().exists()) {
          throw new IOException("Failed to create HDDS storage dir " +
              hddsVolume.getHddsRootDir());
        }
      } catch (IOException e) {
        HddsVolume volume = new HddsVolume.Builder(locationString)
            .failedVolume(true).build();
        failedVolumeMap.put(locationString, volume);
        LOG.error("Failed to parse the storage location: " + locationString, e);
      }
    }

    checkAllVolumes();

    if (volumeMap.size() == 0) {
      throw new DiskOutOfSpaceException("No storage locations configured");
    }

    // Ensure volume threads are stopped and scm df is saved during shutdown.
    shutdownHook = () -> {
      saveVolumeSetUsed();
    };
    ShutdownHookManager.get().addShutdownHook(shutdownHook,
        SHUTDOWN_HOOK_PRIORITY);
  }

  /**
   * Run a synchronous parallel check of all HDDS volumes, removing
   * failed volumes.
   */
  private void checkAllVolumes() throws IOException {
    List<HddsVolume> allVolumes = getVolumesList();
    Set<HddsVolume> failedVolumes;
    try {
      failedVolumes = volumeChecker.checkAllVolumes(allVolumes);
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while running disk check", e);
    }

    if (failedVolumes.size() > 0) {
      LOG.warn("checkAllVolumes got {} failed volumes - {}",
          failedVolumes.size(), failedVolumes);
      handleVolumeFailures(failedVolumes);
    } else {
      LOG.debug("checkAllVolumes encountered no failures");
    }
  }

  /**
   * Handle one or more failed volumes.
   * @param failedVolumes
   */
  private void handleVolumeFailures(Set<HddsVolume> failedVolumes) {
    for (HddsVolume v: failedVolumes) {
      this.writeLock();
      try {
        // Immediately mark the volume as failed so it is unavailable
        // for new containers.
        volumeMap.remove(v.getHddsRootDir().getPath());
        failedVolumeMap.putIfAbsent(v.getHddsRootDir().getPath(), v);
      } finally {
        this.writeUnlock();
      }

      // TODO:
      // 1. Mark all closed containers on the volume as unhealthy.
      // 2. Consider stopping IO on open containers and tearing down
      //    active pipelines.
      // 3. Handle Ratis log disk failure.
    }
  }

  /**
   * If Version file exists and the {@link VolumeSet#clusterID} is not set yet,
   * assign it the value from Version file. Otherwise, check that the given
   * id matches with the id from version file.
   * @param idFromVersionFile value of the property from Version file
   * @throws InconsistentStorageStateException
   */
  private void checkAndSetClusterID(String idFromVersionFile)
      throws InconsistentStorageStateException {
    // If the clusterID is null (not set), assign it the value
    // from version file.
    if (this.clusterID == null) {
      this.clusterID = idFromVersionFile;
      return;
    }

    // If the clusterID is already set, it should match with the value from the
    // version file.
    if (!idFromVersionFile.equals(this.clusterID)) {
      throw new InconsistentStorageStateException(
          "Mismatched ClusterIDs. VolumeSet has: " + this.clusterID +
              ", and version file has: " + idFromVersionFile);
    }
  }

  /**
   * Acquire Volume Set Read lock.
   */
  public void readLock() {
    volumeSetRWLock.readLock().lock();
  }

  /**
   * Release Volume Set Read lock.
   */
  public void readUnlock() {
    volumeSetRWLock.readLock().unlock();
  }

  /**
   * Acquire Volume Set Write lock.
   */
  public void writeLock() {
    volumeSetRWLock.writeLock().lock();
  }

  /**
   * Release Volume Set Write lock.
   */
  public void writeUnlock() {
    volumeSetRWLock.writeLock().unlock();
  }


  private HddsVolume createVolume(String locationString,
      StorageType storageType) throws IOException {
    HddsVolume.Builder volumeBuilder = new HddsVolume.Builder(locationString)
        .conf(conf)
        .datanodeUuid(datanodeUuid)
        .clusterID(clusterID)
        .storageType(storageType);
    return volumeBuilder.build();
  }


  // Add a volume to VolumeSet
  boolean addVolume(String dataDir) {
    return addVolume(dataDir, StorageType.DEFAULT);
  }

  // Add a volume to VolumeSet
  private boolean addVolume(String volumeRoot, StorageType storageType) {
    String hddsRoot = HddsVolumeUtil.getHddsRoot(volumeRoot);
    boolean success;

    this.writeLock();
    try {
      if (volumeMap.containsKey(hddsRoot)) {
        LOG.warn("Volume : {} already exists in VolumeMap", hddsRoot);
        success = false;
      } else {
        if (failedVolumeMap.containsKey(hddsRoot)) {
          failedVolumeMap.remove(hddsRoot);
        }

        HddsVolume hddsVolume = createVolume(volumeRoot, storageType);
        volumeMap.put(hddsVolume.getHddsRootDir().getPath(), hddsVolume);
        volumeStateMap.get(hddsVolume.getStorageType()).add(hddsVolume);

        LOG.info("Added Volume : {} to VolumeSet",
            hddsVolume.getHddsRootDir().getPath());
        success = true;
      }
    } catch (IOException ex) {
      LOG.error("Failed to add volume " + volumeRoot + " to VolumeSet", ex);
      success = false;
    } finally {
      this.writeUnlock();
    }
    return success;
  }

  // Mark a volume as failed
  public void failVolume(String dataDir) {
    String hddsRoot = HddsVolumeUtil.getHddsRoot(dataDir);

    this.writeLock();
    try {
      if (volumeMap.containsKey(hddsRoot)) {
        HddsVolume hddsVolume = volumeMap.get(hddsRoot);
        hddsVolume.failVolume();

        volumeMap.remove(hddsRoot);
        volumeStateMap.get(hddsVolume.getStorageType()).remove(hddsVolume);
        failedVolumeMap.put(hddsRoot, hddsVolume);

        LOG.info("Moving Volume : {} to failed Volumes", hddsRoot);
      } else if (failedVolumeMap.containsKey(hddsRoot)) {
        LOG.info("Volume : {} is not active", hddsRoot);
      } else {
        LOG.warn("Volume : {} does not exist in VolumeSet", hddsRoot);
      }
    } finally {
      this.writeUnlock();
    }
  }

  // Remove a volume from the VolumeSet completely.
  public void removeVolume(String dataDir) throws IOException {
    String hddsRoot = HddsVolumeUtil.getHddsRoot(dataDir);

    this.writeLock();
    try {
      if (volumeMap.containsKey(hddsRoot)) {
        HddsVolume hddsVolume = volumeMap.get(hddsRoot);
        hddsVolume.shutdown();

        volumeMap.remove(hddsRoot);
        volumeStateMap.get(hddsVolume.getStorageType()).remove(hddsVolume);

        LOG.info("Removed Volume : {} from VolumeSet", hddsRoot);
      } else if (failedVolumeMap.containsKey(hddsRoot)) {
        HddsVolume hddsVolume = failedVolumeMap.get(hddsRoot);
        hddsVolume.setState(VolumeState.NON_EXISTENT);

        failedVolumeMap.remove(hddsRoot);
        LOG.info("Removed Volume : {} from failed VolumeSet", hddsRoot);
      } else {
        LOG.warn("Volume : {} does not exist in VolumeSet", hddsRoot);
      }
    } finally {
      this.writeUnlock();
    }
  }

  /**
   * This method, call shutdown on each volume to shutdown volume usage
   * thread and write scmUsed on each volume.
   */
  private void saveVolumeSetUsed() {
    for (HddsVolume hddsVolume : volumeMap.values()) {
      try {
        hddsVolume.shutdown();
      } catch (Exception ex) {
        LOG.error("Failed to shutdown volume : " + hddsVolume.getHddsRootDir(),
            ex);
      }
    }
  }

  /**
   * Shutdown the volumeset.
   */
  public void shutdown() {
    saveVolumeSetUsed();
    stopDiskChecker();
    if (shutdownHook != null) {
      ShutdownHookManager.get().removeShutdownHook(shutdownHook);
    }
  }

  private void stopDiskChecker() {
    periodicDiskChecker.cancel(true);
    volumeChecker.shutdownAndWait(0, TimeUnit.SECONDS);
    diskCheckerservice.shutdownNow();
  }

  @VisibleForTesting
  public List<HddsVolume> getVolumesList() {
    return ImmutableList.copyOf(volumeMap.values());
  }

  @VisibleForTesting
  public List<HddsVolume> getFailedVolumesList() {
    return ImmutableList.copyOf(failedVolumeMap.values());
  }

  @VisibleForTesting
  public Map<String, HddsVolume> getVolumeMap() {
    return ImmutableMap.copyOf(volumeMap);
  }

  @VisibleForTesting
  public Map<StorageType, List<HddsVolume>> getVolumeStateMap() {
    return ImmutableMap.copyOf(volumeStateMap);
  }

  public StorageContainerDatanodeProtocolProtos.NodeReportProto getNodeReport()
      throws IOException {
    boolean failed;
    this.readLock();
    try {
      StorageLocationReport[] reports = new StorageLocationReport[volumeMap
          .size() + failedVolumeMap.size()];
      int counter = 0;
      HddsVolume hddsVolume;
      for (Map.Entry<String, HddsVolume> entry : volumeMap.entrySet()) {
        hddsVolume = entry.getValue();
        VolumeInfo volumeInfo = hddsVolume.getVolumeInfo();
        long scmUsed;
        long remaining;
        long capacity;
        failed = false;
        try {
          scmUsed = volumeInfo.getScmUsed();
          remaining = volumeInfo.getAvailable();
          capacity = volumeInfo.getCapacity();
        } catch (IOException ex) {
          LOG.warn("Failed to get scmUsed and remaining for container " +
              "storage location {}", volumeInfo.getRootDir(), ex);
          // reset scmUsed and remaining if df/du failed.
          scmUsed = 0;
          remaining = 0;
          capacity = 0;
          failed = true;
        }

        StorageLocationReport.Builder builder =
            StorageLocationReport.newBuilder();
        builder.setStorageLocation(volumeInfo.getRootDir())
            .setId(hddsVolume.getStorageID())
            .setFailed(failed)
            .setCapacity(capacity)
            .setRemaining(remaining)
            .setScmUsed(scmUsed)
            .setStorageType(hddsVolume.getStorageType());
        StorageLocationReport r = builder.build();
        reports[counter++] = r;
      }
      for (Map.Entry<String, HddsVolume> entry : failedVolumeMap.entrySet()) {
        hddsVolume = entry.getValue();
        StorageLocationReport.Builder builder = StorageLocationReport
            .newBuilder();
        builder.setStorageLocation(hddsVolume.getHddsRootDir()
            .getAbsolutePath()).setId(hddsVolume.getStorageID()).setFailed(true)
            .setCapacity(0).setRemaining(0).setScmUsed(0).setStorageType(
            hddsVolume.getStorageType());
        StorageLocationReport r = builder.build();
        reports[counter++] = r;
      }
      NodeReportProto.Builder nrb = NodeReportProto.newBuilder();
      for (int i = 0; i < reports.length; i++) {
        nrb.addStorageReport(reports[i].getProtoBufMessage());
      }
      return nrb.build();
    } finally {
      this.readUnlock();
    }
  }
}