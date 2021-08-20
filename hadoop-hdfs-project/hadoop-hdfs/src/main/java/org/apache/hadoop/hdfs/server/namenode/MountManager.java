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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.MountInfo;
import org.apache.hadoop.fs.MountMode;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.protocol.MountException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.ProvidedStorageMap;
import org.apache.hadoop.hdfs.server.common.ProvidedVolumeInfo;
import org.apache.hadoop.hdfs.server.namenode.mountmanager.ProvidedReadCacheManagerSpi;
import org.apache.hadoop.hdfs.server.namenode.mountmanager.ReadCacheManagerFactory;
import org.apache.hadoop.hdfs.server.namenode.syncservice.SyncServiceSatisfier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class manages the mount path for remote stores. It keeps
 * track of the mounts that have completed along with those that are in the
 * process of being created.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MountManager {
  public static final Logger LOG =
      LoggerFactory.getLogger(MountManager.class);
  public static final String MOUNT_TMP_DIR = "/_temporary/_mounts/";
  public static final String IS_MOUNT_XATTR_NAME = "isMount";
  public static final String MOUNT_OP = "addMount";

  /**
   * Map from mount paths, that are being created, to ProvidedVolumeInfo
   * of the remote stores being mounted.
   */
  private final Map<Path, ProvidedVolumeInfo> mountsUnderCreation =
      new HashMap<>();

  /**
   * Map from mount paths to ProvidedVolumeInfo of the remote stores mounted.
   */
  private final Map<Path, ProvidedVolumeInfo> mountPoints = new HashMap<>();

  /**
   * Base temporary location used for creation of mounts.
   */
  private final String rootTempDirectory;
  private final FSNamesystem namesystem;
  private final Pattern mountTempDirectoryPattern;
  private final Set<MountManagerSpi> mountMsgs;
  private ProvidedReadCacheManagerSpi readCacheManager;

  /**
   * A factory method to create {@link MountManager} on need basis. If
   * Provided storage is disabled, a no-op instance is created to avoid any
   * interference.
   * @param conf
   * @param namesystem
   * @return
   */
  public static MountManager createInstance(Configuration conf,
      FSNamesystem namesystem) {
    ProvidedStorageMap providedStorageMap =
        namesystem.getBlockManager().getProvidedStorageMap();
    if (providedStorageMap.isProvidedEnabled()) {
      return new MountManager(conf, namesystem);
    } else {
      LOG.info("Provided storage is not enabled, disable mount manager");
      return new NoOpMountManager(namesystem);
    }
  }

  // For NoOpMountManager use.
  private MountManager(FSNamesystem fsn) {
    rootTempDirectory = null;
    mountTempDirectoryPattern = null;
    namesystem = fsn;
    this.mountMsgs = new HashSet<>();
  }

  private MountManager(Configuration conf, FSNamesystem namesystem) {
    String tempDir = MOUNT_TMP_DIR;
    if (tempDir.endsWith("/")) {
      tempDir = tempDir.substring(0, tempDir.length()-1);
    }
    this.rootTempDirectory = tempDir;
    this.namesystem = namesystem;
    // the path should be of the form TEMPORARY_DIR/VOL_UUID/<mount path>/..
    this.mountTempDirectoryPattern =
        Pattern.compile(String.format("%s/([^/]+)(/.+)", rootTempDirectory));

    this.mountMsgs = new HashSet<>();
    this.mountMsgs.add(ReadMountManager.getInstance(conf, namesystem));
    this.mountMsgs.add(SyncMountManager.getInstance(conf, namesystem));
    this.readCacheManager = ReadCacheManagerFactory.getFactory(conf)
        .newInstance(conf, namesystem, namesystem.getBlockManager());
  }

  /**
   * @return the root path where mounts are created before being
   *         promoted to the final path.
   */
  String getRootTemporaryDir() {
    return rootTempDirectory;
  }

  /**
   * Start tracking a new mount path.
   *
   * @param mountPath mount path.
   * @param providedVolumeInfo {@link ProvidedVolumeInfo} of this mount.
   * @return the temporary path where the mount should be created.
   * @throws MountException if the mount path being added already exists.
   */
  synchronized String startMount(Path mountPath,
      ProvidedVolumeInfo providedVolumeInfo) throws MountException {
    if (!mountPath.isAbsolute()) {
      throw new IllegalArgumentException(
          "Mount path " + mountPath + " is not absolute!");
    }
    if (mountExists(mountPath)) {
      throw MountException.mountAlreadyExistsException(mountPath);
    }
    LOG.info("Starting mount path {} for remote path {}",
        mountPath, providedVolumeInfo.getRemotePath());
    mountsUnderCreation.put(mountPath, providedVolumeInfo);
    return getTemporaryPathForMount(mountPath, providedVolumeInfo);
  }

  void prepareMount(String tempMountPath, String remotePath,
      MountMode mountMode, Map<String, String> remoteConfig,
      Configuration conf) throws IOException {
    Configuration mergedConf = getMergedConfig(conf, remoteConfig);
    // Delegate some operations to specific mount manager.
    getMountManager(mountMode).prepareMount(
        tempMountPath, remotePath, mergedConf);

    // client will use xattr to identify a dir which is also a mount point
    XAttr userXAttr = new XAttr.Builder()
        .setNameSpace(XAttr.NameSpace.USER)
        .setName(MountManager.IS_MOUNT_XATTR_NAME)
        .setValue("true".getBytes())
        .build();
    namesystem.setXAttr(tempMountPath, userXAttr,
        EnumSet.of(XAttrSetFlag.CREATE), false);

    // the mount configuration would be sent to datanodes, as these will be
    // needed for establishing connection with the remote store
    for (Map.Entry<String, String> configEntry : remoteConfig.entrySet()) {
      XAttr trustedXAttr = new XAttr.Builder()
          .setNameSpace(XAttr.NameSpace.TRUSTED)
          .setName("mount.config." + configEntry.getKey())
          .setValue(configEntry.getValue().getBytes())
          .build();
      namesystem.setXAttr(tempMountPath, trustedXAttr,
          EnumSet.of(XAttrSetFlag.CREATE), false);
    }
  }

  /**
   * Create and return mount id.
   */
  UUID createMountId() {
    return UUID.randomUUID();
  }

  /**
   * Merge remote config to hdfs configuration.
   * @param conf  hdfs configuration.
   * @param remoteConfig remote config given from client when adding a
   *                     new mount.
   * @return  The merged configuration.
   */
  public static Configuration getMergedConfig(Configuration conf,
      Map<String, String> remoteConfig) {
    Configuration mergedConfig;
    if (remoteConfig == null || remoteConfig.isEmpty()) {
      mergedConfig = conf;
    } else {
      // Remote config should have a limited impact.
      mergedConfig = new Configuration(conf);
      remoteConfig.forEach((k, v) -> mergedConfig.set(k, v));
    }
    return mergedConfig;
  }

  /**
   * Finish the creation of a mount. This should be called after a
   * mount path has been fully created.
   *
   * @param mountPath the mount path.
   * @param isMounted true, if the mount already exists.
   * @throws MountException if mount path is not in the process of being
   * created or mount already exists.
   */
  synchronized void finishMount(Path mountPath, boolean isMounted)
      throws MountException {
    if (!mountsUnderCreation.containsKey(mountPath)) {
      throw new MountException("Mount path " + mountPath +
          " is not being created");
    }
    if (mountPoints.containsKey(mountPath)) {
      throw MountException.mountAlreadyExistsException(mountPath);
    }
    ProvidedVolumeInfo volInfo = mountsUnderCreation.remove(mountPath);
    mountPoints.put(mountPath, volInfo);
    LOG.info("Finishing mount {}, mount mode is {}.",
        mountPath, volInfo.getMountMode());
    // Manages red cache not only for readOnly mount, but also for others.
    readCacheManager.init(new Path(volInfo.getMountPath()), volInfo);
    try {
      getMountManager(volInfo).finishMount(volInfo, isMounted);
    } catch (IOException e) {
      throw new MountException("Failed to finish mount!", e);
    }
  }

  public MountManagerSpi getMountManager(ProvidedVolumeInfo volInfo)
      throws IOException {
    return getMountManager(volInfo.getMountMode());
  }

  public SyncMountManager getSyncMountManager() {
    try {
      return (SyncMountManager) getMountManager(MountMode.BACKUP);
    } catch (IOException e) {
      LOG.error("Failed to get SyncMountManager");
      return null;
    }
  }

  public MountManagerSpi getMountManager(MountMode mountMode)
      throws IOException {
    Optional<MountManagerSpi> manager = mountMsgs.stream().filter(
        x -> x.isSupervisedMode(mountMode)).findFirst();
    return manager.orElseThrow(() -> new IOException(
        "No mount manager defined for this mode: " + mountMode));
  }

  /**
   * Check if the mount path already exists!
   *
   * @param mountPath the mount path.
   * @return true if the mount path exists or is being created.
   */
  synchronized boolean mountExists(Path mountPath) {
    return mountPoints.containsKey(mountPath) ||
        mountsUnderCreation.containsKey(mountPath);
  }

  /**
   * Remove the mount path from the list of mount paths maintained.
   *
   * @param mountPath the mount path.
   * @return Info of the removed volume.
   * @throws IOException if mount path was not finished.
   */
  synchronized ProvidedVolumeInfo removeMountPoint(Path mountPath)
      throws IOException {
    if (mountsUnderCreation.containsKey(mountPath)) {
      LOG.info("Removing mount under creation: {}", mountPath);
      ProvidedVolumeInfo info = mountsUnderCreation.remove(mountPath);
      return info;
    }
    if (!mountPoints.containsKey(mountPath)) {
      throw new IOException("Mount path " + mountPath +
          " does not exist");
    }
    LOG.info("Removing mount {}", mountPath);
    ProvidedVolumeInfo createdMount = mountPoints.get(mountPath);
    // Call specific mount manager's removeMount method.
    getMountManager(createdMount).removeMount(createdMount);

    // Remove read cache.
    List<BlockInfo> toDeleteList =
        FSDirUtil.getBlockInfos(namesystem, mountPath.toString());
    readCacheManager.removeCachedBlocks(toDeleteList);
    return mountPoints.remove(mountPath);
  }

  /**
   * @param mountPath mount path.
   * @param providedVolumeInfo {@link ProvidedVolumeInfo} of the mount.
   * @return the temporary location where the {@code mountPath} is created.
   */
  @VisibleForTesting
  String getTemporaryPathForMount(Path mountPath,
      ProvidedVolumeInfo providedVolumeInfo) {
    if (!mountPath.isAbsolute()) {
      throw new IllegalArgumentException(
          "Mount path " + mountPath + " is not absolute!");
    }
    String uuid = providedVolumeInfo.getId();
    String relativePath = mountPath.toString().substring(1);
    return new Path(
        new Path(rootTempDirectory, uuid), relativePath).toString();
  }

  /**
   * Get the mount path whose temporary path includes the given path.
   *
   * @param path the path to check.
   * @return the corresponding mount path or null if none exists.
   */
  synchronized Path getMountFromTemporaryPath(String path) {
    Matcher matcher = mountTempDirectoryPattern.matcher(path);
    if (!matcher.matches()) {
      return null;
    }

    String uuid = matcher.group(1);
    Path remainingPath = new Path(matcher.group(2));
    Path mountPath = getMount(remainingPath, mountsUnderCreation);
    if (mountPath == null) {
      return null;
    }
    // check that the UUID in path matches the UUID in the mountinfo.
    String mountUUID = mountsUnderCreation.get(mountPath).getId();
    if (!mountUUID.equals(uuid)) {
      LOG.error(
          "UUID of mounted path: {} doesn't match UUID of the mount: {}",
          uuid, mountUUID);
      return null;
    } else {
      return mountPath;
    }
  }

  /**
   * Get the mount path (completed or under construction) that includes the
   * given path.
   *
   * @param path the path to check.
   * @return the mount path that includes this path or null if none exists.
   */
  synchronized Path getMountPath(Path path) {
    if (path == null) {
      return null;
    }
    Path mount = getMount(path, mountPoints);
    return mount != null ? mount : getMount(path, mountsUnderCreation);
  }

  public ProvidedReadCacheManagerSpi getReadCacheManager() {
    return readCacheManager;
  }

  /**
   * Get the mount path that overlaps with the given path (one is a prefix of
   * the other). Only readOnly Mount is considered.
   *
   * @param path
   * @return the mount path that overlaps with the given path or null if none
   *         exist.
   */
  synchronized Path getOverlappingReadOnlyMount(Path path) {
    if (path == null) {
      return null;
    }
    Path mount = getOverlappingMount(path, mountPoints, MountMode.READONLY);
    return mount != null ? mount
        : getOverlappingMount(path, mountsUnderCreation, MountMode.READONLY);
  }

  /**
   * Similar to above method, except that writeBack mount is considered.
   */
  synchronized Path getOverlappingWriteBackMount(Path path) {
    if (path == null) {
      return null;
    }
    Path mount = getOverlappingMount(path, mountPoints, MountMode.WRITEBACK);
    return mount != null ? mount
        : getOverlappingMount(path, mountsUnderCreation, MountMode.WRITEBACK);
  }

  /**
   * Get the mount path that overlaps with the given path from the given
   * mounts. Only provided mounts with specified mode needs to be checked.
   *
   * @param path
   * @param mounts map of mount paths.
   * @param mountMode the mount mode considered.
   * @return the mount path that overlaps with the given path or null if none
   *         exist.
   */
  private static Path getOverlappingMount(Path path,
      Map<Path, ProvidedVolumeInfo> mounts, MountMode mountMode) {
    String pathStr = path.toString() + "/";
    for (Map.Entry<Path, ProvidedVolumeInfo> mountEntry : mounts.entrySet()) {
      if (mountEntry.getValue().getMountMode() != mountMode) {
        continue;
      }
      Path mount = mountEntry.getKey();
      // add / at the end to make sure that the path is actually a prefix
      String mountStr = mount.toString() + "/";
      if (pathStr.startsWith(mountStr) || mountStr.startsWith(pathStr)) {
        return mount;
      }
    }
    return null;
  }

  /**
   * Get the mount path that includes the given path from the given mounts.
   *
   * @param path
   * @param mounts map of mount paths
   * @return the mount path that includes this path or null if none exists.
   */
  private static Path getMount(Path path,
      Map<Path, ProvidedVolumeInfo> mounts) {
    for (Path mount : mounts.keySet()) {
      // add / at the end to make sure that the path is actually a prefix
      String mountStr = mount.toString() + "/";
      String pathStr = path.toString() + "/";
      if (pathStr.startsWith(mountStr)) {
        return mount;
      }
    }
    return null;
  }

  /**
   * @param mountPath mount path.
   * @return the {@link ProvidedVolumeInfo} associated with {@code mountPath}.
   */
  private synchronized ProvidedVolumeInfo getVolumeInfoForMount(
      Path mountPath) {
    ProvidedVolumeInfo info = mountPoints.get(mountPath);
    if (info == null) {
      info = mountsUnderCreation.get(mountPath);
    }
    return info;
  }

  /**
   * Get the existing mount paths or those being created.
   * @return map of mount to remote path.
   */
  @VisibleForTesting
  public synchronized Map<Path, ProvidedVolumeInfo> getMountPoints() {
    Map<Path, ProvidedVolumeInfo> allMounts = new HashMap<>();
    allMounts.putAll(mountPoints);
    allMounts.putAll(mountsUnderCreation);
    return allMounts;
  }

  /**
   * Get the finished mount paths.
   * @return map of mount to remote path.
   */
  synchronized Map<Path, ProvidedVolumeInfo> getFinishedMounts() {
    return Collections.unmodifiableMap(mountPoints);
  }

  /**
   * @return the map of unfinished mount paths.
   */
  synchronized Map<Path, ProvidedVolumeInfo> getUnfinishedMounts() {
    return Collections.unmodifiableMap(mountsUnderCreation);
  }

  /**
   * Clean up mounts if a failure occured during creation. This methods deletes
   * any intermediate files and state created during the mount process,
   * removes the mount path from the list of pending mounts and logs this
   * removal to the edit log. This method gets the namesystem write lock to
   * perform edit log operations.
   *
   * @param mountPath the mount path to remove.
   */
  void cleanUpMountOnFailure(Path mountPath) {
    cleanUpMountOnFailure(mountPath, true);
  }

  /**
   * Clean up mounts if a failure occurred during creation. This methods
   * deletes any intermediate files and state created during the mount process,
   * and removes the mount path from the list of pending mounts. This method
   * gets the namesystem write lock to perform edit log operations.
   *
   * @param mountPath the mount path to remove.
   * @param logCleanup flag to log the clean up to the edit log.
   */
  void cleanUpMountOnFailure(Path mountPath, boolean logCleanup) {
    // we should get Namesystem lock before getting the lock on this object.
    namesystem.writeLock();
    try {
      synchronized (this) {
        ProvidedVolumeInfo mountInfo = mountsUnderCreation.get(mountPath);
        if (mountInfo == null) {
          LOG.warn("Cleanup called for mount ({}) not under construction.",
              mountPath);
          return;
        }
        String tempMountPath = getTemporaryPathForMount(mountPath, mountInfo);
        // delete the paths that were created
        try {
          if (namesystem.getFileInfo(tempMountPath, false, false, false)
              != null) {
            namesystem.deleteChecked(tempMountPath, true, false, path->{});
          }
        } catch (IOException e) {
          LOG.warn("Exception while cleaning up mount {}: ", mountPath, e);
        }
        mountsUnderCreation.remove(mountPath);
      }
      if (logCleanup) {
        namesystem.getEditLog().logRemoveMountOp(mountPath.toString());
      }
    } finally {
      namesystem.writeUnlock();
    }
    namesystem.getEditLog().logSync();
  }

  /**
   * Update the status of the mounts when the Namenode becomes active. This
   * includes cleanup of the mounts that were not completed at the previous
   * active Namenode. The namesystem write lock needs to be obtained before
   * calling this method.
   *
   * @throws IOException
   */
  synchronized void startService() throws IOException {
    assert namesystem.hasWriteLock();
    if (mountsUnderCreation.size() > 0) {
      // we have some mounts that haven't finished on the previous active.
      // create a copy of the keys to avoid ConcurrentModificationException.
      Set<Path> mountPaths = new HashSet<>(mountsUnderCreation.keySet());
      for (Path mount : mountPaths) {
        // if the mount path doesn't exist, clean it up
        String mountStr = mount.toString();
        if (namesystem.getFileInfo(mountStr, false, false, false) == null) {
          LOG.info("Calling cleanup for mount {} after failover", mount);
          cleanUpMountOnFailure(mount);
        } else {
          // check if the path is actually a mount path.
          List<XAttr> xAttrList = namesystem.listXAttrs(mountStr);
          boolean isMounted = false;
          if (xAttrList.size() > 0) {
            for (XAttr xAttr : xAttrList) {
              if (xAttr.getName().equals(IS_MOUNT_XATTR_NAME) &&
                  xAttr.getNameSpace() == XAttr.NameSpace.USER) {
                isMounted = true;
              }
            }
          }
          if (isMounted) {
            LOG.info("Mount {} was successfully created before failover",
                mount);
            finishMount(mount, isMounted);
          } else {
            LOG.warn("Mount {} was unable to finish successfully before " +
                "failover but the path exists; deleting form list of mounts " +
                "under construction", mount);
            cleanUpMountOnFailure(mount);
          }
        }
      }
    }

    // delete whatever is under the temporary directory for the mounts.
    String tempMountDir = getRootTemporaryDir();
    if (!namesystem.isInSafeMode()
        && namesystem.getFileInfo(tempMountDir, false, false, false) != null) {
      namesystem.delete(tempMountDir, true, false);
    }
    readCacheManager.startService();
    // Start cache service for each mount manager.
    for (MountManagerSpi mountMsg: mountMsgs) {
      mountMsg.startService();
    }
  }

  public void stopService() {
    readCacheManager.stopService();
    for (MountManagerSpi mountMsg: mountMsgs) {
      mountMsg.stopService();
    }
  }

  public SyncServiceSatisfier getSyncServiceSatisfier() {
    return getSyncMountManager().getSyncServiceSatisfier();
  }

  @VisibleForTesting
  Pattern getMountTempDirectoryPattern() {
    return mountTempDirectoryPattern;
  }

  Path getRemotePathUnderMount(INodeFile file) {
    String localPathName = file.getName();
    Path mountPath = getMountFromTemporaryPath(localPathName);
    if (mountPath != null) {
      ProvidedVolumeInfo info = getVolumeInfoForMount(mountPath);
      Path baseRemotePath = new Path(info.getRemotePath());
      // change the prefix of the local name to the remote name.
      String tempPath = getTemporaryPathForMount(mountPath, info);
      String remotePath = localPathName
          .replace(tempPath, baseRemotePath.toString());
      return new Path(remotePath);
    }
    return null;
  }

  /**
   * @param requireStats whether stats for metrics are required.
   * @return List of existing mounts.
   */
  public List<MountInfo> listMounts(boolean requireStats) {
    List<MountInfo> mounts = new ArrayList<>();
    for (ProvidedVolumeInfo volInfo : mountPoints.values()) {
      mounts.add(new MountInfo(volInfo.getMountPath(), volInfo.getRemotePath(),
          volInfo.getMountMode(), MountInfo.MountStatus.CREATED,
          requireStats ? getMetrics(volInfo) : ""));
    }
    // For mounts under creation, there is no useful metric.
    for (ProvidedVolumeInfo volInfo : mountsUnderCreation.values()) {
      mounts.add(new MountInfo(volInfo.getMountPath(), volInfo.getRemotePath(),
          volInfo.getMountMode(), MountInfo.MountStatus.CREATING));
    }
    return mounts;
  }

  /**
   * Get metrics for a given mounted storage.
   */
  public String getMetrics(ProvidedVolumeInfo volumeInfo) {
    try {
      return getMountManager(volumeInfo.getMountMode()).getMetrics(volumeInfo);
    } catch (IOException e) {
      LOG.error("Failed to get metrics!");
      return "";
    }
  }

  /**
   * Get the summary for provided storage cache.
   */
  public String getCacheSummary() {
    long readCacheCapacity = readCacheManager.getCacheCapacityForProvided();
    long readCacheUsed = readCacheManager.getCacheUsedForProvided();
    long writeCacheCapacity =
        getSyncMountManager().getWriteCacheEvictor().getWriteCacheQuota();
    long writeCacheUsed =
        getSyncMountManager().getWriteCacheEvictor().getWriteCacheUsed();
    StringBuilder builder = new StringBuilder();
    builder.append(String.format("%-26s", "Read Cache Capacity: "));
    builder.append(readCacheCapacity + "\n");
    builder.append(String.format("%-26s", "Read Cache Used: "));
    builder.append(readCacheUsed + "\n");
    builder.append(String.format("%-26s", "Write Cache Capacity: "));
    builder.append(writeCacheCapacity + "\n");
    builder.append(String.format("%-26s", "Write Cache Used: "));
    builder.append(writeCacheUsed);
    return builder.toString();
  }

  /**
   * If provided volumes are disabled, this delegate mount manager will
   * disable mount manager functionality by ignoring all requests.
   */
  private static final class NoOpMountManager extends MountManager {
    private NoOpMountManager(FSNamesystem namesystem) {
      super(namesystem);
    }

    @Override
    Path getRemotePathUnderMount(INodeFile file) {
      return null;
    }

    @Override
    synchronized Path getMountFromTemporaryPath(String path) {
      return null;
    }

    @Override
    public void stopService() {
    }

    @Override
    synchronized void startService() {
    }
  }
}
