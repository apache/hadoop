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

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.MountMode;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DisconnectPolicy;
import org.apache.hadoop.hdfs.protocol.MetadataSyncTaskOperation;
import org.apache.hadoop.hdfs.protocol.MountException;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SyncTaskStats;
import org.apache.hadoop.hdfs.server.common.ProvidedVolumeInfo;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectorySnapshottableFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.syncservice.SyncServiceSatisfier;
import org.apache.hadoop.hdfs.server.namenode.syncservice.WriteCacheEvictor;
import org.apache.hadoop.hdfs.server.protocol.BlockSyncTaskExecutionFeedback;
import org.apache.hadoop.hdfs.server.protocol.MetadataSyncTaskExecutionFeedback;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.hadoop.fs.XAttr.NameSpace.USER;
import static org.apache.hadoop.fs.XAttrSetFlag.CREATE;
import static org.apache.hadoop.fs.XAttrSetFlag.REPLACE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY;

/**
 * Plays a role to maintain backup/writeBack mount and do some preparation to
 * sync HDFS data to provided storage.
 */
public class SyncMountManager implements MountManagerSpi {
  private static final Logger LOG =
      LoggerFactory.getLogger(SyncMountManager.class);
  public static final MountMode[] MODE =
      new MountMode[]{MountMode.BACKUP, MountMode.WRITEBACK};

  public static final String PROVIDED_SYNC_PREVIOUS_FROM_SNAPSHOT_NAME =
      "PROVIDED_SYNC_PREVIOUS_FROM_SNAPSHOT_NAME";
  public static final String PROVIDED_SYNC_PREVIOUS_TO_SNAPSHOT_NAME =
      "PROVIDED_SYNC_PREVIOUS_TO_SNAPSHOT_NAME";
  public static final String NO_FROM_SNAPSHOT_YET = "no_snapshot_yet";

  private FSNamesystem fsNamesystem;
  private Configuration conf;
  private Map<ProvidedVolumeInfo, SyncTaskStats> syncMounts;
  private WriteCacheEvictor writeCacheEvictor;
  private boolean storagePolicyEnabled;
  private boolean syncServiceEnabled;
  private final SyncServiceSatisfier syncServiceSatisfier;

  private static SyncMountManager manager = null;

  private SyncMountManager(Configuration conf, FSNamesystem fsNamesystem) {
    this.conf = conf;
    this.fsNamesystem = fsNamesystem;
    this.syncMounts = Maps.newConcurrentMap();
    this.writeCacheEvictor = WriteCacheEvictor.getInstance(conf, fsNamesystem);
    storagePolicyEnabled =
        conf.getBoolean(DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY,
            DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_DEFAULT);
    syncServiceEnabled =
        conf.getBoolean(
            DFSConfigKeys.DFS_NAMENODE_PROVIDED_ENABLED,
            DFSConfigKeys.DFS_NAMENODE_PROVIDED_ENABLED_DEFAULT);
    this.syncServiceSatisfier =
        new SyncServiceSatisfier(fsNamesystem, this, conf);
  }

  public static MountManagerSpi getInstance(Configuration conf,
      FSNamesystem fsNamesystem) {
    if(manager == null) {
      manager = new SyncMountManager(conf, fsNamesystem);
      return manager;
    }
    return manager;
  }

  public WriteCacheEvictor getWriteCacheEvictor() {
    return writeCacheEvictor;
  }

  public Configuration getConf() {
    return conf;
  }

  public void createMount(ProvidedVolumeInfo syncMountToCreate,
      boolean isMounted)
      throws IOException {
    if (!isMounted) {
      initSnapshot(syncMountToCreate);
    }
    syncMounts.put(syncMountToCreate, SyncTaskStats.empty());
    LOG.info("Created {} successfully", syncMountToCreate);
  }

  public void initSnapshot(ProvidedVolumeInfo syncMountToCreate)
      throws IOException {
    String localBackupPath = syncMountToCreate.getMountPath();
    try {
      storeSnapshotNameAsXAttr(syncMountToCreate.getMountPath(),
          NO_FROM_SNAPSHOT_YET, NO_FROM_SNAPSHOT_YET, CREATE);
      fsNamesystem.allowSnapshot(localBackupPath);
      fsNamesystem.createSnapshot(localBackupPath, NO_FROM_SNAPSHOT_YET, true);
    } catch (IOException e) {
      fsNamesystem.disallowSnapshot(localBackupPath);
      throw new IOException("Could not set up directory for " +
          "creating initial snapshot", e);
    }
  }

  // Currently, pause & resume are not well implemented.
  public void pause(ProvidedVolumeInfo syncMount) throws MountException {
    SyncTaskStats stats = syncMounts.remove(syncMount);
    if (stats != null) {
      syncMount.pause();
      syncMounts.put(syncMount, stats);
    }
  }

  public void resume(ProvidedVolumeInfo syncMount) throws MountException {
    SyncTaskStats stats = syncMounts.remove(syncMount);
    if (stats != null) {
      syncMount.resume();
      syncMounts.put(syncMount, stats);
    }
  }

  public SyncTaskStats getStatistics(ProvidedVolumeInfo syncMount)
      throws MountException {
    SyncTaskStats stat = syncMounts.get(syncMount);
    if (stat == null) {
      throw new MountException("Given provided storage mount is not " +
          "found: id=" + syncMount.getId());
    }
    return stat;
  }

  @Override
  public String getMetrics(ProvidedVolumeInfo volumeInfo) {
    SyncTaskStats syncTaskStats;
    try {
      syncTaskStats = getStatistics(volumeInfo);
    } catch (MountException e) {
      return "";
    }
    StringBuilder builder = new StringBuilder();
    for (Map.Entry<MetadataSyncTaskOperation, SyncTaskStats.Metrics> entry :
        syncTaskStats.getMetaSuccesses().entrySet()) {
      builder.append(String.format("%-22s", entry.getKey() + ": "));
      builder.append(entry.getValue() + ", ");
      Integer failedNum = syncTaskStats.getMetaFailures().get(entry.getKey());
      builder.append(" FAILED_NUM=" +
          (failedNum == null ? 0 : failedNum) + "\n");
    }
    builder.append(String.format("%-22s", "DATA_SYNC: "));
    builder.append(syncTaskStats.getBlockSuccesses() + ", ");
    builder.append(" FAILED_NUM=" + syncTaskStats.getBlockFailures());
    return builder.toString();
  }

  public List<ProvidedVolumeInfo> getSyncMounts() {
    return Collections.unmodifiableList(
        Lists.newArrayList(syncMounts.keySet()));
  }

  public List<ProvidedVolumeInfo> getWriteBackMounts() {
    return Collections.unmodifiableList(getSyncMounts().stream()
        .filter(volume -> volume.isWriteBackMountMode())
        .collect(Collectors.toList()));
  }

  public ProvidedVolumeInfo getSyncMountById(String mountId)
      throws MountException {
    Optional<ProvidedVolumeInfo> matchingSyncMount =
        this.syncMounts.keySet().stream()
            .filter(syncMount -> syncMount.getId().equals(mountId))
            .findFirst();
    return matchingSyncMount.orElseThrow(
        () -> MountException.mountDoesNotExistException(mountId));
  }

  /**
   * For syncing data, an initial snapshot just lists the directory as being
   * new, since it's new to the sync system. However, the underlying
   * snapshotting infra doesn't work that way, so we construct the diff here.
   */
  private SnapshotDiffReport performInitialDiff(String localBackupPath,
      String snapshotName) {
    List<SnapshotDiffReport.DiffReportEntry> entryList = Lists.newArrayList();
    SnapshotDiffReport.DiffReportEntry entry =
        new SnapshotDiffReport.DiffReportEntry(
        SnapshotDiffReport.INodeType.DIRECTORY,
            SnapshotDiffReport.DiffType.CREATE,
        ".".getBytes());
    entryList.add(entry);
    return new SnapshotDiffReport(
        localBackupPath, null, snapshotName, entryList);
  }

  public SnapshotDiffReport forceInitialSnapshot(String localBackupPath)
      throws IOException {
    return makeSnapshotAndPerformDiffInternal(localBackupPath,
        NO_FROM_SNAPSHOT_YET);
  }

  public SnapshotDiffReport makeSnapshotAndPerformDiff(String localBackupPath)
      throws IOException {
    String fromSnapshotName = getPreviousToSnapshotName(localBackupPath);
    return makeSnapshotAndPerformDiffInternal(localBackupPath,
        fromSnapshotName);
  }

  public SnapshotDiffReport makeSnapshotAndPerformDiffInternal(
      String localBackupPath, String fromSnapshotName) throws IOException {
    String toSnapshotName = Snapshot.generateDefaultSnapshotName();
    storeSnapshotNameAsXAttr(localBackupPath, fromSnapshotName, toSnapshotName,
        REPLACE);
    fsNamesystem.createSnapshot(localBackupPath, toSnapshotName,
        true);

    if (NO_FROM_SNAPSHOT_YET.equals(fromSnapshotName)) {
      //initial case
      return performInitialDiff(localBackupPath, toSnapshotName);
    } else {
      //Normal case
      return fsNamesystem.getSnapshotDiffReport(
          localBackupPath, fromSnapshotName, toSnapshotName);
    }
  }

  public SnapshotDiffReport performPreviousDiff(String localBackupPath)
      throws IOException {
    String fromSnapshotName = getPreviousFromSnapshotName(localBackupPath);
    String toSnapshotName = getPreviousToSnapshotName(localBackupPath);
    if (NO_FROM_SNAPSHOT_YET.equals(fromSnapshotName)) {
      return performInitialDiff(localBackupPath, toSnapshotName);
    } else {
      return fsNamesystem.getSnapshotDiffReport(localBackupPath,
          fromSnapshotName, toSnapshotName);
    }
  }

  private void storeSnapshotNameAsXAttr(String localBackupPath,
      String fromSnapshotName,
      String toSnapshotName, XAttrSetFlag action) throws IOException {
    XAttr backupFromSnapshotNameXattr = new XAttr.Builder()
        .setNameSpace(USER)
        .setName(PROVIDED_SYNC_PREVIOUS_FROM_SNAPSHOT_NAME)
        .setValue(fromSnapshotName.getBytes())
        .build();
    XAttr backupToSnapshotNameXattr = new XAttr.Builder()
        .setNameSpace(USER)
        .setName(PROVIDED_SYNC_PREVIOUS_TO_SNAPSHOT_NAME)
        .setValue(toSnapshotName.getBytes())
        .build();
    try {
      fsNamesystem.setXAttr(localBackupPath,
          backupFromSnapshotNameXattr,
          EnumSet.of(action), false);
      fsNamesystem.setXAttr(localBackupPath,
          backupToSnapshotNameXattr,
          EnumSet.of(action), false);
    } catch (IOException e) {
      LOG.error("Could not set XAttr on {}", localBackupPath);
      throw e;
    }
  }

  private String getPreviousFromSnapshotName(String localBackupPath)
      throws IOException {
    return getXattrValueByName(localBackupPath,
        PROVIDED_SYNC_PREVIOUS_FROM_SNAPSHOT_NAME);
  }

  private String getPreviousToSnapshotName(String localBackupPath)
      throws IOException {
    return getXattrValueByName(localBackupPath,
        PROVIDED_SYNC_PREVIOUS_TO_SNAPSHOT_NAME);
  }

  private String getXattrValueByName(String localBackupPath, String xattrName)
      throws IOException {
    XAttr xattr = new XAttr.Builder()
        .setNameSpace(USER)
        .setName(xattrName)
        .build();
    List<XAttr> xAttrs = fsNamesystem.getXAttrs(localBackupPath,
        Lists.newArrayList(xattr));
    return xAttrs.stream()
        .findFirst()
        .map(xAttr -> new String(xAttr.getValue()))
        .orElseThrow(() -> new MountException("Failed to get XAttr: " +
            xattrName));
  }

  public synchronized void updateStats(MetadataSyncTaskExecutionFeedback
      feedback) {
    String syncMountId = feedback.getSyncMountId();
    ProvidedVolumeInfo key = findKey(syncMountId);
    SyncTaskStats stat = statify(feedback);
    syncMounts.merge(key, stat, SyncTaskStats::append);
  }

  public synchronized void updateStats(BlockSyncTaskExecutionFeedback
      feedback) {
    String syncMountId = feedback.getSyncMountId();
    ProvidedVolumeInfo key = findKey(syncMountId);
    SyncTaskStats stat = statify(feedback);
    syncMounts.merge(key, stat, SyncTaskStats::append);
  }

  private ProvidedVolumeInfo findKey(String syncMountId) {
    List<ProvidedVolumeInfo> matchings = syncMounts
        .keySet()
        .stream()
        .filter(syncMount -> syncMount.getId().equals(syncMountId))
        .collect(Collectors.toList());

    if (matchings.size() == 1) {
      return matchings.get(0);
    } else {
      throw new IllegalArgumentException("MountId not found: " + syncMountId);
    }
  }
  private SyncTaskStats statify(MetadataSyncTaskExecutionFeedback feedback) {
    return SyncTaskStats.from(feedback);
  }

  private SyncTaskStats statify(BlockSyncTaskExecutionFeedback feedback) {
    return SyncTaskStats.from(feedback);
  }

  public boolean isEmptyDiff(String localPath) {
    try {
      String snapshotName = getPreviousToSnapshotName(localPath);
      SnapshotDiffReport diffReport = fsNamesystem
          .getSnapshotDiffReport(localPath, snapshotName, "");
      List<SnapshotDiffReport.DiffReportEntry> diffList =
          diffReport.getDiffList();
      return diffList.isEmpty();
    } catch (IOException e) {
      LOG.error("Failed to get SnapshotDiffReport for: {}", localPath, e);
      return false;
    }
  }

  @Override
  public boolean isSupervisedMode(MountMode mountMode) {
    return MODE[0] == mountMode || MODE[1] == mountMode;
  }

  public boolean isWriteBackMount(String mountId) {
    try {
      return getSyncMountById(mountId).isWriteBackMountMode();
    } catch (MountException e) {
      LOG.warn(e.getMessage());
      return false;
    }
  }

  @Override
  public void removeMount(ProvidedVolumeInfo volInfo) throws IOException {
    // Currently, a fixed policy is used.
    removeMount(volInfo.getId(), DisconnectPolicy.GRACEFULLY);
  }

  public String removeMount(String mountId, DisconnectPolicy policy)
      throws IOException {
    ProvidedVolumeInfo syncMount = getSyncMountById(mountId);
    checkRemoveMount(syncMount, policy);
    syncMounts.remove(syncMount);
    clearSnapshot(syncMount);
    writeCacheEvictor.removeCacheUnderMount(syncMount.getMountPath());
    return syncMount.getMountPath();
  }

  /**
   * Only GRACEFULLY policy is supported currently. For this policy, the mount
   * cannot be removed until sync is finished at this moment.
   */
  private void checkRemoveMount(ProvidedVolumeInfo syncMount,
      DisconnectPolicy policy) {
    if (policy != DisconnectPolicy.GRACEFULLY) {
      throw new UnsupportedOperationException("Unsupported disconnect policy");
    }
    if (!isSyncFinished(syncMount)) {
      throw new RuntimeException("Sync task is running for this mount, " +
          "please have a try later!");
    }
  }

  /**
   * Prepare adding a new mount. A temp mount path will be created if not
   * exists.
   */
  @Override
  public void prepareMount(String tempMountPath, String remotePath,
      Configuration mergedConf) throws IOException {
    // Verify remote connection.
    verifyConnection(remotePath, mergedConf);
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    String group = mergedConf.get(DFS_PERMISSIONS_SUPERUSERGROUP_KEY,
        DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);
    if (!fsNamesystem.mkdirsChecked(tempMountPath,
        new PermissionStatus(user, group, FsPermission.getDirDefault()),
        true, path -> {
        })) {
      throw new IOException(
          "Unable to create temp mount path for mount use: " + tempMountPath);
    }
  }

  @Override
  public void finishMount(ProvidedVolumeInfo volumeInfo, boolean isMounted)
      throws IOException {
    createMount(volumeInfo, isMounted);
  }

  @Override
  public void startService() throws IOException {
    writeCacheEvictor.start();
    if (!(storagePolicyEnabled && syncServiceEnabled)) {
      LOG.warn("Failed to start sync service satisfier " +
              "as {} set to {} and {} set to {}.",
          DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY, storagePolicyEnabled,
          DFSConfigKeys.DFS_NAMENODE_PROVIDED_ENABLED, syncServiceEnabled);
      return;
    } else if (syncServiceSatisfier.isRunning()) {
      LOG.warn("Sync service satisfier is already running.");
      return;
    }
    syncServiceSatisfier.start();
  }

  @Override
  public void stopService() {
    writeCacheEvictor.stop();
    if (!(storagePolicyEnabled && syncServiceEnabled)) {
      LOG.info("Sync service satisfier is not enabled.");
      return;
    } else if (!syncServiceSatisfier.isRunning()) {
      LOG.info("Sync service satisfier is already stopped.");
      return;
    }
    syncServiceSatisfier.stop();
  }

  public SyncServiceSatisfier getSyncServiceSatisfier() {
    return syncServiceSatisfier;
  }

  public void finishSync(String syncMountId) {
    try {
      LOG.info("Sync finished, start to remove snapshot");
      ProvidedVolumeInfo volumeInfo = getSyncMountById(syncMountId);
      String snapshotName = getPreviousFromSnapshotName(
          volumeInfo.getMountPath());
      fsNamesystem.deleteSnapshot(volumeInfo.getMountPath(),
          snapshotName, true);
    } catch (IOException e) {
      LOG.warn("Failed to delete finished sync snapshot");
    }
  }

  public boolean isSyncFinished(ProvidedVolumeInfo syncMount) {
    try {
      String previousFromSnapshotName = getPreviousFromSnapshotName(
          syncMount.getMountPath());
      String previousToSnapshotName = getPreviousToSnapshotName(
          syncMount.getMountPath());
      if (previousFromSnapshotName.equals(previousToSnapshotName)) {
        return true;
      }
      INodeDirectory dir = fsNamesystem.getFSDirectory().getINode(
          syncMount.getMountPath()).asDirectory();
      DirectorySnapshottableFeature sf =
          dir.getDirectorySnapshottableFeature();
      if (sf == null) {
        return false;
      }
      ReadOnlyList<Snapshot> snapshotList = sf.getSnapshotList();
      for (Snapshot snapshot : snapshotList) {
        String snapshotName = Snapshot.getSnapshotName(snapshot);
        if (snapshotName.equals(previousFromSnapshotName)) {
          return false;
        }
      }
      return true;
    } catch (IOException e) {
      LOG.warn("Failed to get sync status");
      return false;
    }
  }

  private void clearSnapshot(ProvidedVolumeInfo volumeInfo) throws IOException{
    String previousToSnapshotName =
        getPreviousToSnapshotName(volumeInfo.getMountPath());
    fsNamesystem.deleteSnapshot(volumeInfo.getMountPath(),
        previousToSnapshotName, true);
    fsNamesystem.disallowSnapshot(volumeInfo.getMountPath());
  }

  public long getBlkCollectId(String src) throws IOException {
    return FSDirUtil.getBlkCollectId(fsNamesystem, src);
  }
}
