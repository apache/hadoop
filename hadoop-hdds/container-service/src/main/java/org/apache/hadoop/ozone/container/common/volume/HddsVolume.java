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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.sun.istack.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.GetSpaceUsed;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.checker.Checkable;
import org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult;
import org.apache.hadoop.ozone.common.InconsistentStorageStateException;
import org.apache.hadoop.ozone.container.common.DataNodeLayoutVersion;
import org.apache.hadoop.ozone.container.common.helpers.DatanodeVersionFile;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil;

import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.Time;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

/**
 * HddsVolume represents volume in a datanode. {@link VolumeSet} maintains a
 * list of HddsVolumes, one for each volume in the Datanode.
 * {@link VolumeInfo} in encompassed by this class.
 * <p>
 * The disk layout per volume is as follows:
 * <p>../hdds/VERSION
 * <p>{@literal ../hdds/<<scmUuid>>/current/<<containerDir>>/<<containerID
 * >>/metadata}
 * <p>{@literal ../hdds/<<scmUuid>>/current/<<containerDir>>/<<containerID
 * >>/<<dataDir>>}
 * <p>
 * Each hdds volume has its own VERSION file. The hdds volume will have one
 * scmUuid directory for each SCM it is a part of (currently only one SCM is
 * supported).
 *
 * During DN startup, if the VERSION file exists, we verify that the
 * clusterID in the version file matches the clusterID from SCM.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@SuppressWarnings("finalclass")
public class HddsVolume
    implements Checkable<Boolean, VolumeCheckResult> {

  private static final Logger LOG = LoggerFactory.getLogger(HddsVolume.class);

  public static final String HDDS_VOLUME_DIR = "hdds";

  private final File hddsRootDir;
  private final VolumeInfo volumeInfo;
  private VolumeState state;
  private final VolumeIOStats volumeIOStats;

  // VERSION file properties
  private String storageID;       // id of the file system
  private String clusterID;       // id of the cluster
  private String datanodeUuid;    // id of the DataNode
  private long cTime;             // creation time of the file system state
  private int layoutVersion;      // layout version of the storage data

  /**
   * Run a check on the current volume to determine if it is healthy.
   * @param unused context for the check, ignored.
   * @return result of checking the volume.
   * @throws Exception if an exception was encountered while running
   *            the volume check.
   */
  @Override
  public VolumeCheckResult check(@Nullable Boolean unused) throws Exception {
    DiskChecker.checkDir(hddsRootDir);
    return VolumeCheckResult.HEALTHY;
  }

  /**
   * Builder for HddsVolume.
   */
  public static class Builder {
    private final String volumeRootStr;
    private Configuration conf;
    private StorageType storageType;
    private long configuredCapacity;

    private String datanodeUuid;
    private String clusterID;
    private boolean failedVolume = false;

    public Builder(String rootDirStr) {
      this.volumeRootStr = rootDirStr;
    }

    public Builder conf(Configuration config) {
      this.conf = config;
      return this;
    }

    public Builder storageType(StorageType st) {
      this.storageType = st;
      return this;
    }

    public Builder configuredCapacity(long capacity) {
      this.configuredCapacity = capacity;
      return this;
    }

    public Builder datanodeUuid(String datanodeUUID) {
      this.datanodeUuid = datanodeUUID;
      return this;
    }

    public Builder clusterID(String cid) {
      this.clusterID = cid;
      return this;
    }

    // This is added just to create failed volume objects, which will be used
    // to create failed HddsVolume objects in the case of any exceptions caused
    // during creating HddsVolume object.
    public Builder failedVolume(boolean failed) {
      this.failedVolume = failed;
      return this;
    }

    public HddsVolume build() throws IOException {
      return new HddsVolume(this);
    }
  }

  private HddsVolume(Builder b) throws IOException {
    if (!b.failedVolume) {
      StorageLocation location = StorageLocation.parse(b.volumeRootStr);
      hddsRootDir = new File(location.getUri().getPath(), HDDS_VOLUME_DIR);
      this.state = VolumeState.NOT_INITIALIZED;
      this.clusterID = b.clusterID;
      this.datanodeUuid = b.datanodeUuid;
      this.volumeIOStats = new VolumeIOStats();

      VolumeInfo.Builder volumeBuilder =
          new VolumeInfo.Builder(b.volumeRootStr, b.conf)
              .storageType(b.storageType)
              .configuredCapacity(b.configuredCapacity);
      this.volumeInfo = volumeBuilder.build();

      LOG.info("Creating Volume: " + this.hddsRootDir + " of  storage type : " +
          b.storageType + " and capacity : " + volumeInfo.getCapacity());

      initialize();
    } else {
      // Builder is called with failedVolume set, so create a failed volume
      // HddsVolumeObject.
      hddsRootDir = new File(b.volumeRootStr);
      volumeIOStats = null;
      volumeInfo = null;
      storageID = UUID.randomUUID().toString();
      state = VolumeState.FAILED;
    }
  }

  public VolumeInfo getVolumeInfo() {
    return volumeInfo;
  }

  /**
   * Initializes the volume.
   * Creates the Version file if not present,
   * otherwise returns with IOException.
   * @throws IOException
   */
  private void initialize() throws IOException {
    VolumeState intialVolumeState = analyzeVolumeState();
    switch (intialVolumeState) {
    case NON_EXISTENT:
      // Root directory does not exist. Create it.
      if (!hddsRootDir.mkdir()) {
        throw new IOException("Cannot create directory " + hddsRootDir);
      }
      setState(VolumeState.NOT_FORMATTED);
      createVersionFile();
      break;
    case NOT_FORMATTED:
      // Version File does not exist. Create it.
      createVersionFile();
      break;
    case NOT_INITIALIZED:
      // Version File exists. Verify its correctness and update property fields.
      readVersionFile();
      setState(VolumeState.NORMAL);
      break;
    case INCONSISTENT:
      // Volume Root is in an inconsistent state. Skip loading this volume.
      throw new IOException("Volume is in an " + VolumeState.INCONSISTENT +
          " state. Skipped loading volume: " + hddsRootDir.getPath());
    default:
      throw new IOException("Unrecognized initial state : " +
          intialVolumeState + "of volume : " + hddsRootDir);
    }
  }

  private VolumeState analyzeVolumeState() {
    if (!hddsRootDir.exists()) {
      // Volume Root does not exist.
      return VolumeState.NON_EXISTENT;
    }
    if (!hddsRootDir.isDirectory()) {
      // Volume Root exists but is not a directory.
      return VolumeState.INCONSISTENT;
    }
    File[] files = hddsRootDir.listFiles();
    if (files == null || files.length == 0) {
      // Volume Root exists and is empty.
      return VolumeState.NOT_FORMATTED;
    }
    if (!getVersionFile().exists()) {
      // Volume Root is non empty but VERSION file does not exist.
      return VolumeState.INCONSISTENT;
    }
    // Volume Root and VERSION file exist.
    return VolumeState.NOT_INITIALIZED;
  }

  public void format(String cid) throws IOException {
    Preconditions.checkNotNull(cid, "clusterID cannot be null while " +
        "formatting Volume");
    this.clusterID = cid;
    initialize();
  }

  /**
   * Create Version File and write property fields into it.
   * @throws IOException
   */
  private void createVersionFile() throws IOException {
    this.storageID = HddsVolumeUtil.generateUuid();
    this.cTime = Time.now();
    this.layoutVersion = ChunkLayOutVersion.getLatestVersion().getVersion();

    if (this.clusterID == null || datanodeUuid == null) {
      // HddsDatanodeService does not have the cluster information yet. Wait
      // for registration with SCM.
      LOG.debug("ClusterID not available. Cannot format the volume {}",
          this.hddsRootDir.getPath());
      setState(VolumeState.NOT_FORMATTED);
    } else {
      // Write the version file to disk.
      writeVersionFile();
      setState(VolumeState.NORMAL);
    }
  }

  private void writeVersionFile() throws IOException {
    Preconditions.checkNotNull(this.storageID,
        "StorageID cannot be null in Version File");
    Preconditions.checkNotNull(this.clusterID,
        "ClusterID cannot be null in Version File");
    Preconditions.checkNotNull(this.datanodeUuid,
        "DatanodeUUID cannot be null in Version File");
    Preconditions.checkArgument(this.cTime > 0,
        "Creation Time should be positive");
    Preconditions.checkArgument(this.layoutVersion ==
            DataNodeLayoutVersion.getLatestVersion().getVersion(),
        "Version File should have the latest LayOutVersion");

    File versionFile = getVersionFile();
    LOG.debug("Writing Version file to disk, {}", versionFile);

    DatanodeVersionFile dnVersionFile = new DatanodeVersionFile(this.storageID,
        this.clusterID, this.datanodeUuid, this.cTime, this.layoutVersion);
    dnVersionFile.createVersionFile(versionFile);
  }

  /**
   * Read Version File and update property fields.
   * Get common storage fields.
   * Should be overloaded if additional fields need to be read.
   *
   * @throws IOException on error
   */
  private void readVersionFile() throws IOException {
    File versionFile = getVersionFile();
    Properties props = DatanodeVersionFile.readFrom(versionFile);
    if (props.isEmpty()) {
      throw new InconsistentStorageStateException(
          "Version file " + versionFile + " is missing");
    }

    LOG.debug("Reading Version file from disk, {}", versionFile);
    this.storageID = HddsVolumeUtil.getStorageID(props, versionFile);
    this.clusterID = HddsVolumeUtil.getClusterID(props, versionFile,
        this.clusterID);
    this.datanodeUuid = HddsVolumeUtil.getDatanodeUUID(props, versionFile,
        this.datanodeUuid);
    this.cTime = HddsVolumeUtil.getCreationTime(props, versionFile);
    this.layoutVersion = HddsVolumeUtil.getLayOutVersion(props, versionFile);
  }

  private File getVersionFile() {
    return HddsVolumeUtil.getVersionFile(hddsRootDir);
  }

  public File getHddsRootDir() {
    return hddsRootDir;
  }

  public StorageType getStorageType() {
    if(volumeInfo != null) {
      return volumeInfo.getStorageType();
    }
    return StorageType.DEFAULT;
  }

  public String getStorageID() {
    return storageID;
  }

  public String getClusterID() {
    return clusterID;
  }

  public String getDatanodeUuid() {
    return datanodeUuid;
  }

  public long getCTime() {
    return cTime;
  }

  public int getLayoutVersion() {
    return layoutVersion;
  }

  public VolumeState getStorageState() {
    return state;
  }

  public long getCapacity() throws IOException {
    if(volumeInfo != null) {
      return volumeInfo.getCapacity();
    }
    return 0;
  }

  public long getAvailable() throws IOException {
    if(volumeInfo != null) {
      return volumeInfo.getAvailable();
    }
    return 0;
  }

  public void setState(VolumeState state) {
    this.state = state;
  }

  public boolean isFailed() {
    return (state == VolumeState.FAILED);
  }

  public VolumeIOStats getVolumeIOStats() {
    return volumeIOStats;
  }

  public void failVolume() {
    setState(VolumeState.FAILED);
    if (volumeInfo != null) {
      volumeInfo.shutdownUsageThread();
    }
  }

  public void shutdown() {
    this.state = VolumeState.NON_EXISTENT;
    if (volumeInfo != null) {
      volumeInfo.shutdownUsageThread();
    }
  }

  /**
   * VolumeState represents the different states a HddsVolume can be in.
   * NORMAL          =&gt; Volume can be used for storage
   * FAILED          =&gt; Volume has failed due and can no longer be used for
   *                    storing containers.
   * NON_EXISTENT    =&gt; Volume Root dir does not exist
   * INCONSISTENT    =&gt; Volume Root dir is not empty but VERSION file is
   *                    missing or Volume Root dir is not a directory
   * NOT_FORMATTED   =&gt; Volume Root exists but not formatted(no VERSION file)
   * NOT_INITIALIZED =&gt; VERSION file exists but has not been verified for
   *                    correctness.
   */
  public enum VolumeState {
    NORMAL,
    FAILED,
    NON_EXISTENT,
    INCONSISTENT,
    NOT_FORMATTED,
    NOT_INITIALIZED
  }

  /**
   * Only for testing. Do not use otherwise.
   */
  @VisibleForTesting
  public void setScmUsageForTesting(GetSpaceUsed scmUsageForTest) {
    if (volumeInfo != null) {
      volumeInfo.setScmUsageForTesting(scmUsageForTest);
    }
  }
}
