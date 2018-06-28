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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.GetSpaceUsed;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.ozone.common.InconsistentStorageStateException;
import org.apache.hadoop.ozone.container.common.DataNodeLayoutVersion;
import org.apache.hadoop.ozone.container.common.helpers.DatanodeVersionFile;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil;

import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * HddsVolume represents volume in a datanode. {@link VolumeSet} maitains a
 * list of HddsVolumes, one for each volume in the Datanode.
 * {@link VolumeInfo} in encompassed by this class.
 */
public final class HddsVolume {

  private static final Logger LOG = LoggerFactory.getLogger(HddsVolume.class);

  public static final String HDDS_VOLUME_DIR = "hdds";

  private final File hddsRootDir;
  private final VolumeInfo volumeInfo;
  private VolumeState state;

  // VERSION file properties
  private String storageID;       // id of the file system
  private String clusterID;       // id of the cluster
  private String datanodeUuid;    // id of the DataNode
  private long cTime;             // creation time of the file system state
  private int layoutVersion;      // layout version of the storage data

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

    public HddsVolume build() throws IOException {
      return new HddsVolume(this);
    }
  }

  private HddsVolume(Builder b) throws IOException {
    Preconditions.checkNotNull(b.volumeRootStr,
        "Volume root dir cannot be null");
    Preconditions.checkNotNull(b.datanodeUuid, "DatanodeUUID cannot be null");
    Preconditions.checkNotNull(b.conf, "Configuration cannot be null");

    StorageLocation location = StorageLocation.parse(b.volumeRootStr);
    hddsRootDir = new File(location.getUri().getPath(), HDDS_VOLUME_DIR);
    this.state = VolumeState.NOT_INITIALIZED;
    this.clusterID = b.clusterID;
    this.datanodeUuid = b.datanodeUuid;

    VolumeInfo.Builder volumeBuilder =
        new VolumeInfo.Builder(b.volumeRootStr, b.conf)
        .storageType(b.storageType)
        .configuredCapacity(b.configuredCapacity);
    this.volumeInfo = volumeBuilder.build();

    LOG.info("Creating Volume: " + this.hddsRootDir + " of  storage type : " +
        b.storageType + " and capacity : " + volumeInfo.getCapacity());

    initialize();
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
    default:
      throw new IOException("Unrecognized initial state : " +
          intialVolumeState + "of volume : " + hddsRootDir);
    }
  }

  private VolumeState analyzeVolumeState() {
    if (!hddsRootDir.exists()) {
      return VolumeState.NON_EXISTENT;
    }
    if (!getVersionFile().exists()) {
      return VolumeState.NOT_FORMATTED;
    }
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
    return volumeInfo.getStorageType();
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
    return volumeInfo.getCapacity();
  }

  public long getAvailable() throws IOException {
    return volumeInfo.getAvailable();
  }

  public void setState(VolumeState state) {
    this.state = state;
  }

  public boolean isFailed() {
    return (state == VolumeState.FAILED);
  }

  public void failVolume() {
    setState(VolumeState.FAILED);
    volumeInfo.shutdownUsageThread();
  }

  public void shutdown() {
    this.state = VolumeState.NON_EXISTENT;
    volumeInfo.shutdownUsageThread();
  }

  /**
   * VolumeState represents the different states a HddsVolume can be in.
   */
  public enum VolumeState {
    NORMAL,
    FAILED,
    NON_EXISTENT,
    NOT_FORMATTED,
    NOT_INITIALIZED
  }

  /**
   * Only for testing. Do not use otherwise.
   */
  @VisibleForTesting
  public void setScmUsageForTesting(GetSpaceUsed scmUsageForTest) {
    volumeInfo.setScmUsageForTesting(scmUsageForTest);
  }


}
