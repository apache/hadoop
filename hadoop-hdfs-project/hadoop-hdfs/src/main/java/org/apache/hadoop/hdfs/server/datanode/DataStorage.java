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

package org.apache.hadoop.hdfs.server.datanode;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.HardLink;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DiskChecker;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** 
 * Data storage information file.
 * <p>
 * @see Storage
 */
@InterfaceAudience.Private
public class DataStorage extends Storage {

  public final static String BLOCK_SUBDIR_PREFIX = "subdir";
  final static String STORAGE_DIR_DETACHED = "detach";
  public final static String STORAGE_DIR_RBW = "rbw";
  public final static String STORAGE_DIR_FINALIZED = "finalized";
  public final static String STORAGE_DIR_LAZY_PERSIST = "lazypersist";
  public final static String STORAGE_DIR_TMP = "tmp";

  /**
   * Set of bpids for which 'trash' is currently enabled.
   * When trash is enabled block files are moved under a separate
   * 'trash' folder instead of being deleted right away. This can
   * be useful during rolling upgrades, for example.
   * The set is backed by a concurrent HashMap.
   *
   * Even if trash is enabled, it is not used if a layout upgrade
   * is in progress for a storage directory i.e. if the previous
   * directory exists.
   */
  private Set<String> trashEnabledBpids;

  /**
   * Datanode UUID that this storage is currently attached to. This
   *  is the same as the legacy StorageID for datanodes that were
   *  upgraded from a pre-UUID version. For compatibility with prior
   *  versions of Datanodes we cannot make this field a UUID.
   */
  private String datanodeUuid = null;
  
  // Maps block pool IDs to block pool storage
  private final Map<String, BlockPoolSliceStorage> bpStorageMap
      = Collections.synchronizedMap(new HashMap<String, BlockPoolSliceStorage>());


  DataStorage() {
    super(NodeType.DATA_NODE);
    trashEnabledBpids = Collections.newSetFromMap(
        new ConcurrentHashMap<String, Boolean>());
  }
  
  public BlockPoolSliceStorage getBPStorage(String bpid) {
    return bpStorageMap.get(bpid);
  }
  
  public DataStorage(StorageInfo storageInfo) {
    super(storageInfo);
  }

  public synchronized String getDatanodeUuid() {
    return datanodeUuid;
  }

  public synchronized void setDatanodeUuid(String newDatanodeUuid) {
    this.datanodeUuid = newDatanodeUuid;
  }

  /** Create an ID for this storage.
   * @return true if a new storage ID was generated.
   * */
  public synchronized boolean createStorageID(
      StorageDirectory sd, boolean regenerateStorageIds) {
    final String oldStorageID = sd.getStorageUuid();
    if (oldStorageID == null || regenerateStorageIds) {
      sd.setStorageUuid(DatanodeStorage.generateUuid());
      LOG.info("Generated new storageID " + sd.getStorageUuid() +
          " for directory " + sd.getRoot() +
          (oldStorageID == null ? "" : (" to replace " + oldStorageID)));
      return true;
    }
    return false;
  }

  /**
   * Enable trash for the specified block pool storage. Even if trash is
   * enabled by the caller, it is superseded by the 'previous' directory
   * if a layout upgrade is in progress.
   */
  public void enableTrash(String bpid) {
    if (trashEnabledBpids.add(bpid)) {
      getBPStorage(bpid).stopTrashCleaner();
      LOG.info("Enabled trash for bpid " + bpid);
    }
  }

  public void clearTrash(String bpid) {
    if (trashEnabledBpids.contains(bpid)) {
      getBPStorage(bpid).clearTrash();
      trashEnabledBpids.remove(bpid);
      LOG.info("Cleared trash for bpid " + bpid);
    }
  }

  public boolean trashEnabled(String bpid) {
    return trashEnabledBpids.contains(bpid);
  }

  public void setRollingUpgradeMarker(String bpid) throws IOException {
    getBPStorage(bpid).setRollingUpgradeMarkers(storageDirs);
  }

  public void clearRollingUpgradeMarker(String bpid) throws IOException {
    getBPStorage(bpid).clearRollingUpgradeMarkers(storageDirs);
  }

  /**
   * If rolling upgrades are in progress then do not delete block files
   * immediately. Instead we move the block files to an intermediate
   * 'trash' directory. If there is a subsequent rollback, then the block
   * files will be restored from trash.
   *
   * @return trash directory if rolling upgrade is in progress, null
   *         otherwise.
   */
  public String getTrashDirectoryForBlockFile(String bpid, File blockFile) {
    if (trashEnabledBpids.contains(bpid)) {
      return getBPStorage(bpid).getTrashDirectory(blockFile);
    }
    return null;
  }

  /**
   * VolumeBuilder holds the metadata (e.g., the storage directories) of the
   * prepared volume returned from {@link prepareVolume()}. Calling {@link build()}
   * to add the metadata to {@link DataStorage} so that this prepared volume can
   * be active.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  static public class VolumeBuilder {
    private DataStorage storage;
    /** Volume level storage directory. */
    private StorageDirectory sd;
    /** Mapping from block pool ID to an array of storage directories. */
    private Map<String, List<StorageDirectory>> bpStorageDirMap =
        Maps.newHashMap();

    @VisibleForTesting
    public VolumeBuilder(DataStorage storage, StorageDirectory sd) {
      this.storage = storage;
      this.sd = sd;
    }

    public final StorageDirectory getStorageDirectory() {
      return this.sd;
    }

    private void addBpStorageDirectories(String bpid,
        List<StorageDirectory> dirs) {
      bpStorageDirMap.put(bpid, dirs);
    }

    /**
     * Add loaded metadata of a data volume to {@link DataStorage}.
     */
    public void build() {
      assert this.sd != null;
      synchronized (storage) {
        for (Map.Entry<String, List<StorageDirectory>> e :
            bpStorageDirMap.entrySet()) {
          final String bpid = e.getKey();
          BlockPoolSliceStorage bpStorage = this.storage.bpStorageMap.get(bpid);
          assert bpStorage != null;
          for (StorageDirectory bpSd : e.getValue()) {
            bpStorage.addStorageDir(bpSd);
          }
        }
        storage.addStorageDir(sd);
      }
    }
  }

  private StorageDirectory loadStorageDirectory(DataNode datanode,
      NamespaceInfo nsInfo, File dataDir, StartupOption startOpt)
      throws IOException {
    StorageDirectory sd = new StorageDirectory(dataDir, null, false);
    try {
      StorageState curState = sd.analyzeStorage(startOpt, this);
      // sd is locked but not opened
      switch (curState) {
      case NORMAL:
        break;
      case NON_EXISTENT:
        LOG.info("Storage directory " + dataDir + " does not exist");
        throw new IOException("Storage directory " + dataDir
            + " does not exist");
      case NOT_FORMATTED: // format
        LOG.info("Storage directory " + dataDir + " is not formatted for "
            + nsInfo.getBlockPoolID());
        LOG.info("Formatting ...");
        format(sd, nsInfo, datanode.getDatanodeUuid());
        break;
      default:  // recovery part is common
        sd.doRecover(curState);
      }

      // 2. Do transitions
      // Each storage directory is treated individually.
      // During startup some of them can upgrade or roll back
      // while others could be up-to-date for the regular startup.
      doTransition(datanode, sd, nsInfo, startOpt);

      // 3. Update successfully loaded storage.
      setServiceLayoutVersion(getServiceLayoutVersion());
      writeProperties(sd);

      return sd;
    } catch (IOException ioe) {
      sd.unlock();
      throw ioe;
    }
  }

  /**
   * Prepare a storage directory. It creates a builder which can be used to add
   * to the volume. If the volume cannot be added, it is OK to discard the
   * builder later.
   *
   * @param datanode DataNode object.
   * @param volume the root path of a storage directory.
   * @param nsInfos an array of namespace infos.
   * @return a VolumeBuilder that holds the metadata of this storage directory
   * and can be added to DataStorage later.
   * @throws IOException if encounters I/O errors.
   *
   * Note that if there is IOException, the state of DataStorage is not modified.
   */
  public VolumeBuilder prepareVolume(DataNode datanode, File volume,
      List<NamespaceInfo> nsInfos) throws IOException {
    if (containsStorageDir(volume)) {
      final String errorMessage = "Storage directory is in use";
      LOG.warn(errorMessage + ".");
      throw new IOException(errorMessage);
    }

    StorageDirectory sd = loadStorageDirectory(
        datanode, nsInfos.get(0), volume, StartupOption.HOTSWAP);
    VolumeBuilder builder =
        new VolumeBuilder(this, sd);
    for (NamespaceInfo nsInfo : nsInfos) {
      List<File> bpDataDirs = Lists.newArrayList();
      bpDataDirs.add(BlockPoolSliceStorage.getBpRoot(
          nsInfo.getBlockPoolID(), new File(volume, STORAGE_DIR_CURRENT)));
      makeBlockPoolDataDir(bpDataDirs, null);

      BlockPoolSliceStorage bpStorage;
      final String bpid = nsInfo.getBlockPoolID();
      synchronized (this) {
        bpStorage = this.bpStorageMap.get(bpid);
        if (bpStorage == null) {
          bpStorage = new BlockPoolSliceStorage(
              nsInfo.getNamespaceID(), bpid, nsInfo.getCTime(),
              nsInfo.getClusterID());
          addBlockPoolStorage(bpid, bpStorage);
        }
      }
      builder.addBpStorageDirectories(
          bpid, bpStorage.loadBpStorageDirectories(
              datanode, nsInfo, bpDataDirs, StartupOption.HOTSWAP));
    }
    return builder;
  }

  /**
   * Add a list of volumes to be managed by DataStorage. If the volume is empty,
   * format it, otherwise recover it from previous transitions if required.
   *
   * @param datanode the reference to DataNode.
   * @param nsInfo namespace information
   * @param dataDirs array of data storage directories
   * @param startOpt startup option
   * @return a list of successfully loaded volumes.
   * @throws IOException
   */
  @VisibleForTesting
  synchronized List<StorageLocation> addStorageLocations(DataNode datanode,
      NamespaceInfo nsInfo, Collection<StorageLocation> dataDirs,
      StartupOption startOpt) throws IOException {
    final String bpid = nsInfo.getBlockPoolID();
    List<StorageLocation> successVolumes = Lists.newArrayList();
    for (StorageLocation dataDir : dataDirs) {
      File root = dataDir.getFile();
      if (!containsStorageDir(root)) {
        try {
          // It first ensures the datanode level format is completed.
          StorageDirectory sd = loadStorageDirectory(
              datanode, nsInfo, root, startOpt);
          addStorageDir(sd);
        } catch (IOException e) {
          LOG.warn(e);
          continue;
        }
      } else {
        LOG.info("Storage directory " + dataDir + " has already been used.");
      }

      List<File> bpDataDirs = new ArrayList<File>();
      bpDataDirs.add(BlockPoolSliceStorage.getBpRoot(bpid, new File(root,
              STORAGE_DIR_CURRENT)));
      try {
        makeBlockPoolDataDir(bpDataDirs, null);
        BlockPoolSliceStorage bpStorage = this.bpStorageMap.get(bpid);
        if (bpStorage == null) {
          bpStorage = new BlockPoolSliceStorage(
              nsInfo.getNamespaceID(), bpid, nsInfo.getCTime(),
              nsInfo.getClusterID());
        }

        bpStorage.recoverTransitionRead(datanode, nsInfo, bpDataDirs, startOpt);
        addBlockPoolStorage(bpid, bpStorage);
      } catch (IOException e) {
        LOG.warn("Failed to add storage for block pool: " + bpid + " : "
            + e.getMessage());
        continue;
      }
      successVolumes.add(dataDir);
    }
    return successVolumes;
  }

  /**
   * Remove storage dirs from DataStorage. All storage dirs are removed even when the
   * IOException is thrown.
   *
   * @param dirsToRemove a set of storage directories to be removed.
   * @throws IOException if I/O error when unlocking storage directory.
   */
  synchronized void removeVolumes(final Set<File> dirsToRemove)
      throws IOException {
    if (dirsToRemove.isEmpty()) {
      return;
    }

    StringBuilder errorMsgBuilder = new StringBuilder();
    for (Iterator<StorageDirectory> it = this.storageDirs.iterator();
         it.hasNext(); ) {
      StorageDirectory sd = it.next();
      if (dirsToRemove.contains(sd.getRoot())) {
        // Remove the block pool level storage first.
        for (Map.Entry<String, BlockPoolSliceStorage> entry :
            this.bpStorageMap.entrySet()) {
          String bpid = entry.getKey();
          BlockPoolSliceStorage bpsStorage = entry.getValue();
          File bpRoot =
              BlockPoolSliceStorage.getBpRoot(bpid, sd.getCurrentDir());
          bpsStorage.remove(bpRoot.getAbsoluteFile());
        }

        it.remove();
        try {
          sd.unlock();
        } catch (IOException e) {
          LOG.warn(String.format(
            "I/O error attempting to unlock storage directory %s.",
            sd.getRoot()), e);
          errorMsgBuilder.append(String.format("Failed to remove %s: %s%n",
              sd.getRoot(), e.getMessage()));
        }
      }
    }
    if (errorMsgBuilder.length() > 0) {
      throw new IOException(errorMsgBuilder.toString());
    }
  }

  /**
   * Analyze storage directories for a specific block pool.
   * Recover from previous transitions if required.
   * Perform fs state transition if necessary depending on the namespace info.
   * Read storage info.
   * <br>
   * This method should be synchronized between multiple DN threads.  Only the
   * first DN thread does DN level storage dir recoverTransitionRead.
   *
   * @param datanode DataNode
   * @param nsInfo Namespace info of namenode corresponding to the block pool
   * @param dataDirs Storage directories
   * @param startOpt startup option
   * @throws IOException on error
   */
  void recoverTransitionRead(DataNode datanode, NamespaceInfo nsInfo,
      Collection<StorageLocation> dataDirs, StartupOption startOpt) throws IOException {
    if (addStorageLocations(datanode, nsInfo, dataDirs, startOpt).isEmpty()) {
      throw new IOException("All specified directories are failed to load.");
    }
  }

  /**
   * Create physical directory for block pools on the data node
   * 
   * @param dataDirs
   *          List of data directories
   * @param conf
   *          Configuration instance to use.
   * @throws IOException on errors
   */
  static void makeBlockPoolDataDir(Collection<File> dataDirs,
      Configuration conf) throws IOException {
    if (conf == null)
      conf = new HdfsConfiguration();

    LocalFileSystem localFS = FileSystem.getLocal(conf);
    FsPermission permission = new FsPermission(conf.get(
        DFSConfigKeys.DFS_DATANODE_DATA_DIR_PERMISSION_KEY,
        DFSConfigKeys.DFS_DATANODE_DATA_DIR_PERMISSION_DEFAULT));
    for (File data : dataDirs) {
      try {
        DiskChecker.checkDir(localFS, new Path(data.toURI()), permission);
      } catch ( IOException e ) {
        LOG.warn("Invalid directory in: " + data.getCanonicalPath() + ": "
            + e.getMessage());
      }
    }
  }

  void format(StorageDirectory sd, NamespaceInfo nsInfo,
              String datanodeUuid) throws IOException {
    sd.clearDirectory(); // create directory
    this.layoutVersion = HdfsServerConstants.DATANODE_LAYOUT_VERSION;
    this.clusterID = nsInfo.getClusterID();
    this.namespaceID = nsInfo.getNamespaceID();
    this.cTime = 0;
    setDatanodeUuid(datanodeUuid);

    if (sd.getStorageUuid() == null) {
      // Assign a new Storage UUID.
      sd.setStorageUuid(DatanodeStorage.generateUuid());
    }

    writeProperties(sd);
  }

  /*
   * Set ClusterID, StorageID, StorageType, CTime into
   * DataStorage VERSION file.
   * Always called just before writing the properties to
   * the VERSION file.
  */
  @Override
  protected void setPropertiesFromFields(Properties props, 
                           StorageDirectory sd 
                           ) throws IOException {
    props.setProperty("storageType", storageType.toString());
    props.setProperty("clusterID", clusterID);
    props.setProperty("cTime", String.valueOf(cTime));
    props.setProperty("layoutVersion", String.valueOf(layoutVersion));
    props.setProperty("storageID", sd.getStorageUuid());

    String datanodeUuid = getDatanodeUuid();
    if (datanodeUuid != null) {
      props.setProperty("datanodeUuid", datanodeUuid);
    }

    // Set NamespaceID in version before federation
    if (!DataNodeLayoutVersion.supports(
        LayoutVersion.Feature.FEDERATION, layoutVersion)) {
      props.setProperty("namespaceID", String.valueOf(namespaceID));
    }
  }

  /*
   * Read ClusterID, StorageID, StorageType, CTime from 
   * DataStorage VERSION file and verify them.
   * Always called just after reading the properties from the VERSION file.
   */
  @Override
  protected void setFieldsFromProperties(Properties props, StorageDirectory sd)
      throws IOException {
    setFieldsFromProperties(props, sd, false, 0);
  }

  private void setFieldsFromProperties(Properties props, StorageDirectory sd,
      boolean overrideLayoutVersion, int toLayoutVersion) throws IOException {
    if (overrideLayoutVersion) {
      this.layoutVersion = toLayoutVersion;
    } else {
      setLayoutVersion(props, sd);
    }
    setcTime(props, sd);
    checkStorageType(props, sd);
    setClusterId(props, layoutVersion, sd);
    
    // Read NamespaceID in version before federation
    if (!DataNodeLayoutVersion.supports(
        LayoutVersion.Feature.FEDERATION, layoutVersion)) {
      setNamespaceID(props, sd);
    }
    

    // valid storage id, storage id may be empty
    String ssid = props.getProperty("storageID");
    if (ssid == null) {
      throw new InconsistentFSStateException(sd.getRoot(), "file "
          + STORAGE_FILE_VERSION + " is invalid.");
    }
    String sid = sd.getStorageUuid();
    if (!(sid == null || sid.equals("") ||
          ssid.equals("") || sid.equals(ssid))) {
      throw new InconsistentFSStateException(sd.getRoot(),
          "has incompatible storage Id.");
    }

    if (sid == null) { // update id only if it was null
      sd.setStorageUuid(ssid);
    }

    // Update the datanode UUID if present.
    if (props.getProperty("datanodeUuid") != null) {
      String dnUuid = props.getProperty("datanodeUuid");

      if (getDatanodeUuid() == null) {
        setDatanodeUuid(dnUuid);
      } else if (getDatanodeUuid().compareTo(dnUuid) != 0) {
        throw new InconsistentFSStateException(sd.getRoot(),
            "Root " + sd.getRoot() + ": DatanodeUuid=" + dnUuid +
            ", does not match " + getDatanodeUuid() + " from other" +
            " StorageDirectory.");
      }
    }
  }

  @Override
  public boolean isPreUpgradableLayout(StorageDirectory sd) throws IOException {
    File oldF = new File(sd.getRoot(), "storage");
    if (!oldF.exists()) {
      return false;
    }
    // check the layout version inside the storage file
    // Lock and Read old storage file
    try (RandomAccessFile oldFile = new RandomAccessFile(oldF, "rws");
      FileLock oldLock = oldFile.getChannel().tryLock()) {
      if (null == oldLock) {
        LOG.error("Unable to acquire file lock on path " + oldF.toString());
        throw new OverlappingFileLockException();
      }
      oldFile.seek(0);
      int oldVersion = oldFile.readInt();
      if (oldVersion < LAST_PRE_UPGRADE_LAYOUT_VERSION) {
        return false;
      }
    }
    return true;
  }
  
  /** Read VERSION file for rollback */
  void readProperties(StorageDirectory sd, int rollbackLayoutVersion)
      throws IOException {
    Properties props = readPropertiesFile(sd.getVersionFile());
    setFieldsFromProperties(props, sd, true, rollbackLayoutVersion);
  }

  /**
   * Analize which and whether a transition of the fs state is required
   * and perform it if necessary.
   * 
   * Rollback if the rollback startup option was specified.
   * Upgrade if this.LV > LAYOUT_VERSION
   * Regular startup if this.LV = LAYOUT_VERSION
   * 
   * @param datanode Datanode to which this storage belongs to
   * @param sd  storage directory
   * @param nsInfo  namespace info
   * @param startOpt  startup option
   * @throws IOException
   */
  private void doTransition( DataNode datanode,
                             StorageDirectory sd, 
                             NamespaceInfo nsInfo, 
                             StartupOption startOpt
                             ) throws IOException {
    if (startOpt == StartupOption.ROLLBACK) {
      doRollback(sd, nsInfo); // rollback if applicable
    }
    readProperties(sd);
    checkVersionUpgradable(this.layoutVersion);
    assert this.layoutVersion >= HdfsServerConstants.DATANODE_LAYOUT_VERSION :
      "Future version is not allowed";
    
    boolean federationSupported = 
      DataNodeLayoutVersion.supports(
          LayoutVersion.Feature.FEDERATION, layoutVersion);
    // For pre-federation version - validate the namespaceID
    if (!federationSupported &&
        getNamespaceID() != nsInfo.getNamespaceID()) {
      throw new IOException("Incompatible namespaceIDs in "
          + sd.getRoot().getCanonicalPath() + ": namenode namespaceID = "
          + nsInfo.getNamespaceID() + "; datanode namespaceID = "
          + getNamespaceID());
    }
    
    // For version that supports federation, validate clusterID
    if (federationSupported
        && !getClusterID().equals(nsInfo.getClusterID())) {
      throw new IOException("Incompatible clusterIDs in "
          + sd.getRoot().getCanonicalPath() + ": namenode clusterID = "
          + nsInfo.getClusterID() + "; datanode clusterID = " + getClusterID());
    }

    // Clusters previously upgraded from layout versions earlier than
    // ADD_DATANODE_AND_STORAGE_UUIDS failed to correctly generate a
    // new storage ID. We check for that and fix it now.
    boolean haveValidStorageId =
        DataNodeLayoutVersion.supports(
            LayoutVersion.Feature.ADD_DATANODE_AND_STORAGE_UUIDS, layoutVersion) &&
            DatanodeStorage.isValidStorageId(sd.getStorageUuid());

    // regular start up.
    if (this.layoutVersion == HdfsServerConstants.DATANODE_LAYOUT_VERSION) {
      createStorageID(sd, !haveValidStorageId);
      return; // regular startup
    }

    // do upgrade
    if (this.layoutVersion > HdfsServerConstants.DATANODE_LAYOUT_VERSION) {
      doUpgrade(datanode, sd, nsInfo);  // upgrade
      createStorageID(sd, !haveValidStorageId);
      return;
    }
    
    // layoutVersion < DATANODE_LAYOUT_VERSION. I.e. stored layout version is newer
    // than the version supported by datanode. This should have been caught
    // in readProperties(), even if rollback was not carried out or somehow
    // failed.
    throw new IOException("BUG: The stored LV = " + this.getLayoutVersion()
        + " is newer than the supported LV = "
        + HdfsServerConstants.DATANODE_LAYOUT_VERSION);
  }

  /**
   * Upgrade -- Move current storage into a backup directory,
   * and hardlink all its blocks into the new current directory.
   * 
   * Upgrade from pre-0.22 to 0.22 or later release e.g. 0.19/0.20/ => 0.22/0.23
   * <ul>
   * <li> If <SD>/previous exists then delete it </li>
   * <li> Rename <SD>/current to <SD>/previous.tmp </li>
   * <li>Create new <SD>/current/<bpid>/current directory<li>
   * <ul>
   * <li> Hard links for block files are created from <SD>/previous.tmp 
   * to <SD>/current/<bpid>/current </li>
   * <li> Saves new version file in <SD>/current/<bpid>/current directory </li>
   * </ul>
   * <li> Rename <SD>/previous.tmp to <SD>/previous </li>
   * </ul>
   * 
   * There should be only ONE namenode in the cluster for first 
   * time upgrade to 0.22
   * @param sd  storage directory
   * @throws IOException on error
   */
  void doUpgrade(DataNode datanode, StorageDirectory sd, NamespaceInfo nsInfo)
      throws IOException {
    // If the existing on-disk layout version supportes federation, simply
    // update its layout version.
    if (DataNodeLayoutVersion.supports(
        LayoutVersion.Feature.FEDERATION, layoutVersion)) {
      // The VERSION file is already read in. Override the layoutVersion 
      // field and overwrite the file. The upgrade work is handled by
      // {@link BlockPoolSliceStorage#doUpgrade}
      LOG.info("Updating layout version from " + layoutVersion + " to "
          + HdfsServerConstants.DATANODE_LAYOUT_VERSION + " for storage "
          + sd.getRoot());
      layoutVersion = HdfsServerConstants.DATANODE_LAYOUT_VERSION;
      writeProperties(sd);
      return;
    }
    
    LOG.info("Upgrading storage directory " + sd.getRoot()
             + ".\n   old LV = " + this.getLayoutVersion()
             + "; old CTime = " + this.getCTime()
             + ".\n   new LV = " + HdfsServerConstants.DATANODE_LAYOUT_VERSION
             + "; new CTime = " + nsInfo.getCTime());
    
    File curDir = sd.getCurrentDir();
    File prevDir = sd.getPreviousDir();
    File bbwDir = new File(sd.getRoot(), Storage.STORAGE_1_BBW);

    assert curDir.exists() : "Data node current directory must exist.";
    // Cleanup directory "detach"
    cleanupDetachDir(new File(curDir, STORAGE_DIR_DETACHED));
    
    // 1. delete <SD>/previous dir before upgrading
    if (prevDir.exists())
      deleteDir(prevDir);
    // get previous.tmp directory, <SD>/previous.tmp
    File tmpDir = sd.getPreviousTmp();
    assert !tmpDir.exists() : 
      "Data node previous.tmp directory must not exist.";
    
    // 2. Rename <SD>/current to <SD>/previous.tmp
    rename(curDir, tmpDir);
    
    // 3. Format BP and hard link blocks from previous directory
    File curBpDir = BlockPoolSliceStorage.getBpRoot(nsInfo.getBlockPoolID(), curDir);
    BlockPoolSliceStorage bpStorage = new BlockPoolSliceStorage(nsInfo.getNamespaceID(), 
        nsInfo.getBlockPoolID(), nsInfo.getCTime(), nsInfo.getClusterID());
    bpStorage.format(curDir, nsInfo);
    linkAllBlocks(datanode, tmpDir, bbwDir, new File(curBpDir,
        STORAGE_DIR_CURRENT));
    
    // 4. Write version file under <SD>/current
    layoutVersion = HdfsServerConstants.DATANODE_LAYOUT_VERSION;
    clusterID = nsInfo.getClusterID();
    writeProperties(sd);
    
    // 5. Rename <SD>/previous.tmp to <SD>/previous
    rename(tmpDir, prevDir);
    LOG.info("Upgrade of " + sd.getRoot()+ " is complete");
    addBlockPoolStorage(nsInfo.getBlockPoolID(), bpStorage);
  }

  /**
   * Cleanup the detachDir. 
   * 
   * If the directory is not empty report an error; 
   * Otherwise remove the directory.
   * 
   * @param detachDir detach directory
   * @throws IOException if the directory is not empty or it can not be removed
   */
  private void cleanupDetachDir(File detachDir) throws IOException {
    if (!DataNodeLayoutVersion.supports(
        LayoutVersion.Feature.APPEND_RBW_DIR, layoutVersion) &&
        detachDir.exists() && detachDir.isDirectory() ) {
      
        if (FileUtil.list(detachDir).length != 0 ) {
          throw new IOException("Detached directory " + detachDir +
              " is not empty. Please manually move each file under this " +
              "directory to the finalized directory if the finalized " +
              "directory tree does not have the file.");
        } else if (!detachDir.delete()) {
          throw new IOException("Cannot remove directory " + detachDir);
        }
    }
  }
  
  /** 
   * Rolling back to a snapshot in previous directory by moving it to current
   * directory.
   * Rollback procedure:
   * <br>
   * If previous directory exists:
   * <ol>
   * <li> Rename current to removed.tmp </li>
   * <li> Rename previous to current </li>
   * <li> Remove removed.tmp </li>
   * </ol>
   * 
   * If previous directory does not exist and the current version supports
   * federation, perform a simple rollback of layout version. This does not
   * involve saving/restoration of actual data.
   */
  void doRollback( StorageDirectory sd,
                   NamespaceInfo nsInfo
                   ) throws IOException {
    File prevDir = sd.getPreviousDir();
    // This is a regular startup or a post-federation rollback
    if (!prevDir.exists()) {
      if (DataNodeLayoutVersion.supports(LayoutVersion.Feature.FEDERATION,
          HdfsServerConstants.DATANODE_LAYOUT_VERSION)) {
        readProperties(sd, HdfsServerConstants.DATANODE_LAYOUT_VERSION);
        writeProperties(sd);
        LOG.info("Layout version rolled back to "
            + HdfsServerConstants.DATANODE_LAYOUT_VERSION + " for storage "
            + sd.getRoot());
      }
      return;
    }
    DataStorage prevInfo = new DataStorage();
    prevInfo.readPreviousVersionProperties(sd);

    // We allow rollback to a state, which is either consistent with
    // the namespace state or can be further upgraded to it.
    if (!(prevInfo.getLayoutVersion() >= HdfsServerConstants.DATANODE_LAYOUT_VERSION
          && prevInfo.getCTime() <= nsInfo.getCTime()))  // cannot rollback
      throw new InconsistentFSStateException(sd.getRoot(),
          "Cannot rollback to a newer state.\nDatanode previous state: LV = "
              + prevInfo.getLayoutVersion() + " CTime = " + prevInfo.getCTime()
              + " is newer than the namespace state: LV = "
              + HdfsServerConstants.DATANODE_LAYOUT_VERSION + " CTime = "
              + nsInfo.getCTime());
    LOG.info("Rolling back storage directory " + sd.getRoot()
        + ".\n   target LV = " + HdfsServerConstants.DATANODE_LAYOUT_VERSION
        + "; target CTime = " + nsInfo.getCTime());
    File tmpDir = sd.getRemovedTmp();
    assert !tmpDir.exists() : "removed.tmp directory must not exist.";
    // rename current to tmp
    File curDir = sd.getCurrentDir();
    assert curDir.exists() : "Current directory must exist.";
    rename(curDir, tmpDir);
    // rename previous to current
    rename(prevDir, curDir);
    // delete tmp dir
    deleteDir(tmpDir);
    LOG.info("Rollback of " + sd.getRoot() + " is complete");
  }
  
  /**
   * Finalize procedure deletes an existing snapshot.
   * <ol>
   * <li>Rename previous to finalized.tmp directory</li>
   * <li>Fully delete the finalized.tmp directory</li>
   * </ol>
   * 
   * Do nothing, if previous directory does not exist
   */
  void doFinalize(StorageDirectory sd) throws IOException {
    File prevDir = sd.getPreviousDir();
    if (!prevDir.exists())
      return; // already discarded
    
    final String dataDirPath = sd.getRoot().getCanonicalPath();
    LOG.info("Finalizing upgrade for storage directory " 
             + dataDirPath 
             + ".\n   cur LV = " + this.getLayoutVersion()
             + "; cur CTime = " + this.getCTime());
    assert sd.getCurrentDir().exists() : "Current directory must exist.";
    final File tmpDir = sd.getFinalizedTmp();//finalized.tmp directory
    final File bbwDir = new File(sd.getRoot(), Storage.STORAGE_1_BBW);
    // 1. rename previous to finalized.tmp
    rename(prevDir, tmpDir);

    // 2. delete finalized.tmp dir in a separate thread
    // Also delete the blocksBeingWritten from HDFS 1.x and earlier, if
    // it exists.
    new Daemon(new Runnable() {
        @Override
        public void run() {
          try {
            deleteDir(tmpDir);
            if (bbwDir.exists()) {
              deleteDir(bbwDir);
            }
          } catch(IOException ex) {
            LOG.error("Finalize upgrade for " + dataDirPath + " failed", ex);
          }
          LOG.info("Finalize upgrade for " + dataDirPath + " is complete");
        }
        @Override
        public String toString() { return "Finalize " + dataDirPath; }
      }).start();
  }
  
  
  /*
   * Finalize the upgrade for a block pool
   * This also empties trash created during rolling upgrade and disables
   * trash functionality.
   */
  void finalizeUpgrade(String bpID) throws IOException {
    // To handle finalizing a snapshot taken at datanode level while
    // upgrading to federation, if datanode level snapshot previous exists, 
    // then finalize it. Else finalize the corresponding BP.
    for (StorageDirectory sd : storageDirs) {
      File prevDir = sd.getPreviousDir();
      if (prevDir.exists()) {
        // data node level storage finalize
        doFinalize(sd);
      } else {
        // block pool storage finalize using specific bpID
        BlockPoolSliceStorage bpStorage = bpStorageMap.get(bpID);
        bpStorage.doFinalize(sd.getCurrentDir());
      }
    }
  }

  /**
   * Hardlink all finalized and RBW blocks in fromDir to toDir
   *
   * @param fromDir      The directory where the 'from' snapshot is stored
   * @param fromBbwDir   In HDFS 1.x, the directory where blocks
   *                     that are under construction are stored.
   * @param toDir        The current data directory
   *
   * @throws IOException If error occurs during hardlink
   */
  private void linkAllBlocks(DataNode datanode, File fromDir, File fromBbwDir,
      File toDir) throws IOException {
    HardLink hardLink = new HardLink();
    // do the link
    int diskLayoutVersion = this.getLayoutVersion();
    if (DataNodeLayoutVersion.supports(
        LayoutVersion.Feature.APPEND_RBW_DIR, diskLayoutVersion)) {
      // hardlink finalized blocks in tmpDir/finalized
      linkBlocks(datanode, new File(fromDir, STORAGE_DIR_FINALIZED),
          new File(toDir, STORAGE_DIR_FINALIZED), diskLayoutVersion, hardLink);
      // hardlink rbw blocks in tmpDir/rbw
      linkBlocks(datanode, new File(fromDir, STORAGE_DIR_RBW),
          new File(toDir, STORAGE_DIR_RBW), diskLayoutVersion, hardLink);
    } else { // pre-RBW version
      // hardlink finalized blocks in tmpDir
      linkBlocks(datanode, fromDir, new File(toDir, STORAGE_DIR_FINALIZED),
          diskLayoutVersion, hardLink);      
      if (fromBbwDir.exists()) {
        /*
         * We need to put the 'blocksBeingWritten' from HDFS 1.x into the rbw
         * directory.  It's a little messy, because the blocksBeingWriten was
         * NOT underneath the 'current' directory in those releases.  See
         * HDFS-3731 for details.
         */
        linkBlocks(datanode, fromBbwDir,
            new File(toDir, STORAGE_DIR_RBW), diskLayoutVersion, hardLink);
      }
    } 
    LOG.info( hardLink.linkStats.report() );
  }

  private static class LinkArgs {
    File src;
    File dst;

    LinkArgs(File src, File dst) {
      this.src = src;
      this.dst = dst;
    }
  }

  static void linkBlocks(DataNode datanode, File from, File to, int oldLV,
      HardLink hl) throws IOException {
    boolean upgradeToIdBasedLayout = false;
    // If we are upgrading from a version older than the one where we introduced
    // block ID-based layout AND we're working with the finalized directory,
    // we'll need to upgrade from the old flat layout to the block ID-based one
    if (oldLV > DataNodeLayoutVersion.Feature.BLOCKID_BASED_LAYOUT.getInfo().
        getLayoutVersion() && to.getName().equals(STORAGE_DIR_FINALIZED)) {
      upgradeToIdBasedLayout = true;
    }

    final ArrayList<LinkArgs> idBasedLayoutSingleLinks = Lists.newArrayList();
    linkBlocksHelper(from, to, oldLV, hl, upgradeToIdBasedLayout, to,
        idBasedLayoutSingleLinks);

    // Detect and remove duplicate entries.
    final ArrayList<LinkArgs> duplicates =
        findDuplicateEntries(idBasedLayoutSingleLinks);
    if (!duplicates.isEmpty()) {
      LOG.error("There are " + duplicates.size() + " duplicate block " +
          "entries within the same volume.");
      removeDuplicateEntries(idBasedLayoutSingleLinks, duplicates);
    }

    int numLinkWorkers = datanode.getConf().getInt(
        DFSConfigKeys.DFS_DATANODE_BLOCK_ID_LAYOUT_UPGRADE_THREADS_KEY,
        DFSConfigKeys.DFS_DATANODE_BLOCK_ID_LAYOUT_UPGRADE_THREADS);
    ExecutorService linkWorkers = Executors.newFixedThreadPool(numLinkWorkers);
    final int step = idBasedLayoutSingleLinks.size() / numLinkWorkers + 1;
    List<Future<Void>> futures = Lists.newArrayList();
    for (int i = 0; i < idBasedLayoutSingleLinks.size(); i += step) {
      final int iCopy = i;
      futures.add(linkWorkers.submit(new Callable<Void>() {
        @Override
        public Void call() throws IOException {
          int upperBound = Math.min(iCopy + step,
              idBasedLayoutSingleLinks.size());
          for (int j = iCopy; j < upperBound; j++) {
            LinkArgs cur = idBasedLayoutSingleLinks.get(j);
            HardLink.createHardLink(cur.src, cur.dst);
          }
          return null;
        }
      }));
    }
    linkWorkers.shutdown();
    for (Future<Void> f : futures) {
      Futures.get(f, IOException.class);
    }
  }

  /**
   * Find duplicate entries with an array of LinkArgs.
   * Duplicate entries are entries with the same last path component.
   */
  static ArrayList<LinkArgs> findDuplicateEntries(ArrayList<LinkArgs> all) {
    // Find duplicates by sorting the list by the final path component.
    Collections.sort(all, new Comparator<LinkArgs>() {
      /**
       * Compare two LinkArgs objects, such that objects with the same
       * terminal source path components are grouped together.
       */
      @Override
      public int compare(LinkArgs a, LinkArgs b) {
        return ComparisonChain.start().
            compare(a.src.getName(), b.src.getName()).
            compare(a.src, b.src).
            compare(a.dst, b.dst).
            result();
      }
    });
    final ArrayList<LinkArgs> duplicates = Lists.newArrayList();
    Long prevBlockId = null;
    boolean prevWasMeta = false;
    boolean addedPrev = false;
    for (int i = 0; i < all.size(); i++) {
      LinkArgs args = all.get(i);
      long blockId = Block.getBlockId(args.src.getName());
      boolean isMeta = Block.isMetaFilename(args.src.getName());
      if ((prevBlockId == null) ||
          (prevBlockId.longValue() != blockId)) {
        prevBlockId = blockId;
        addedPrev = false;
      } else if (isMeta == prevWasMeta) {
        // If we saw another file for the same block ID previously,
        // and it had the same meta-ness as this file, we have a
        // duplicate.
        duplicates.add(args);
        if (!addedPrev) {
          duplicates.add(all.get(i - 1));
        }
        addedPrev = true;
      } else {
        addedPrev = false;
      }
      prevWasMeta = isMeta;
    }
    return duplicates;
  }

  /**
   * Remove duplicate entries from the list.
   * We do this by choosing:
   * 1. the entries with the highest genstamp (this takes priority),
   * 2. the entries with the longest block files,
   * 3. arbitrarily, if neither #1 nor #2 gives a clear winner.
   *
   * Block and metadata files form a pair-- if you take a metadata file from
   * one subdirectory, you must also take the block file from that
   * subdirectory.
   */
  private static void removeDuplicateEntries(ArrayList<LinkArgs> all,
                                             ArrayList<LinkArgs> duplicates) {
    // Maps blockId -> metadata file with highest genstamp
    TreeMap<Long, List<LinkArgs>> highestGenstamps =
        new TreeMap<Long, List<LinkArgs>>();
    for (LinkArgs duplicate : duplicates) {
      if (!Block.isMetaFilename(duplicate.src.getName())) {
        continue;
      }
      long blockId = Block.getBlockId(duplicate.src.getName());
      List<LinkArgs> prevHighest = highestGenstamps.get(blockId);
      if (prevHighest == null) {
        List<LinkArgs> highest = new LinkedList<LinkArgs>();
        highest.add(duplicate);
        highestGenstamps.put(blockId, highest);
        continue;
      }
      long prevGenstamp =
          Block.getGenerationStamp(prevHighest.get(0).src.getName());
      long genstamp = Block.getGenerationStamp(duplicate.src.getName());
      if (genstamp < prevGenstamp) {
        continue;
      }
      if (genstamp > prevGenstamp) {
        prevHighest.clear();
      }
      prevHighest.add(duplicate);
    }

    // Remove data / metadata entries that don't have the highest genstamp
    // from the duplicates list.
    for (Iterator<LinkArgs> iter = duplicates.iterator(); iter.hasNext(); ) {
      LinkArgs duplicate = iter.next();
      long blockId = Block.getBlockId(duplicate.src.getName());
      List<LinkArgs> highest = highestGenstamps.get(blockId);
      if (highest != null) {
        boolean found = false;
        for (LinkArgs high : highest) {
          if (high.src.getParent().equals(duplicate.src.getParent())) {
            found = true;
            break;
          }
        }
        if (!found) {
          LOG.warn("Unexpectedly low genstamp on " +
                   duplicate.src.getAbsolutePath() + ".");
          iter.remove();
        }
      }
    }

    // Find the longest block files
    // We let the "last guy win" here, since we're only interested in
    // preserving one block file / metadata file pair.
    TreeMap<Long, LinkArgs> longestBlockFiles = new TreeMap<Long, LinkArgs>();
    for (LinkArgs duplicate : duplicates) {
      if (Block.isMetaFilename(duplicate.src.getName())) {
        continue;
      }
      long blockId = Block.getBlockId(duplicate.src.getName());
      LinkArgs prevLongest = longestBlockFiles.get(blockId);
      if (prevLongest == null) {
        longestBlockFiles.put(blockId, duplicate);
        continue;
      }
      long blockLength = duplicate.src.length();
      long prevBlockLength = prevLongest.src.length();
      if (blockLength < prevBlockLength) {
        LOG.warn("Unexpectedly short length on " +
            duplicate.src.getAbsolutePath() + ".");
        continue;
      }
      if (blockLength > prevBlockLength) {
        LOG.warn("Unexpectedly short length on " +
            prevLongest.src.getAbsolutePath() + ".");
      }
      longestBlockFiles.put(blockId, duplicate);
    }

    // Remove data / metadata entries that aren't the longest, or weren't
    // arbitrarily selected by us.
    for (Iterator<LinkArgs> iter = all.iterator(); iter.hasNext(); ) {
      LinkArgs args = iter.next();
      long blockId = Block.getBlockId(args.src.getName());
      LinkArgs bestDuplicate = longestBlockFiles.get(blockId);
      if (bestDuplicate == null) {
        continue; // file has no duplicates
      }
      if (!bestDuplicate.src.getParent().equals(args.src.getParent())) {
        LOG.warn("Discarding " + args.src.getAbsolutePath() + ".");
        iter.remove();
      }
    }
  }

  static void linkBlocksHelper(File from, File to, int oldLV, HardLink hl,
  boolean upgradeToIdBasedLayout, File blockRoot,
      List<LinkArgs> idBasedLayoutSingleLinks) throws IOException {
    if (!from.exists()) {
      return;
    }
    if (!from.isDirectory()) {
      HardLink.createHardLink(from, to);
      hl.linkStats.countSingleLinks++;
      return;
    }
    // from is a directory
    hl.linkStats.countDirs++;
    
    String[] blockNames = from.list(new java.io.FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith(Block.BLOCK_FILE_PREFIX);
      }
    });

    // If we are upgrading to block ID-based layout, we don't want to recreate
    // any subdirs from the source that contain blocks, since we have a new
    // directory structure
    if (!upgradeToIdBasedLayout || !to.getName().startsWith(
        BLOCK_SUBDIR_PREFIX)) {
      if (!to.mkdirs())
        throw new IOException("Cannot create directory " + to);
    }

    // Block files just need hard links with the same file names
    // but a different directory
    if (blockNames.length > 0) {
      if (upgradeToIdBasedLayout) {
        for (String blockName : blockNames) {
          long blockId = Block.getBlockId(blockName);
          File blockLocation = DatanodeUtil.idToBlockDir(blockRoot, blockId);
          if (!blockLocation.exists()) {
            if (!blockLocation.mkdirs()) {
              throw new IOException("Failed to mkdirs " + blockLocation);
            }
          }
          idBasedLayoutSingleLinks.add(new LinkArgs(new File(from, blockName),
              new File(blockLocation, blockName)));
          hl.linkStats.countSingleLinks++;
        }
      } else {
        HardLink.createHardLinkMult(from, blockNames, to);
        hl.linkStats.countMultLinks++;
        hl.linkStats.countFilesMultLinks += blockNames.length;
      }
    } else {
      hl.linkStats.countEmptyDirs++;
    }
    
    // Now take care of the rest of the files and subdirectories
    String[] otherNames = from.list(new java.io.FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.startsWith(BLOCK_SUBDIR_PREFIX);
        }
      });
    for(int i = 0; i < otherNames.length; i++)
      linkBlocksHelper(new File(from, otherNames[i]),
          new File(to, otherNames[i]), oldLV, hl, upgradeToIdBasedLayout,
          blockRoot, idBasedLayoutSingleLinks);
  }

  /**
   * Add bpStorage into bpStorageMap
   */
  private void addBlockPoolStorage(String bpID, BlockPoolSliceStorage bpStorage
      ) {
    if (!this.bpStorageMap.containsKey(bpID)) {
      this.bpStorageMap.put(bpID, bpStorage);
    }
  }

  synchronized void removeBlockPoolStorage(String bpId) {
    bpStorageMap.remove(bpId);
  }
}
