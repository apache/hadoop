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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.HardLink;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Lists;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ComparisonChain;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;

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
  private volatile String datanodeUuid = null;
  
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

  public String getDatanodeUuid() {
    return datanodeUuid;
  }

  public void setDatanodeUuid(String newDatanodeUuid) {
    this.datanodeUuid = newDatanodeUuid;
  }

  private static boolean createStorageID(StorageDirectory sd, int lv,
      Configuration conf) {
    // Clusters previously upgraded from layout versions earlier than
    // ADD_DATANODE_AND_STORAGE_UUIDS failed to correctly generate a
    // new storage ID. We check for that and fix it now.
    final boolean haveValidStorageId = DataNodeLayoutVersion.supports(
        LayoutVersion.Feature.ADD_DATANODE_AND_STORAGE_UUIDS, lv)
        && DatanodeStorage.isValidStorageId(sd.getStorageUuid());
    return createStorageID(sd, !haveValidStorageId, conf);
  }

  /** Create an ID for this storage.
   * @return true if a new storage ID was generated.
   * */
  public static boolean createStorageID(
      StorageDirectory sd, boolean regenerateStorageIds, Configuration conf) {
    final String oldStorageID = sd.getStorageUuid();
    if (sd.getStorageLocation() != null &&
        sd.getStorageLocation().getStorageType() == StorageType.PROVIDED) {
      // Only one provided storage id is supported.
      // TODO support multiple provided storage ids
      sd.setStorageUuid(conf.get(DFSConfigKeys.DFS_PROVIDER_STORAGEUUID,
          DFSConfigKeys.DFS_PROVIDER_STORAGEUUID_DEFAULT));
      return false;
    }
    if (oldStorageID == null || regenerateStorageIds) {
      sd.setStorageUuid(DatanodeStorage.generateUuid());
      LOG.info("Generated new storageID {} for directory {} {}", sd
              .getStorageUuid(), sd.getRoot(),
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
      LOG.info("Enabled trash for bpid {}",  bpid);
    }
  }

  public void clearTrash(String bpid) {
    if (trashEnabledBpids.contains(bpid)) {
      getBPStorage(bpid).clearTrash();
      trashEnabledBpids.remove(bpid);
      LOG.info("Cleared trash for bpid {}", bpid);
    }
  }

  public boolean trashEnabled(String bpid) {
    return trashEnabledBpids.contains(bpid);
  }

  public void setRollingUpgradeMarker(String bpid) throws IOException {
    getBPStorage(bpid).setRollingUpgradeMarkers(getStorageDirs());
  }

  public void clearRollingUpgradeMarker(String bpid) throws IOException {
    getBPStorage(bpid).clearRollingUpgradeMarkers(getStorageDirs());
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
  public String getTrashDirectoryForReplica(String bpid, ReplicaInfo info) {
    if (trashEnabledBpids.contains(bpid)) {
      return getBPStorage(bpid).getTrashDirectory(info);
    }
    return null;
  }

  /**
   * VolumeBuilder holds the metadata (e.g., the storage directories) of the
   * prepared volume returned from
   * {@link #prepareVolume(DataNode, StorageLocation, List)}.
   * Calling {@link VolumeBuilder#build()}
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
      NamespaceInfo nsInfo, StorageLocation location, StartupOption startOpt,
      List<Callable<StorageDirectory>> callables) throws IOException {
    StorageDirectory sd = new StorageDirectory(null, false, location);
    try {
      StorageState curState = sd.analyzeStorage(startOpt, this, true);
      // sd is locked but not opened
      switch (curState) {
      case NORMAL:
        break;
      case NON_EXISTENT:
        LOG.info("Storage directory with location {} does not exist", location);
        throw new IOException("Storage directory with location " + location
            + " does not exist");
      case NOT_FORMATTED: // format
        LOG.info("Storage directory with location {} is not formatted for "
            + "namespace {}. Formatting...", location, nsInfo.getNamespaceID());
        format(sd, nsInfo, datanode.getDatanodeUuid(), datanode.getConf());
        break;
      default:  // recovery part is common
        sd.doRecover(curState);
      }

      // 2. Do transitions
      // Each storage directory is treated individually.
      // During startup some of them can upgrade or roll back
      // while others could be up-to-date for the regular startup.
      if (!doTransition(sd, nsInfo, startOpt, callables, datanode.getConf())) {

        // 3. Update successfully loaded storage.
        setServiceLayoutVersion(getServiceLayoutVersion());
        writeProperties(sd);
      }

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
   * @param location the StorageLocation for the storage directory.
   * @param nsInfos an array of namespace infos.
   * @return a VolumeBuilder that holds the metadata of this storage directory
   * and can be added to DataStorage later.
   * @throws IOException if encounters I/O errors.
   *
   * Note that if there is IOException, the state of DataStorage is not modified.
   */
  public VolumeBuilder prepareVolume(DataNode datanode,
      StorageLocation location, List<NamespaceInfo> nsInfos)
          throws IOException {
    if (containsStorageDir(location)) {
      final String errorMessage = "Storage directory is in use.";
      LOG.warn(errorMessage);
      throw new IOException(errorMessage);
    }

    StorageDirectory sd = loadStorageDirectory(
        datanode, nsInfos.get(0), location, StartupOption.HOTSWAP, null);
    VolumeBuilder builder =
        new VolumeBuilder(this, sd);
    for (NamespaceInfo nsInfo : nsInfos) {
      location.makeBlockPoolDir(nsInfo.getBlockPoolID(), datanode.getConf());

      final BlockPoolSliceStorage bpStorage = getBlockPoolSliceStorage(nsInfo);
      final List<StorageDirectory> dirs = bpStorage.loadBpStorageDirectories(
          nsInfo, location, StartupOption.HOTSWAP, null, datanode.getConf());
      builder.addBpStorageDirectories(nsInfo.getBlockPoolID(), dirs);
    }
    return builder;
  }

  static int getParallelVolumeLoadThreadsNum(int dataDirs, Configuration conf) {
    final String key
        = DFSConfigKeys.DFS_DATANODE_PARALLEL_VOLUME_LOAD_THREADS_NUM_KEY;
    final int n = conf.getInt(key, dataDirs);
    if (n < 1) {
      throw new HadoopIllegalArgumentException(key + " = " + n + " < 1");
    }
    final int min = Math.min(n, dataDirs);
    LOG.info("Using {} threads to upgrade data directories ({}={}, "
        + "dataDirs={})", min, key, n, dataDirs);
    return min;
  }

  static class UpgradeTask {
    private final StorageLocation dataDir;
    private final Future<StorageDirectory> future;

    UpgradeTask(StorageLocation dataDir, Future<StorageDirectory> future) {
      this.dataDir = dataDir;
      this.future = future;
    }
  }

  /**
   * Add a list of volumes to be managed by DataStorage. If the volume is empty,
   * format it, otherwise recover it from previous transitions if required.
   *
   * @param datanode the reference to DataNode.
   * @param nsInfo namespace information
   * @param dataDirs array of data storage directories
   * @param startOpt startup option
   * @return a list of successfully loaded storage directories.
   */
  @VisibleForTesting
  synchronized List<StorageDirectory> addStorageLocations(DataNode datanode,
      NamespaceInfo nsInfo, Collection<StorageLocation> dataDirs,
      StartupOption startOpt) throws IOException {
    final int numThreads = getParallelVolumeLoadThreadsNum(
        dataDirs.size(), datanode.getConf());
    final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    try {
      final List<StorageLocation> successLocations = loadDataStorage(
          datanode, nsInfo, dataDirs, startOpt, executor);

      if (successLocations.isEmpty()) {
        return Lists.newArrayList();
      }

      return loadBlockPoolSliceStorage(
          datanode, nsInfo, successLocations, startOpt, executor);
    } finally {
      executor.shutdown();
    }
  }

  private List<StorageLocation> loadDataStorage(DataNode datanode,
      NamespaceInfo nsInfo, Collection<StorageLocation> dataDirs,
      StartupOption startOpt, ExecutorService executor) throws IOException {
    final List<StorageLocation> success = Lists.newArrayList();
    final List<UpgradeTask> tasks = Lists.newArrayList();
    for (StorageLocation dataDir : dataDirs) {
      if (!containsStorageDir(dataDir)) {
        try {
          // It first ensures the datanode level format is completed.
          final List<Callable<StorageDirectory>> callables
              = Lists.newArrayList();
          final StorageDirectory sd = loadStorageDirectory(
              datanode, nsInfo, dataDir, startOpt, callables);
          if (callables.isEmpty()) {
            addStorageDir(sd);
            success.add(dataDir);
          } else {
            for(Callable<StorageDirectory> c : callables) {
              tasks.add(new UpgradeTask(dataDir, executor.submit(c)));
            }
          }
        } catch (IOException e) {
          LOG.warn("Failed to add storage directory {}", dataDir, e);
        }
      } else {
        LOG.info("Storage directory {} has already been used.", dataDir);
        success.add(dataDir);
      }
    }

    if (!tasks.isEmpty()) {
      LOG.info("loadDataStorage: {} upgrade tasks", tasks.size());
      for(UpgradeTask t : tasks) {
        try {
          addStorageDir(t.future.get());
          success.add(t.dataDir);
        } catch (ExecutionException e) {
          LOG.warn("Failed to upgrade storage directory {}", t.dataDir, e);
        } catch (InterruptedException e) {
          throw DFSUtilClient.toInterruptedIOException("Task interrupted", e);
        }
      }
    }

    return success;
  }

  private List<StorageDirectory> loadBlockPoolSliceStorage(DataNode datanode,
      NamespaceInfo nsInfo, Collection<StorageLocation> dataDirs,
      StartupOption startOpt, ExecutorService executor) throws IOException {
    final String bpid = nsInfo.getBlockPoolID();
    final BlockPoolSliceStorage bpStorage = getBlockPoolSliceStorage(nsInfo);
    Map<StorageLocation, List<Callable<StorageDirectory>>> upgradeCallableMap =
        new HashMap<>();
    final List<StorageDirectory> success = Lists.newArrayList();
    final List<UpgradeTask> tasks = Lists.newArrayList();
    for (StorageLocation dataDir : dataDirs) {
      dataDir.makeBlockPoolDir(bpid, datanode.getConf());
      try {
        final List<Callable<StorageDirectory>> sdCallables =
            Lists.newArrayList();
        final List<StorageDirectory> dirs = bpStorage.recoverTransitionRead(
            nsInfo, dataDir, startOpt, sdCallables, datanode.getConf());
        if (sdCallables.isEmpty()) {
          for(StorageDirectory sd : dirs) {
            success.add(sd);
          }
        } else {
          upgradeCallableMap.put(dataDir, sdCallables);
        }
      } catch (IOException e) {
        LOG.warn("Failed to add storage directory {} for block pool {}",
            dataDir, bpid, e);
      }
    }

    for (Map.Entry<StorageLocation, List<Callable<StorageDirectory>>> entry :
        upgradeCallableMap.entrySet()) {
      for(Callable<StorageDirectory> c : entry.getValue()) {
        tasks.add(new UpgradeTask(entry.getKey(), executor.submit(c)));
      }
    }

    if (!tasks.isEmpty()) {
      LOG.info("loadBlockPoolSliceStorage: {} upgrade tasks", tasks.size());
      for(UpgradeTask t : tasks) {
        try {
          success.add(t.future.get());
        } catch (ExecutionException e) {
          LOG.warn("Failed to upgrade storage directory {} for block pool {}",
              t.dataDir, bpid, e);
        } catch (InterruptedException e) {
          throw DFSUtilClient.toInterruptedIOException("Task interrupted", e);
        }
      }
    }

    return success;
  }

  /**
   * Remove storage dirs from DataStorage. All storage dirs are removed even when the
   * IOException is thrown.
   *
   * @param storageLocations a set of storage directories to be removed.
   * @throws IOException if I/O error when unlocking storage directory.
   */
  synchronized void removeVolumes(
      final Collection<StorageLocation> storageLocations)
      throws IOException {
    if (storageLocations.isEmpty()) {
      return;
    }

    StringBuilder errorMsgBuilder = new StringBuilder();
    for (Iterator<StorageDirectory> it = getStorageDirs().iterator();
         it.hasNext(); ) {
      StorageDirectory sd = it.next();
      StorageLocation sdLocation = sd.getStorageLocation();
      if (storageLocations.contains(sdLocation)) {
        // Remove the block pool level storage first.
        for (Map.Entry<String, BlockPoolSliceStorage> entry :
            this.bpStorageMap.entrySet()) {
          String bpid = entry.getKey();
          BlockPoolSliceStorage bpsStorage = entry.getValue();
          File bpRoot =
              BlockPoolSliceStorage.getBpRoot(bpid, sd.getCurrentDir());
          bpsStorage.remove(bpRoot.getAbsoluteFile());
        }

        getStorageDirs().remove(sd);
        try {
          sd.unlock();
        } catch (IOException e) {
          LOG.warn("I/O error attempting to unlock storage directory {}.",
              sd.getRoot(), e);
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
      throw new IOException("All specified directories have failed to load.");
    }
  }

  void format(StorageDirectory sd, NamespaceInfo nsInfo,
              String newDatanodeUuid, Configuration conf) throws IOException {
    sd.clearDirectory(); // create directory
    this.layoutVersion = DataNodeLayoutVersion.getCurrentLayoutVersion();
    this.clusterID = nsInfo.getClusterID();
    this.namespaceID = nsInfo.getNamespaceID();
    this.cTime = 0;
    setDatanodeUuid(newDatanodeUuid);

    createStorageID(sd, false, conf);
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
    if (props == null) {
      return;
    }
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
        LOG.error("Unable to acquire file lock on path {}", oldF);
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
   * Analyze which and whether a transition of the fs state is required
   * and perform it if necessary.
   * 
   * Rollback if the rollback startup option was specified.
   * Upgrade if this.LV > LAYOUT_VERSION
   * Regular startup if this.LV = LAYOUT_VERSION
   * 
   * @param sd  storage directory
   * @param nsInfo  namespace info
   * @param startOpt  startup option
   * @return true if the new properties has been written.
   */
  private boolean doTransition(StorageDirectory sd, NamespaceInfo nsInfo,
      StartupOption startOpt, List<Callable<StorageDirectory>> callables,
      Configuration conf) throws IOException {
    if (sd.getStorageLocation().getStorageType() == StorageType.PROVIDED) {
      createStorageID(sd, layoutVersion, conf);
      return false; // regular start up for PROVIDED storage directories
    }
    if (startOpt == StartupOption.ROLLBACK) {
      doRollback(sd, nsInfo); // rollback if applicable
    }
    readProperties(sd);
    checkVersionUpgradable(this.layoutVersion);
    assert this.layoutVersion >=
        DataNodeLayoutVersion.getCurrentLayoutVersion() :
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

    // regular start up.
    if (this.layoutVersion == DataNodeLayoutVersion.getCurrentLayoutVersion()) {
      createStorageID(sd, layoutVersion, conf);
      return false; // need to write properties
    }

    // do upgrade
    if (this.layoutVersion > DataNodeLayoutVersion.getCurrentLayoutVersion()) {
      if (federationSupported) {
        // If the existing on-disk layout version supports federation,
        // simply update the properties.
        upgradeProperties(sd, conf);
      } else {
        doUpgradePreFederation(sd, nsInfo, callables, conf);
      }
      return true; // doUgrade already has written properties
    }
    
    // layoutVersion < DATANODE_LAYOUT_VERSION. I.e. stored layout version is newer
    // than the version supported by datanode. This should have been caught
    // in readProperties(), even if rollback was not carried out or somehow
    // failed.
    throw new IOException("BUG: The stored LV = " + this.getLayoutVersion()
        + " is newer than the supported LV = "
        + DataNodeLayoutVersion.getCurrentLayoutVersion());
  }

  /**
   * Upgrade from a pre-federation layout.
   * Move current storage into a backup directory,
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
   */
  void doUpgradePreFederation(final StorageDirectory sd,
      final NamespaceInfo nsInfo,
      final List<Callable<StorageDirectory>> callables,
      final Configuration conf) throws IOException {
    final int oldLV = getLayoutVersion();
    LOG.info("Upgrading storage directory {}.\n old LV = {}; old CTime = {}"
            + ".\n new LV = {}; new CTime = {}", sd.getRoot(), oldLV,
        this.getCTime(), DataNodeLayoutVersion.getCurrentLayoutVersion(),
        nsInfo.getCTime());
    
    final File curDir = sd.getCurrentDir();
    final File prevDir = sd.getPreviousDir();
    final File bbwDir = new File(sd.getRoot(), Storage.STORAGE_1_BBW);

    assert curDir.exists() : "Data node current directory must exist.";
    // Cleanup directory "detach"
    cleanupDetachDir(new File(curDir, STORAGE_DIR_DETACHED));
    
    // 1. delete <SD>/previous dir before upgrading
    if (prevDir.exists())
      deleteDir(prevDir);
    // get previous.tmp directory, <SD>/previous.tmp
    final File tmpDir = sd.getPreviousTmp();
    assert !tmpDir.exists() : 
      "Data node previous.tmp directory must not exist.";
    
    // 2. Rename <SD>/current to <SD>/previous.tmp
    rename(curDir, tmpDir);
    
    // 3.1. Format BP
    File curBpDir = BlockPoolSliceStorage.getBpRoot(nsInfo.getBlockPoolID(), curDir);
    BlockPoolSliceStorage bpStorage = getBlockPoolSliceStorage(nsInfo);
    bpStorage.format(curDir, nsInfo);

    final File toDir = new File(curBpDir, STORAGE_DIR_CURRENT);
    if (callables == null) {
      doUpgrade(sd, nsInfo, prevDir, tmpDir, bbwDir, toDir, oldLV, conf);
    } else {
      callables.add(new Callable<StorageDirectory>() {
        @Override
        public StorageDirectory call() throws Exception {
          doUpgrade(sd, nsInfo, prevDir, tmpDir, bbwDir, toDir, oldLV, conf);
          return sd;
        }
      });
    }
  }

  private void doUpgrade(final StorageDirectory sd,
      final NamespaceInfo nsInfo, final File prevDir,
      final File tmpDir, final File bbwDir, final File toDir, final int oldLV,
      Configuration conf) throws IOException {
    // 3.2. Link block files from <SD>/previous.tmp to <SD>/current
    linkAllBlocks(tmpDir, bbwDir, toDir, oldLV, conf);

    // 4. Write version file under <SD>/current
    clusterID = nsInfo.getClusterID();
    upgradeProperties(sd, conf);
    
    // 5. Rename <SD>/previous.tmp to <SD>/previous
    rename(tmpDir, prevDir);
    LOG.info("Upgrade of {} is complete", sd.getRoot());
  }

  void upgradeProperties(StorageDirectory sd, Configuration conf)
      throws IOException {
    createStorageID(sd, layoutVersion, conf);
    LOG.info("Updating layout version from {} to {} for storage {}",
        layoutVersion, DataNodeLayoutVersion.getCurrentLayoutVersion(),
        sd.getRoot());
    layoutVersion = DataNodeLayoutVersion.getCurrentLayoutVersion();
    writeProperties(sd);
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
          DataNodeLayoutVersion.getCurrentLayoutVersion())) {
        readProperties(sd, DataNodeLayoutVersion.getCurrentLayoutVersion());
        writeProperties(sd);
        LOG.info("Layout version rolled back to {} for storage {}",
            DataNodeLayoutVersion.getCurrentLayoutVersion(), sd.getRoot());
      }
      return;
    }
    DataStorage prevInfo = new DataStorage();
    prevInfo.readPreviousVersionProperties(sd);

    // We allow rollback to a state, which is either consistent with
    // the namespace state or can be further upgraded to it.
    if (!(prevInfo.getLayoutVersion() >=
        DataNodeLayoutVersion.getCurrentLayoutVersion()
        && prevInfo.getCTime() <= nsInfo.getCTime())) {  // cannot rollback
      throw new InconsistentFSStateException(sd.getRoot(),
          "Cannot rollback to a newer state.\nDatanode previous state: LV = "
              + prevInfo.getLayoutVersion() + " CTime = " + prevInfo.getCTime()
              + " is newer than the namespace state: LV = "
              + DataNodeLayoutVersion.getCurrentLayoutVersion() + " CTime = "
              + nsInfo.getCTime());
    }
    LOG.info("Rolling back storage directory {}.\n   target LV = {}; target "
            + "CTime = {}", sd.getRoot(),
        DataNodeLayoutVersion.getCurrentLayoutVersion(), nsInfo.getCTime());
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
    LOG.info("Rollback of {} is complete", sd.getRoot());
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
    LOG.info("Finalizing upgrade for storage directory {}.\n   cur LV = {}; "
        + "cur CTime = {}", dataDirPath, this.getLayoutVersion(), this
        .getCTime());
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
    for (StorageDirectory sd : getStorageDirs()) {
      File prevDir = sd.getPreviousDir();
      if (prevDir != null && prevDir.exists()) {
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
  private static void linkAllBlocks(File fromDir, File fromBbwDir, File toDir,
      int diskLayoutVersion, Configuration conf) throws IOException {
    HardLink hardLink = new HardLink();
    // do the link
    if (DataNodeLayoutVersion.supports(
        LayoutVersion.Feature.APPEND_RBW_DIR, diskLayoutVersion)) {
      // hardlink finalized blocks in tmpDir/finalized
      linkBlocks(fromDir, toDir, STORAGE_DIR_FINALIZED,
          diskLayoutVersion, hardLink, conf);
      // hardlink rbw blocks in tmpDir/rbw
      linkBlocks(fromDir, toDir, STORAGE_DIR_RBW,
          diskLayoutVersion, hardLink, conf);
    } else { // pre-RBW version
      // hardlink finalized blocks in tmpDir
      linkBlocks(fromDir, new File(toDir, STORAGE_DIR_FINALIZED),
          diskLayoutVersion, hardLink, conf);
      if (fromBbwDir.exists()) {
        /*
         * We need to put the 'blocksBeingWritten' from HDFS 1.x into the rbw
         * directory.  It's a little messy, because the blocksBeingWriten was
         * NOT underneath the 'current' directory in those releases.  See
         * HDFS-3731 for details.
         */
        linkBlocks(fromBbwDir, new File(toDir, STORAGE_DIR_RBW),
            diskLayoutVersion, hardLink, conf);
      }
    }
    LOG.info("Linked blocks from {} to {}. {}", fromDir, toDir, hardLink
        .linkStats.report());
  }

  private static class LinkArgs {
    private File srcDir;
    private File dstDir;
    private String blockFile;

    LinkArgs(File srcDir, File dstDir, String blockFile) {
      this.srcDir = srcDir;
      this.dstDir = dstDir;
      this.blockFile = blockFile;
    }

    public File src() {
      return new File(srcDir, blockFile);
    }

    public File dst() {
      return new File(dstDir, blockFile);
    }

    public String blockFile() {
      return blockFile;
    }
  }

  static void linkBlocks(File fromDir, File toDir, String subdir, int oldLV,
      HardLink hl, Configuration conf) throws IOException {
    linkBlocks(new File(fromDir, subdir), new File(toDir, subdir),
        oldLV, hl, conf);
  }

  private static void linkBlocks(File from, File to, int oldLV,
      HardLink hl, Configuration conf) throws IOException {
    LOG.info("Start linking block files from {} to {}", from, to);
    boolean upgradeToIdBasedLayout = false;
    // If we are upgrading from a version older than the one where we introduced
    // block ID-based layout (32x32) AND we're working with the finalized
    // directory, we'll need to upgrade from the old layout to the new one. The
    // upgrade path from pre-blockid based layouts (>-56) and blockid based
    // 256x256 layouts (-56) is fortunately the same.
    if (oldLV > DataNodeLayoutVersion.Feature.BLOCKID_BASED_LAYOUT_32_by_32
        .getInfo().getLayoutVersion()
        && to.getName().equals(STORAGE_DIR_FINALIZED)) {
      upgradeToIdBasedLayout = true;
    }

    final ArrayList<LinkArgs> idBasedLayoutSingleLinks = Lists.newArrayList();
    final Map<File, File> pathCache = new HashMap<>();
    linkBlocksHelper(from, to, hl, upgradeToIdBasedLayout, to,
        idBasedLayoutSingleLinks, pathCache);

    // Detect and remove duplicate entries.
    final ArrayList<LinkArgs> duplicates =
        findDuplicateEntries(idBasedLayoutSingleLinks);
    if (!duplicates.isEmpty()) {
      LOG.error("There are {} duplicate block " +
          "entries within the same volume.", duplicates.size());
      removeDuplicateEntries(idBasedLayoutSingleLinks, duplicates);
    }

    final int numLinkWorkers = conf.getInt(
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
            HardLink.createHardLink(cur.src(), cur.dst());
          }
          return null;
        }
      }));
    }
    linkWorkers.shutdown();
    for (Future<Void> f : futures) {
      try {
        f.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
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
            compare(a.blockFile(), b.blockFile()).
            compare(a.src(), b.src()).
            compare(a.dst(), b.dst()).
            result();
      }
    });
    final ArrayList<LinkArgs> duplicates = Lists.newArrayList();
    Long prevBlockId = null;
    boolean prevWasMeta = false;
    boolean addedPrev = false;
    for (int i = 0; i < all.size(); i++) {
      LinkArgs args = all.get(i);
      long blockId = Block.getBlockId(args.blockFile());
      boolean isMeta = Block.isMetaFilename(args.blockFile());
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
      if (!Block.isMetaFilename(duplicate.blockFile())) {
        continue;
      }
      long blockId = Block.getBlockId(duplicate.blockFile());
      List<LinkArgs> prevHighest = highestGenstamps.get(blockId);
      if (prevHighest == null) {
        List<LinkArgs> highest = new LinkedList<LinkArgs>();
        highest.add(duplicate);
        highestGenstamps.put(blockId, highest);
        continue;
      }
      long prevGenstamp =
          Block.getGenerationStamp(prevHighest.get(0).blockFile());
      long genstamp = Block.getGenerationStamp(duplicate.blockFile());
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
      long blockId = Block.getBlockId(duplicate.blockFile());
      List<LinkArgs> highest = highestGenstamps.get(blockId);
      if (highest != null) {
        boolean found = false;
        for (LinkArgs high : highest) {
          if (high.src().getParent().equals(duplicate.src().getParent())) {
            found = true;
            break;
          }
        }
        if (!found) {
          LOG.warn("Unexpectedly low genstamp on {}.",
              duplicate.src().getAbsolutePath());
          iter.remove();
        }
      }
    }

    // Find the longest block files
    // We let the "last guy win" here, since we're only interested in
    // preserving one block file / metadata file pair.
    TreeMap<Long, LinkArgs> longestBlockFiles = new TreeMap<Long, LinkArgs>();
    for (LinkArgs duplicate : duplicates) {
      if (Block.isMetaFilename(duplicate.blockFile())) {
        continue;
      }
      long blockId = Block.getBlockId(duplicate.blockFile());
      LinkArgs prevLongest = longestBlockFiles.get(blockId);
      if (prevLongest == null) {
        longestBlockFiles.put(blockId, duplicate);
        continue;
      }
      long blockLength = duplicate.src().length();
      long prevBlockLength = prevLongest.src().length();
      if (blockLength < prevBlockLength) {
        LOG.warn("Unexpectedly short length on {}.",
            duplicate.src().getAbsolutePath());
        continue;
      }
      if (blockLength > prevBlockLength) {
        LOG.warn("Unexpectedly short length on {}.",
            prevLongest.src().getAbsolutePath());
      }
      longestBlockFiles.put(blockId, duplicate);
    }

    // Remove data / metadata entries that aren't the longest, or weren't
    // arbitrarily selected by us.
    for (Iterator<LinkArgs> iter = all.iterator(); iter.hasNext(); ) {
      LinkArgs args = iter.next();
      long blockId = Block.getBlockId(args.blockFile());
      LinkArgs bestDuplicate = longestBlockFiles.get(blockId);
      if (bestDuplicate == null) {
        continue; // file has no duplicates
      }
      if (!bestDuplicate.src().getParent().equals(args.src().getParent())) {
        LOG.warn("Discarding {}.", args.src().getAbsolutePath());
        iter.remove();
      }
    }
  }

  static void linkBlocksHelper(File from, File to, HardLink hl,
      boolean upgradeToIdBasedLayout, File blockRoot,
      List<LinkArgs> idBasedLayoutSingleLinks, Map<File, File> pathCache)
      throws IOException {
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
          /**
           * The destination path is 32x32, so 1024 distinct paths. Therefore
           * we cache the destination path and reuse the same File object on
           * potentially thousands of blocks located on this volume.
           * This method is called recursively so the cache is passed through
           * each recursive call. There is one cache per volume, and it is only
           * accessed by a single thread so no locking is needed.
           */
          File cachedDest = pathCache
              .computeIfAbsent(blockLocation, k -> blockLocation);
          idBasedLayoutSingleLinks.add(new LinkArgs(from,
              cachedDest, blockName));
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

    if (otherNames != null) {
      for (int i = 0; i < otherNames.length; i++) {
        linkBlocksHelper(new File(from, otherNames[i]),
            new File(to, otherNames[i]), hl, upgradeToIdBasedLayout,
            blockRoot, idBasedLayoutSingleLinks, pathCache);
      }
    }
  }

  /**
   * Get the BlockPoolSliceStorage from {@link #bpStorageMap}.
   * If the object is not found, create a new object and put it to the map.
   */
  synchronized BlockPoolSliceStorage getBlockPoolSliceStorage(
      final NamespaceInfo nsInfo) {
    final String bpid = nsInfo.getBlockPoolID();
    BlockPoolSliceStorage bpStorage = bpStorageMap.get(bpid);
    if (bpStorage == null) {
      bpStorage = new BlockPoolSliceStorage(nsInfo.getNamespaceID(), bpid,
            nsInfo.getCTime(), nsInfo.getClusterID());
      bpStorageMap.put(bpid, bpStorage);
    }
    return bpStorage;
  }

  synchronized void removeBlockPoolStorage(String bpId) {
    bpStorageMap.remove(bpId);
  }

  /**
   * Prefer FileIoProvider#fullydelete.
   * @param dir
   * @return
   */
  @Deprecated
  public static boolean fullyDelete(final File dir) {
    boolean result = FileUtil.fullyDelete(dir);
    return result;
  }
}
