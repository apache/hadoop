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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DiskChecker;

import java.io.*;
import java.nio.channels.FileLock;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/** 
 * Data storage information file.
 * <p>
 * @see Storage
 */
@InterfaceAudience.Private
public class DataStorage extends Storage {

  public final static String BLOCK_SUBDIR_PREFIX = "subdir";
  final static String BLOCK_FILE_PREFIX = "blk_";
  final static String COPY_FILE_PREFIX = "dncp_";
  final static String STORAGE_DIR_DETACHED = "detach";
  public final static String STORAGE_DIR_RBW = "rbw";
  public final static String STORAGE_DIR_FINALIZED = "finalized";
  public final static String STORAGE_DIR_TMP = "tmp";

  // Set of bpids for which 'trash' is currently enabled.
  // When trash is enabled block files are moved under a separate
  // 'trash' folder instead of being deleted right away. This can
  // be useful during rolling upgrades, for example.
  // The set is backed by a concurrent HashMap.
  private Set<String> trashEnabledBpids;

  /**
   * Datanode UUID that this storage is currently attached to. This
   *  is the same as the legacy StorageID for datanodes that were
   *  upgraded from a pre-UUID version. For compatibility with prior
   *  versions of Datanodes we cannot make this field a UUID.
   */
  private String datanodeUuid = null;

  // Flag to ensure we only initialize storage once
  private boolean initialized = false;
  
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

  /** Create an ID for this storage. */
  public synchronized void createStorageID(StorageDirectory sd) {
    if (sd.getStorageUuid() == null) {
      sd.setStorageUuid(DatanodeStorage.generateUuid());
    }
  }

  /**
   * Enable trash for the specified block pool storage.
   */
  public void enableTrash(String bpid) {
    if (trashEnabledBpids.add(bpid)) {
      LOG.info("Enabled trash for bpid " + bpid);
    }
  }

  public void restoreTrash(String bpid) {
    if (trashEnabledBpids.contains(bpid)) {
      getBPStorage(bpid).restoreTrash();
      trashEnabledBpids.remove(bpid);
      LOG.info("Restored trash for bpid " + bpid);
    }
  }

  public boolean trashEnabled(String bpid) {
    return trashEnabledBpids.contains(bpid);
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
      return ((BlockPoolSliceStorage) getBPStorage(bpid)).getTrashDirectory(blockFile);
    }
    return null;
  }
  
  /**
   * Analyze storage directories.
   * Recover from previous transitions if required. 
   * Perform fs state transition if necessary depending on the namespace info.
   * Read storage info.
   * <br>
   * This method should be synchronized between multiple DN threads.  Only the 
   * first DN thread does DN level storage dir recoverTransitionRead.
   * 
   * @param nsInfo namespace information
   * @param dataDirs array of data storage directories
   * @param startOpt startup option
   * @throws IOException
   */
  synchronized void recoverTransitionRead(DataNode datanode,
      NamespaceInfo nsInfo, Collection<StorageLocation> dataDirs,
      StartupOption startOpt)
      throws IOException {
    if (initialized) {
      // DN storage has been initialized, no need to do anything
      return;
    }
    LOG.info("Data-node version: " + HdfsConstants.DATANODE_LAYOUT_VERSION
        + " and name-node layout version: " + nsInfo.getLayoutVersion());
    
    // 1. For each data directory calculate its state and 
    // check whether all is consistent before transitioning.
    // Format and recover.
    this.storageDirs = new ArrayList<StorageDirectory>(dataDirs.size());
    ArrayList<StorageState> dataDirStates = new ArrayList<StorageState>(dataDirs.size());
    for(Iterator<StorageLocation> it = dataDirs.iterator(); it.hasNext();) {
      File dataDir = it.next().getFile();
      StorageDirectory sd = new StorageDirectory(dataDir);
      StorageState curState;
      try {
        curState = sd.analyzeStorage(startOpt, this);
        // sd is locked but not opened
        switch(curState) {
        case NORMAL:
          break;
        case NON_EXISTENT:
          // ignore this storage
          LOG.info("Storage directory " + dataDir + " does not exist");
          it.remove();
          continue;
        case NOT_FORMATTED: // format
          LOG.info("Storage directory " + dataDir + " is not formatted");
          LOG.info("Formatting ...");
          format(sd, nsInfo, datanode.getDatanodeUuid());
          break;
        default:  // recovery part is common
          sd.doRecover(curState);
        }
      } catch (IOException ioe) {
        sd.unlock();
        LOG.warn("Ignoring storage directory " + dataDir
            + " due to an exception", ioe);
        //continue with other good dirs
        continue;
      }
      // add to the storage list
      addStorageDir(sd);
      dataDirStates.add(curState);
    }

    if (dataDirs.size() == 0 || dataDirStates.size() == 0)  // none of the data dirs exist
      throw new IOException(
          "All specified directories are not accessible or do not exist.");

    // 2. Do transitions
    // Each storage directory is treated individually.
    // During startup some of them can upgrade or rollback 
    // while others could be uptodate for the regular startup.
    for(int idx = 0; idx < getNumStorageDirs(); idx++) {
      doTransition(datanode, getStorageDir(idx), nsInfo, startOpt);
      createStorageID(getStorageDir(idx));
    }
    
    // 3. Update all storages. Some of them might have just been formatted.
    this.writeAll();
    
    // 4. mark DN storage is initialized
    this.initialized = true;
  }

  /**
   * recoverTransitionRead for a specific block pool
   * 
   * @param datanode DataNode
   * @param bpID Block pool Id
   * @param nsInfo Namespace info of namenode corresponding to the block pool
   * @param dataDirs Storage directories
   * @param startOpt startup option
   * @throws IOException on error
   */
  void recoverTransitionRead(DataNode datanode, String bpID, NamespaceInfo nsInfo,
      Collection<StorageLocation> dataDirs, StartupOption startOpt) throws IOException {
    // First ensure datanode level format/snapshot/rollback is completed
    recoverTransitionRead(datanode, nsInfo, dataDirs, startOpt);

    // Create list of storage directories for the block pool
    Collection<File> bpDataDirs = new ArrayList<File>();
    for(StorageLocation dir : dataDirs) {
      File dnRoot = dir.getFile();
      File bpRoot = BlockPoolSliceStorage.getBpRoot(bpID, new File(dnRoot,
          STORAGE_DIR_CURRENT));
      bpDataDirs.add(bpRoot);
    }
    // mkdir for the list of BlockPoolStorage
    makeBlockPoolDataDir(bpDataDirs, null);
    BlockPoolSliceStorage bpStorage = new BlockPoolSliceStorage(
        nsInfo.getNamespaceID(), bpID, nsInfo.getCTime(), nsInfo.getClusterID());
    
    bpStorage.recoverTransitionRead(datanode, nsInfo, bpDataDirs, startOpt);
    addBlockPoolStorage(bpID, bpStorage);
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
    this.layoutVersion = HdfsConstants.DATANODE_LAYOUT_VERSION;
    this.clusterID = nsInfo.getClusterID();
    this.namespaceID = nsInfo.getNamespaceID();
    this.cTime = 0;
    this.datanodeUuid = datanodeUuid;

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
    if (!oldF.exists())
      return false;
    // check the layout version inside the storage file
    // Lock and Read old storage file
    RandomAccessFile oldFile = new RandomAccessFile(oldF, "rws");
    FileLock oldLock = oldFile.getChannel().tryLock();
    try {
      oldFile.seek(0);
      int oldVersion = oldFile.readInt();
      if (oldVersion < LAST_PRE_UPGRADE_LAYOUT_VERSION)
        return false;
    } finally {
      oldLock.release();
      oldFile.close();
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
    assert this.layoutVersion >= HdfsConstants.DATANODE_LAYOUT_VERSION :
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
    
    // After addition of the federation feature, ctime check is only 
    // meaningful at BlockPoolSliceStorage level. 

    // regular start up. 
    if (this.layoutVersion == HdfsConstants.DATANODE_LAYOUT_VERSION)
      return; // regular startup
    
    // do upgrade
    if (this.layoutVersion > HdfsConstants.DATANODE_LAYOUT_VERSION) {
      doUpgrade(sd, nsInfo);  // upgrade
      return;
    }
    
    // layoutVersion < DATANODE_LAYOUT_VERSION. I.e. stored layout version is newer
    // than the version supported by datanode. This should have been caught
    // in readProperties(), even if rollback was not carried out or somehow
    // failed.
    throw new IOException("BUG: The stored LV = " + this.getLayoutVersion()
        + " is newer than the supported LV = "
        + HdfsConstants.DATANODE_LAYOUT_VERSION);
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
  void doUpgrade(StorageDirectory sd, NamespaceInfo nsInfo) throws IOException {
    // If the existing on-disk layout version supportes federation, simply
    // update its layout version.
    if (DataNodeLayoutVersion.supports(
        LayoutVersion.Feature.FEDERATION, layoutVersion)) {
      // The VERSION file is already read in. Override the layoutVersion 
      // field and overwrite the file.
      LOG.info("Updating layout version from " + layoutVersion + " to "
          + HdfsConstants.DATANODE_LAYOUT_VERSION + " for storage "
          + sd.getRoot());
      layoutVersion = HdfsConstants.DATANODE_LAYOUT_VERSION;
      writeProperties(sd);
      return;
    }
    
    LOG.info("Upgrading storage directory " + sd.getRoot()
             + ".\n   old LV = " + this.getLayoutVersion()
             + "; old CTime = " + this.getCTime()
             + ".\n   new LV = " + HdfsConstants.DATANODE_LAYOUT_VERSION
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
    linkAllBlocks(tmpDir, bbwDir, new File(curBpDir, STORAGE_DIR_CURRENT));
    
    // 4. Write version file under <SD>/current
    layoutVersion = HdfsConstants.DATANODE_LAYOUT_VERSION;
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
          HdfsConstants.DATANODE_LAYOUT_VERSION)) {
        readProperties(sd, HdfsConstants.DATANODE_LAYOUT_VERSION);
        writeProperties(sd);
        LOG.info("Layout version rolled back to "
            + HdfsConstants.DATANODE_LAYOUT_VERSION + " for storage "
            + sd.getRoot());
      }
      return;
    }
    DataStorage prevInfo = new DataStorage();
    prevInfo.readPreviousVersionProperties(sd);

    // We allow rollback to a state, which is either consistent with
    // the namespace state or can be further upgraded to it.
    if (!(prevInfo.getLayoutVersion() >= HdfsConstants.DATANODE_LAYOUT_VERSION
          && prevInfo.getCTime() <= nsInfo.getCTime()))  // cannot rollback
      throw new InconsistentFSStateException(sd.getRoot(),
          "Cannot rollback to a newer state.\nDatanode previous state: LV = "
              + prevInfo.getLayoutVersion() + " CTime = " + prevInfo.getCTime()
              + " is newer than the namespace state: LV = "
              + HdfsConstants.DATANODE_LAYOUT_VERSION + " CTime = "
              + nsInfo.getCTime());
    LOG.info("Rolling back storage directory " + sd.getRoot()
        + ".\n   target LV = " + HdfsConstants.DATANODE_LAYOUT_VERSION
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
  private void linkAllBlocks(File fromDir, File fromBbwDir, File toDir)
      throws IOException {
    HardLink hardLink = new HardLink();
    // do the link
    int diskLayoutVersion = this.getLayoutVersion();
    if (DataNodeLayoutVersion.supports(
        LayoutVersion.Feature.APPEND_RBW_DIR, diskLayoutVersion)) {
      // hardlink finalized blocks in tmpDir/finalized
      linkBlocks(new File(fromDir, STORAGE_DIR_FINALIZED), 
          new File(toDir, STORAGE_DIR_FINALIZED), diskLayoutVersion, hardLink);
      // hardlink rbw blocks in tmpDir/rbw
      linkBlocks(new File(fromDir, STORAGE_DIR_RBW), 
          new File(toDir, STORAGE_DIR_RBW), diskLayoutVersion, hardLink);
    } else { // pre-RBW version
      // hardlink finalized blocks in tmpDir
      linkBlocks(fromDir, new File(toDir, STORAGE_DIR_FINALIZED), 
          diskLayoutVersion, hardLink);      
      if (fromBbwDir.exists()) {
        /*
         * We need to put the 'blocksBeingWritten' from HDFS 1.x into the rbw
         * directory.  It's a little messy, because the blocksBeingWriten was
         * NOT underneath the 'current' directory in those releases.  See
         * HDFS-3731 for details.
         */
        linkBlocks(fromBbwDir,
            new File(toDir, STORAGE_DIR_RBW), diskLayoutVersion, hardLink);
      }
    } 
    LOG.info( hardLink.linkStats.report() );
  }
  
  static void linkBlocks(File from, File to, int oldLV, HardLink hl) 
  throws IOException {
    if (!from.exists()) {
      return;
    }
    if (!from.isDirectory()) {
      if (from.getName().startsWith(COPY_FILE_PREFIX)) {
        FileInputStream in = new FileInputStream(from);
        try {
          FileOutputStream out = new FileOutputStream(to);
          try {
            IOUtils.copyBytes(in, out, 16*1024);
            hl.linkStats.countPhysicalFileCopies++;
          } finally {
            out.close();
          }
        } finally {
          in.close();
        }
      } else {
        HardLink.createHardLink(from, to);
        hl.linkStats.countSingleLinks++;
      }
      return;
    }
    // from is a directory
    hl.linkStats.countDirs++;
    
    if (!to.mkdirs())
      throw new IOException("Cannot create directory " + to);
    
    String[] blockNames = from.list(new java.io.FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith(BLOCK_FILE_PREFIX);
      }
    });

    // Block files just need hard links with the same file names
    // but a different directory
    if (blockNames.length > 0) {
      HardLink.createHardLinkMult(from, blockNames, to);
      hl.linkStats.countMultLinks++;
      hl.linkStats.countFilesMultLinks += blockNames.length;
    } else {
      hl.linkStats.countEmptyDirs++;
    }
    
    // Now take care of the rest of the files and subdirectories
    String[] otherNames = from.list(new java.io.FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.startsWith(BLOCK_SUBDIR_PREFIX) 
            || name.startsWith(COPY_FILE_PREFIX);
        }
      });
    for(int i = 0; i < otherNames.length; i++)
      linkBlocks(new File(from, otherNames[i]), 
          new File(to, otherNames[i]), oldLV, hl);
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
