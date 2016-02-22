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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.HardLink;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.util.Daemon;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Manages storage for the set of BlockPoolSlices which share a particular 
 * block pool id, on this DataNode.
 * 
 * This class supports the following functionality:
 * <ol>
 * <li> Formatting a new block pool storage</li>
 * <li> Recovering a storage state to a consistent state (if possible></li>
 * <li> Taking a snapshot of the block pool during upgrade</li>
 * <li> Rolling back a block pool to a previous snapshot</li>
 * <li> Finalizing block storage by deletion of a snapshot</li>
 * </ul>
 * 
 * @see Storage
 */
@InterfaceAudience.Private
public class BlockPoolSliceStorage extends Storage {
  static final String TRASH_ROOT_DIR = "trash";

  /**
   * A marker file that is created on each root directory if a rolling upgrade
   * is in progress. The NN does not inform the DN when a rolling upgrade is
   * finalized. All the DN can infer is whether or not a rolling upgrade is
   * currently in progress. When the rolling upgrade is not in progress:
   *   1. If the marker file is present, then a rolling upgrade just completed.
   *      If a 'previous' directory exists, it can be deleted now.
   *   2. If the marker file is absent, then a regular upgrade may be in
   *      progress. Do not delete the 'previous' directory.
   */
  static final String ROLLING_UPGRADE_MARKER_FILE = "RollingUpgradeInProgress";

  private static final String BLOCK_POOL_ID_PATTERN_BASE =
      Pattern.quote(File.separator) +
      "BP-\\d+-\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}-\\d+" +
      Pattern.quote(File.separator);

  private static final Pattern BLOCK_POOL_PATH_PATTERN = Pattern.compile(
      "^(.*)(" + BLOCK_POOL_ID_PATTERN_BASE + ")(.*)$");

  private static final Pattern BLOCK_POOL_CURRENT_PATH_PATTERN = Pattern.compile(
      "^(.*)(" + BLOCK_POOL_ID_PATTERN_BASE + ")(" + STORAGE_DIR_CURRENT + ")(.*)$");

  private static final Pattern BLOCK_POOL_TRASH_PATH_PATTERN = Pattern.compile(
      "^(.*)(" + BLOCK_POOL_ID_PATTERN_BASE + ")(" + TRASH_ROOT_DIR + ")(.*)$");

  private String blockpoolID = ""; // id of the blockpool
  private Daemon trashCleaner;

  public BlockPoolSliceStorage(StorageInfo storageInfo, String bpid) {
    super(storageInfo);
    blockpoolID = bpid;
  }

  /**
   * These maps are used as an optimization to avoid one filesystem operation
   * per storage on each heartbeat response.
   */
  private static Set<String> storagesWithRollingUpgradeMarker;
  private static Set<String> storagesWithoutRollingUpgradeMarker;

  BlockPoolSliceStorage(int namespaceID, String bpID, long cTime,
      String clusterId) {
    super(NodeType.DATA_NODE);
    this.namespaceID = namespaceID;
    this.blockpoolID = bpID;
    this.cTime = cTime;
    this.clusterID = clusterId;
    storagesWithRollingUpgradeMarker = Collections.newSetFromMap(
        new ConcurrentHashMap<String, Boolean>());
    storagesWithoutRollingUpgradeMarker = Collections.newSetFromMap(
        new ConcurrentHashMap<String, Boolean>());
  }

  private BlockPoolSliceStorage() {
    super(NodeType.DATA_NODE);
    storagesWithRollingUpgradeMarker = Collections.newSetFromMap(
        new ConcurrentHashMap<String, Boolean>());
    storagesWithoutRollingUpgradeMarker = Collections.newSetFromMap(
        new ConcurrentHashMap<String, Boolean>());
  }

  // Expose visibility for VolumeBuilder#commit().
  public void addStorageDir(StorageDirectory sd) {
    super.addStorageDir(sd);
  }

  /**
   * Load one storage directory. Recover from previous transitions if required.
   *
   * @param nsInfo namespace information
   * @param dataDir the root path of the storage directory
   * @param startOpt startup option
   * @return the StorageDirectory successfully loaded.
   * @throws IOException
   */
  private StorageDirectory loadStorageDirectory(NamespaceInfo nsInfo,
      File dataDir, StartupOption startOpt,
      List<Callable<StorageDirectory>> callables, Configuration conf)
          throws IOException {
    StorageDirectory sd = new StorageDirectory(dataDir, null, true);
    try {
      StorageState curState = sd.analyzeStorage(startOpt, this);
      // sd is locked but not opened
      switch (curState) {
      case NORMAL:
        break;
      case NON_EXISTENT:
        LOG.info("Block pool storage directory " + dataDir + " does not exist");
        throw new IOException("Storage directory " + dataDir
            + " does not exist");
      case NOT_FORMATTED: // format
        LOG.info("Block pool storage directory " + dataDir
            + " is not formatted for " + nsInfo.getBlockPoolID()
            + ". Formatting ...");
        format(sd, nsInfo);
        break;
      default:  // recovery part is common
        sd.doRecover(curState);
      }

      // 2. Do transitions
      // Each storage directory is treated individually.
      // During startup some of them can upgrade or roll back
      // while others could be up-to-date for the regular startup.
      if (!doTransition(sd, nsInfo, startOpt, callables, conf)) {

        // 3. Check CTime and update successfully loaded storage.
        if (getCTime() != nsInfo.getCTime()) {
          throw new IOException("Datanode CTime (=" + getCTime()
              + ") is not equal to namenode CTime (=" + nsInfo.getCTime() + ")");
        }
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
   * Analyze and load storage directories. Recover from previous transitions if
   * required.
   *
   * The block pool storages are either all analyzed or none of them is loaded.
   * Therefore, a failure on loading any block pool storage results a faulty
   * data volume.
   *
   * @param nsInfo namespace information
   * @param dataDirs storage directories of block pool
   * @param startOpt startup option
   * @return an array of loaded block pool directories.
   * @throws IOException on error
   */
  List<StorageDirectory> loadBpStorageDirectories(NamespaceInfo nsInfo,
      Collection<File> dataDirs, StartupOption startOpt,
      List<Callable<StorageDirectory>> callables, Configuration conf)
          throws IOException {
    List<StorageDirectory> succeedDirs = Lists.newArrayList();
    try {
      for (File dataDir : dataDirs) {
        if (containsStorageDir(dataDir)) {
          throw new IOException(
              "BlockPoolSliceStorage.recoverTransitionRead: " +
                  "attempt to load an used block storage: " + dataDir);
        }
        final StorageDirectory sd = loadStorageDirectory(
            nsInfo, dataDir, startOpt, callables, conf);
        succeedDirs.add(sd);
      }
    } catch (IOException e) {
      LOG.warn("Failed to analyze storage directories for block pool "
          + nsInfo.getBlockPoolID(), e);
      throw e;
    }
    return succeedDirs;
  }

  /**
   * Analyze storage directories. Recover from previous transitions if required.
   *
   * The block pool storages are either all analyzed or none of them is loaded.
   * Therefore, a failure on loading any block pool storage results a faulty
   * data volume.
   *
   * @param nsInfo namespace information
   * @param dataDirs storage directories of block pool
   * @param startOpt startup option
   * @throws IOException on error
   */
  List<StorageDirectory> recoverTransitionRead(NamespaceInfo nsInfo,
      Collection<File> dataDirs, StartupOption startOpt,
      List<Callable<StorageDirectory>> callables, Configuration conf)
          throws IOException {
    LOG.info("Analyzing storage directories for bpid " + nsInfo.getBlockPoolID());
    final List<StorageDirectory> loaded = loadBpStorageDirectories(
        nsInfo, dataDirs, startOpt, callables, conf);
    for (StorageDirectory sd : loaded) {
      addStorageDir(sd);
    }
    return loaded;
  }

  /**
   * Format a block pool slice storage. 
   * @param dnCurDir DataStorage current directory
   * @param nsInfo the name space info
   * @throws IOException Signals that an I/O exception has occurred.
   */
  void format(File dnCurDir, NamespaceInfo nsInfo) throws IOException {
    File curBpDir = getBpRoot(nsInfo.getBlockPoolID(), dnCurDir);
    StorageDirectory bpSdir = new StorageDirectory(curBpDir);
    format(bpSdir, nsInfo);
  }

  /**
   * Format a block pool slice storage. 
   * @param bpSdir the block pool storage
   * @param nsInfo the name space info
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void format(StorageDirectory bpSdir, NamespaceInfo nsInfo) throws IOException {
    LOG.info("Formatting block pool " + blockpoolID + " directory "
        + bpSdir.getCurrentDir());
    bpSdir.clearDirectory(); // create directory
    this.layoutVersion = HdfsServerConstants.DATANODE_LAYOUT_VERSION;
    this.cTime = nsInfo.getCTime();
    this.namespaceID = nsInfo.getNamespaceID();
    this.blockpoolID = nsInfo.getBlockPoolID();
    writeProperties(bpSdir);
  }

  /**
   * Remove block pool level storage directory.
   * @param absPathToRemove the absolute path of the root for the block pool
   *                        level storage to remove.
   */
  void remove(File absPathToRemove) {
    Preconditions.checkArgument(absPathToRemove.isAbsolute());
    LOG.info("Removing block level storage: " + absPathToRemove);
    for (Iterator<StorageDirectory> it = this.storageDirs.iterator();
         it.hasNext(); ) {
      StorageDirectory sd = it.next();
      if (sd.getRoot().getAbsoluteFile().equals(absPathToRemove)) {
        it.remove();
        break;
      }
    }
  }

  /**
   * Set layoutVersion, namespaceID and blockpoolID into block pool storage
   * VERSION file
   */
  @Override
  protected void setPropertiesFromFields(Properties props, StorageDirectory sd)
      throws IOException {
    props.setProperty("layoutVersion", String.valueOf(layoutVersion));
    props.setProperty("namespaceID", String.valueOf(namespaceID));
    props.setProperty("blockpoolID", blockpoolID);
    props.setProperty("cTime", String.valueOf(cTime));
  }

  /** Validate and set block pool ID */
  private void setBlockPoolID(File storage, String bpid)
      throws InconsistentFSStateException {
    if (bpid == null || bpid.equals("")) {
      throw new InconsistentFSStateException(storage, "file "
          + STORAGE_FILE_VERSION + " is invalid.");
    }
    
    if (!blockpoolID.equals("") && !blockpoolID.equals(bpid)) {
      throw new InconsistentFSStateException(storage,
          "Unexpected blockpoolID " + bpid + ". Expected " + blockpoolID);
    }
    blockpoolID = bpid;
  }
  
  @Override
  protected void setFieldsFromProperties(Properties props, StorageDirectory sd)
      throws IOException {
    setLayoutVersion(props, sd);
    setNamespaceID(props, sd);
    setcTime(props, sd);
    
    String sbpid = props.getProperty("blockpoolID");
    setBlockPoolID(sd.getRoot(), sbpid);
  }

  /**
   * Analyze whether a transition of the BP state is required and
   * perform it if necessary.
   * <br>
   * Rollback if previousLV >= LAYOUT_VERSION && prevCTime <= namenode.cTime.
   * Upgrade if this.LV > LAYOUT_VERSION || this.cTime < namenode.cTime Regular
   * startup if this.LV = LAYOUT_VERSION && this.cTime = namenode.cTime
   * 
   * @param sd storage directory <SD>/current/<bpid>
   * @param nsInfo namespace info
   * @param startOpt startup option
   * @return true if the new properties has been written.
   */
  private boolean doTransition(StorageDirectory sd, NamespaceInfo nsInfo,
      StartupOption startOpt, List<Callable<StorageDirectory>> callables,
      Configuration conf) throws IOException {
    if (startOpt == StartupOption.ROLLBACK && sd.getPreviousDir().exists()) {
      Preconditions.checkState(!getTrashRootDir(sd).exists(),
          sd.getPreviousDir() + " and " + getTrashRootDir(sd) + " should not " +
          " both be present.");
      doRollback(sd, nsInfo); // rollback if applicable
    } else if (startOpt == StartupOption.ROLLBACK &&
        !sd.getPreviousDir().exists()) {
      // Restore all the files in the trash. The restored files are retained
      // during rolling upgrade rollback. They are deleted during rolling
      // upgrade downgrade.
      int restored = restoreBlockFilesFromTrash(getTrashRootDir(sd));
      LOG.info("Restored " + restored + " block files from trash.");
    }
    readProperties(sd);
    checkVersionUpgradable(this.layoutVersion);
    assert this.layoutVersion >= HdfsServerConstants.DATANODE_LAYOUT_VERSION
       : "Future version is not allowed";
    if (getNamespaceID() != nsInfo.getNamespaceID()) {
      throw new IOException("Incompatible namespaceIDs in "
          + sd.getRoot().getCanonicalPath() + ": namenode namespaceID = "
          + nsInfo.getNamespaceID() + "; datanode namespaceID = "
          + getNamespaceID());
    }
    if (!blockpoolID.equals(nsInfo.getBlockPoolID())) {
      throw new IOException("Incompatible blockpoolIDs in "
          + sd.getRoot().getCanonicalPath() + ": namenode blockpoolID = "
          + nsInfo.getBlockPoolID() + "; datanode blockpoolID = "
          + blockpoolID);
    }
    if (this.layoutVersion == HdfsServerConstants.DATANODE_LAYOUT_VERSION
        && this.cTime == nsInfo.getCTime()) {
      return false; // regular startup
    }
    if (this.layoutVersion > HdfsServerConstants.DATANODE_LAYOUT_VERSION) {
      int restored = restoreBlockFilesFromTrash(getTrashRootDir(sd));
      LOG.info("Restored " + restored + " block files from trash " +
        "before the layout upgrade. These blocks will be moved to " +
        "the previous directory during the upgrade");
    }
    if (this.layoutVersion > HdfsServerConstants.DATANODE_LAYOUT_VERSION
        || this.cTime < nsInfo.getCTime()) {
      doUpgrade(sd, nsInfo, callables, conf); // upgrade
      return true;
    }
    // layoutVersion == LAYOUT_VERSION && this.cTime > nsInfo.cTime
    // must shutdown
    throw new IOException("Datanode state: LV = " + this.getLayoutVersion()
        + " CTime = " + this.getCTime()
        + " is newer than the namespace state: LV = "
        + nsInfo.getLayoutVersion() + " CTime = " + nsInfo.getCTime());
  }

  /**
   * Upgrade to any release after 0.22 (0.22 included) release e.g. 0.22 => 0.23
   * Upgrade procedure is as follows:
   * <ol>
   * <li>If <SD>/current/<bpid>/previous exists then delete it</li>
   * <li>Rename <SD>/current/<bpid>/current to
   * <SD>/current/bpid/current/previous.tmp</li>
   * <li>Create new <SD>current/<bpid>/current directory</li>
   * <ol>
   * <li>Hard links for block files are created from previous.tmp to current</li>
   * <li>Save new version file in current directory</li>
   * </ol>
   * <li>Rename previous.tmp to previous</li> </ol>
   * 
   * @param bpSd storage directory <SD>/current/<bpid>
   * @param nsInfo Namespace Info from the namenode
   * @throws IOException on error
   */
  private void doUpgrade(final StorageDirectory bpSd,
      final NamespaceInfo nsInfo,
      final List<Callable<StorageDirectory>> callables,
      final Configuration conf) throws IOException {
    // Upgrading is applicable only to release with federation or after
    if (!DataNodeLayoutVersion.supports(
        LayoutVersion.Feature.FEDERATION, layoutVersion)) {
      return;
    }
    final int oldLV = getLayoutVersion();
    LOG.info("Upgrading block pool storage directory " + bpSd.getRoot()
        + ".\n   old LV = " + oldLV
        + "; old CTime = " + this.getCTime()
        + ".\n   new LV = " + HdfsServerConstants.DATANODE_LAYOUT_VERSION
        + "; new CTime = " + nsInfo.getCTime());
    // get <SD>/previous directory
    String dnRoot = getDataNodeStorageRoot(bpSd.getRoot().getCanonicalPath());
    StorageDirectory dnSdStorage = new StorageDirectory(new File(dnRoot));
    File dnPrevDir = dnSdStorage.getPreviousDir();
    
    // If <SD>/previous directory exists delete it
    if (dnPrevDir.exists()) {
      deleteDir(dnPrevDir);
    }
    final File bpCurDir = bpSd.getCurrentDir();
    final File bpPrevDir = bpSd.getPreviousDir();
    assert bpCurDir.exists() : "BP level current directory must exist.";
    cleanupDetachDir(new File(bpCurDir, DataStorage.STORAGE_DIR_DETACHED));
    
    // 1. Delete <SD>/current/<bpid>/previous dir before upgrading
    if (bpPrevDir.exists()) {
      deleteDir(bpPrevDir);
    }
    final File bpTmpDir = bpSd.getPreviousTmp();
    assert !bpTmpDir.exists() : "previous.tmp directory must not exist.";
    
    // 2. Rename <SD>/current/<bpid>/current to
    //    <SD>/current/<bpid>/previous.tmp
    rename(bpCurDir, bpTmpDir);
    
    final String name = "block pool " + blockpoolID + " at " + bpSd.getRoot();
    if (callables == null) {
      doUpgrade(name, bpSd, nsInfo, bpPrevDir, bpTmpDir, bpCurDir, oldLV, conf);
    } else {
      callables.add(new Callable<StorageDirectory>() {
        @Override
        public StorageDirectory call() throws Exception {
          doUpgrade(name, bpSd, nsInfo, bpPrevDir, bpTmpDir, bpCurDir, oldLV,
              conf);
          return bpSd;
        }
      });
    }
  }

  private void doUpgrade(String name, final StorageDirectory bpSd,
      NamespaceInfo nsInfo, final File bpPrevDir, final File bpTmpDir,
      final File bpCurDir, final int oldLV, Configuration conf)
          throws IOException {
    // 3. Create new <SD>/current with block files hardlinks and VERSION
    linkAllBlocks(bpTmpDir, bpCurDir, oldLV, conf);
    this.layoutVersion = HdfsServerConstants.DATANODE_LAYOUT_VERSION;
    assert this.namespaceID == nsInfo.getNamespaceID() 
        : "Data-node and name-node layout versions must be the same.";
    this.cTime = nsInfo.getCTime();
    writeProperties(bpSd);
    
    // 4.rename <SD>/current/<bpid>/previous.tmp to
    // <SD>/current/<bpid>/previous
    rename(bpTmpDir, bpPrevDir);
    LOG.info("Upgrade of " + name + " is complete");
  }

  /**
   * Cleanup the detachDir.
   * 
   * If the directory is not empty report an error; Otherwise remove the
   * directory.
   * 
   * @param detachDir detach directory
   * @throws IOException if the directory is not empty or it can not be removed
   */
  private void cleanupDetachDir(File detachDir) throws IOException {
    if (!DataNodeLayoutVersion.supports(
        LayoutVersion.Feature.APPEND_RBW_DIR, layoutVersion)
        && detachDir.exists() && detachDir.isDirectory()) {

      if (FileUtil.list(detachDir).length != 0) {
        throw new IOException("Detached directory " + detachDir
            + " is not empty. Please manually move each file under this "
            + "directory to the finalized directory if the finalized "
            + "directory tree does not have the file.");
      } else if (!detachDir.delete()) {
        throw new IOException("Cannot remove directory " + detachDir);
      }
    }
  }

  /**
   * Restore all files from the trash directory to their corresponding
   * locations under current/
   */
  private int restoreBlockFilesFromTrash(File trashRoot)
      throws  IOException {
    int filesRestored = 0;
    File[] children = trashRoot.exists() ? trashRoot.listFiles() : null;
    if (children == null) {
      return 0;
    }

    File restoreDirectory = null;
    for (File child : children) {
      if (child.isDirectory()) {
        // Recurse to process subdirectories.
        filesRestored += restoreBlockFilesFromTrash(child);
        continue;
      }

      if (restoreDirectory == null) {
        restoreDirectory = new File(getRestoreDirectory(child));
        if (!restoreDirectory.exists() && !restoreDirectory.mkdirs()) {
          throw new IOException("Failed to create directory " + restoreDirectory);
        }
      }

      final File newChild = new File(restoreDirectory, child.getName());

      if (newChild.exists() && newChild.length() >= child.length()) {
        // Failsafe - we should not hit this case but let's make sure
        // we never overwrite a newer version of a block file with an
        // older version.
        LOG.info("Not overwriting " + newChild + " with smaller file from " +
                     "trash directory. This message can be safely ignored.");
      } else if (!child.renameTo(newChild)) {
        throw new IOException("Failed to rename " + child + " to " + newChild);
      } else {
        ++filesRestored;
      }
    }
    FileUtil.fullyDelete(trashRoot);
    return filesRestored;
  }

  /*
   * Roll back to old snapshot at the block pool level
   * If previous directory exists: 
   * <ol>
   * <li>Rename <SD>/current/<bpid>/current to removed.tmp</li>
   * <li>Rename * <SD>/current/<bpid>/previous to current</li>
   * <li>Remove removed.tmp</li>
   * </ol>
   * 
   * Do nothing if previous directory does not exist.
   * @param bpSd Block pool storage directory at <SD>/current/<bpid>
   */
  void doRollback(StorageDirectory bpSd, NamespaceInfo nsInfo)
      throws IOException {
    File prevDir = bpSd.getPreviousDir();
    // regular startup if previous dir does not exist
    if (!prevDir.exists())
      return;
    // read attributes out of the VERSION file of previous directory
    BlockPoolSliceStorage prevInfo = new BlockPoolSliceStorage();
    prevInfo.readPreviousVersionProperties(bpSd);

    // We allow rollback to a state, which is either consistent with
    // the namespace state or can be further upgraded to it.
    // In another word, we can only roll back when ( storedLV >= software LV)
    // && ( DN.previousCTime <= NN.ctime)
    if (!(prevInfo.getLayoutVersion() >= HdfsServerConstants.DATANODE_LAYOUT_VERSION &&
        prevInfo.getCTime() <= nsInfo.getCTime())) { // cannot rollback
      throw new InconsistentFSStateException(bpSd.getRoot(),
          "Cannot rollback to a newer state.\nDatanode previous state: LV = "
              + prevInfo.getLayoutVersion() + " CTime = " + prevInfo.getCTime()
              + " is newer than the namespace state: LV = "
              + HdfsServerConstants.DATANODE_LAYOUT_VERSION + " CTime = " + nsInfo.getCTime());
    }
    
    LOG.info("Rolling back storage directory " + bpSd.getRoot()
        + ".\n   target LV = " + nsInfo.getLayoutVersion()
        + "; target CTime = " + nsInfo.getCTime());
    File tmpDir = bpSd.getRemovedTmp();
    assert !tmpDir.exists() : "removed.tmp directory must not exist.";
    // 1. rename current to tmp
    File curDir = bpSd.getCurrentDir();
    assert curDir.exists() : "Current directory must exist.";
    rename(curDir, tmpDir);
    
    // 2. rename previous to current
    rename(prevDir, curDir);
    
    // 3. delete removed.tmp dir
    deleteDir(tmpDir);
    LOG.info("Rollback of " + bpSd.getRoot() + " is complete");
  }

  /*
   * Finalize the block pool storage by deleting <BP>/previous directory
   * that holds the snapshot.
   */
  void doFinalize(File dnCurDir) throws IOException {
    File bpRoot = getBpRoot(blockpoolID, dnCurDir);
    StorageDirectory bpSd = new StorageDirectory(bpRoot);
    // block pool level previous directory
    File prevDir = bpSd.getPreviousDir();
    if (!prevDir.exists()) {
      return; // already finalized
    }
    final String dataDirPath = bpSd.getRoot().getCanonicalPath();
    LOG.info("Finalizing upgrade for storage directory " + dataDirPath
        + ".\n   cur LV = " + this.getLayoutVersion() + "; cur CTime = "
        + this.getCTime());
    assert bpSd.getCurrentDir().exists() : "Current directory must exist.";
    
    // rename previous to finalized.tmp
    final File tmpDir = bpSd.getFinalizedTmp();
    rename(prevDir, tmpDir);

    // delete finalized.tmp dir in a separate thread
    new Daemon(new Runnable() {
      @Override
      public void run() {
        try {
          deleteDir(tmpDir);
        } catch (IOException ex) {
          LOG.error("Finalize upgrade for " + dataDirPath + " failed.", ex);
        }
        LOG.info("Finalize upgrade for " + dataDirPath + " is complete.");
      }

      @Override
      public String toString() {
        return "Finalize " + dataDirPath;
      }
    }).start();
  }

  /**
   * Hardlink all finalized and RBW blocks in fromDir to toDir
   * 
   * @param fromDir directory where the snapshot is stored
   * @param toDir the current data directory
   * @throws IOException if error occurs during hardlink
   */
  private static void linkAllBlocks(File fromDir, File toDir,
      int diskLayoutVersion, Configuration conf) throws IOException {
    // do the link
    // hardlink finalized blocks in tmpDir
    HardLink hardLink = new HardLink();
    DataStorage.linkBlocks(fromDir, toDir, DataStorage.STORAGE_DIR_FINALIZED,
        diskLayoutVersion, hardLink, conf);
    DataStorage.linkBlocks(fromDir, toDir, DataStorage.STORAGE_DIR_RBW,
        diskLayoutVersion, hardLink, conf);
    LOG.info("Linked blocks from " + fromDir + " to " + toDir + ". "
        + hardLink.linkStats.report());
  }

  /**
   * gets the data node storage directory based on block pool storage
   */
  private static String getDataNodeStorageRoot(String bpRoot) {
    Matcher matcher = BLOCK_POOL_PATH_PATTERN.matcher(bpRoot);
    if (matcher.matches()) {
      // return the data node root directory
      return matcher.group(1);
    }
    return bpRoot;
  }

  @Override
  public String toString() {
    return super.toString() + ";bpid=" + blockpoolID;
  }
  
  /**
   * Get a block pool storage root based on data node storage root
   * @param bpID block pool ID
   * @param dnCurDir data node storage root directory
   * @return root directory for block pool storage
   */
  public static File getBpRoot(String bpID, File dnCurDir) {
    return new File(dnCurDir, bpID);
  }

  @Override
  public boolean isPreUpgradableLayout(StorageDirectory sd) throws IOException {
    return false;
  }

  private File getTrashRootDir(StorageDirectory sd) {
    return new File(sd.getRoot(), TRASH_ROOT_DIR);
  }

  /**
   * Determine whether we can use trash for the given blockFile. Trash
   * is disallowed if a 'previous' directory exists for the
   * storage directory containing the block.
   */
  @VisibleForTesting
  public boolean isTrashAllowed(File blockFile) {
    Matcher matcher = BLOCK_POOL_CURRENT_PATH_PATTERN.matcher(blockFile.getParent());
    String previousDir = matcher.replaceFirst("$1$2" + STORAGE_DIR_PREVIOUS);
    return !(new File(previousDir)).exists();
  }

  /**
   * Get a target subdirectory under trash/ for a given block file that is being
   * deleted.
   *
   * The subdirectory structure under trash/ mirrors that under current/ to keep
   * implicit memory of where the files are to be restored (if necessary).
   *
   * @return the trash directory for a given block file that is being deleted.
   */
  public String getTrashDirectory(File blockFile) {
    if (isTrashAllowed(blockFile)) {
      Matcher matcher = BLOCK_POOL_CURRENT_PATH_PATTERN.matcher(blockFile.getParent());
      String trashDirectory = matcher.replaceFirst("$1$2" + TRASH_ROOT_DIR + "$4");
      return trashDirectory;
    }
    return null;
  }

  /**
   * Get a target subdirectory under current/ for a given block file that is being
   * restored from trash.
   *
   * The subdirectory structure under trash/ mirrors that under current/ to keep
   * implicit memory of where the files are to be restored.
   *
   * @return the target directory to restore a previously deleted block file.
   */
  @VisibleForTesting
  String getRestoreDirectory(File blockFile) {
    Matcher matcher = BLOCK_POOL_TRASH_PATH_PATTERN.matcher(blockFile.getParent());
    String restoreDirectory = matcher.replaceFirst("$1$2" + STORAGE_DIR_CURRENT + "$4");
    LOG.info("Restoring " + blockFile + " to " + restoreDirectory);
    return restoreDirectory;
  }

  /**
   * Delete all files and directories in the trash directories.
   */
  public void clearTrash() {
    final List<File> trashRoots = new ArrayList<>();
    for (StorageDirectory sd : storageDirs) {
      File trashRoot = getTrashRootDir(sd);
      if (trashRoot.exists() && sd.getPreviousDir().exists()) {
        LOG.error("Trash and PreviousDir shouldn't both exist for storage "
            + "directory " + sd);
        assert false;
      } else {
        trashRoots.add(trashRoot);
      }
    }

    stopTrashCleaner();
    trashCleaner = new Daemon(new Runnable() {
      @Override
      public void run() {
        for(File trashRoot : trashRoots){
          FileUtil.fullyDelete(trashRoot);
          LOG.info("Cleared trash for storage directory " + trashRoot);
        }
      }

      @Override
      public String toString() {
        return "clearTrash() for " + blockpoolID;
      }
    });
    trashCleaner.start();
  }

  public void stopTrashCleaner() {
    if (trashCleaner != null) {
      trashCleaner.interrupt();
    }
  }

  /** trash is enabled if at least one storage directory contains trash root */
  @VisibleForTesting
  public boolean trashEnabled() {
    for (StorageDirectory sd : storageDirs) {
      if (getTrashRootDir(sd).exists()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Create a rolling upgrade marker file for each BP storage root, if it
   * does not exist already.
   */
  public void setRollingUpgradeMarkers(List<StorageDirectory> dnStorageDirs)
      throws IOException {
    for (StorageDirectory sd : dnStorageDirs) {
      File bpRoot = getBpRoot(blockpoolID, sd.getCurrentDir());
      File markerFile = new File(bpRoot, ROLLING_UPGRADE_MARKER_FILE);
      if (!storagesWithRollingUpgradeMarker.contains(bpRoot.toString())) {
        if (!markerFile.exists() && markerFile.createNewFile()) {
          LOG.info("Created " + markerFile);
        } else {
          LOG.info(markerFile + " already exists.");
        }
        storagesWithRollingUpgradeMarker.add(bpRoot.toString());
        storagesWithoutRollingUpgradeMarker.remove(bpRoot.toString());
      }
    }
  }

  /**
   * Check whether the rolling upgrade marker file exists for each BP storage
   * root. If it does exist, then the marker file is cleared and more
   * importantly the layout upgrade is finalized.
   */
  public void clearRollingUpgradeMarkers(List<StorageDirectory> dnStorageDirs)
      throws IOException {
    for (StorageDirectory sd : dnStorageDirs) {
      File bpRoot = getBpRoot(blockpoolID, sd.getCurrentDir());
      File markerFile = new File(bpRoot, ROLLING_UPGRADE_MARKER_FILE);
      if (!storagesWithoutRollingUpgradeMarker.contains(bpRoot.toString())) {
        if (markerFile.exists()) {
          LOG.info("Deleting " + markerFile);
          doFinalize(sd.getCurrentDir());
          if (!markerFile.delete()) {
            LOG.warn("Failed to delete " + markerFile);
          }
        }
        storagesWithoutRollingUpgradeMarker.add(bpRoot.toString());
        storagesWithRollingUpgradeMarker.remove(bpRoot.toString());
      }
    }
  }
}
