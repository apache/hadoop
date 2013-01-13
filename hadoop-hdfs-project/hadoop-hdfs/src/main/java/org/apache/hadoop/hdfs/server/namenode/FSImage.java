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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.server.common.Storage.FormatConfirmable;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.server.common.Util;
import static org.apache.hadoop.util.Time.now;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;

import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.protocol.CheckpointCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.IdGenerator;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * FSImage handles checkpointing and logging of the namespace edits.
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSImage implements Closeable {
  public static final Log LOG = LogFactory.getLog(FSImage.class.getName());

  protected FSEditLog editLog = null;
  private boolean isUpgradeFinalized = false;

  protected NNStorage storage;
  
  /**
   * The last transaction ID that was either loaded from an image
   * or loaded by loading edits files.
   */
  protected long lastAppliedTxId = 0;

  final private Configuration conf;

  protected NNStorageRetentionManager archivalManager;
  protected IdGenerator blockIdGenerator;

  /**
   * Construct an FSImage
   * @param conf Configuration
   * @throws IOException if default directories are invalid.
   */
  public FSImage(Configuration conf) throws IOException {
    this(conf,
         FSNamesystem.getNamespaceDirs(conf),
         FSNamesystem.getNamespaceEditsDirs(conf));
  }

  /**
   * Construct the FSImage. Set the default checkpoint directories.
   *
   * Setup storage and initialize the edit log.
   *
   * @param conf Configuration
   * @param imageDirs Directories the image can be stored in.
   * @param editsDirs Directories the editlog can be stored in.
   * @throws IOException if directories are invalid.
   */
  protected FSImage(Configuration conf,
                    Collection<URI> imageDirs,
                    List<URI> editsDirs)
      throws IOException {
    this.conf = conf;

    storage = new NNStorage(conf, imageDirs, editsDirs);
    if(conf.getBoolean(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_KEY,
                       DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_DEFAULT)) {
      storage.setRestoreFailedStorage(true);
    }

    this.editLog = new FSEditLog(conf, storage, editsDirs);
    
    archivalManager = new NNStorageRetentionManager(conf, storage, editLog);
  }
 
  void format(FSNamesystem fsn, String clusterId) throws IOException {
    long fileCount = fsn.getTotalFiles();
    // Expect 1 file, which is the root inode
    Preconditions.checkState(fileCount == 1,
        "FSImage.format should be called with an uninitialized namesystem, has " +
        fileCount + " files");
    // BlockIdGenerator is defined during formatting
    // currently there is only one BlockIdGenerator
    blockIdGenerator = createBlockIdGenerator(fsn);
    NamespaceInfo ns = NNStorage.newNamespaceInfo();
    ns.clusterID = clusterId;
    
    storage.format(ns);
    editLog.formatNonFileJournals(ns);
    saveFSImageInAllDirs(fsn, 0);
  }
  
  /**
   * Check whether the storage directories and non-file journals exist.
   * If running in interactive mode, will prompt the user for each
   * directory to allow them to format anyway. Otherwise, returns
   * false, unless 'force' is specified.
   * 
   * @param force format regardless of whether dirs exist
   * @param interactive prompt the user when a dir exists
   * @return true if formatting should proceed
   * @throws IOException if some storage cannot be accessed
   */
  boolean confirmFormat(boolean force, boolean interactive) throws IOException {
    List<FormatConfirmable> confirms = Lists.newArrayList();
    for (StorageDirectory sd : storage.dirIterable(null)) {
      confirms.add(sd);
    }
    
    confirms.addAll(editLog.getFormatConfirmables());
    return Storage.confirmFormat(confirms, force, interactive);
  }
  
  /**
   * Analyze storage directories.
   * Recover from previous transitions if required. 
   * Perform fs state transition if necessary depending on the namespace info.
   * Read storage info. 
   * 
   * @throws IOException
   * @return true if the image needs to be saved or false otherwise
   */
  boolean recoverTransitionRead(StartupOption startOpt, FSNamesystem target,
      MetaRecoveryContext recovery) throws IOException {
    assert startOpt != StartupOption.FORMAT : 
      "NameNode formatting should be performed before reading the image";
    
    Collection<URI> imageDirs = storage.getImageDirectories();
    Collection<URI> editsDirs = editLog.getEditURIs();

    // none of the data dirs exist
    if((imageDirs.size() == 0 || editsDirs.size() == 0) 
                             && startOpt != StartupOption.IMPORT)  
      throw new IOException(
          "All specified directories are not accessible or do not exist.");
    
    // 1. For each data directory calculate its state and 
    // check whether all is consistent before transitioning.
    Map<StorageDirectory, StorageState> dataDirStates = 
             new HashMap<StorageDirectory, StorageState>();
    boolean isFormatted = recoverStorageDirs(startOpt, dataDirStates);

    if (LOG.isTraceEnabled()) {
      LOG.trace("Data dir states:\n  " +
        Joiner.on("\n  ").withKeyValueSeparator(": ")
        .join(dataDirStates));
    }
    
    if (!isFormatted && startOpt != StartupOption.ROLLBACK 
                     && startOpt != StartupOption.IMPORT) {
      throw new IOException("NameNode is not formatted.");      
    }


    int layoutVersion = storage.getLayoutVersion();
    if (layoutVersion < Storage.LAST_PRE_UPGRADE_LAYOUT_VERSION) {
      NNStorage.checkVersionUpgradable(storage.getLayoutVersion());
    }
    if (startOpt != StartupOption.UPGRADE
        && layoutVersion < Storage.LAST_PRE_UPGRADE_LAYOUT_VERSION
        && layoutVersion != HdfsConstants.LAYOUT_VERSION) {
      throw new IOException(
          "\nFile system image contains an old layout version " 
          + storage.getLayoutVersion() + ".\nAn upgrade to version "
          + HdfsConstants.LAYOUT_VERSION + " is required.\n"
          + "Please restart NameNode with -upgrade option.");
    }
    
    storage.processStartupOptionsForUpgrade(startOpt, layoutVersion);

    // 2. Format unformatted dirs.
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      StorageState curState = dataDirStates.get(sd);
      switch(curState) {
      case NON_EXISTENT:
        throw new IOException(StorageState.NON_EXISTENT + 
                              " state cannot be here");
      case NOT_FORMATTED:
        LOG.info("Storage directory " + sd.getRoot() + " is not formatted.");
        LOG.info("Formatting ...");
        sd.clearDirectory(); // create empty currrent dir
        break;
      default:
        break;
      }
    }

    // 3. Do transitions
    switch(startOpt) {
    case UPGRADE:
      doUpgrade(target);
      return false; // upgrade saved image already
    case IMPORT:
      doImportCheckpoint(target);
      return false; // import checkpoint saved image already
    case ROLLBACK:
      doRollback();
      break;
    case REGULAR:
    default:
      // just load the image
    }
    
    return loadFSImage(target, recovery);
  }
  
  /**
   * For each storage directory, performs recovery of incomplete transitions
   * (eg. upgrade, rollback, checkpoint) and inserts the directory's storage
   * state into the dataDirStates map.
   * @param dataDirStates output of storage directory states
   * @return true if there is at least one valid formatted storage directory
   */
  private boolean recoverStorageDirs(StartupOption startOpt,
      Map<StorageDirectory, StorageState> dataDirStates) throws IOException {
    boolean isFormatted = false;
    for (Iterator<StorageDirectory> it = 
                      storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      StorageState curState;
      try {
        curState = sd.analyzeStorage(startOpt, storage);
        String nameserviceId = DFSUtil.getNamenodeNameServiceId(conf);
        if (curState != StorageState.NORMAL && HAUtil.isHAEnabled(conf, nameserviceId)) {
          throw new IOException("Cannot start an HA namenode with name dirs " +
              "that need recovery. Dir: " + sd + " state: " + curState);
        }
        // sd is locked but not opened
        switch(curState) {
        case NON_EXISTENT:
          // name-node fails if any of the configured storage dirs are missing
          throw new InconsistentFSStateException(sd.getRoot(),
                      "storage directory does not exist or is not accessible.");
        case NOT_FORMATTED:
          break;
        case NORMAL:
          break;
        default:  // recovery is possible
          sd.doRecover(curState);      
        }
        if (curState != StorageState.NOT_FORMATTED 
            && startOpt != StartupOption.ROLLBACK) {
          // read and verify consistency with other directories
          storage.readProperties(sd);
          isFormatted = true;
        }
        if (startOpt == StartupOption.IMPORT && isFormatted)
          // import of a checkpoint is allowed only into empty image directories
          throw new IOException("Cannot import image from a checkpoint. " 
              + " NameNode already contains an image in " + sd.getRoot());
      } catch (IOException ioe) {
        sd.unlock();
        throw ioe;
      }
      dataDirStates.put(sd,curState);
    }
    return isFormatted;
  }

  private void doUpgrade(FSNamesystem target) throws IOException {
    // Upgrade is allowed only if there are 
    // no previous fs states in any of the directories
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      if (sd.getPreviousDir().exists())
        throw new InconsistentFSStateException(sd.getRoot(),
            "previous fs state should not exist during upgrade. "
            + "Finalize or rollback first.");
    }

    // load the latest image
    this.loadFSImage(target, null);

    // Do upgrade for each directory
    long oldCTime = storage.getCTime();
    storage.cTime = now();  // generate new cTime for the state
    int oldLV = storage.getLayoutVersion();
    storage.layoutVersion = HdfsConstants.LAYOUT_VERSION;
    
    List<StorageDirectory> errorSDs =
      Collections.synchronizedList(new ArrayList<StorageDirectory>());
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      LOG.info("Starting upgrade of image directory " + sd.getRoot()
               + ".\n   old LV = " + oldLV
               + "; old CTime = " + oldCTime
               + ".\n   new LV = " + storage.getLayoutVersion()
               + "; new CTime = " + storage.getCTime());
      try {
        File curDir = sd.getCurrentDir();
        File prevDir = sd.getPreviousDir();
        File tmpDir = sd.getPreviousTmp();
        assert curDir.exists() : "Current directory must exist.";
        assert !prevDir.exists() : "previous directory must not exist.";
        assert !tmpDir.exists() : "previous.tmp directory must not exist.";
        assert !editLog.isSegmentOpen() : "Edits log must not be open.";

        // rename current to tmp
        NNStorage.rename(curDir, tmpDir);
        
        if (!curDir.mkdir()) {
          throw new IOException("Cannot create directory " + curDir);
        }
      } catch (Exception e) {
        LOG.error("Failed to move aside pre-upgrade storage " +
            "in image directory " + sd.getRoot(), e);
        errorSDs.add(sd);
        continue;
      }
    }
    storage.reportErrorsOnDirectories(errorSDs);
    errorSDs.clear();

    saveFSImageInAllDirs(target, editLog.getLastWrittenTxId());

    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      try {
        // Write the version file, since saveFsImage above only makes the
        // fsimage_<txid>, and the directory is otherwise empty.
        storage.writeProperties(sd);
        
        File prevDir = sd.getPreviousDir();
        File tmpDir = sd.getPreviousTmp();
        // rename tmp to previous
        NNStorage.rename(tmpDir, prevDir);
      } catch (IOException ioe) {
        LOG.error("Unable to rename temp to previous for " + sd.getRoot(), ioe);
        errorSDs.add(sd);
        continue;
      }
      LOG.info("Upgrade of " + sd.getRoot() + " is complete.");
    }
    storage.reportErrorsOnDirectories(errorSDs);

    isUpgradeFinalized = false;
    if (!storage.getRemovedStorageDirs().isEmpty()) {
      //during upgrade, it's a fatal error to fail any storage directory
      throw new IOException("Upgrade failed in "
          + storage.getRemovedStorageDirs().size()
          + " storage directory(ies), previously logged.");
    }
  }

  private void doRollback() throws IOException {
    // Rollback is allowed only if there is 
    // a previous fs states in at least one of the storage directories.
    // Directories that don't have previous state do not rollback
    boolean canRollback = false;
    FSImage prevState = new FSImage(conf);
    prevState.getStorage().layoutVersion = HdfsConstants.LAYOUT_VERSION;
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      File prevDir = sd.getPreviousDir();
      if (!prevDir.exists()) {  // use current directory then
        LOG.info("Storage directory " + sd.getRoot()
                 + " does not contain previous fs state.");
        // read and verify consistency with other directories
        storage.readProperties(sd);
        continue;
      }

      // read and verify consistency of the prev dir
      prevState.getStorage().readPreviousVersionProperties(sd);

      if (prevState.getLayoutVersion() != HdfsConstants.LAYOUT_VERSION) {
        throw new IOException(
          "Cannot rollback to storage version " +
          prevState.getLayoutVersion() +
          " using this version of the NameNode, which uses storage version " +
          HdfsConstants.LAYOUT_VERSION + ". " +
          "Please use the previous version of HDFS to perform the rollback.");
      }
      canRollback = true;
    }
    if (!canRollback)
      throw new IOException("Cannot rollback. None of the storage "
                            + "directories contain previous fs state.");

    // Now that we know all directories are going to be consistent
    // Do rollback for each directory containing previous state
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      File prevDir = sd.getPreviousDir();
      if (!prevDir.exists())
        continue;

      LOG.info("Rolling back storage directory " + sd.getRoot()
               + ".\n   new LV = " + prevState.getStorage().getLayoutVersion()
               + "; new CTime = " + prevState.getStorage().getCTime());
      File tmpDir = sd.getRemovedTmp();
      assert !tmpDir.exists() : "removed.tmp directory must not exist.";
      // rename current to tmp
      File curDir = sd.getCurrentDir();
      assert curDir.exists() : "Current directory must exist.";
      NNStorage.rename(curDir, tmpDir);
      // rename previous to current
      NNStorage.rename(prevDir, curDir);

      // delete tmp dir
      NNStorage.deleteDir(tmpDir);
      LOG.info("Rollback of " + sd.getRoot()+ " is complete.");
    }
    isUpgradeFinalized = true;
  }

  private void doFinalize(StorageDirectory sd) throws IOException {
    File prevDir = sd.getPreviousDir();
    if (!prevDir.exists()) { // already discarded
      LOG.info("Directory " + prevDir + " does not exist.");
      LOG.info("Finalize upgrade for " + sd.getRoot()+ " is not required.");
      return;
    }
    LOG.info("Finalizing upgrade for storage directory " 
             + sd.getRoot() + "."
             + (storage.getLayoutVersion()==0 ? "" :
                   "\n   cur LV = " + storage.getLayoutVersion()
                   + "; cur CTime = " + storage.getCTime()));
    assert sd.getCurrentDir().exists() : "Current directory must exist.";
    final File tmpDir = sd.getFinalizedTmp();
    // rename previous to tmp and remove
    NNStorage.rename(prevDir, tmpDir);
    NNStorage.deleteDir(tmpDir);
    isUpgradeFinalized = true;
    LOG.info("Finalize upgrade for " + sd.getRoot()+ " is complete.");
  }

  /**
   * Load image from a checkpoint directory and save it into the current one.
   * @param target the NameSystem to import into
   * @throws IOException
   */
  void doImportCheckpoint(FSNamesystem target) throws IOException {
    Collection<URI> checkpointDirs =
      FSImage.getCheckpointDirs(conf, null);
    List<URI> checkpointEditsDirs =
      FSImage.getCheckpointEditsDirs(conf, null);

    if (checkpointDirs == null || checkpointDirs.isEmpty()) {
      throw new IOException("Cannot import image from a checkpoint. "
                            + "\"dfs.namenode.checkpoint.dir\" is not set." );
    }
    
    if (checkpointEditsDirs == null || checkpointEditsDirs.isEmpty()) {
      throw new IOException("Cannot import image from a checkpoint. "
                            + "\"dfs.namenode.checkpoint.dir\" is not set." );
    }

    FSImage realImage = target.getFSImage();
    FSImage ckptImage = new FSImage(conf, 
                                    checkpointDirs, checkpointEditsDirs);
    target.dir.fsImage = ckptImage;
    // load from the checkpoint dirs
    try {
      ckptImage.recoverTransitionRead(StartupOption.REGULAR, target, null);
    } finally {
      ckptImage.close();
    }
    // return back the real image
    realImage.getStorage().setStorageInfo(ckptImage.getStorage());
    realImage.getEditLog().setNextTxId(ckptImage.getEditLog().getLastWrittenTxId()+1);
    realImage.initEditLog();

    target.dir.fsImage = realImage;
    realImage.getStorage().setBlockPoolID(ckptImage.getBlockPoolID());

    // and save it but keep the same checkpointTime
    saveNamespace(target);
    getStorage().writeAll();
  }

  void finalizeUpgrade() throws IOException {
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      doFinalize(sd);
    }
  }

  boolean isUpgradeFinalized() {
    return isUpgradeFinalized;
  }

  public FSEditLog getEditLog() {
    return editLog;
  }

  @VisibleForTesting
  void setEditLogForTesting(FSEditLog newLog) {
    editLog = newLog;
  }

  void openEditLogForWrite() throws IOException {
    assert editLog != null : "editLog must be initialized";
    editLog.openForWrite();
    storage.writeTransactionIdFileToStorage(editLog.getCurSegmentTxId());
  };
  
  /**
   * Toss the current image and namesystem, reloading from the specified
   * file.
   */
  void reloadFromImageFile(File file, FSNamesystem target) throws IOException {
    target.clear();
    LOG.debug("Reloading namespace from " + file);
    loadFSImage(file, target, null);
  }

  /**
   * Choose latest image from one of the directories,
   * load it and merge with the edits.
   * 
   * Saving and loading fsimage should never trigger symlink resolution. 
   * The paths that are persisted do not have *intermediate* symlinks 
   * because intermediate symlinks are resolved at the time files, 
   * directories, and symlinks are created. All paths accessed while 
   * loading or saving fsimage should therefore only see symlinks as 
   * the final path component, and the functions called below do not
   * resolve symlinks that are the final path component.
   *
   * @return whether the image should be saved
   * @throws IOException
   */
  boolean loadFSImage(FSNamesystem target, MetaRecoveryContext recovery)
      throws IOException {
    FSImageStorageInspector inspector = storage.readAndInspectDirs();
    
    isUpgradeFinalized = inspector.isUpgradeFinalized();
 
    FSImageStorageInspector.FSImageFile imageFile 
      = inspector.getLatestImage();   
    boolean needToSave = inspector.needToSave();

    Iterable<EditLogInputStream> editStreams = null;

    initEditLog();

    if (LayoutVersion.supports(Feature.TXID_BASED_LAYOUT, 
                               getLayoutVersion())) {
      // If we're open for write, we're either non-HA or we're the active NN, so
      // we better be able to load all the edits. If we're the standby NN, it's
      // OK to not be able to read all of edits right now.
      long toAtLeastTxId = editLog.isOpenForWrite() ? inspector.getMaxSeenTxId() : 0;
      editStreams = editLog.selectInputStreams(imageFile.getCheckpointTxId() + 1,
          toAtLeastTxId, recovery, false);
    } else {
      editStreams = FSImagePreTransactionalStorageInspector
        .getEditLogStreams(storage);
    }
 
    LOG.debug("Planning to load image :\n" + imageFile);
    for (EditLogInputStream l : editStreams) {
      LOG.debug("Planning to load edit log stream: " + l);
    }
    if (!editStreams.iterator().hasNext()) {
      LOG.info("No edit log streams selected.");
    }
    
    try {
      StorageDirectory sdForProperties = imageFile.sd;
      storage.readProperties(sdForProperties);

      if (LayoutVersion.supports(Feature.TXID_BASED_LAYOUT,
                                 getLayoutVersion())) {
        // For txid-based layout, we should have a .md5 file
        // next to the image file
        loadFSImage(imageFile.getFile(), target, recovery);
      } else if (LayoutVersion.supports(Feature.FSIMAGE_CHECKSUM,
                                        getLayoutVersion())) {
        // In 0.22, we have the checksum stored in the VERSION file.
        String md5 = storage.getDeprecatedProperty(
            NNStorage.DEPRECATED_MESSAGE_DIGEST_PROPERTY);
        if (md5 == null) {
          throw new InconsistentFSStateException(sdForProperties.getRoot(),
              "Message digest property " +
              NNStorage.DEPRECATED_MESSAGE_DIGEST_PROPERTY +
              " not set for storage directory " + sdForProperties.getRoot());
        }
        loadFSImage(imageFile.getFile(), new MD5Hash(md5), target, recovery);
      } else {
        // We don't have any record of the md5sum
        loadFSImage(imageFile.getFile(), null, target, recovery);
      }
    } catch (IOException ioe) {
      FSEditLog.closeAllStreams(editStreams);
      throw new IOException("Failed to load image from " + imageFile, ioe);
    }
    long txnsAdvanced = loadEdits(editStreams, target, recovery);
    needToSave |= needsResaveBasedOnStaleCheckpoint(imageFile.getFile(),
                                                    txnsAdvanced);
    editLog.setNextTxId(lastAppliedTxId + 1);
    return needToSave;
  }

  public void initEditLog() {
    Preconditions.checkState(getNamespaceID() != 0,
        "Must know namespace ID before initting edit log");
    String nameserviceId = DFSUtil.getNamenodeNameServiceId(conf);
    if (!HAUtil.isHAEnabled(conf, nameserviceId)) {
      editLog.initJournalsForWrite();
      editLog.recoverUnclosedStreams();
    } else {
      editLog.initSharedJournalsForRead();
    }
  }

  /**
   * @param imageFile the image file that was loaded
   * @param numEditsLoaded the number of edits loaded from edits logs
   * @return true if the NameNode should automatically save the namespace
   * when it is started, due to the latest checkpoint being too old.
   */
  private boolean needsResaveBasedOnStaleCheckpoint(
      File imageFile, long numEditsLoaded) {
    final long checkpointPeriod = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY, 
        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_DEFAULT);
    final long checkpointTxnCount = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY, 
        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_DEFAULT);
    long checkpointAge = Time.now() - imageFile.lastModified();

    return (checkpointAge > checkpointPeriod * 1000) ||
           (numEditsLoaded > checkpointTxnCount);
  }
  
  /**
   * Load the specified list of edit files into the image.
   */
  public long loadEdits(Iterable<EditLogInputStream> editStreams,
      FSNamesystem target, MetaRecoveryContext recovery) throws IOException {
    LOG.debug("About to load edits:\n  " + Joiner.on("\n  ").join(editStreams));
    
    long prevLastAppliedTxId = lastAppliedTxId;  
    try {    
      FSEditLogLoader loader = new FSEditLogLoader(target, lastAppliedTxId);
      
      // Load latest edits
      for (EditLogInputStream editIn : editStreams) {
        LOG.info("Reading " + editIn + " expecting start txid #" +
              (lastAppliedTxId + 1));
        try {
          loader.loadFSEdits(editIn, lastAppliedTxId + 1, recovery);
        } finally {
          // Update lastAppliedTxId even in case of error, since some ops may
          // have been successfully applied before the error.
          lastAppliedTxId = loader.getLastAppliedTxId();
        }
        // If we are in recovery mode, we may have skipped over some txids.
        if (editIn.getLastTxId() != HdfsConstants.INVALID_TXID) {
          lastAppliedTxId = editIn.getLastTxId();
        }
      }
    } finally {
      FSEditLog.closeAllStreams(editStreams);
      // update the counts
      target.dir.updateCountForINodeWithQuota();   
    }
    return lastAppliedTxId - prevLastAppliedTxId;
  }


  /**
   * Load the image namespace from the given image file, verifying
   * it against the MD5 sum stored in its associated .md5 file.
   */
  private void loadFSImage(File imageFile, FSNamesystem target,
      MetaRecoveryContext recovery) throws IOException {
    MD5Hash expectedMD5 = MD5FileUtils.readStoredMd5ForFile(imageFile);
    if (expectedMD5 == null) {
      throw new IOException("No MD5 file found corresponding to image file "
          + imageFile);
    }
    loadFSImage(imageFile, expectedMD5, target, recovery);
  }
  
  /**
   * Load in the filesystem image from file. It's a big list of
   * filenames and blocks.
   */
  private void loadFSImage(File curFile, MD5Hash expectedMd5,
      FSNamesystem target, MetaRecoveryContext recovery) throws IOException {
    FSImageFormat.Loader loader = new FSImageFormat.Loader(
        conf, target);
    loader.load(curFile);
    // BlockIdGenerator is determined after loading image
    // currently there is only one BlockIdGenerator
    blockIdGenerator = createBlockIdGenerator(target);
    target.setBlockPoolId(this.getBlockPoolID());

    // Check that the image digest we loaded matches up with what
    // we expected
    MD5Hash readImageMd5 = loader.getLoadedImageMd5();
    if (expectedMd5 != null &&
        !expectedMd5.equals(readImageMd5)) {
      throw new IOException("Image file " + curFile +
          " is corrupt with MD5 checksum of " + readImageMd5 +
          " but expecting " + expectedMd5);
    }

    long txId = loader.getLoadedImageTxId();
    LOG.info("Loaded image for txid " + txId + " from " + curFile);
    lastAppliedTxId = txId;
    storage.setMostRecentCheckpointInfo(txId, curFile.lastModified());
  }

  /**
   * Save the contents of the FS image to the file.
   */
  void saveFSImage(SaveNamespaceContext context, StorageDirectory sd)
      throws IOException {
    long txid = context.getTxId();
    File newFile = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE_NEW, txid);
    File dstFile = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE, txid);
    
    FSImageFormat.Saver saver = new FSImageFormat.Saver(context);
    FSImageCompression compression = FSImageCompression.createCompression(conf);
    saver.save(newFile, compression);
    
    MD5FileUtils.saveMD5File(dstFile, saver.getSavedDigest());
    storage.setMostRecentCheckpointInfo(txid, Time.now());
  }

  /**
   * FSImageSaver is being run in a separate thread when saving
   * FSImage. There is one thread per each copy of the image.
   *
   * FSImageSaver assumes that it was launched from a thread that holds
   * FSNamesystem lock and waits for the execution of FSImageSaver thread
   * to finish.
   * This way we are guaranteed that the namespace is not being updated
   * while multiple instances of FSImageSaver are traversing it
   * and writing it out.
   */
  private class FSImageSaver implements Runnable {
    private final SaveNamespaceContext context;
    private StorageDirectory sd;

    public FSImageSaver(SaveNamespaceContext context, StorageDirectory sd) {
      this.context = context;
      this.sd = sd;
    }

    @Override
    public void run() {
      try {
        saveFSImage(context, sd);
      } catch (SaveNamespaceCancelledException snce) {
        LOG.info("Cancelled image saving for " + sd.getRoot() +
            ": " + snce.getMessage());
        // don't report an error on the storage dir!
      } catch (Throwable t) {
        LOG.error("Unable to save image for " + sd.getRoot(), t);
        context.reportErrorOnStorageDirectory(sd);
      }
    }
    
    @Override
    public String toString() {
      return "FSImageSaver for " + sd.getRoot() +
             " of type " + sd.getStorageDirType();
    }
  }
  
  private void waitForThreads(List<Thread> threads) {
    for (Thread thread : threads) {
      while (thread.isAlive()) {
        try {
          thread.join();
        } catch (InterruptedException iex) {
          LOG.error("Caught interrupted exception while waiting for thread " +
                    thread.getName() + " to finish. Retrying join");
        }        
      }
    }
  }
  
  /**
   * @see #saveNamespace(FSNamesystem, Canceler)
   */
  public synchronized void saveNamespace(FSNamesystem source)
      throws IOException {
    saveNamespace(source, null);
  }
  
  /**
   * Save the contents of the FS image to a new image file in each of the
   * current storage directories.
   * @param canceler 
   */
  public synchronized void saveNamespace(FSNamesystem source,
      Canceler canceler) throws IOException {
    assert editLog != null : "editLog must be initialized";
    storage.attemptRestoreRemovedStorage();

    boolean editLogWasOpen = editLog.isSegmentOpen();
    
    if (editLogWasOpen) {
      editLog.endCurrentLogSegment(true);
    }
    long imageTxId = getLastAppliedOrWrittenTxId();
    try {
      saveFSImageInAllDirs(source, imageTxId, canceler);
      storage.writeAll();
    } finally {
      if (editLogWasOpen) {
        editLog.startLogSegment(imageTxId + 1, true);
        // Take this opportunity to note the current transaction.
        // Even if the namespace save was cancelled, this marker
        // is only used to determine what transaction ID is required
        // for startup. So, it doesn't hurt to update it unnecessarily.
        storage.writeTransactionIdFileToStorage(imageTxId + 1);
      }
    }
  }

  /**
   * @see #saveFSImageInAllDirs(FSNamesystem, long, Canceler)
   */
  protected synchronized void saveFSImageInAllDirs(FSNamesystem source, long txid)
      throws IOException {
    saveFSImageInAllDirs(source, txid, null);
  }

  protected synchronized void saveFSImageInAllDirs(FSNamesystem source, long txid,
      Canceler canceler)
      throws IOException {    
    if (storage.getNumStorageDirs(NameNodeDirType.IMAGE) == 0) {
      throw new IOException("No image directories available!");
    }
    if (canceler == null) {
      canceler = new Canceler();
    }
    SaveNamespaceContext ctx = new SaveNamespaceContext(
        source, txid, canceler);
    
    try {
      List<Thread> saveThreads = new ArrayList<Thread>();
      // save images into current
      for (Iterator<StorageDirectory> it
             = storage.dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
        StorageDirectory sd = it.next();
        FSImageSaver saver = new FSImageSaver(ctx, sd);
        Thread saveThread = new Thread(saver, saver.toString());
        saveThreads.add(saveThread);
        saveThread.start();
      }
      waitForThreads(saveThreads);
      saveThreads.clear();
      storage.reportErrorsOnDirectories(ctx.getErrorSDs());
  
      if (storage.getNumStorageDirs(NameNodeDirType.IMAGE) == 0) {
        throw new IOException(
          "Failed to save in any storage directories while saving namespace.");
      }
      if (canceler.isCancelled()) {
        deleteCancelledCheckpoint(txid);
        ctx.checkCancelled(); // throws
        assert false : "should have thrown above!";
      }
  
      renameCheckpoint(txid);
  
      // Since we now have a new checkpoint, we can clean up some
      // old edit logs and checkpoints.
      purgeOldStorage();
    } finally {
      // Notify any threads waiting on the checkpoint to be canceled
      // that it is complete.
      ctx.markComplete();
      ctx = null;
    }
  }

  /**
   * Purge any files in the storage directories that are no longer
   * necessary.
   */
  public void purgeOldStorage() {
    try {
      archivalManager.purgeOldStorage();
    } catch (Exception e) {
      LOG.warn("Unable to purge old storage", e);
    }
  }

  /**
   * Renames new image
   */
  private void renameCheckpoint(long txid) throws IOException {
    ArrayList<StorageDirectory> al = null;

    for (StorageDirectory sd : storage.dirIterable(NameNodeDirType.IMAGE)) {
      try {
        renameCheckpointInDir(sd, txid);
      } catch (IOException ioe) {
        LOG.warn("Unable to rename checkpoint in " + sd, ioe);
        if (al == null) {
          al = Lists.newArrayList();
        }
        al.add(sd);
      }
    }
    if(al != null) storage.reportErrorsOnDirectories(al);
  }
  
  /**
   * Deletes the checkpoint file in every storage directory,
   * since the checkpoint was cancelled.
   */
  private void deleteCancelledCheckpoint(long txid) throws IOException {
    ArrayList<StorageDirectory> al = Lists.newArrayList();

    for (StorageDirectory sd : storage.dirIterable(NameNodeDirType.IMAGE)) {
      File ckpt = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE_NEW, txid);
      if (ckpt.exists() && !ckpt.delete()) {
        LOG.warn("Unable to delete cancelled checkpoint in " + sd);
        al.add(sd);            
      }
    }
    storage.reportErrorsOnDirectories(al);
  }


  private void renameCheckpointInDir(StorageDirectory sd, long txid)
      throws IOException {
    File ckpt = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE_NEW, txid);
    File curFile = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE, txid);
    // renameTo fails on Windows if the destination file 
    // already exists.
    if(LOG.isDebugEnabled()) {
      LOG.debug("renaming  " + ckpt.getAbsolutePath() 
                + " to " + curFile.getAbsolutePath());
    }
    if (!ckpt.renameTo(curFile)) {
      if (!curFile.delete() || !ckpt.renameTo(curFile)) {
        throw new IOException("renaming  " + ckpt.getAbsolutePath() + " to "  + 
            curFile.getAbsolutePath() + " FAILED");
      }
    }    
  }

  CheckpointSignature rollEditLog() throws IOException {
    getEditLog().rollEditLog();
    // Record this log segment ID in all of the storage directories, so
    // we won't miss this log segment on a restart if the edits directories
    // go missing.
    storage.writeTransactionIdFileToStorage(getEditLog().getCurSegmentTxId());
    return new CheckpointSignature(this);
  }

  /**
   * Start checkpoint.
   * <p>
   * If backup storage contains image that is newer than or incompatible with 
   * what the active name-node has, then the backup node should shutdown.<br>
   * If the backup image is older than the active one then it should 
   * be discarded and downloaded from the active node.<br>
   * If the images are the same then the backup image will be used as current.
   * 
   * @param bnReg the backup node registration.
   * @param nnReg this (active) name-node registration.
   * @return {@link NamenodeCommand} if backup node should shutdown or
   * {@link CheckpointCommand} prescribing what backup node should 
   *         do with its image.
   * @throws IOException
   */
  NamenodeCommand startCheckpoint(NamenodeRegistration bnReg, // backup node
                                  NamenodeRegistration nnReg) // active name-node
  throws IOException {
    LOG.info("Start checkpoint at txid " + getEditLog().getLastWrittenTxId());
    String msg = null;
    // Verify that checkpoint is allowed
    if(bnReg.getNamespaceID() != storage.getNamespaceID())
      msg = "Name node " + bnReg.getAddress()
            + " has incompatible namespace id: " + bnReg.getNamespaceID()
            + " expected: " + storage.getNamespaceID();
    else if(bnReg.isRole(NamenodeRole.NAMENODE))
      msg = "Name node " + bnReg.getAddress()
            + " role " + bnReg.getRole() + ": checkpoint is not allowed.";
    else if(bnReg.getLayoutVersion() < storage.getLayoutVersion()
        || (bnReg.getLayoutVersion() == storage.getLayoutVersion()
            && bnReg.getCTime() > storage.getCTime()))
      // remote node has newer image age
      msg = "Name node " + bnReg.getAddress()
            + " has newer image layout version: LV = " +bnReg.getLayoutVersion()
            + " cTime = " + bnReg.getCTime()
            + ". Current version: LV = " + storage.getLayoutVersion()
            + " cTime = " + storage.getCTime();
    if(msg != null) {
      LOG.error(msg);
      return new NamenodeCommand(NamenodeProtocol.ACT_SHUTDOWN);
    }
    boolean needToReturnImg = true;
    if(storage.getNumStorageDirs(NameNodeDirType.IMAGE) == 0)
      // do not return image if there are no image directories
      needToReturnImg = false;
    CheckpointSignature sig = rollEditLog();
    return new CheckpointCommand(sig, needToReturnImg);
  }

  /**
   * End checkpoint.
   * <p>
   * Validate the current storage info with the given signature.
   * 
   * @param sig to validate the current storage info against
   * @throws IOException if the checkpoint fields are inconsistent
   */
  void endCheckpoint(CheckpointSignature sig) throws IOException {
    LOG.info("End checkpoint at txid " + getEditLog().getLastWrittenTxId());
    sig.validateStorageInfo(this);
  }

  /**
   * This is called by the 2NN after having downloaded an image, and by
   * the NN after having received a new image from the 2NN. It
   * renames the image from fsimage_N.ckpt to fsimage_N and also
   * saves the related .md5 file into place.
   */
  public synchronized void saveDigestAndRenameCheckpointImage(
      long txid, MD5Hash digest) throws IOException {
    renameCheckpoint(txid);
    List<StorageDirectory> badSds = Lists.newArrayList();
    
    for (StorageDirectory sd : storage.dirIterable(NameNodeDirType.IMAGE)) {
      File imageFile = NNStorage.getImageFile(sd, txid);
      try {
        MD5FileUtils.saveMD5File(imageFile, digest);
      } catch (IOException ioe) {
        badSds.add(sd);
      }
    }
    storage.reportErrorsOnDirectories(badSds);
    
    // So long as this is the newest image available,
    // advertise it as such to other checkpointers
    // from now on
    if (txid > storage.getMostRecentCheckpointTxId()) {
      storage.setMostRecentCheckpointInfo(txid, Time.now());
    }
  }

  @Override
  synchronized public void close() throws IOException {
    if (editLog != null) { // 2NN doesn't have any edit log
      getEditLog().close();
    }
    storage.close();
  }


  /**
   * Retrieve checkpoint dirs from configuration.
   *
   * @param conf the Configuration
   * @param defaultValue a default value for the attribute, if null
   * @return a Collection of URIs representing the values in 
   * dfs.namenode.checkpoint.dir configuration property
   */
  static Collection<URI> getCheckpointDirs(Configuration conf,
      String defaultValue) {
    Collection<String> dirNames = conf.getTrimmedStringCollection(
        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY);
    if (dirNames.size() == 0 && defaultValue != null) {
      dirNames.add(defaultValue);
    }
    return Util.stringCollectionAsURIs(dirNames);
  }

  static List<URI> getCheckpointEditsDirs(Configuration conf,
      String defaultName) {
    Collection<String> dirNames = conf.getTrimmedStringCollection(
        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY);
    if (dirNames.size() == 0 && defaultName != null) {
      dirNames.add(defaultName);
    }
    return Util.stringCollectionAsURIs(dirNames);
  }

  public NNStorage getStorage() {
    return storage;
  }

  public int getLayoutVersion() {
    return storage.getLayoutVersion();
  }
  
  public int getNamespaceID() {
    return storage.getNamespaceID();
  }
  
  public String getClusterID() {
    return storage.getClusterID();
  }
  
  public String getBlockPoolID() {
    return storage.getBlockPoolID();
  }

  public synchronized long getLastAppliedTxId() {
    return lastAppliedTxId;
  }

  public long getLastAppliedOrWrittenTxId() {
    return Math.max(lastAppliedTxId,
        editLog != null ? editLog.getLastWrittenTxId() : 0);
  }

  public void updateLastAppliedTxIdFromWritten() {
    this.lastAppliedTxId = editLog.getLastWrittenTxId();
  }

  public synchronized long getMostRecentCheckpointTxId() {
    return storage.getMostRecentCheckpointTxId();
  }

  public long getUniqueBlockId() {
    return blockIdGenerator.nextValue();
  }

  public IdGenerator createBlockIdGenerator(FSNamesystem fsn) {
    return new RandomBlockIdGenerator(fsn);
  }
}
