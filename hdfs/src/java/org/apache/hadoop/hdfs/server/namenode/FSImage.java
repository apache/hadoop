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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.Util;
import static org.apache.hadoop.hdfs.server.common.Util.now;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NNStorageListener;
import org.apache.hadoop.hdfs.server.protocol.CheckpointCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.hdfs.DFSConfigKeys;

/**
 * FSImage handles checkpointing and logging of the namespace edits.
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSImage implements NNStorageListener, Closeable {
  protected static final Log LOG = LogFactory.getLog(FSImage.class.getName());

  private static final SimpleDateFormat DATE_FORM =
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  // checkpoint states
  enum CheckpointStates{START, ROLLED_EDITS, UPLOAD_START, UPLOAD_DONE; }

  protected FSNamesystem namesystem = null;
  protected FSEditLog editLog = null;
  private boolean isUpgradeFinalized = false;
  protected MD5Hash newImageDigest = null;

  protected NNStorage storage = null;

  /**
   * URIs for importing an image from a checkpoint. In the default case,
   * URIs will represent directories. 
   */
  private Collection<URI> checkpointDirs;
  private Collection<URI> checkpointEditsDirs;

  private Configuration conf;

  /**
   * Can fs-image be rolled?
   */
  volatile protected CheckpointStates ckptState = FSImage.CheckpointStates.START; 

  /**
   */
  FSImage() {
    this((FSNamesystem)null);
  }

  /**
   * Constructor
   * @param conf Configuration
   */
  FSImage(Configuration conf) throws IOException {
    this();
    this.conf = conf; // TODO we have too many constructors, this is a mess

    if(conf.getBoolean(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_KEY, 
        DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_DEFAULT)) {
      NameNode.LOG.info("set FSImage.restoreFailedStorage");
      storage.setRestoreFailedStorage(true);
    }
    setCheckpointDirectories(FSImage.getCheckpointDirs(conf, null),
        FSImage.getCheckpointEditsDirs(conf, null));
  }

  private FSImage(FSNamesystem ns) {
    this.conf = new Configuration();
    
    storage = new NNStorage(conf);
    if (ns != null) {
      storage.setUpgradeManager(ns.upgradeManager);
    }
    storage.registerListener(this);

    this.editLog = new FSEditLog(storage);
    setFSNamesystem(ns);
  }

  /**
   * @throws IOException 
   */
  FSImage(Collection<URI> fsDirs, Collection<URI> fsEditsDirs) 
      throws IOException {
    this();
    storage.setStorageDirectories(fsDirs, fsEditsDirs);
  }

  public FSImage(StorageInfo storageInfo, String bpid) {
    storage = new NNStorage(storageInfo, bpid);
  }

  /**
   * Represents an Image (image and edit file).
   * @throws IOException 
   */
  FSImage(URI imageDir) throws IOException {
    this();
    ArrayList<URI> dirs = new ArrayList<URI>(1);
    ArrayList<URI> editsDirs = new ArrayList<URI>(1);
    dirs.add(imageDir);
    editsDirs.add(imageDir);
    storage.setStorageDirectories(dirs, editsDirs);
  }
  
  protected FSNamesystem getFSNamesystem() {
    return namesystem;
  }

  void setFSNamesystem(FSNamesystem ns) {
    namesystem = ns;
    if (ns != null) {
      storage.setUpgradeManager(ns.upgradeManager);
    }
  }

  void setCheckpointDirectories(Collection<URI> dirs,
                                Collection<URI> editsDirs) {
    checkpointDirs = dirs;
    checkpointEditsDirs = editsDirs;
  }
  
  /**
   * Analyze storage directories.
   * Recover from previous transitions if required. 
   * Perform fs state transition if necessary depending on the namespace info.
   * Read storage info. 
   * 
   * @param dataDirs
   * @param startOpt startup option
   * @throws IOException
   * @return true if the image needs to be saved or false otherwise
   */
  boolean recoverTransitionRead(Collection<URI> dataDirs,
                                Collection<URI> editsDirs,
                                StartupOption startOpt)
      throws IOException {
    assert startOpt != StartupOption.FORMAT : 
      "NameNode formatting should be performed before reading the image";
    
    // none of the data dirs exist
    if((dataDirs.size() == 0 || editsDirs.size() == 0) 
                             && startOpt != StartupOption.IMPORT)  
      throw new IOException(
          "All specified directories are not accessible or do not exist.");
    
    if(startOpt == StartupOption.IMPORT 
        && (checkpointDirs == null || checkpointDirs.isEmpty()))
      throw new IOException("Cannot import image from a checkpoint. "
                            + "\"dfs.namenode.checkpoint.dir\" is not set." );

    if(startOpt == StartupOption.IMPORT 
        && (checkpointEditsDirs == null || checkpointEditsDirs.isEmpty()))
      throw new IOException("Cannot import image from a checkpoint. "
                            + "\"dfs.namenode.checkpoint.dir\" is not set." );
    
    storage.setStorageDirectories(dataDirs, editsDirs);
    // 1. For each data directory calculate its state and 
    // check whether all is consistent before transitioning.
    Map<StorageDirectory, StorageState> dataDirStates = 
             new HashMap<StorageDirectory, StorageState>();
    boolean isFormatted = false;
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      StorageState curState;
      try {
        curState = sd.analyzeStorage(startOpt);
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
          sd.read(); // read and verify consistency with other directories
          isFormatted = true;
        }
        if (startOpt == StartupOption.IMPORT && isFormatted)
          // import of a checkpoint is allowed only into empty image directories
          throw new IOException("Cannot import image from a checkpoint. " 
              + " NameNode already contains an image in "+ sd.getRoot());
      } catch (IOException ioe) {
        sd.unlock();
        throw ioe;
      }
      dataDirStates.put(sd,curState);
    }
    
    if (!isFormatted && startOpt != StartupOption.ROLLBACK 
                     && startOpt != StartupOption.IMPORT)
      throw new IOException("NameNode is not formatted.");
    int layoutVersion = storage.getLayoutVersion();
    if (layoutVersion < Storage.LAST_PRE_UPGRADE_LAYOUT_VERSION) {
      NNStorage.checkVersionUpgradable(storage.getLayoutVersion());
    }
    if (startOpt != StartupOption.UPGRADE
        && layoutVersion < Storage.LAST_PRE_UPGRADE_LAYOUT_VERSION
        && layoutVersion != FSConstants.LAYOUT_VERSION) {
      throw new IOException(
          "\nFile system image contains an old layout version " 
          + storage.getLayoutVersion() + ".\nAn upgrade to version "
          + FSConstants.LAYOUT_VERSION + " is required.\n"
          + "Please restart NameNode with -upgrade option.");
    }
    
    storage.processStartupOptionsForUpgrade(startOpt, layoutVersion);

    // check whether distributed upgrade is required and/or should be continued
    storage.verifyDistributedUpgradeProgress(startOpt);

    // 2. Format unformatted dirs.
    storage.setCheckpointTime(0L);
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
      doUpgrade();
      return false; // upgrade saved image already
    case IMPORT:
      doImportCheckpoint();
      return false; // import checkpoint saved image already
    case ROLLBACK:
      doRollback();
      break;
    case REGULAR:
      // just load the image
    }
    
    boolean needToSave = loadFSImage();

    assert editLog != null : "editLog must be initialized";
    if(!editLog.isOpen())
      editLog.open();
    
    return needToSave;
  }

  private void doUpgrade() throws IOException {
    if(storage.getDistributedUpgradeState()) {
      // only distributed upgrade need to continue
      // don't do version upgrade
      this.loadFSImage();
      storage.initializeDistributedUpgrade();
      return;
    }
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
    this.loadFSImage();

    // Do upgrade for each directory
    long oldCTime = storage.getCTime();
    storage.cTime = now();  // generate new cTime for the state
    int oldLV = storage.getLayoutVersion();
    storage.layoutVersion = FSConstants.LAYOUT_VERSION;
    storage.setCheckpointTime(now());
    
    List<StorageDirectory> errorSDs =
      Collections.synchronizedList(new ArrayList<StorageDirectory>());
    List<Thread> saveThreads = new ArrayList<Thread>();
    File curDir, prevDir, tmpDir;
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      LOG.info("Starting upgrade of image directory " + sd.getRoot()
               + ".\n   old LV = " + oldLV
               + "; old CTime = " + oldCTime
               + ".\n   new LV = " + storage.getLayoutVersion()
               + "; new CTime = " + storage.getCTime());
      try {
        curDir = sd.getCurrentDir();
        prevDir = sd.getPreviousDir();
        tmpDir = sd.getPreviousTmp();
        assert curDir.exists() : "Current directory must exist.";
        assert !prevDir.exists() : "prvious directory must not exist.";
        assert !tmpDir.exists() : "prvious.tmp directory must not exist.";
        assert !editLog.isOpen() : "Edits log must not be open.";

        // rename current to tmp
        NNStorage.rename(curDir, tmpDir);
        
        // launch thread to save new image
        FSImageSaver saver = new FSImageSaver(sd, errorSDs);
        Thread saveThread = new Thread(saver, saver.toString());
        saveThreads.add(saveThread);
        saveThread.start();
        
      } catch (Exception e) {
        LOG.error("Failed upgrade of image directory " + sd.getRoot(), e);
        errorSDs.add(sd);
        continue;
      }
    }
    waitForThreads(saveThreads);
    saveThreads.clear();

    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      if (errorSDs.contains(sd)) continue;
      try {
        prevDir = sd.getPreviousDir();
        tmpDir = sd.getPreviousTmp();
        // rename tmp to previous
        NNStorage.rename(tmpDir, prevDir);
      } catch (IOException ioe) {
        LOG.error("Unable to rename temp to previous for " + sd.getRoot(), ioe);
        errorSDs.add(sd);
        continue;
      }
      LOG.info("Upgrade of " + sd.getRoot() + " is complete.");
    }
    isUpgradeFinalized = false;
    if (!errorSDs.isEmpty()) {
      storage.reportErrorsOnDirectories(errorSDs);
      //during upgrade, it's a fatal error to fail any storage directory
      throw new IOException("Upgrade failed in " + errorSDs.size()
          + " storage directory(ies), previously logged.");
    }
    storage.initializeDistributedUpgrade();
    editLog.open();
  }

  private void doRollback() throws IOException {
    // Rollback is allowed only if there is 
    // a previous fs states in at least one of the storage directories.
    // Directories that don't have previous state do not rollback
    boolean canRollback = false;
    FSImage prevState = new FSImage(getFSNamesystem());
    prevState.getStorage().layoutVersion = FSConstants.LAYOUT_VERSION;
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      File prevDir = sd.getPreviousDir();
      if (!prevDir.exists()) {  // use current directory then
        LOG.info("Storage directory " + sd.getRoot()
                 + " does not contain previous fs state.");
        sd.read(); // read and verify consistency with other directories
        continue;
      }
      StorageDirectory sdPrev 
        = prevState.getStorage().new StorageDirectory(sd.getRoot());

      // read and verify consistency of the prev dir
      sdPrev.read(sdPrev.getPreviousVersionFile());
      if (prevState.getLayoutVersion() != FSConstants.LAYOUT_VERSION) {
        throw new IOException(
          "Cannot rollback to storage version " +
          prevState.getLayoutVersion() +
          " using this version of the NameNode, which uses storage version " +
          FSConstants.LAYOUT_VERSION + ". " +
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
    // check whether name-node can start in regular mode
    storage.verifyDistributedUpgradeProgress(StartupOption.REGULAR);
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
   * @throws IOException
   */
  void doImportCheckpoint() throws IOException {
    FSNamesystem fsNamesys = getFSNamesystem();
    FSImage ckptImage = new FSImage(fsNamesys);
    // replace real image with the checkpoint image
    FSImage realImage = fsNamesys.getFSImage();
    assert realImage == this;
    fsNamesys.dir.fsImage = ckptImage;
    // load from the checkpoint dirs
    try {
      ckptImage.recoverTransitionRead(checkpointDirs, checkpointEditsDirs,
                                              StartupOption.REGULAR);
    } finally {
      ckptImage.close();
    }
    // return back the real image
    realImage.getStorage().setStorageInfo(ckptImage.getStorage());
    storage.setCheckpointTime(ckptImage.getStorage().getCheckpointTime());
    fsNamesys.dir.fsImage = realImage;
    realImage.getStorage().setBlockPoolID(ckptImage.getBlockPoolID());
    // and save it but keep the same checkpointTime
    saveNamespace(false);
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

  //
  // Atomic move sequence, to recover from interrupted checkpoint
  //
  boolean recoverInterruptedCheckpoint(StorageDirectory nameSD,
                                       StorageDirectory editsSD) 
                                       throws IOException {
    boolean needToSave = false;
    File curFile = NNStorage.getStorageFile(nameSD, NameNodeFile.IMAGE);
    File ckptFile = NNStorage.getStorageFile(nameSD, NameNodeFile.IMAGE_NEW);

    //
    // If we were in the midst of a checkpoint
    //
    if (ckptFile.exists()) {
      needToSave = true;
      if (NNStorage.getStorageFile(editsSD, NameNodeFile.EDITS_NEW).exists()) {
        //
        // checkpointing migth have uploaded a new
        // merged image, but we discard it here because we are
        // not sure whether the entire merged image was uploaded
        // before the namenode crashed.
        //
        if (!ckptFile.delete()) {
          throw new IOException("Unable to delete " + ckptFile);
        }
      } else {
        //
        // checkpointing was in progress when the namenode
        // shutdown. The fsimage.ckpt was created and the edits.new
        // file was moved to edits. We complete that checkpoint by
        // moving fsimage.new to fsimage. There is no need to 
        // update the fstime file here. renameTo fails on Windows
        // if the destination file already exists.
        //
        if (!ckptFile.renameTo(curFile)) {
          if (!curFile.delete())
            LOG.warn("Unable to delete dir " + curFile + " before rename");
          if (!ckptFile.renameTo(curFile)) {
            throw new IOException("Unable to rename " + ckptFile +
                                  " to " + curFile);
          }
        }
      }
    }
    return needToSave;
  }

  /**
   * Choose latest image from one of the directories,
   * load it and merge with the edits from that directory.
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
  boolean loadFSImage() throws IOException {
    long latestNameCheckpointTime = Long.MIN_VALUE;
    long latestEditsCheckpointTime = Long.MIN_VALUE;
    boolean needToSave = false;
    isUpgradeFinalized = true;
    
    StorageDirectory latestNameSD = null;
    StorageDirectory latestEditsSD = null;
    
    Collection<String> imageDirs = new ArrayList<String>();
    Collection<String> editsDirs = new ArrayList<String>();
    
    // Set to determine if all of storageDirectories share the same checkpoint
    Set<Long> checkpointTimes = new HashSet<Long>();

    // Process each of the storage directories to find the pair of
    // newest image file and edit file
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();

      // Was the file just formatted?
      if (!sd.getVersionFile().exists()) {
        needToSave |= true;
        continue;
      }
      
      boolean imageExists = false;
      boolean editsExists = false;
      
      // Determine if sd is image, edits or both
      if (sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE)) {
        imageExists = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE).exists();
        imageDirs.add(sd.getRoot().getCanonicalPath());
      }
      
      if (sd.getStorageDirType().isOfType(NameNodeDirType.EDITS)) {
        editsExists = NNStorage.getStorageFile(sd, NameNodeFile.EDITS).exists();
        editsDirs.add(sd.getRoot().getCanonicalPath());
      }
      
      long checkpointTime = storage.readCheckpointTime(sd);

      checkpointTimes.add(checkpointTime);
      
      if (sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE) && 
         (latestNameCheckpointTime < checkpointTime) && imageExists) {
        latestNameCheckpointTime = checkpointTime;
        latestNameSD = sd;
      }
      
      if (sd.getStorageDirType().isOfType(NameNodeDirType.EDITS) && 
           (latestEditsCheckpointTime < checkpointTime) && editsExists) {
        latestEditsCheckpointTime = checkpointTime;
        latestEditsSD = sd;
      }
      
      // check that we have a valid, non-default checkpointTime
      if (checkpointTime <= 0L)
        needToSave |= true;
      
      // set finalized flag
      isUpgradeFinalized = isUpgradeFinalized && !sd.getPreviousDir().exists();
    }

    // We should have at least one image and one edits dirs
    if (latestNameSD == null)
      throw new IOException("Image file is not found in " + imageDirs);
    if (latestEditsSD == null)
      throw new IOException("Edits file is not found in " + editsDirs);

    // Make sure we are loading image and edits from same checkpoint
    if (latestNameCheckpointTime > latestEditsCheckpointTime
        && latestNameSD != latestEditsSD
        && latestNameSD.getStorageDirType() == NameNodeDirType.IMAGE
        && latestEditsSD.getStorageDirType() == NameNodeDirType.EDITS) {
      // This is a rare failure when NN has image-only and edits-only
      // storage directories, and fails right after saving images,
      // in some of the storage directories, but before purging edits.
      // See -NOTE- in saveNamespace().
      LOG.error("This is a rare failure scenario!!!");
      LOG.error("Image checkpoint time " + latestNameCheckpointTime +
                " > edits checkpoint time " + latestEditsCheckpointTime);
      LOG.error("Name-node will treat the image as the latest state of " +
                "the namespace. Old edits will be discarded.");
    } else if (latestNameCheckpointTime != latestEditsCheckpointTime)
      throw new IOException("Inconsistent storage detected, " +
                      "image and edits checkpoint times do not match. " +
                      "image checkpoint time = " + latestNameCheckpointTime +
                      "edits checkpoint time = " + latestEditsCheckpointTime);
    
    // If there was more than one checkpointTime recorded we should save
    needToSave |= checkpointTimes.size() != 1;
    
    // Recover from previous interrupted checkpoint, if any
    needToSave |= recoverInterruptedCheckpoint(latestNameSD, latestEditsSD);

    //
    // Load in bits
    //
    latestNameSD.read();
    needToSave |= loadFSImage(NNStorage.getStorageFile(latestNameSD,
                                                       NameNodeFile.IMAGE));
    
    // Load latest edits
    if (latestNameCheckpointTime > latestEditsCheckpointTime)
      // the image is already current, discard edits
      needToSave |= true;
    else // latestNameCheckpointTime == latestEditsCheckpointTime
      needToSave |= (loadFSEdits(latestEditsSD) > 0);
    
    return needToSave;
  }

  /**
   * Load in the filesystem image from file. It's a big list of
   * filenames and blocks.  Return whether we should
   * "re-save" and consolidate the edit-logs
   */
  boolean loadFSImage(File curFile) throws IOException {
    FSImageFormat.Loader loader = new FSImageFormat.Loader(
        conf, getFSNamesystem());
    loader.load(curFile);
    namesystem.setBlockPoolId(this.getBlockPoolID());

    // Check that the image digest we loaded matches up with what
    // we expected
    MD5Hash readImageMd5 = loader.getLoadedImageMd5();
    if (storage.getImageDigest() == null) {
      storage.setImageDigest(readImageMd5); // set this fsimage's checksum
    } else if (!storage.getImageDigest().equals(readImageMd5)) {
      throw new IOException("Image file " + curFile +
          " is corrupt with MD5 checksum of " + readImageMd5 +
          " but expecting " + storage.getImageDigest());
    }

    storage.namespaceID = loader.getLoadedNamespaceID();
    storage.layoutVersion = loader.getLoadedImageVersion();

    boolean needToSave =
      loader.getLoadedImageVersion() != FSConstants.LAYOUT_VERSION;
    return needToSave;
  }

  /**
   * Load and merge edits from two edits files
   * 
   * @param sd storage directory
   * @return number of edits loaded
   * @throws IOException
   */
  int loadFSEdits(StorageDirectory sd) throws IOException {
    FSEditLogLoader loader = new FSEditLogLoader(namesystem);
    
    int numEdits = 0;
    EditLogFileInputStream edits =
      new EditLogFileInputStream(NNStorage.getStorageFile(sd,
                                                          NameNodeFile.EDITS));
    
    numEdits = loader.loadFSEdits(edits);
    edits.close();
    File editsNew = NNStorage.getStorageFile(sd, NameNodeFile.EDITS_NEW);
    
    if (editsNew.exists() && editsNew.length() > 0) {
      edits = new EditLogFileInputStream(editsNew);
      numEdits += loader.loadFSEdits(edits);
      edits.close();
    }
    
    // update the counts.
    getFSNamesystem().dir.updateCountForINodeWithQuota();    
    
    return numEdits;
  }

  /**
   * Save the contents of the FS image to the file.
   */
  void saveFSImage(File newFile) throws IOException {
    FSImageFormat.Saver saver = new FSImageFormat.Saver();
    FSImageCompression compression = FSImageCompression.createCompression(conf);
    saver.save(newFile, getFSNamesystem(), compression);
    storage.setImageDigest(saver.getSavedDigest());
  }

  /**
   * FSImageSaver is being run in a separate thread when saving
   * FSImage. There is one thread per each copy of the image.
   *
   * FSImageSaver assumes that it was launched from a thread that holds
   * FSNamesystem lock and waits for the execution of FSImageSaver thread
   * to finish.
   * This way we are guraranteed that the namespace is not being updated
   * while multiple instances of FSImageSaver are traversing it
   * and writing it out.
   */
  private class FSImageSaver implements Runnable {
    private StorageDirectory sd;
    private List<StorageDirectory> errorSDs;
    
    FSImageSaver(StorageDirectory sd, List<StorageDirectory> errorSDs) {
      this.sd = sd;
      this.errorSDs = errorSDs;
    }
    
    public void run() {
      try {
        saveCurrent(sd);
      } catch (Throwable t) {
        LOG.error("Unable to save image for " + sd.getRoot(), t);
        errorSDs.add(sd);
      }
    }
    
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
          LOG.error("Caught exception while waiting for thread " +
                    thread.getName() + " to finish. Retrying join");
        }        
      }
    }
  }
  /**
   * Save the contents of the FS image and create empty edits.
   * 
   * In order to minimize the recovery effort in case of failure during
   * saveNamespace the algorithm reduces discrepancy between directory states
   * by performing updates in the following order:
   * <ol>
   * <li> rename current to lastcheckpoint.tmp for all of them,</li>
   * <li> save image and recreate edits for all of them,</li>
   * <li> rename lastcheckpoint.tmp to previous.checkpoint.</li>
   * </ol>
   * On stage (2) we first save all images, then recreate edits.
   * Otherwise the name-node may purge all edits and fail,
   * in which case the journal will be lost.
   */
  void saveNamespace(boolean renewCheckpointTime) throws IOException {
 
    // try to restore all failed edit logs here
    assert editLog != null : "editLog must be initialized";
    storage.attemptRestoreRemovedStorage();

    editLog.close();
    if(renewCheckpointTime)
      storage.setCheckpointTime(now());
    List<StorageDirectory> errorSDs =
      Collections.synchronizedList(new ArrayList<StorageDirectory>());

    // mv current -> lastcheckpoint.tmp
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      try {
        storage.moveCurrent(sd);
      } catch(IOException ie) {
        LOG.error("Unable to move current for " + sd.getRoot(), ie);
        errorSDs.add(sd);
      }
    }

    List<Thread> saveThreads = new ArrayList<Thread>();
    // save images into current
    for (Iterator<StorageDirectory> it
           = storage.dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      StorageDirectory sd = it.next();
      if (errorSDs.contains(sd)) {
        continue;
      }
      try {
        FSImageSaver saver = new FSImageSaver(sd, errorSDs);
        Thread saveThread = new Thread(saver, saver.toString());
        saveThreads.add(saveThread);
        saveThread.start();
      } catch (Exception e) {
        LOG.error("Failed save to image directory " + sd.getRoot(), e);
        errorSDs.add(sd);
        continue;
      }
    }
    waitForThreads(saveThreads);
    saveThreads.clear();

    // -NOTE-
    // If NN has image-only and edits-only storage directories and fails here
    // the image will have the latest namespace state.
    // During startup the image-only directories will recover by discarding
    // lastcheckpoint.tmp, while
    // the edits-only directories will recover by falling back
    // to the old state contained in their lastcheckpoint.tmp.
    // The edits directories should be discarded during startup because their
    // checkpointTime is older than that of image directories.
    // recreate edits in current
    for (Iterator<StorageDirectory> it
           = storage.dirIterator(NameNodeDirType.EDITS); it.hasNext();) {
      StorageDirectory sd = it.next();
      if (errorSDs.contains(sd)) {
        continue;
      }

      // if this directory already stores the image and edits, then it was
      // already processed in the earlier loop.
      if (sd.getStorageDirType() == NameNodeDirType.IMAGE_AND_EDITS) {
        continue;
      }

      try {
        FSImageSaver saver = new FSImageSaver(sd, errorSDs);
        Thread saveThread = new Thread(saver, saver.toString());
        saveThreads.add(saveThread);
        saveThread.start();
      } catch (Exception e) {
        LOG.error("Failed save to edits directory " + sd.getRoot(), e);
        errorSDs.add(sd);
        continue;
      }
    }
    waitForThreads(saveThreads);

    // mv lastcheckpoint.tmp -> previous.checkpoint
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      if (errorSDs.contains(sd)) {
        continue;
      }
      try {
        storage.moveLastCheckpoint(sd);
      } catch(IOException ie) {
        LOG.error("Unable to move last checkpoint for " + sd.getRoot(), ie);
        errorSDs.add(sd);
        continue;
      }
    }
    
    try {
      storage.reportErrorsOnDirectories(errorSDs);
      
      // If there was an error in every storage dir, each one will have been
      // removed from the list of storage directories.
      if (storage.getNumStorageDirs(NameNodeDirType.IMAGE) == 0 ||
          storage.getNumStorageDirs(NameNodeDirType.EDITS) == 0) {
        throw new IOException("Failed to save any storage directories while saving namespace");
      }
      
      if(!editLog.isOpen()) editLog.open();
    } finally {
      ckptState = CheckpointStates.UPLOAD_DONE;
    }
  }

  /**
   * Save current image and empty journal into {@code current} directory.
   */
  protected void saveCurrent(StorageDirectory sd) throws IOException {
    if (storage.getLayoutVersion() != FSConstants.LAYOUT_VERSION) {
      throw new IllegalStateException(
        "NN with storage version " + FSConstants.LAYOUT_VERSION  +
        "cannot save an image with version " + storage.getLayoutVersion());
    }
    File curDir = sd.getCurrentDir();
    NameNodeDirType dirType = (NameNodeDirType)sd.getStorageDirType();
    // save new image or new edits
    if (!curDir.exists() && !curDir.mkdir())
      throw new IOException("Cannot create directory " + curDir);
    if (dirType.isOfType(NameNodeDirType.IMAGE))
      saveFSImage(NNStorage.getStorageFile(sd, NameNodeFile.IMAGE));
    if (dirType.isOfType(NameNodeDirType.EDITS))
      editLog.createEditLogFile(NNStorage.getStorageFile(sd,
                                                         NameNodeFile.EDITS));
    // write version and time files
    sd.write();
  }


  /**
   * Moves fsimage.ckpt to fsImage and edits.new to edits
   * Reopens the new edits file.
   */
  void rollFSImage(CheckpointSignature sig, 
      boolean renewCheckpointTime) throws IOException {
    sig.validateStorageInfo(this);
    rollFSImage(true);
  }

  private void rollFSImage(boolean renewCheckpointTime)
  throws IOException {
    if (ckptState != CheckpointStates.UPLOAD_DONE
      && !(ckptState == CheckpointStates.ROLLED_EDITS
      && storage.getNumStorageDirs(NameNodeDirType.IMAGE) == 0)) {
      throw new IOException("Cannot roll fsImage before rolling edits log.");
    }

    for (Iterator<StorageDirectory> it 
           = storage.dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      StorageDirectory sd = it.next();
      File ckpt = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE_NEW);
      if (!ckpt.exists()) {
        throw new IOException("Checkpoint file " + ckpt +
                              " does not exist");
      }
    }
    editLog.purgeEditLog(); // renamed edits.new to edits
    if(LOG.isDebugEnabled()) {
      LOG.debug("rollFSImage after purgeEditLog: storageList=" 
                + storage.listStorageDirectories());
    }
    //
    // Renames new image
    //
    renameCheckpoint();
    resetVersion(renewCheckpointTime, newImageDigest);
  }

  /**
   * Renames new image
   */
  void renameCheckpoint() throws IOException {
    ArrayList<StorageDirectory> al = null;
    for (Iterator<StorageDirectory> it 
           = storage.dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      StorageDirectory sd = it.next();
      File ckpt = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE_NEW);
      File curFile = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE);
      // renameTo fails on Windows if the destination file 
      // already exists.
      if(LOG.isDebugEnabled()) {
        LOG.debug("renaming  " + ckpt.getAbsolutePath() 
                  + " to " + curFile.getAbsolutePath());
      }
      if (!ckpt.renameTo(curFile)) {
        if (!curFile.delete() || !ckpt.renameTo(curFile)) {
          LOG.warn("renaming  " + ckpt.getAbsolutePath() + " to "  + 
              curFile.getAbsolutePath() + " FAILED");

          if(al == null) al = new ArrayList<StorageDirectory> (1);
          al.add(sd);
        }
      }
    }
    if(al != null) storage.reportErrorsOnDirectories(al);
  }

  /**
   * Updates version and fstime files in all directories (fsimage and edits).
   */
  void resetVersion(boolean renewCheckpointTime, MD5Hash newImageDigest) 
      throws IOException {
    storage.layoutVersion = FSConstants.LAYOUT_VERSION;
    if(renewCheckpointTime)
      storage.setCheckpointTime(now());
    storage.setImageDigest(newImageDigest);
    
    ArrayList<StorageDirectory> al = null;
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      // delete old edits if sd is the image only the directory
      if (!sd.getStorageDirType().isOfType(NameNodeDirType.EDITS)) {
        File editsFile = NNStorage.getStorageFile(sd, NameNodeFile.EDITS);
        if(editsFile.exists() && !editsFile.delete())
          throw new IOException("Cannot delete edits file " 
                                + editsFile.getCanonicalPath());
      }
      // delete old fsimage if sd is the edits only the directory
      if (!sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE)) {
        File imageFile = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE);
        if(imageFile.exists() && !imageFile.delete())
          throw new IOException("Cannot delete image file " 
                                + imageFile.getCanonicalPath());
      }
      try {
        sd.write();
      } catch (IOException e) {
        LOG.error("Cannot write file " + sd.getRoot(), e);
        
        if(al == null) al = new ArrayList<StorageDirectory> (1);
        al.add(sd);       
      }
    }
    if(al != null) storage.reportErrorsOnDirectories(al);
    ckptState = FSImage.CheckpointStates.START;
  }

  CheckpointSignature rollEditLog() throws IOException {
    getEditLog().rollEditLog();
    ckptState = CheckpointStates.ROLLED_EDITS;
    // If checkpoint fails this should be the most recent image, therefore
    storage.incrementCheckpointTime();
    return new CheckpointSignature(this);
  }

  /**
   * This is called just before a new checkpoint is uploaded to the
   * namenode.
   */
  void validateCheckpointUpload(CheckpointSignature sig) throws IOException {
    if (ckptState != CheckpointStates.ROLLED_EDITS) {
      throw new IOException("Namenode is not expecting an new image " +
                             ckptState);
    } 
    // verify token
    long modtime = getEditLog().getFsEditTime();
    if (sig.editsTime != modtime) {
      throw new IOException("Namenode has an edit log with timestamp of " +
                            DATE_FORM.format(new Date(modtime)) +
                            " but new checkpoint was created using editlog " +
                            " with timestamp " + 
                            DATE_FORM.format(new Date(sig.editsTime)) + 
                            ". Checkpoint Aborted.");
    }
    sig.validateStorageInfo(this);
    ckptState = FSImage.CheckpointStates.UPLOAD_START;
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
            && bnReg.getCTime() > storage.getCTime())
        || (bnReg.getLayoutVersion() == storage.getLayoutVersion()
            && bnReg.getCTime() == storage.getCTime()
            && bnReg.getCheckpointTime() > storage.getCheckpointTime()))
      // remote node has newer image age
      msg = "Name node " + bnReg.getAddress()
            + " has newer image layout version: LV = " +bnReg.getLayoutVersion()
            + " cTime = " + bnReg.getCTime()
            + " checkpointTime = " + bnReg.getCheckpointTime()
            + ". Current version: LV = " + storage.getLayoutVersion()
            + " cTime = " + storage.getCTime()
            + " checkpointTime = " + storage.getCheckpointTime();
    if(msg != null) {
      LOG.error(msg);
      return new NamenodeCommand(NamenodeProtocol.ACT_SHUTDOWN);
    }
    boolean isImgObsolete = true;
    if(bnReg.getLayoutVersion() == storage.getLayoutVersion()
        && bnReg.getCTime() == storage.getCTime()
        && bnReg.getCheckpointTime() == storage.getCheckpointTime())
      isImgObsolete = false;
    boolean needToReturnImg = true;
    if(storage.getNumStorageDirs(NameNodeDirType.IMAGE) == 0)
      // do not return image if there are no image directories
      needToReturnImg = false;
    CheckpointSignature sig = rollEditLog();
    getEditLog().logJSpoolStart(bnReg, nnReg);
    return new CheckpointCommand(sig, isImgObsolete, needToReturnImg);
  }

  /**
   * End checkpoint.
   * <p>
   * Rename uploaded checkpoint to the new image;
   * purge old edits file;
   * rename edits.new to edits;
   * redirect edit log streams to the new edits;
   * update checkpoint time if the remote node is a checkpoint only node.
   * 
   * @param sig
   * @param remoteNNRole
   * @throws IOException
   */
  void endCheckpoint(CheckpointSignature sig,
                     NamenodeRole remoteNNRole) throws IOException {
    sig.validateStorageInfo(this);
    // Renew checkpoint time for the active if the other is a checkpoint-node.
    // The checkpoint-node should have older image for the next checkpoint 
    // to take effect.
    // The backup-node always has up-to-date image and will have the same
    // checkpoint time as the active node.
    boolean renewCheckpointTime = remoteNNRole.equals(NamenodeRole.CHECKPOINT);
    rollFSImage(sig, renewCheckpointTime);
  }

  CheckpointStates getCheckpointState() {
    return ckptState;
  }

  void setCheckpointState(CheckpointStates cs) {
    ckptState = cs;
  }

  /**
   * This is called when a checkpoint upload finishes successfully.
   */
  synchronized void checkpointUploadDone() {
    ckptState = CheckpointStates.UPLOAD_DONE;
  }

  synchronized public void close() throws IOException {
    getEditLog().close();
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
    Collection<String> dirNames = conf.getStringCollection(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY);
    if (dirNames.size() == 0 && defaultValue != null) {
      dirNames.add(defaultValue);
    }
    return Util.stringCollectionAsURIs(dirNames);
  }

  static Collection<URI> getCheckpointEditsDirs(Configuration conf,
      String defaultName) {
    Collection<String> dirNames = 
      conf.getStringCollection(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY);
    if (dirNames.size() == 0 && defaultName != null) {
      dirNames.add(defaultName);
    }
    return Util.stringCollectionAsURIs(dirNames);
  }

  public NNStorage getStorage() {
    return storage;
  }

  @Override // NNStorageListener
  public void errorOccurred(StorageDirectory sd) throws IOException {
    // do nothing,
  }

  @Override // NNStorageListener
  public void formatOccurred(StorageDirectory sd) throws IOException {
    if (sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE)) {
      sd.lock();
      try {
        saveCurrent(sd);
      } finally {
        sd.unlock();
      }
      LOG.info("Storage directory " + sd.getRoot()
               + " has been successfully formatted.");
    }
  };

  @Override // NNStorageListener
  public void directoryAvailable(StorageDirectory sd) throws IOException {
    // do nothing
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
}
