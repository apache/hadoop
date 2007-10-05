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
package org.apache.hadoop.dfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.lang.Math;

import org.apache.hadoop.dfs.FSConstants.StartupOption;
import org.apache.hadoop.dfs.FSConstants.NodeType;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.WritableComparable;

/**
 * FSImage handles checkpointing and logging of the namespace edits.
 * 
 */
class FSImage extends Storage {

  //
  // The filenames used for storing the images
  //
  enum NameNodeFile {
    IMAGE     ("fsimage"),
    TIME      ("fstime"),
    EDITS     ("edits"),
    IMAGE_NEW ("fsimage.ckpt"),
    EDITS_NEW ("edits.new");
    
    private String fileName = null;
    private NameNodeFile(String name) {this.fileName = name;}
    String getName() {return fileName;}
  }
  
  private long checkpointTime = -1L;
  private FSEditLog editLog = null;
  private boolean isUpgradeFinalized = false;

  /**
   */
  FSImage() {
    super(NodeType.NAME_NODE);
    this.editLog = new FSEditLog(this);
  }

  /**
   */
  FSImage(Collection<File> fsDirs) throws IOException {
    this();
    setStorageDirectories(fsDirs);
  }

  FSImage(StorageInfo storageInfo) {
    super(NodeType.NAME_NODE, storageInfo);
  }

  /**
   * Represents an Image (image and edit file).
   */
  FSImage(File imageDir) throws IOException {
    this();
    ArrayList<File> dirs = new ArrayList<File>(1);
    dirs.add(imageDir);
    setStorageDirectories(dirs);
  }
  
  void setStorageDirectories(Collection<File> fsDirs) throws IOException {
    this.storageDirs = new ArrayList<StorageDirectory>(fsDirs.size());
    for(Iterator<File> it = fsDirs.iterator(); it.hasNext();)
      this.addStorageDir(new StorageDirectory(it.next()));
  }

  /**
   */
  File getImageFile(int imageDirIdx, NameNodeFile type) {
    return getImageFile(getStorageDir(imageDirIdx), type);
  }
  
  static File getImageFile(StorageDirectory sd, NameNodeFile type) {
    return new File(sd.getCurrentDir(), type.getName());
  }
  
  File getEditFile(int idx) {
    return getImageFile(idx, NameNodeFile.EDITS);
  }
  
  File getEditNewFile(int idx) {
    return getImageFile(idx, NameNodeFile.EDITS_NEW);
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
   */
  void recoverTransitionRead(Collection<File> dataDirs,
                             StartupOption startOpt
                             ) throws IOException {
    assert startOpt != StartupOption.FORMAT : 
      "NameNode formatting should be performed before reading the image";
    // 1. For each data directory calculate its state and 
    // check whether all is consistent before transitioning.
    this.storageDirs = new ArrayList<StorageDirectory>(dataDirs.size());
    AbstractList<StorageState> dataDirStates = 
      new ArrayList<StorageState>(dataDirs.size());
    boolean isFormatted = false;
    for(Iterator<File> it = dataDirs.iterator(); it.hasNext();) {
      File dataDir = it.next();
      StorageDirectory sd = new StorageDirectory(dataDir);
      StorageState curState;
      try {
        curState = sd.analyzeStorage(startOpt);
        // sd is locked but not opened
        switch(curState) {
        case NON_EXISTENT:
          // name-node fails if any of the configured storage dirs are missing
          throw new InconsistentFSStateException(sd.root,
                                                 "storage directory does not exist or is not accessible.");
        case NOT_FORMATTED:
          break;
        case CONVERT:
          if (convertLayout(sd)) // need to reformat empty image
            curState = StorageState.NOT_FORMATTED;
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
      } catch (IOException ioe) {
        sd.unlock();
        throw ioe;
      }
      // add to the storage list
      addStorageDir(sd);
      dataDirStates.add(curState);
    }

    if (dataDirs.size() == 0)  // none of the data dirs exist
      throw new IOException(
                            "All specified directories are not accessible or do not exist.");
    if (!isFormatted && startOpt != StartupOption.ROLLBACK)
      throw new IOException("NameNode is not formatted.");
    if (startOpt != StartupOption.UPGRADE
          && layoutVersion < LAST_PRE_UPGRADE_LAYOUT_VERSION
          && layoutVersion != FSConstants.LAYOUT_VERSION)
        throw new IOException(
                          "\nFile system image contains an old layout version " + layoutVersion
                          + ".\nAn upgrade to version " + FSConstants.LAYOUT_VERSION
                          + " is required.\nPlease restart NameNode with -upgrade option.");
    // check whether distributed upgrade is reguired and/or should be continued
    verifyDistributedUpgradeProgress(startOpt);

    // 2. Format unformatted dirs.
    this.checkpointTime = 0L;
    for(int idx = 0; idx < getNumStorageDirs(); idx++) {
      StorageDirectory sd = getStorageDir(idx);
      StorageState curState = dataDirStates.get(idx);
      switch(curState) {
      case NON_EXISTENT:
        assert false : StorageState.NON_EXISTENT + " state cannot be here";
      case NOT_FORMATTED:
        LOG.info("Storage directory " + sd.root + " is not formatted.");
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
      break;
    case ROLLBACK:
      doRollback();
      // and now load that image
    case REGULAR:
      if (loadFSImage())
        saveFSImage();
    }
    assert editLog != null : "editLog must be initialized";
    if(!editLog.isOpen())
      editLog.open();
  }

  private void doUpgrade() throws IOException {
    if(getDistributedUpgradeState()) {
      // only distributed upgrade need to continue
      // don't do version upgrade
      this.loadFSImage();
      initializeDistributedUpgrade();
      return;
    }
    // Upgrade is allowed only if there are 
    // no previous fs states in any of the directories
    for(int idx = 0; idx < getNumStorageDirs(); idx++) {
      StorageDirectory sd = getStorageDir(idx);
      if (sd.getPreviousDir().exists())
        throw new InconsistentFSStateException(sd.root,
                                               "previous fs state should not exist during upgrade. "
                                               + "Finalize or rollback first.");
    }

    // load the latest image
    this.loadFSImage();

    // Do upgrade for each directory
    long oldCTime = this.getCTime();
    this.cTime = FSNamesystem.now();  // generate new cTime for the state
    int oldLV = this.getLayoutVersion();
    this.layoutVersion = FSConstants.LAYOUT_VERSION;
    this.checkpointTime = FSNamesystem.now();
    for(int idx = 0; idx < getNumStorageDirs(); idx++) {
      StorageDirectory sd = getStorageDir(idx);
      LOG.info("Upgrading image directory " + sd.root 
               + ".\n   old LV = " + oldLV
               + "; old CTime = " + oldCTime
               + ".\n   new LV = " + this.getLayoutVersion()
               + "; new CTime = " + this.getCTime());
      File curDir = sd.getCurrentDir();
      File prevDir = sd.getPreviousDir();
      File tmpDir = sd.getPreviousTmp();
      assert curDir.exists() : "Current directory must exist.";
      assert !prevDir.exists() : "prvious directory must not exist.";
      assert !tmpDir.exists() : "prvious.tmp directory must not exist.";
      // rename current to tmp
      rename(curDir, tmpDir);
      // save new image
      if (!curDir.mkdir())
        throw new IOException("Cannot create directory " + curDir);
      saveFSImage(getImageFile(sd, NameNodeFile.IMAGE));
      editLog.createEditLogFile(getImageFile(sd, NameNodeFile.EDITS));
      // write version and time files
      sd.write();
      // rename tmp to previous
      rename(tmpDir, prevDir);
      isUpgradeFinalized = false;
      LOG.info("Upgrade of " + sd.root + " is complete.");
    }
    initializeDistributedUpgrade();
    editLog.open();
  }

  private void doRollback() throws IOException {
    // Rollback is allowed only if there is 
    // a previous fs states in at least one of the storage directories.
    // Directories that don't have previous state do not rollback
    boolean canRollback = false;
    FSImage prevState = new FSImage();
    prevState.layoutVersion = FSConstants.LAYOUT_VERSION;
    for(int idx = 0; idx < getNumStorageDirs(); idx++) {
      StorageDirectory sd = getStorageDir(idx);
      File prevDir = sd.getPreviousDir();
      if (!prevDir.exists()) {  // use current directory then
        LOG.info("Storage directory " + sd.root
                 + " does not contain previous fs state.");
        sd.read(); // read and verify consistency with other directories
        continue;
      }
      StorageDirectory sdPrev = prevState.new StorageDirectory(sd.root);
      sdPrev.read(sdPrev.getPreviousVersionFile());  // read and verify consistency of the prev dir
      canRollback = true;
    }
    if (!canRollback)
      throw new IOException("Cannot rollback. " 
                            + "None of the storage directories contain previous fs state.");

    // Now that we know all directories are going to be consistent
    // Do rollback for each directory containing previous state
    for(int idx = 0; idx < getNumStorageDirs(); idx++) {
      StorageDirectory sd = getStorageDir(idx);
      File prevDir = sd.getPreviousDir();
      if (!prevDir.exists())
        continue;

      LOG.info("Rolling back storage directory " + sd.root 
               + ".\n   new LV = " + prevState.getLayoutVersion()
               + "; new CTime = " + prevState.getCTime());
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
      LOG.info("Rollback of " + sd.root + " is complete.");
    }
    isUpgradeFinalized = true;
    // check whether name-node can start in regular mode
    verifyDistributedUpgradeProgress(StartupOption.REGULAR);
  }

  private void doFinalize(StorageDirectory sd) throws IOException {
    File prevDir = sd.getPreviousDir();
    if (!prevDir.exists())
      return; // already discarded
    LOG.info("Finalizing upgrade for storage directory " 
             + sd.root 
             + ".\n   cur LV = " + this.getLayoutVersion()
             + "; cur CTime = " + this.getCTime());
    assert sd.getCurrentDir().exists() : "Current directory must exist.";
    final File tmpDir = sd.getFinalizedTmp();
    // rename previous to tmp and remove
    rename(prevDir, tmpDir);
    deleteDir(tmpDir);
    isUpgradeFinalized = true;
    LOG.info("Finalize upgrade for " + sd.root + " is complete.");
  }

  void finalizeUpgrade() throws IOException {
    for(int idx = 0; idx < getNumStorageDirs(); idx++)
      doFinalize(getStorageDir(idx));
  }

  boolean isUpgradeFinalized() {
    return isUpgradeFinalized;
  }

  protected void getFields(Properties props, 
                           StorageDirectory sd 
                           ) throws IOException {
    super.getFields(props, sd);
    if (layoutVersion == 0)
      throw new IOException("NameNode directory " 
                            + sd.root + " is not formatted.");
    String sDUS, sDUV;
    sDUS = props.getProperty("distributedUpgradeState"); 
    sDUV = props.getProperty("distributedUpgradeVersion");
    setDistributedUpgradeState(
        sDUS == null? false : Boolean.parseBoolean(sDUS),
        sDUV == null? getLayoutVersion() : Integer.parseInt(sDUV));
    this.checkpointTime = readCheckpointTime(sd);
  }

  long readCheckpointTime(StorageDirectory sd) throws IOException {
    File timeFile = getImageFile(sd, NameNodeFile.TIME);
    long timeStamp = 0L;
    if (timeFile.exists() && timeFile.canRead()) {
      DataInputStream in = new DataInputStream(new FileInputStream(timeFile));
      try {
        timeStamp = in.readLong();
      } finally {
        in.close();
      }
    }
    return timeStamp;
  }

  /**
   * Write last checkpoint time and version file into the storage directory.
   * 
   * The version file should always be written last.
   * Missing or corrupted version file indicates that 
   * the checkpoint is not valid.
   * 
   * @param sd storage directory
   * @throws IOException
   */
  protected void setFields(Properties props, 
                           StorageDirectory sd 
                           ) throws IOException {
    super.setFields(props, sd);
    boolean uState = getDistributedUpgradeState();
    int uVersion = getDistributedUpgradeVersion();
    if(uState && uVersion != getLayoutVersion()) {
      props.setProperty("distributedUpgradeState", Boolean.toString(uState));
      props.setProperty("distributedUpgradeVersion", Integer.toString(uVersion)); 
    }
    writeCheckpointTime(sd);
  }

  /**
   * Write last checkpoint time into a separate file.
   * 
   * @param sd
   * @throws IOException
   */
  void writeCheckpointTime(StorageDirectory sd) throws IOException {
    if (checkpointTime < 0L)
      return; // do not write negative time
    File timeFile = getImageFile(sd, NameNodeFile.TIME);
    if (timeFile.exists()) { timeFile.delete(); }
    DataOutputStream out = new DataOutputStream(
                                                new FileOutputStream(timeFile));
    try {
      out.writeLong(checkpointTime);
    } finally {
      out.close();
    }
  }

  /**
   * If there is an IO Error on any log operations, remove that
   * directory from the list of directories. If no more directories
   * remain, then raise an exception that will possibly cause the
   * server to exit
   */
  void processIOError(int index) throws IOException {
    int nrDirs = getNumStorageDirs();
    assert(index >= 0 && index < nrDirs);
    if (nrDirs <= 1)
      throw new IOException("Checkpoint directories inaccessible.");
    storageDirs.remove(index);
  }

  FSEditLog getEditLog() {
    return editLog;
  }

  boolean isConversionNeeded(StorageDirectory sd) throws IOException {
    File oldImageDir = new File(sd.root, "image");
    if (!oldImageDir.exists())
      throw new InconsistentFSStateException(sd.root,
          oldImageDir + " does not exist.");
    // check the layout version inside the image file
    File oldF = new File(oldImageDir, "fsimage");
    RandomAccessFile oldFile = new RandomAccessFile(oldF, "rws");
    try {
      oldFile.seek(0);
      int odlVersion = oldFile.readInt();
      if (odlVersion < LAST_PRE_UPGRADE_LAYOUT_VERSION)
        return false;
    } finally {
      oldFile.close();
    }
    // check consistency of the old storage
    if (!oldImageDir.isDirectory())
      throw new InconsistentFSStateException(sd.root,
                                             oldImageDir + " is not a directory.");
    if (!oldImageDir.canWrite())
      throw new InconsistentFSStateException(sd.root,
                                             oldImageDir + " is not writable.");
    return true;
  }
  
  private boolean convertLayout(StorageDirectory sd) throws IOException {
    assert FSConstants.LAYOUT_VERSION < LAST_PRE_UPGRADE_LAYOUT_VERSION :
      "Bad current layout version: FSConstants.LAYOUT_VERSION should decrease";
    File oldImageDir = new File(sd.root, "image");
    assert oldImageDir.exists() : "Old image directory is missing";
    File oldImage = new File(oldImageDir, "fsimage");
    
    LOG.info("Old layout version directory " + oldImageDir
             + " is found. New layout version is "
             + FSConstants.LAYOUT_VERSION);
    LOG.info("Trying to convert ...");

    // we did not use locking for the pre upgrade layout, so we cannot prevent 
    // old name-nodes from running in the same directory as the new ones

    // check new storage
    File newImageDir = sd.getCurrentDir();
    File versionF = sd.getVersionFile();
    if (versionF.exists())
      throw new IOException("Version file already exists: " + versionF);
    if (newImageDir.exists()) // // somebody created current dir manually
      deleteDir(newImageDir);

    // move old image files into new location
    rename(oldImageDir, newImageDir);
    File oldEdits1 = new File(sd.root, "edits");
    // move old edits into data
    if (oldEdits1.exists())
      rename(oldEdits1, getImageFile(sd, NameNodeFile.EDITS));
    File oldEdits2 = new File(sd.root, "edits.new");
    if (oldEdits2.exists())
      rename(oldEdits2, getImageFile(sd, NameNodeFile.EDITS_NEW));

    // Write new layout with 
    // setting layoutVersion = LAST_PRE_UPGRADE_LAYOUT_VERSION
    // means the actual version should be obtained from the image file
    this.layoutVersion = LAST_PRE_UPGRADE_LAYOUT_VERSION;
    File newImageFile = getImageFile(sd, NameNodeFile.IMAGE);
    boolean needReformat = false;
    if (!newImageFile.exists()) {
      // in pre upgrade versions image file was allowed not to exist
      // we treat it as non formatted then
      LOG.info("Old image file " + oldImage + " does not exist. ");
      needReformat = true;
    } else {
      sd.write();
    }
    LOG.info("Conversion of " + oldImage + " is complete.");
    return needReformat;
  }

  //
  // Atomic move sequence, to recover from interrupted checkpoint
  //
  void recoverInterruptedCheckpoint(StorageDirectory sd) throws IOException {
    File curFile = getImageFile(sd, NameNodeFile.IMAGE);
    File ckptFile = getImageFile(sd, NameNodeFile.IMAGE_NEW);

    //
    // If we were in the midst of a checkpoint
    //
    if (ckptFile.exists()) {
      if (getImageFile(sd, NameNodeFile.EDITS_NEW).exists()) {
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
          curFile.delete();
          if (!ckptFile.renameTo(curFile)) {
            throw new IOException("Unable to rename " + ckptFile +
                                  " to " + curFile);
          }
        }
      }
    }
  }

  /**
   * Choose latest image from one of the directories,
   * load it and merge with the edits from that directory.
   * 
   * @return whether the image should be saved
   * @throws IOException
   */
  boolean loadFSImage() throws IOException {
    // Now check all curFiles and see which is the newest
    long latestCheckpointTime = Long.MIN_VALUE;
    StorageDirectory latestSD = null;
    boolean needToSave = false;
    isUpgradeFinalized = true;
    for (int idx = 0; idx < getNumStorageDirs(); idx++) {
      StorageDirectory sd = getStorageDir(idx);
      recoverInterruptedCheckpoint(sd);
      if (!sd.getVersionFile().exists()) {
        needToSave |= true;
        continue; // some of them might have just been formatted
      }
      assert getImageFile(sd, NameNodeFile.IMAGE).exists() :
        "Image file must exist.";
      checkpointTime = readCheckpointTime(sd);
      if (latestCheckpointTime < checkpointTime) {
        latestCheckpointTime = checkpointTime;
        latestSD = sd;
      }
      if (checkpointTime <= 0L)
        needToSave |= true;
      // set finalized flag
      isUpgradeFinalized = isUpgradeFinalized && !sd.getPreviousDir().exists();
    }
    assert latestSD != null : "Latest storage directory was not determined.";

    //
    // Load in bits
    //
    latestSD.read();
    needToSave |= loadFSImage(getImageFile(latestSD, NameNodeFile.IMAGE));

    //
    // read in the editlog from the same directory from
    // which we read in the image
    //
    needToSave |= (loadFSEdits(latestSD) > 0);
    
    return needToSave;
  }

  /**
   * Load in the filesystem imagefrom file. It's a big list of
   * filenames and blocks.  Return whether we should
   * "re-save" and consolidate the edit-logs
   */
  boolean loadFSImage(File curFile) throws IOException {
    assert this.getLayoutVersion() < 0 : "Negative layout version is expected.";
    assert curFile != null : "curFile is null";

    FSNamesystem fsNamesys = FSNamesystem.getFSNamesystem();
    FSDirectory fsDir = fsNamesys.dir;

    //
    // Load in bits
    //
    boolean needToSave = true;
    DataInputStream in = new DataInputStream(
                                             new BufferedInputStream(
                                                                     new FileInputStream(curFile)));
    try {
      /*
       * TODO we need to change format of the image file
       * it should not contain version and namespace fields
       */
      // read image version: first appeared in version -1
      int imgVersion = in.readInt();
      // read namespaceID: first appeared in version -2
      if (imgVersion <= -2)
        this.namespaceID = in.readInt();
      // read number of files
      int numFiles = 0;
      // version 0 does not store version #
      // starts directly with the number of files
      if (imgVersion >= 0) {
        numFiles = imgVersion;
        imgVersion = 0;
      } else {
        numFiles = in.readInt();
      }
      this.layoutVersion = imgVersion;

      needToSave = (imgVersion != FSConstants.LAYOUT_VERSION);

      // read file info
      short replication = FSNamesystem.getFSNamesystem().getDefaultReplication();
      for (int i = 0; i < numFiles; i++) {
        UTF8 name = new UTF8();
        long modificationTime = 0;
        long blockSize = 0;
        name.readFields(in);
        // version 0 does not support per file replication
        if (!(imgVersion >= 0)) {
          replication = in.readShort(); // other versions do
          replication = FSEditLog.adjustReplication(replication);
        }
        if (imgVersion <= -5) {
          modificationTime = in.readLong();
        }
        if (imgVersion <= -8) {
          blockSize = in.readLong();
        }
        int numBlocks = in.readInt();
        Block blocks[] = null;

        // for older versions, a blocklist of size 0
        // indicates a directory.
        if ((-9 <= imgVersion && numBlocks > 0) ||
            (imgVersion < -9 && numBlocks >= 0)) {
          blocks = new Block[numBlocks];
          for (int j = 0; j < numBlocks; j++) {
            blocks[j] = new Block();
            blocks[j].readFields(in);
          }
        }
        // Older versions of HDFS does not store the block size in inode.
        // If the file has more than one block, use the size of the 
        // first block as the blocksize. Otherwise use the default block size.
        //
        if (-8 <= imgVersion && blockSize == 0) {
          if (numBlocks > 1) {
            blockSize = blocks[0].getNumBytes();
          } else {
            long first = ((numBlocks == 1) ? blocks[0].getNumBytes(): 0);
            blockSize = Math.max(fsNamesys.getDefaultBlockSize(), first);
          }
        }
        fsDir.unprotectedAddFile(name.toString(), blocks, replication,
                                 modificationTime, blockSize);
      }
      
      // load datanode info
      this.loadDatanodes(imgVersion, in);
    } finally {
      in.close();
    }
    
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
    int numEdits = 0;
    numEdits = editLog.loadFSEdits(getImageFile(sd, NameNodeFile.EDITS));
    File editsNew = getImageFile(sd, NameNodeFile.EDITS_NEW);
    if (editsNew.exists()) 
      numEdits += editLog.loadFSEdits(editsNew);
    return numEdits;
  }

  /**
   * Save the contents of the FS image to the file.
   */
  void saveFSImage(File newFile ) throws IOException {
    FSNamesystem fsNamesys = FSNamesystem.getFSNamesystem();
    FSDirectory fsDir = fsNamesys.dir;
    //
    // Write out data
    //
    DataOutputStream out = new DataOutputStream(
                                                new BufferedOutputStream(
                                                                         new FileOutputStream(newFile)));
    try {
      out.writeInt(FSConstants.LAYOUT_VERSION);
      out.writeInt(namespaceID);
      out.writeInt(fsDir.rootDir.numItemsInTree() - 1);
      saveImage("", fsDir.rootDir, out);
      saveDatanodes(out);
    } finally {
      out.close();
    }
  }

  /**
   * Save the contents of the FS image
   * and create empty edits.
   */
  void saveFSImage() throws IOException {
    editLog.createNewIfMissing();
    for (int idx = 0; idx < getNumStorageDirs(); idx++) {
      StorageDirectory sd = getStorageDir(idx);
      saveFSImage(getImageFile(sd, NameNodeFile.IMAGE_NEW));
      editLog.createEditLogFile(getImageFile(sd, NameNodeFile.EDITS));
      File editsNew = getImageFile(sd, NameNodeFile.EDITS_NEW);
      if (editsNew.exists()) 
        editLog.createEditLogFile(editsNew);
    }
    rollFSImage();
  }

  /**
   * Generate new namespaceID.
   * 
   * namespaceID is a persistent attribute of the namespace.
   * It is generated when the namenode is formatted and remains the same
   * during the life cycle of the namenode.
   * When a datanodes register they receive it as the registrationID,
   * which is checked every time the datanode is communicating with the 
   * namenode. Datanodes that do not 'know' the namespaceID are rejected.
   * 
   * @return new namespaceID
   */
  private int newNamespaceID() {
    Random r = new Random();
    r.setSeed(FSNamesystem.now());
    int newID = 0;
    while(newID == 0)
      newID = r.nextInt(0x7FFFFFFF);  // use 31 bits only
    return newID;
  }

  /** Create new dfs name directory.  Caution: this destroys all files
   * in this filesystem. */
  void format(StorageDirectory sd) throws IOException {
    sd.clearDirectory(); // create currrent dir
    sd.lock();
    try {
      saveFSImage(getImageFile(sd, NameNodeFile.IMAGE));
      editLog.createEditLogFile(getImageFile(sd, NameNodeFile.EDITS));
      sd.write();
    } finally {
      sd.unlock();
    }
    LOG.info("Storage directory " + sd.root 
             + " has been successfully formatted.");
  }

  public void format() throws IOException {
    this.layoutVersion = FSConstants.LAYOUT_VERSION;
    this.namespaceID = newNamespaceID();
    this.cTime = 0L;
    this.checkpointTime = FSNamesystem.now();
    for(int idx = 0; idx < getNumStorageDirs(); idx++) {
      StorageDirectory sd = getStorageDir(idx);
      format(sd);
    }
  }

  /**
   * Save file tree image starting from the given root.
   */
  private static void saveImage(String parentPrefix, 
                                INode inode, 
                                DataOutputStream out) throws IOException {
    String fullName = "";
    if (inode.getParent() != null) {
      fullName = parentPrefix + "/" + inode.getLocalName();
      new UTF8(fullName).write(out);
      if (!inode.isDirectory()) {  // write file inode
        INodeFile fileINode = (INodeFile)inode;
        out.writeShort(fileINode.getReplication());
        out.writeLong(inode.getModificationTime());
        out.writeLong(fileINode.getPreferredBlockSize());
        Block[] blocks = fileINode.getBlocks();
        out.writeInt(blocks.length);
        for (Block blk : blocks)
          blk.write(out);
        return;
      }
      // write directory inode
      out.writeShort(0);  // replication
      out.writeLong(inode.getModificationTime());
      out.writeLong(0);   // preferred block size
      out.writeInt(-1);    // # of blocks
    }
    for(INode child : ((INodeDirectory)inode).getChildren()) {
      saveImage(fullName, child, out);
    }
  }

  /**
   * Earlier version used to store all the known datanodes.
   * DFS don't store datanodes anymore.
   * 
   * @param out output stream
   * @throws IOException
   */
  void saveDatanodes(DataOutputStream out) throws IOException {
    // we don't store datanodes anymore.
    out.writeInt(0);    
  }

  void loadDatanodes(int version, DataInputStream in) throws IOException {
    if (version > -3) // pre datanode image version
      return;
    int size = in.readInt();
    for(int i = 0; i < size; i++) {
      DatanodeImage nodeImage = new DatanodeImage();
      nodeImage.readFields(in);
      // We don't need to add these descriptors any more.
    }
  }

  /**
   * Moves fsimage.ckpt to fsImage and edits.new to edits
   * Reopens the new edits file.
   */
  void rollFSImage() throws IOException {
    //
    // First, verify that edits.new and fsimage.ckpt exists in all
    // checkpoint directories.
    //
    if (!editLog.existsNew()) {
      throw new IOException("New Edits file does not exist");
    }
    for(int idx = 0; idx < getNumStorageDirs(); idx++) {
      StorageDirectory sd = getStorageDir(idx);
      File ckpt = getImageFile(sd, NameNodeFile.IMAGE_NEW);
      if (!ckpt.exists()) {
        throw new IOException("Checkpoint file " + ckpt +
                              " does not exist");
      }
    }
    editLog.purgeEditLog(); // renamed edits.new to edits

    //
    // Renames new image
    //
    for(int idx = 0; idx < getNumStorageDirs(); idx++) {
      StorageDirectory sd = getStorageDir(idx);
      File ckpt = getImageFile(sd, NameNodeFile.IMAGE_NEW);
      File curFile = getImageFile(sd, NameNodeFile.IMAGE);
      // renameTo fails on Windows if the destination file 
      // already exists.
      if (!ckpt.renameTo(curFile)) {
        curFile.delete();
        if (!ckpt.renameTo(curFile)) {
          editLog.processIOError(idx);
          idx--;
        }
      }
    }

    //
    // Updates the fstime file and write version file
    //
    this.layoutVersion = FSConstants.LAYOUT_VERSION;
    this.checkpointTime = FSNamesystem.now();
    for(int idx = 0; idx < getNumStorageDirs(); idx++) {
      StorageDirectory sd = getStorageDir(idx);
      try {
        sd.write();
      } catch (IOException e) {
        LOG.error("Cannot write file " + sd.root, e);
        editLog.processIOError(idx);
        idx--;
      }
    }
  }

  void close() throws IOException {
    getEditLog().close();
    unlockAll();
  }

  /**
   * Return the name of the image file.
   */
  File getFsImageName() {
    return getImageFile(0, NameNodeFile.IMAGE);
  }

  /**
   * Return the name of the image file that is uploaded by periodic
   * checkpointing.
   */
  File[] getFsImageNameCheckpoint() {
    File[] list = new File[getNumStorageDirs()];
    for(int i = 0; i < getNumStorageDirs(); i++) {
      list[i] = getImageFile(getStorageDir(i), NameNodeFile.IMAGE_NEW);
    }
    return list;
  }

  /**
   * DatanodeImage is used to store persistent information
   * about datanodes into the fsImage.
   */
  static class DatanodeImage implements WritableComparable {
    DatanodeDescriptor              node;

    DatanodeImage() {
      node = new DatanodeDescriptor();
    }

    DatanodeImage(DatanodeDescriptor from) {
      node = from;
    }

    /** 
     * Returns the underlying Datanode Descriptor
     */
    DatanodeDescriptor getDatanodeDescriptor() { 
      return node; 
    }

    public int compareTo(Object o) {
      return node.compareTo(o);
    }

    public boolean equals(Object o) {
      return node.equals(o);
    }

    public int hashCode() {
      return node.hashCode();
    }

    /////////////////////////////////////////////////
    // Writable
    /////////////////////////////////////////////////
    /**
     * Public method that serializes the information about a
     * Datanode to be stored in the fsImage.
     */
    public void write(DataOutput out) throws IOException {
      DatanodeID id = new DatanodeID(node.getName(), node.getStorageID(),
                                     node.getInfoPort());
      id.write(out);
      out.writeLong(node.getCapacity());
      out.writeLong(node.getRemaining());
      out.writeLong(node.getLastUpdate());
      out.writeInt(node.getXceiverCount());
    }

    /**
     * Public method that reads a serialized Datanode
     * from the fsImage.
     */
    public void readFields(DataInput in) throws IOException {
      DatanodeID id = new DatanodeID();
      id.readFields(in);
      long capacity = in.readLong();
      long remaining = in.readLong();
      long lastUpdate = in.readLong();
      int xceiverCount = in.readInt();

      // update the DatanodeDescriptor with the data we read in
      node.updateRegInfo(id);
      node.setStorageID(id.getStorageID());
      node.setCapacity(capacity);
      node.setRemaining(remaining);
      node.setLastUpdate(lastUpdate);
      node.setXceiverCount(xceiverCount);
    }
  }

  protected void corruptPreUpgradeStorage(File rootDir) throws IOException {
    File oldImageDir = new File(rootDir, "image");
    if (!oldImageDir.exists())
      if (!oldImageDir.mkdir())
        throw new IOException("Cannot create directory " + oldImageDir);
    File oldImage = new File(oldImageDir, "fsimage");
    if (!oldImage.exists())
      // recreate old image file to let pre-upgrade versions fail
      if (!oldImage.createNewFile())
        throw new IOException("Cannot create file " + oldImage);
    RandomAccessFile oldFile = new RandomAccessFile(oldImage, "rws");
    // write new version into old image file
    try {
      writeCorruptedData(oldFile);
    } finally {
      oldFile.close();
    }
  }

  private boolean getDistributedUpgradeState() {
    return FSNamesystem.getFSNamesystem().getDistributedUpgradeState();
  }

  private int getDistributedUpgradeVersion() {
    return FSNamesystem.getFSNamesystem().getDistributedUpgradeVersion();
  }

  private void setDistributedUpgradeState(boolean uState, int uVersion) {
    FSNamesystem.getFSNamesystem().upgradeManager.setUpgradeState(uState, uVersion);
  }

  private void verifyDistributedUpgradeProgress(StartupOption startOpt
                                                ) throws IOException {
    if(startOpt == StartupOption.ROLLBACK)
      return;
    UpgradeManager um = FSNamesystem.getFSNamesystem().upgradeManager;
    assert um != null : "FSNameSystem.upgradeManager is null.";
    if(startOpt != StartupOption.UPGRADE) {
      if(um.getUpgradeState())
        throw new IOException(
                    "\n   Previous distributed upgrade was not completed. "
                  + "\n   Please restart NameNode with -upgrade option.");
      if(um.getDistributedUpgrades() != null)
        throw new IOException("\n   Distributed upgrade for NameNode version " 
          + um.getUpgradeVersion() + " to current LV " + FSConstants.LAYOUT_VERSION
          + " is required.\n   Please restart NameNode with -upgrade option.");
    }
  }

  private void initializeDistributedUpgrade() throws IOException {
    UpgradeManagerNamenode um = FSNamesystem.getFSNamesystem().upgradeManager;
    if(! um.initializeUpgrade())
      return;
    // write new upgrade state into disk
    FSNamesystem.getFSNamesystem().getFSImage().writeAll();
    NameNode.LOG.info("\n   Distributed upgrade for NameNode version " 
        + um.getUpgradeVersion() + " to current LV " 
        + FSConstants.LAYOUT_VERSION + " is initialized.");
  }
}
