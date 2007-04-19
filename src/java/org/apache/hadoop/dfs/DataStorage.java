package org.apache.hadoop.dfs;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

import org.apache.hadoop.dfs.FSConstants.StartupOption;
import org.apache.hadoop.dfs.FSConstants.NodeType;
import org.apache.hadoop.dfs.FSImage.NameNodeFile;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.fs.FileUtil.HardLink;

/** 
 * Data storage information file.
 * <p>
 * @see Storage
 * @author Konstantin Shvachko
 */
class DataStorage extends Storage {
  // Constants
  final static String BLOCK_SUBDIR_PREFIX = "subdir";
  final static String BLOCK_FILE_PREFIX = "blk_";
  
  private String storageID;

  DataStorage() {
    super(NodeType.DATA_NODE);
    storageID = "";
  }
  
  DataStorage(int nsID, long cT, String strgID) {
    super(NodeType.DATA_NODE, nsID, cT);
    this.storageID = strgID;
  }
  
  DataStorage(StorageInfo storageInfo, String strgID) {
    super(NodeType.DATA_NODE, storageInfo);
    this.storageID = strgID;
  }

  String getStorageID() {
    return storageID;
  }
  
  void setStorageID(String newStorageID) {
    this.storageID = newStorageID;
  }
  
  /**
   * Analyze storage directories.
   * Recover from previous transitions if required. 
   * Perform fs state transition if necessary depending on the namespace info.
   * Read storage info. 
   * 
   * @param nsInfo namespace information
   * @param dataDirs array of data storage directories
   * @param startOpt startup option
   * @throws IOException
   */
  void recoverTransitionRead(NamespaceInfo nsInfo,
                             Collection<File> dataDirs,
                             StartupOption startOpt
                             ) throws IOException {
    assert FSConstants.LAYOUT_VERSION == nsInfo.getLayoutVersion() :
      "Data-node and name-node layout versions must be the same.";
    
    // 1. For each data directory calculate its state and 
    // check whether all is consistent before transitioning.
    // Format and recover.
    this.storageID = "";
    this.storageDirs = new ArrayList<StorageDirectory>(dataDirs.size());
    ArrayList<StorageState> dataDirStates = new ArrayList<StorageState>(dataDirs.size());
    for(Iterator<File> it = dataDirs.iterator(); it.hasNext();) {
      File dataDir = it.next();
      StorageDirectory sd = new StorageDirectory(dataDir);
      StorageState curState;
      try {
        curState = sd.analyzeStorage(startOpt);
        // sd is locked but not opened
        switch(curState) {
        case NORMAL:
          break;
        case NON_EXISTENT:
          // ignore this storage
          LOG.info("Storage directory " + dataDir + " does not exist.");
          it.remove();
          continue;
        case CONVERT:
          convertLayout(sd, nsInfo);
          break;
        case NOT_FORMATTED: // format
          LOG.info("Storage directory " + dataDir + " is not formatted.");
          LOG.info("Formatting ...");
          format(sd, nsInfo);
          break;
        default:  // recovery part is common
          sd.doRecover(curState);
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

    // 2. Do transitions
    // Each storage directory is treated individually.
    // During sturtup some of them can upgrade or rollback 
    // while others could be uptodate for the regular startup.
    for(int idx = 0; idx < getNumStorageDirs(); idx++) {
      doTransition(getStorageDir(idx), nsInfo, startOpt);
      assert this.getLayoutVersion() == nsInfo.getLayoutVersion() :
        "Data-node and name-node layout versions must be the same.";
      assert this.getCTime() == nsInfo.getCTime() :
        "Data-node and name-node CTimes must be the same.";
    }
    
    // 3. Update all storages. Some of them might have just been formatted.
    this.writeAll();
  }

  void format(StorageDirectory sd, NamespaceInfo nsInfo) throws IOException {
    sd.clearDirectory(); // create directory
    this.layoutVersion = FSConstants.LAYOUT_VERSION;
    this.namespaceID = nsInfo.getNamespaceID();
    this.cTime = 0;
    // store storageID as it currently is
    sd.write();
  }

  protected void setFields(Properties props, 
                           StorageDirectory sd 
                           ) throws IOException {
    super.setFields(props, sd);
    props.setProperty("storageID", storageID);
  }

  protected void getFields(Properties props, 
                           StorageDirectory sd 
                           ) throws IOException {
    super.getFields(props, sd);
    String ssid = props.getProperty("storageID");
    if (ssid == null ||
        !("".equals(storageID) || "".equals(ssid) ||
          storageID.equals(ssid)))
      throw new InconsistentFSStateException(sd.root,
                                             "has incompatible storage Id.");
    if ("".equals(storageID)) // update id only if it was empty
      storageID = ssid;
  }

  boolean isConversionNeeded(StorageDirectory sd) throws IOException {
    File oldF = new File(sd.root, "storage");
    if (!oldF.exists())
      return false;
    // check consistency of the old storage
    File oldDataDir = new File(sd.root, "data");
    if (!oldDataDir.exists()) 
      throw new InconsistentFSStateException(sd.root,
                                             "Old layout block directory " + oldDataDir + " is missing"); 
    if (!oldDataDir.isDirectory())
      throw new InconsistentFSStateException(sd.root,
                                             oldDataDir + " is not a directory.");
    if (!oldDataDir.canWrite())
      throw new InconsistentFSStateException(sd.root,
                                             oldDataDir + " is not writable.");
    return true;
  }
  
  /**
   * Automatic conversion from the old layout version to the new one.
   * 
   * @param sd storage directory
   * @param nsInfo namespace information
   * @throws IOException
   */
  private void convertLayout(StorageDirectory sd,
                             NamespaceInfo nsInfo 
                             ) throws IOException {
    assert FSConstants.LAYOUT_VERSION < LAST_PRE_UPGRADE_LAYOUT_VERSION :
      "Bad current layout version: FSConstants.LAYOUT_VERSION should decrease";
    File oldF = new File(sd.root, "storage");
    File oldDataDir = new File(sd.root, "data");
    assert oldF.exists() : "Old datanode layout \"storage\" file is missing";
    assert oldDataDir.exists() : "Old layout block directory \"data\" is missing";
    LOG.info("Old layout version file " + oldF
             + " is found. New layout version is "
             + FSConstants.LAYOUT_VERSION);
    LOG.info("Converting ...");
    
    // Lock and Read old storage file
    RandomAccessFile oldFile = new RandomAccessFile(oldF, "rws");
    if (oldFile == null)
      throw new IOException("Cannot read file: " + oldF);
    FileLock oldLock = oldFile.getChannel().tryLock();
    if (oldLock == null)
      throw new IOException("Cannot lock file: " + oldF);
    try {
      oldFile.seek(0);
      int odlVersion = oldFile.readInt();
      if (odlVersion < LAST_PRE_UPGRADE_LAYOUT_VERSION)
        throw new IncorrectVersionException(odlVersion, "file " + oldF,
                                            LAST_PRE_UPGRADE_LAYOUT_VERSION);
      String odlStorageID = org.apache.hadoop.io.UTF8.readString(oldFile);
  
      // check new storage
      File newDataDir = sd.getCurrentDir();
      File versionF = sd.getVersionFile();
      if (versionF.exists())
        throw new IOException("Version file already exists: " + versionF);
      if (newDataDir.exists()) // somebody created current dir manually
        deleteDir(newDataDir);
      // Write new layout
      rename(oldDataDir, newDataDir);
  
      this.layoutVersion = FSConstants.LAYOUT_VERSION;
      this.namespaceID = nsInfo.getNamespaceID();
      this.cTime = 0;
      this.storageID = odlStorageID;
      sd.write();
      // close and unlock old file
    } finally {
      oldLock.release();
      oldFile.close();
    }
    // move old storage file into current dir
    rename(oldF, new File(sd.getCurrentDir(), "storage"));
    LOG.info("Conversion of " + oldF + " is complete.");
  }

  /**
   * Analize which and whether a transition of the fs state is required
   * and perform it if necessary.
   * 
   * Rollback if previousLV >= LAYOUT_VERSION && prevCTime <= namenode.cTime
   * Upgrade if this.LV > LAYOUT_VERSION || this.cTime < namenode.cTime
   * Regular startup if this.LV = LAYOUT_VERSION && this.cTime = namenode.cTime
   * 
   * @param sd  storage directory
   * @param nsInfo  namespace info
   * @param startOpt  startup option
   * @throws IOException
   */
  private void doTransition( StorageDirectory sd, 
                             NamespaceInfo nsInfo, 
                             StartupOption startOpt
                             ) throws IOException {
    if (startOpt == StartupOption.ROLLBACK)
      doRollback(sd, nsInfo); // rollback if applicable
    sd.read();
    assert this.layoutVersion >= FSConstants.LAYOUT_VERSION :
      "Future version is not allowed";
    if (getNamespaceID() != nsInfo.getNamespaceID())
      throw new IOException(
                            "Incompatible namespaceIDs in " + sd.root.getCanonicalPath()
                            + ": namenode namespaceID = " + nsInfo.getNamespaceID() 
                            + "; datanode namespaceID = " + getNamespaceID());
    if (this.layoutVersion == FSConstants.LAYOUT_VERSION 
        && this.cTime == nsInfo.getCTime())
      return; // regular startup
    if (this.layoutVersion > FSConstants.LAYOUT_VERSION
        || this.cTime < nsInfo.getCTime()) {
      doUpgrade(sd, nsInfo);  // upgrade
      return;
    }
    // layoutVersion == LAYOUT_VERSION && this.cTime > nsInfo.cTime
    // must shutdown
    throw new IOException("Datanode state: LV = " + this.getLayoutVersion() 
                          + " CTime = " + this.getCTime() 
                          + " is newer than the namespace state: LV = "
                          + nsInfo.getLayoutVersion() 
                          + " CTime = " + nsInfo.getCTime());
  }

  /**
   * Move current storage into a backup directory,
   * and hardlink all its blocks into the new current directory.
   * 
   * @param sd  storage directory
   * @throws IOException
   */
  void doUpgrade(StorageDirectory sd,
                 NamespaceInfo nsInfo
                 ) throws IOException {
    LOG.info("Upgrading storage directory " + sd.root 
             + ".\n   old LV = " + this.getLayoutVersion()
             + "; old CTime = " + this.getCTime()
             + ".\n   new LV = " + nsInfo.getLayoutVersion()
             + "; new CTime = " + nsInfo.getCTime());
    File curDir = sd.getCurrentDir();
    File prevDir = sd.getPreviousDir();
    assert curDir.exists() : "Current directory must exist.";
    // delete previous dir before upgrading
    if (prevDir.exists())
      deleteDir(prevDir);
    File tmpDir = sd.getPreviousTmp();
    assert !tmpDir.exists() : "previous.tmp directory must not exist.";
    // rename current to tmp
    rename(curDir, tmpDir);
    // hardlink blocks
    linkBlocks(tmpDir, curDir);
    // write version file
    this.layoutVersion = FSConstants.LAYOUT_VERSION;
    assert this.namespaceID == nsInfo.getNamespaceID() :
      "Data-node and name-node layout versions must be the same.";
    this.cTime = nsInfo.getCTime();
    sd.write();
    // rename tmp to previous
    rename(tmpDir, prevDir);
    LOG.info("Upgrade of " + sd.root + " is complete.");
  }

  void doRollback( StorageDirectory sd,
                   NamespaceInfo nsInfo
                   ) throws IOException {
    File prevDir = sd.getPreviousDir();
    // regular startup if previous dir does not exist
    if (!prevDir.exists())
      return;
    DataStorage prevInfo = new DataStorage();
    StorageDirectory prevSD = prevInfo.new StorageDirectory(sd.root);
    prevSD.read(prevSD.getPreviousVersionFile());

    // We allow rollback to a state, which is either consistent with
    // the namespace state or can be further upgraded to it.
    if (!(prevInfo.getLayoutVersion() >= FSConstants.LAYOUT_VERSION
          && prevInfo.getCTime() <= nsInfo.getCTime()))  // cannot rollback
      throw new InconsistentFSStateException(prevSD.root,
                                             "Cannot rollback to a newer state.\nDatanode previous state: LV = " 
                                             + prevInfo.getLayoutVersion() + " CTime = " + prevInfo.getCTime() 
                                             + " is newer than the namespace state: LV = "
                                             + nsInfo.getLayoutVersion() + " CTime = " + nsInfo.getCTime());
    LOG.info("Rolling back storage directory " + sd.root 
             + ".\n   target LV = " + nsInfo.getLayoutVersion()
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
    LOG.info("Rollback of " + sd.root + " is complete.");
  }

  void doFinalize(StorageDirectory sd) throws IOException {
    File prevDir = sd.getPreviousDir();
    if (!prevDir.exists())
      return; // already discarded
    final String dataDirPath = sd.root.getCanonicalPath();
    LOG.info("Finalizing upgrade for storage directory " 
             + dataDirPath 
             + ".\n   cur LV = " + this.getLayoutVersion()
             + "; cur CTime = " + this.getCTime());
    assert sd.getCurrentDir().exists() : "Current directory must exist.";
    final File tmpDir = sd.getFinalizedTmp();
    // rename previous to tmp
    rename(prevDir, tmpDir);

    // delete tmp dir in a separate thread
    new Daemon(new Runnable() {
        public void run() {
          try {
            deleteDir(tmpDir);
          } catch(IOException ex) {
            LOG.error("Finalize upgrade for " + dataDirPath + " failed.", ex);
          }
          LOG.info("Finalize upgrade for " + dataDirPath + " is complete.");
        }
        public String toString() { return "Finalize " + dataDirPath; }
      }).start();
  }
  
  void finalizeUpgrade() throws IOException {
    for (Iterator<StorageDirectory> it = storageDirs.iterator(); it.hasNext();) {
      doFinalize(it.next());
    }
  }
  
  static void linkBlocks(File from, File to) throws IOException {
    if (!from.isDirectory()) {
      HardLink.createHardLink(from, to);
      return;
    }
    // from is a directory
    if (!to.mkdir())
      throw new IOException("Cannot create directory " + to);
    String[] blockNames = from.list(new java.io.FilenameFilter() {
        public boolean accept(File dir, String name) {
          return name.startsWith(BLOCK_SUBDIR_PREFIX) 
            || name.startsWith(BLOCK_FILE_PREFIX);
        }
      });
    
    for(int i = 0; i < blockNames.length; i++)
      linkBlocks(new File(from, blockNames[i]), new File(to, blockNames[i]));
  }
}
