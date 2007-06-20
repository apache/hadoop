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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.dfs.FSConstants.StartupOption;
import org.apache.hadoop.dfs.FSConstants.NodeType;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;

/**
 * Common class for storage information.
 * 
 * TODO namespaceID should be long and computed as hash(address + port)
 */
class StorageInfo {
  int   layoutVersion;  // Version read from the stored file.
  int   namespaceID;    // namespace id of the storage
  long  cTime;          // creation timestamp
  
  StorageInfo () {
    this(0, 0, 0L);
  }
  
  StorageInfo(int layoutV, int nsID, long cT) {
    layoutVersion = layoutV;
    namespaceID = nsID;
    cTime = cT;
  }
  
  StorageInfo(StorageInfo from) {
    layoutVersion = from.layoutVersion;
    namespaceID = from.namespaceID;
    cTime = from.cTime;
  }

  public int    getLayoutVersion(){ return layoutVersion; }
  public int    getNamespaceID()  { return namespaceID; }
  public long   getCTime()        { return cTime; }
}

/**
 * Storage information file.
 * <p>
 * Local storage information is stored in a separate file VERSION.
 * It contains type of the node, 
 * the storage layout version, the namespace id, and 
 * the fs state creation time.
 * <p>
 * Local storage can reside in multiple directories. 
 * Each directory should contain the same VERSION file as the others.
 * During startup Hadoop servers (name-node and data-nodes) read their local 
 * storage information from them.
 * <p>
 * The servers hold a lock for each storage directory while they run so that 
 * other nodes were not able to startup sharing the same storage.
 * The locks are released when the servers stop (normally or abnormally).
 * 
 */
abstract class Storage extends StorageInfo {
  public static final Log LOG = LogFactory.getLog("org.apache.hadoop.dfs.Storage");

  // Constants
  
  // last layout version that did not suppot upgrades
  protected static final int LAST_PRE_UPGRADE_LAYOUT_VERSION = -3;
  
  private   static final String STORAGE_FILE_LOCK     = "in_use.lock";
  protected static final String STORAGE_FILE_VERSION  = "VERSION";
  private   static final String STORAGE_DIR_CURRENT   = "current";
  private   static final String STORAGE_DIR_PREVIOUS  = "previous";
  private   static final String STORAGE_TMP_REMOVED   = "removed.tmp";
  private   static final String STORAGE_TMP_PREVIOUS  = "previous.tmp";
  private   static final String STORAGE_TMP_FINALIZED = "finalized.tmp";
  
  protected enum StorageState {
    NON_EXISTENT,
    NOT_FORMATTED,
    CONVERT,
    COMPLETE_UPGRADE,
    RECOVER_UPGRADE,
    COMPLETE_FINALIZE,
    COMPLETE_ROLLBACK,
    RECOVER_ROLLBACK,
    NORMAL;
  }
  
  private NodeType storageType;    // Type of the node using this storage 
  protected List<StorageDirectory> storageDirs = new ArrayList<StorageDirectory>();
  
  /**
   * One of the storage directories.
   */
  class StorageDirectory {
    File              root; // root directory
    FileLock          lock; // storage lock
    
    StorageDirectory(File dir) {
      this.root = dir;
      this.lock = null;
    }

    /**
     * Read version file.
     * 
     * @throws IOException if file cannot be read or contains inconsistent data
     */
    void read() throws IOException {
      read(getVersionFile());
    }
    
    void read(File from) throws IOException {
      RandomAccessFile file = new RandomAccessFile(from, "rws");
      FileInputStream in = null;
      try {
        in = new FileInputStream(file.getFD());
        file.seek(0);
        Properties props = new Properties();
        props.load(in);
        getFields(props, this);
      } finally {
        if (in != null) {
          in.close();
        }
        file.close();
      }
    }

    /**
     * Write version file.
     * 
     * @throws IOException
     */
    void write() throws IOException {
      corruptPreUpgradeStorage(root);
      write(getVersionFile());
    }

    void write(File to) throws IOException {
      Properties props = new Properties();
      setFields(props, this);
      RandomAccessFile file = new RandomAccessFile(to, "rws");
      FileOutputStream out = null;
      try {
        file.seek(0);
        out = new FileOutputStream(file.getFD());
        props.store(out, null);
      } finally {
        if (out != null) {
          out.close();
        }
        file.close();
      }
    }

    /**
     * Clear and re-create storage directory.
     * <p>
     * Removes contents of the current directory and creates an empty directory.
     * 
     * This does not fully format storage directory. 
     * It cannot write the version file since it should be written last after  
     * all other storage type dependent files are written.
     * Derived storage is responsible for setting specific storage values and
     * writing the version file to disk.
     * 
     * @throws IOException
     */
    void clearDirectory() throws IOException {
      File curDir = this.getCurrentDir();
      if (curDir.exists())
        if (!(FileUtil.fullyDelete(curDir)))
          throw new IOException("Cannot remove current directory: " + curDir);
      if (!curDir.mkdirs())
        throw new IOException("Cannot create directory " + curDir);
    }

    File getCurrentDir() {
      return new File(root, STORAGE_DIR_CURRENT);
    }
    File getVersionFile() {
      return new File(new File(root, STORAGE_DIR_CURRENT), STORAGE_FILE_VERSION);
    }
    File getPreviousVersionFile() {
      return new File(new File(root, STORAGE_DIR_PREVIOUS), STORAGE_FILE_VERSION);
    }
    File getPreviousDir() {
      return new File(root, STORAGE_DIR_PREVIOUS);
    }
    File getPreviousTmp() {
      return new File(root, STORAGE_TMP_PREVIOUS);
    }
    File getRemovedTmp() {
      return new File(root, STORAGE_TMP_REMOVED);
    }
    File getFinalizedTmp() {
      return new File(root, STORAGE_TMP_FINALIZED);
    }

    /**
     * Check consistency of the storage directory
     * 
     * @param startOpt a startup option.
     *  
     * @return state {@link StorageState} of the storage directory 
     * @throws {@link InconsistentFSStateException} if directory state is not 
     * consistent and cannot be recovered 
     */
    StorageState analyzeStorage(StartupOption startOpt) throws IOException {
      assert root != null : "root is null";
      String rootPath = root.getCanonicalPath();
      try { // check that storage exists
        if (!root.exists()) {
          // storage directory does not exist
          if (startOpt != StartupOption.FORMAT) {
            LOG.info("Storage directory " + rootPath + " does not exist.");
            return StorageState.NON_EXISTENT;
          }
          LOG.info(rootPath + " does not exist. Creating ...");
          if (!root.mkdirs())
            throw new IOException("Cannot create directory " + rootPath);
        }
        // or is inaccessible
        if (!root.isDirectory()) {
          LOG.info(rootPath + "is not a directory.");
          return StorageState.NON_EXISTENT;
        }
        if (!root.canWrite()) {
          LOG.info("Cannot access storage directory " + rootPath);
          return StorageState.NON_EXISTENT;
        }
      } catch(SecurityException ex) {
        LOG.info("Cannot access storage directory " + rootPath, ex);
        return StorageState.NON_EXISTENT;
      }

      this.lock(); // lock storage if it exists

      if (startOpt == StartupOption.FORMAT)
        return StorageState.NOT_FORMATTED;
      // check whether a conversion is required
      if (isConversionNeeded(this))
        return StorageState.CONVERT;
      // check whether current directory is valid
      File versionFile = getVersionFile();
      boolean hasCurrent = versionFile.exists();

      // check which directories exist
      boolean hasPrevious = getPreviousDir().exists();
      boolean hasPreviousTmp = getPreviousTmp().exists();
      boolean hasRemovedTmp = getRemovedTmp().exists();
      boolean hasFinalizedTmp = getFinalizedTmp().exists();

      if (!(hasPreviousTmp || hasRemovedTmp || hasFinalizedTmp)) {
        // no temp dirs - no recovery
        if (hasCurrent)
          return StorageState.NORMAL;
        if (hasPrevious)
          throw new InconsistentFSStateException(root,
                                                 "version file in current directory it is missing.");
        return StorageState.NOT_FORMATTED;
      }

      if ((hasPreviousTmp?1:0)+(hasRemovedTmp?1:0)+(hasFinalizedTmp?1:0) > 1)
        // more than one temp dirs
        throw new InconsistentFSStateException(root,
                                               "too many temporary directories.");

      // # of temp dirs == 1 should either recover or complete a transition
      if (hasFinalizedTmp) {
        if (hasPrevious)
          throw new InconsistentFSStateException(root,
                                                 STORAGE_DIR_PREVIOUS + " and " + STORAGE_TMP_FINALIZED
                                                 + "cannot exist together.");
        return StorageState.COMPLETE_FINALIZE;
      }

      if (hasPreviousTmp) {
        if (hasPrevious)
          throw new InconsistentFSStateException(root,
                                                 STORAGE_DIR_PREVIOUS + " and " + STORAGE_TMP_PREVIOUS
                                                 + " cannot exist together.");
        if (hasCurrent)
          return StorageState.COMPLETE_UPGRADE;
        return StorageState.RECOVER_UPGRADE;
      }
      
      assert hasRemovedTmp : "hasRemovedTmp must be true";
      if (!(hasCurrent ^ hasPrevious))
        throw new InconsistentFSStateException(root,
                                               "one and only one directory " + STORAGE_DIR_CURRENT 
                                               + " or " + STORAGE_DIR_PREVIOUS 
                                               + " must be present when " + STORAGE_TMP_REMOVED
                                               + " exists.");
      if (hasCurrent)
        return StorageState.COMPLETE_ROLLBACK;
      return StorageState.RECOVER_ROLLBACK;
    }

    /**
     * Complete or recover storage state from previously failed transition.
     * 
     * @param curState specifies what/how the state should be recovered
     * @throws IOException
     */
    void doRecover(StorageState curState) throws IOException {
      File curDir = getCurrentDir();
      String rootPath = root.getCanonicalPath();
      switch(curState) {
      case COMPLETE_UPGRADE:  // mv previous.tmp -> previous
        LOG.info("Completing previous upgrade for storage directory " 
                 + rootPath + ".");
        rename(getPreviousTmp(), getPreviousDir());
        return;
      case RECOVER_UPGRADE:   // mv previous.tmp -> current
        LOG.info("Recovering storage directory " + rootPath
                 + " from previous upgrade.");
        if (curDir.exists())
          deleteDir(curDir);
        rename(getPreviousTmp(), curDir);
        return;
      case COMPLETE_ROLLBACK: // rm removed.tmp
        LOG.info("Completing previous rollback for storage directory "
                 + rootPath + ".");
        deleteDir(getRemovedTmp());
        return;
      case RECOVER_ROLLBACK:  // mv removed.tmp -> current
        LOG.info("Recovering storage directory " + rootPath
                 + " from previous rollback.");
        rename(getRemovedTmp(), curDir);
        return;
      case COMPLETE_FINALIZE: // rm finalized.tmp
        LOG.info("Completing previous finalize for storage directory "
                 + rootPath + ".");
        deleteDir(getFinalizedTmp());
        return;
      default:
        throw new IOException("Unexpected FS state: " + curState);
      }
    }

    /**
     * Lock storage.
     * 
     * @throws IOException if locking fails
     */
    void lock() throws IOException {
      File lockF = new File(root, STORAGE_FILE_LOCK);
      lockF.deleteOnExit();
      RandomAccessFile file = new RandomAccessFile(lockF, "rws");
      try {
        this.lock = file.getChannel().tryLock();
      } catch(IOException e) {
        LOG.info(StringUtils.stringifyException(e));
        file.close();
        throw e;
      }
      if (lock == null) {
        String msg = "Cannot lock storage " + this.root 
          + ". The directory is already locked.";
        LOG.info(msg);
        file.close();
        throw new IOException(msg);
      }
    }

    /**
     * Unlock storage.
     * 
     * @throws IOException
     */
    void unlock() throws IOException {
      if (this.lock == null)
        return;
      this.lock.release();
      lock.channel().close();
    }
  }

  /**
   * Create empty storage info of the specified type
   */
  Storage(NodeType type) {
    super();
    this.storageType = type;
  }
  
  Storage(NodeType type, int nsID, long cT) {
    super(FSConstants.LAYOUT_VERSION, nsID, cT);
    this.storageType = type;
  }
  
  Storage(NodeType type, StorageInfo storageInfo) {
    super(storageInfo);
    this.storageType = type;
  }
  
  int getNumStorageDirs() {
    return storageDirs.size();
  }
  
  StorageDirectory getStorageDir(int idx) {
    return storageDirs.get(idx);
  }
  
  protected void addStorageDir(StorageDirectory sd) {
    storageDirs.add(sd);
  }
  
  abstract boolean isConversionNeeded(StorageDirectory sd) throws IOException;
  
  /**
   * Get common storage fields.
   * Should be overloaded if additional fields need to be get.
   * 
   * @param props
   * @throws IOException
   */
  protected void getFields(Properties props, 
                           StorageDirectory sd 
                           ) throws IOException {
    String sv, st, sid, sct;
    sv = props.getProperty("layoutVersion");
    st = props.getProperty("storageType");
    sid = props.getProperty("namespaceID");
    sct = props.getProperty("cTime");
    if (sv == null || st == null || sid == null || sct == null)
      throw new InconsistentFSStateException(sd.root,
                                             "file " + STORAGE_FILE_VERSION + " is invalid.");
    int rv = Integer.parseInt(sv);
    NodeType rt = NodeType.valueOf(st);
    int rid = Integer.parseInt(sid);
    long rct = Long.parseLong(sct);
    if (!storageType.equals(rt) ||
        !((namespaceID == 0) || (rid == 0) || namespaceID == rid))
      throw new InconsistentFSStateException(sd.root,
                                             "is incompatible with others.");
    if (rv < FSConstants.LAYOUT_VERSION) // future version
      throw new IncorrectVersionException(rv, "storage directory " 
                                          + sd.root.getCanonicalPath());
    layoutVersion = rv;
    storageType = rt;
    namespaceID = rid;
    cTime = rct;
  }
  
  /**
   * Set common storage fields.
   * Should be overloaded if additional fields need to be set.
   * 
   * @param props
   * @throws IOException
   */
  protected void setFields(Properties props, 
                           StorageDirectory sd 
                           ) throws IOException {
    props.setProperty("layoutVersion", String.valueOf(layoutVersion));
    props.setProperty("storageType", storageType.toString());
    props.setProperty("namespaceID", String.valueOf(namespaceID));
    props.setProperty("cTime", String.valueOf(cTime));
  }

  static void rename(File from, File to) throws IOException {
    if (!from.renameTo(to))
      throw new IOException("Failed to rename " 
                            + from.getCanonicalPath() + " to " + to.getCanonicalPath());
  }

  static void deleteDir(File dir) throws IOException {
    if (!FileUtil.fullyDelete(dir))
      throw new IOException("Failed to delete " + dir.getCanonicalPath());
  }
  
  /**
   * Write all data storage files.
   * @throws IOException
   */
  public void writeAll() throws IOException {
    this.layoutVersion = FSConstants.LAYOUT_VERSION;
    for (Iterator<StorageDirectory> it = storageDirs.iterator(); it.hasNext();) {
      it.next().write();
    }
  }

  /**
   * Close all the version files.
   * @throws IOException
   */
  public void unlockAll() throws IOException {
    for (Iterator<StorageDirectory> it = storageDirs.iterator(); it.hasNext();) {
      it.next().unlock();
    }
  }

  public static String getBuildVersion() {
    return VersionInfo.getRevision();
  }

  static String getRegistrationID(StorageInfo storage) {
    return "NS-" + Integer.toString(storage.getNamespaceID())
      + "-" + Integer.toString(storage.getLayoutVersion())
      + "-" + Long.toString(storage.getCTime());
  }

  // Pre-upgrade version compatibility
  protected abstract void corruptPreUpgradeStorage(File rootDir) throws IOException;

  protected void writeCorruptedData(RandomAccessFile file) throws IOException {
    final String messageForPreUpgradeVersion =
      "\nThis file is INTENTIONALLY CORRUPTED so that versions\n"
      + "of Hadoop prior to 0.13 (which are incompatible\n"
      + "with this directory layout) will fail to start.\n";
  
    file.seek(0);
    file.writeInt(FSConstants.LAYOUT_VERSION);
    org.apache.hadoop.io.UTF8.writeString(file, "");
    file.writeBytes(messageForPreUpgradeVersion);
    file.getFD().sync();
  }
}
