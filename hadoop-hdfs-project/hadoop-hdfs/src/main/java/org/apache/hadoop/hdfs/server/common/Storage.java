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
package org.apache.hadoop.hdfs.server.common;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIOException;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.VersionInfo;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;



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
@InterfaceAudience.Private
public abstract class Storage extends StorageInfo {
  public static final Log LOG = LogFactory.getLog(Storage.class.getName());

  // last layout version that did not support upgrades
  public static final int LAST_PRE_UPGRADE_LAYOUT_VERSION = -3;
  
  // this corresponds to Hadoop-0.18
  public static final int LAST_UPGRADABLE_LAYOUT_VERSION = -16;
  protected static final String LAST_UPGRADABLE_HADOOP_VERSION = "Hadoop-0.18";
  
  /** Layout versions of 0.20.203 release */
  public static final int[] LAYOUT_VERSIONS_203 = {-19, -31};

  public    static final String STORAGE_FILE_LOCK     = "in_use.lock";
  public    static final String STORAGE_DIR_CURRENT   = "current";
  public    static final String STORAGE_DIR_PREVIOUS  = "previous";
  public    static final String STORAGE_TMP_REMOVED   = "removed.tmp";
  public    static final String STORAGE_TMP_PREVIOUS  = "previous.tmp";
  public    static final String STORAGE_TMP_FINALIZED = "finalized.tmp";
  public    static final String STORAGE_TMP_LAST_CKPT = "lastcheckpoint.tmp";
  public    static final String STORAGE_PREVIOUS_CKPT = "previous.checkpoint";
  
  /**
   * The blocksBeingWritten directory which was used in some 1.x and earlier
   * releases.
   */
  public static final String STORAGE_1_BBW = "blocksBeingWritten";
  
  public enum StorageState {
    NON_EXISTENT,
    NOT_FORMATTED,
    COMPLETE_UPGRADE,
    RECOVER_UPGRADE,
    COMPLETE_FINALIZE,
    COMPLETE_ROLLBACK,
    RECOVER_ROLLBACK,
    COMPLETE_CHECKPOINT,
    RECOVER_CHECKPOINT,
    NORMAL;
  }
  
  /**
   * An interface to denote storage directory type
   * Implementations can define a type for storage directory by implementing
   * this interface.
   */
  @InterfaceAudience.Private
  public interface StorageDirType {
    public StorageDirType getStorageDirType();
    public boolean isOfType(StorageDirType type);
  }
  
  protected List<StorageDirectory> storageDirs = new ArrayList<StorageDirectory>();
  
  private class DirIterator implements Iterator<StorageDirectory> {
    final StorageDirType dirType;
    final boolean includeShared;
    int prevIndex; // for remove()
    int nextIndex; // for next()
    
    DirIterator(StorageDirType dirType, boolean includeShared) {
      this.dirType = dirType;
      this.nextIndex = 0;
      this.prevIndex = 0;
      this.includeShared = includeShared;
    }
    
    @Override
    public boolean hasNext() {
      if (storageDirs.isEmpty() || nextIndex >= storageDirs.size())
        return false;
      if (dirType != null || !includeShared) {
        while (nextIndex < storageDirs.size()) {
          if (shouldReturnNextDir())
            break;
          nextIndex++;
        }
        if (nextIndex >= storageDirs.size())
         return false;
      }
      return true;
    }
    
    @Override
    public StorageDirectory next() {
      StorageDirectory sd = getStorageDir(nextIndex);
      prevIndex = nextIndex;
      nextIndex++;
      if (dirType != null || !includeShared) {
        while (nextIndex < storageDirs.size()) {
          if (shouldReturnNextDir())
            break;
          nextIndex++;
        }
      }
      return sd;
    }
    
    @Override
    public void remove() {
      nextIndex = prevIndex; // restore previous state
      storageDirs.remove(prevIndex); // remove last returned element
      hasNext(); // reset nextIndex to correct place
    }
    
    private boolean shouldReturnNextDir() {
      StorageDirectory sd = getStorageDir(nextIndex);
      return (dirType == null || sd.getStorageDirType().isOfType(dirType)) &&
          (includeShared || !sd.isShared());
    }
  }
  
  /**
   * @return A list of the given File in every available storage directory,
   * regardless of whether it might exist.
   */
  public List<File> getFiles(StorageDirType dirType, String fileName) {
    ArrayList<File> list = new ArrayList<File>();
    Iterator<StorageDirectory> it =
      (dirType == null) ? dirIterator() : dirIterator(dirType);
    for ( ;it.hasNext(); ) {
      list.add(new File(it.next().getCurrentDir(), fileName));
    }
    return list;
  }


  /**
   * Return default iterator
   * This iterator returns all entries in storageDirs
   */
  public Iterator<StorageDirectory> dirIterator() {
    return dirIterator(null);
  }
  
  /**
   * Return iterator based on Storage Directory Type
   * This iterator selects entries in storageDirs of type dirType and returns
   * them via the Iterator
   */
  public Iterator<StorageDirectory> dirIterator(StorageDirType dirType) {
    return dirIterator(dirType, true);
  }
  
  /**
   * Return all entries in storageDirs, potentially excluding shared dirs.
   * @param includeShared whether or not to include shared dirs.
   * @return an iterator over the configured storage dirs.
   */
  public Iterator<StorageDirectory> dirIterator(boolean includeShared) {
    return dirIterator(null, includeShared);
  }
  
  /**
   * @param dirType all entries will be of this type of dir
   * @param includeShared true to include any shared directories,
   *        false otherwise
   * @return an iterator over the configured storage dirs.
   */
  public Iterator<StorageDirectory> dirIterator(StorageDirType dirType,
      boolean includeShared) {
    return new DirIterator(dirType, includeShared);
  }
  
  public Iterable<StorageDirectory> dirIterable(final StorageDirType dirType) {
    return new Iterable<StorageDirectory>() {
      @Override
      public Iterator<StorageDirectory> iterator() {
        return dirIterator(dirType);
      }
    };
  }
  
  
  /**
   * generate storage list (debug line)
   */
  public String listStorageDirectories() {
    StringBuilder buf = new StringBuilder();
    for (StorageDirectory sd : storageDirs) {
      buf.append(sd.getRoot() + "(" + sd.getStorageDirType() + ");");
    }
    return buf.toString();
  }
  
  /**
   * One of the storage directories.
   */
  @InterfaceAudience.Private
  public static class StorageDirectory implements FormatConfirmable {
    final File root;              // root directory
    // whether or not this dir is shared between two separate NNs for HA, or
    // between multiple block pools in the case of federation.
    final boolean isShared;
    final StorageDirType dirType; // storage dir type
    FileLock lock;                // storage lock

    private String storageUuid = null;      // Storage directory identifier.
    
    public StorageDirectory(File dir) {
      // default dirType is null
      this(dir, null, false);
    }
    
    public StorageDirectory(File dir, StorageDirType dirType) {
      this(dir, dirType, false);
    }
    
    public void setStorageUuid(String storageUuid) {
      this.storageUuid = storageUuid;
    }

    public String getStorageUuid() {
      return storageUuid;
    }

    /**
     * Constructor
     * @param dir directory corresponding to the storage
     * @param dirType storage directory type
     * @param isShared whether or not this dir is shared between two NNs. true
     *          disables locking on the storage directory, false enables locking
     */
    public StorageDirectory(File dir, StorageDirType dirType, boolean isShared) {
      this.root = dir;
      this.lock = null;
      this.dirType = dirType;
      this.isShared = isShared;
    }
    
    /**
     * Get root directory of this storage
     */
    public File getRoot() {
      return root;
    }

    /**
     * Get storage directory type
     */
    public StorageDirType getStorageDirType() {
      return dirType;
    }    

    public void read(File from, Storage storage) throws IOException {
      Properties props = readPropertiesFile(from);
      storage.setFieldsFromProperties(props, this);
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
    public void clearDirectory() throws IOException {
      File curDir = this.getCurrentDir();
      if (curDir.exists())
        if (!(FileUtil.fullyDelete(curDir)))
          throw new IOException("Cannot remove current directory: " + curDir);
      if (!curDir.mkdirs())
        throw new IOException("Cannot create directory " + curDir);
    }

    /**
     * Directory {@code current} contains latest files defining
     * the file system meta-data.
     * 
     * @return the directory path
     */
    public File getCurrentDir() {
      return new File(root, STORAGE_DIR_CURRENT);
    }

    /**
     * File {@code VERSION} contains the following fields:
     * <ol>
     * <li>node type</li>
     * <li>layout version</li>
     * <li>namespaceID</li>
     * <li>fs state creation time</li>
     * <li>other fields specific for this node type</li>
     * </ol>
     * The version file is always written last during storage directory updates.
     * The existence of the version file indicates that all other files have
     * been successfully written in the storage directory, the storage is valid
     * and does not need to be recovered.
     * 
     * @return the version file path
     */
    public File getVersionFile() {
      return new File(new File(root, STORAGE_DIR_CURRENT), STORAGE_FILE_VERSION);
    }

    /**
     * File {@code VERSION} from the {@code previous} directory.
     * 
     * @return the previous version file path
     */
    public File getPreviousVersionFile() {
      return new File(new File(root, STORAGE_DIR_PREVIOUS), STORAGE_FILE_VERSION);
    }

    /**
     * Directory {@code previous} contains the previous file system state,
     * which the system can be rolled back to.
     * 
     * @return the directory path
     */
    public File getPreviousDir() {
      return new File(root, STORAGE_DIR_PREVIOUS);
    }

    /**
     * {@code previous.tmp} is a transient directory, which holds
     * current file system state while the new state is saved into the new
     * {@code current} during upgrade.
     * If the saving succeeds {@code previous.tmp} will be moved to
     * {@code previous}, otherwise it will be renamed back to 
     * {@code current} by the recovery procedure during startup.
     * 
     * @return the directory path
     */
    public File getPreviousTmp() {
      return new File(root, STORAGE_TMP_PREVIOUS);
    }

    /**
     * {@code removed.tmp} is a transient directory, which holds
     * current file system state while the previous state is moved into
     * {@code current} during rollback.
     * If the moving succeeds {@code removed.tmp} will be removed,
     * otherwise it will be renamed back to 
     * {@code current} by the recovery procedure during startup.
     * 
     * @return the directory path
     */
    public File getRemovedTmp() {
      return new File(root, STORAGE_TMP_REMOVED);
    }

    /**
     * {@code finalized.tmp} is a transient directory, which holds
     * the {@code previous} file system state while it is being removed
     * in response to the finalize request.
     * Finalize operation will remove {@code finalized.tmp} when completed,
     * otherwise the removal will resume upon the system startup.
     * 
     * @return the directory path
     */
    public File getFinalizedTmp() {
      return new File(root, STORAGE_TMP_FINALIZED);
    }

    /**
     * {@code lastcheckpoint.tmp} is a transient directory, which holds
     * current file system state while the new state is saved into the new
     * {@code current} during regular namespace updates.
     * If the saving succeeds {@code lastcheckpoint.tmp} will be moved to
     * {@code previous.checkpoint}, otherwise it will be renamed back to 
     * {@code current} by the recovery procedure during startup.
     * 
     * @return the directory path
     */
    public File getLastCheckpointTmp() {
      return new File(root, STORAGE_TMP_LAST_CKPT);
    }

    /**
     * {@code previous.checkpoint} is a directory, which holds the previous
     * (before the last save) state of the storage directory.
     * The directory is created as a reference only, it does not play role
     * in state recovery procedures, and is recycled automatically, 
     * but it may be useful for manual recovery of a stale state of the system.
     * 
     * @return the directory path
     */
    public File getPreviousCheckpoint() {
      return new File(root, STORAGE_PREVIOUS_CKPT);
    }

    /**
     * Check consistency of the storage directory
     * 
     * @param startOpt a startup option.
     *  
     * @return state {@link StorageState} of the storage directory 
     * @throws InconsistentFSStateException if directory state is not 
     * consistent and cannot be recovered.
     * @throws IOException
     */
    public StorageState analyzeStorage(StartupOption startOpt, Storage storage)
        throws IOException {
      assert root != null : "root is null";
      boolean hadMkdirs = false;
      String rootPath = root.getCanonicalPath();
      try { // check that storage exists
        if (!root.exists()) {
          // storage directory does not exist
          if (startOpt != StartupOption.FORMAT &&
              startOpt != StartupOption.HOTSWAP) {
            LOG.warn("Storage directory " + rootPath + " does not exist");
            return StorageState.NON_EXISTENT;
          }
          LOG.info(rootPath + " does not exist. Creating ...");
          if (!root.mkdirs())
            throw new IOException("Cannot create directory " + rootPath);
          hadMkdirs = true;
        }
        // or is inaccessible
        if (!root.isDirectory()) {
          LOG.warn(rootPath + "is not a directory");
          return StorageState.NON_EXISTENT;
        }
        if (!FileUtil.canWrite(root)) {
          LOG.warn("Cannot access storage directory " + rootPath);
          return StorageState.NON_EXISTENT;
        }
      } catch(SecurityException ex) {
        LOG.warn("Cannot access storage directory " + rootPath, ex);
        return StorageState.NON_EXISTENT;
      }

      this.lock(); // lock storage if it exists

      // If startOpt is HOTSWAP, it returns NOT_FORMATTED for empty directory,
      // while it also checks the layout version.
      if (startOpt == HdfsServerConstants.StartupOption.FORMAT ||
          (startOpt == StartupOption.HOTSWAP && hadMkdirs))
        return StorageState.NOT_FORMATTED;

      if (startOpt != HdfsServerConstants.StartupOption.IMPORT) {
        storage.checkOldLayoutStorage(this);
      }

      // check whether current directory is valid
      File versionFile = getVersionFile();
      boolean hasCurrent = versionFile.exists();

      // check which directories exist
      boolean hasPrevious = getPreviousDir().exists();
      boolean hasPreviousTmp = getPreviousTmp().exists();
      boolean hasRemovedTmp = getRemovedTmp().exists();
      boolean hasFinalizedTmp = getFinalizedTmp().exists();
      boolean hasCheckpointTmp = getLastCheckpointTmp().exists();

      if (!(hasPreviousTmp || hasRemovedTmp
          || hasFinalizedTmp || hasCheckpointTmp)) {
        // no temp dirs - no recovery
        if (hasCurrent)
          return StorageState.NORMAL;
        if (hasPrevious)
          throw new InconsistentFSStateException(root,
                              "version file in current directory is missing.");
        return StorageState.NOT_FORMATTED;
      }

      if ((hasPreviousTmp?1:0) + (hasRemovedTmp?1:0)
          + (hasFinalizedTmp?1:0) + (hasCheckpointTmp?1:0) > 1)
        // more than one temp dirs
        throw new InconsistentFSStateException(root,
                                               "too many temporary directories.");

      // # of temp dirs == 1 should either recover or complete a transition
      if (hasCheckpointTmp) {
        return hasCurrent ? StorageState.COMPLETE_CHECKPOINT
                          : StorageState.RECOVER_CHECKPOINT;
      }

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
    public void doRecover(StorageState curState) throws IOException {
      File curDir = getCurrentDir();
      String rootPath = root.getCanonicalPath();
      switch(curState) {
      case COMPLETE_UPGRADE:  // mv previous.tmp -> previous
        LOG.info("Completing previous upgrade for storage directory " 
                 + rootPath);
        rename(getPreviousTmp(), getPreviousDir());
        return;
      case RECOVER_UPGRADE:   // mv previous.tmp -> current
        LOG.info("Recovering storage directory " + rootPath
                 + " from previous upgrade");
        if (curDir.exists())
          deleteDir(curDir);
        rename(getPreviousTmp(), curDir);
        return;
      case COMPLETE_ROLLBACK: // rm removed.tmp
        LOG.info("Completing previous rollback for storage directory "
                 + rootPath);
        deleteDir(getRemovedTmp());
        return;
      case RECOVER_ROLLBACK:  // mv removed.tmp -> current
        LOG.info("Recovering storage directory " + rootPath
                 + " from previous rollback");
        rename(getRemovedTmp(), curDir);
        return;
      case COMPLETE_FINALIZE: // rm finalized.tmp
        LOG.info("Completing previous finalize for storage directory "
                 + rootPath);
        deleteDir(getFinalizedTmp());
        return;
      case COMPLETE_CHECKPOINT: // mv lastcheckpoint.tmp -> previous.checkpoint
        LOG.info("Completing previous checkpoint for storage directory " 
                 + rootPath);
        File prevCkptDir = getPreviousCheckpoint();
        if (prevCkptDir.exists())
          deleteDir(prevCkptDir);
        rename(getLastCheckpointTmp(), prevCkptDir);
        return;
      case RECOVER_CHECKPOINT:  // mv lastcheckpoint.tmp -> current
        LOG.info("Recovering storage directory " + rootPath
                 + " from failed checkpoint");
        if (curDir.exists())
          deleteDir(curDir);
        rename(getLastCheckpointTmp(), curDir);
        return;
      default:
        throw new IOException("Unexpected FS state: " + curState
            + " for storage directory: " + rootPath);
      }
    }
    
    /**
     * @return true if the storage directory should prompt the user prior
     * to formatting (i.e if the directory appears to contain some data)
     * @throws IOException if the SD cannot be accessed due to an IO error
     */
    @Override
    public boolean hasSomeData() throws IOException {
      // Its alright for a dir not to exist, or to exist (properly accessible)
      // and be completely empty.
      if (!root.exists()) return false;
      
      if (!root.isDirectory()) {
        // a file where you expect a directory should not cause silent
        // formatting
        return true;
      }
      
      if (FileUtil.listFiles(root).length == 0) {
        // Empty dir can format without prompt.
        return false;
      }
      
      return true;
    }
    
    public boolean isShared() {
      return isShared;
    }


    /**
     * Lock storage to provide exclusive access.
     * 
     * <p> Locking is not supported by all file systems.
     * E.g., NFS does not consistently support exclusive locks.
     * 
     * <p> If locking is supported we guarantee exclusive access to the
     * storage directory. Otherwise, no guarantee is given.
     * 
     * @throws IOException if locking fails
     */
    public void lock() throws IOException {
      if (isShared()) {
        LOG.info("Locking is disabled for " + this.root);
        return;
      }
      FileLock newLock = tryLock();
      if (newLock == null) {
        String msg = "Cannot lock storage " + this.root 
          + ". The directory is already locked";
        LOG.info(msg);
        throw new IOException(msg);
      }
      // Don't overwrite lock until success - this way if we accidentally
      // call lock twice, the internal state won't be cleared by the second
      // (failed) lock attempt
      lock = newLock;
    }

    /**
     * Attempts to acquire an exclusive lock on the storage.
     * 
     * @return A lock object representing the newly-acquired lock or
     * <code>null</code> if storage is already locked.
     * @throws IOException if locking fails.
     */
    @SuppressWarnings("resource")
    FileLock tryLock() throws IOException {
      boolean deletionHookAdded = false;
      File lockF = new File(root, STORAGE_FILE_LOCK);
      if (!lockF.exists()) {
        lockF.deleteOnExit();
        deletionHookAdded = true;
      }
      RandomAccessFile file = new RandomAccessFile(lockF, "rws");
      String jvmName = ManagementFactory.getRuntimeMXBean().getName();
      FileLock res = null;
      try {
        res = file.getChannel().tryLock();
        if (null == res) {
          throw new OverlappingFileLockException();
        }
        file.write(jvmName.getBytes(Charsets.UTF_8));
        LOG.info("Lock on " + lockF + " acquired by nodename " + jvmName);
      } catch(OverlappingFileLockException oe) {
        // Cannot read from the locked file on Windows.
        String lockingJvmName = Path.WINDOWS ? "" : (" " + file.readLine());
        LOG.error("It appears that another node " + lockingJvmName
            + " has already locked the storage directory: " + root, oe);
        file.close();
        return null;
      } catch(IOException e) {
        LOG.error("Failed to acquire lock on " + lockF
            + ". If this storage directory is mounted via NFS, " 
            + "ensure that the appropriate nfs lock services are running.", e);
        file.close();
        throw e;
      }
      if (!deletionHookAdded) {
        // If the file existed prior to our startup, we didn't
        // call deleteOnExit above. But since we successfully locked
        // the dir, we can take care of cleaning it up.
        lockF.deleteOnExit();
      }
      return res;
    }

    /**
     * Unlock storage.
     * 
     * @throws IOException
     */
    public void unlock() throws IOException {
      if (this.lock == null)
        return;
      this.lock.release();
      lock.channel().close();
      lock = null;
    }
    
    @Override
    public String toString() {
      return "Storage Directory " + this.root;
    }

    /**
     * Check whether underlying file system supports file locking.
     * 
     * @return <code>true</code> if exclusive locks are supported or
     *         <code>false</code> otherwise.
     * @throws IOException
     * @see StorageDirectory#lock()
     */
    public boolean isLockSupported() throws IOException {
      FileLock firstLock = null;
      FileLock secondLock = null;
      try {
        firstLock = lock;
        if(firstLock == null) {
          firstLock = tryLock();
          if(firstLock == null)
            return true;
        }
        secondLock = tryLock();
        if(secondLock == null)
          return true;
      } finally {
        if(firstLock != null && firstLock != lock) {
          firstLock.release();
          firstLock.channel().close();
        }
        if(secondLock != null) {
          secondLock.release();
          secondLock.channel().close();
        }
      }
      return false;
    }
  }

  /**
   * Create empty storage info of the specified type
   */
  protected Storage(NodeType type) {
    super(type);
  }
  
  protected Storage(StorageInfo storageInfo) {
    super(storageInfo);
  }
  
  public int getNumStorageDirs() {
    return storageDirs.size();
  }
  
  public StorageDirectory getStorageDir(int idx) {
    return storageDirs.get(idx);
  }
  
  /**
   * @return the storage directory, with the precondition that this storage
   * has exactly one storage directory
   */
  public StorageDirectory getSingularStorageDir() {
    Preconditions.checkState(storageDirs.size() == 1);
    return storageDirs.get(0);
  }
  
  protected void addStorageDir(StorageDirectory sd) {
    storageDirs.add(sd);
  }

  /**
   * Returns true if the storage directory on the given directory is already
   * loaded.
   * @param root the root directory of a {@link StorageDirectory}
   * @throws IOException if failed to get canonical path.
   */
  protected boolean containsStorageDir(File root) throws IOException {
    for (StorageDirectory sd : storageDirs) {
      if (sd.getRoot().getCanonicalPath().equals(root.getCanonicalPath())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Return true if the layout of the given storage directory is from a version
   * of Hadoop prior to the introduction of the "current" and "previous"
   * directories which allow upgrade and rollback.
   */
  public abstract boolean isPreUpgradableLayout(StorageDirectory sd)
  throws IOException;

  /**
   * Check if the given storage directory comes from a version of Hadoop
   * prior to when the directory layout changed (ie 0.13). If this is
   * the case, this method throws an IOException.
   */
  private void checkOldLayoutStorage(StorageDirectory sd) throws IOException {
    if (isPreUpgradableLayout(sd)) {
      checkVersionUpgradable(0);
    }
  }

  /**
   * Checks if the upgrade from {@code oldVersion} is supported.
   * @param oldVersion the version of the metadata to check with the current
   *                   version
   * @throws IOException if upgrade is not supported
   */
  public static void checkVersionUpgradable(int oldVersion) 
                                     throws IOException {
    if (oldVersion > LAST_UPGRADABLE_LAYOUT_VERSION) {
      String msg = "*********** Upgrade is not supported from this " +
                   " older version " + oldVersion + 
                   " of storage to the current version." + 
                   " Please upgrade to " + LAST_UPGRADABLE_HADOOP_VERSION +
                   " or a later version and then upgrade to current" +
                   " version. Old layout version is " + 
                   (oldVersion == 0 ? "'too old'" : (""+oldVersion)) +
                   " and latest layout version this software version can" +
                   " upgrade from is " + LAST_UPGRADABLE_LAYOUT_VERSION +
                   ". ************";
      LOG.error(msg);
      throw new IOException(msg); 
    }
    
  }
  
  /**
   * Iterate over each of the {@link FormatConfirmable} objects,
   * potentially checking with the user whether it should be formatted.
   * 
   * If running in interactive mode, will prompt the user for each
   * directory to allow them to format anyway. Otherwise, returns
   * false, unless 'force' is specified.
   * 
   * @param force format regardless of whether dirs exist
   * @param interactive prompt the user when a dir exists
   * @return true if formatting should proceed
   * @throws IOException if some storage cannot be accessed
   */
  public static boolean confirmFormat(
      Iterable<? extends FormatConfirmable> items,
      boolean force, boolean interactive) throws IOException {
    for (FormatConfirmable item : items) {
      if (!item.hasSomeData())
        continue;
      if (force) { // Don't confirm, always format.
        System.err.println(
            "Data exists in " + item + ". Formatting anyway.");
        continue;
      }
      if (!interactive) { // Don't ask - always don't format
        System.err.println(
            "Running in non-interactive mode, and data appears to exist in " +
            item + ". Not formatting.");
        return false;
      }
      if (!ToolRunner.confirmPrompt("Re-format filesystem in " + item + " ?")) {
        System.err.println("Format aborted in " + item);
        return false;
      }
    }
    
    return true;
  }
  
  /**
   * Interface for classes which need to have the user confirm their
   * formatting during NameNode -format and other similar operations.
   * 
   * This is currently a storage directory or journal manager.
   */
  @InterfaceAudience.Private
  public interface FormatConfirmable {
    /**
     * @return true if the storage seems to have some valid data in it,
     * and the user should be required to confirm the format. Otherwise,
     * false.
     * @throws IOException if the storage cannot be accessed at all.
     */
    public boolean hasSomeData() throws IOException;
    
    /**
     * @return a string representation of the formattable item, suitable
     * for display to the user inside a prompt
     */
    public String toString();
  }
  
  /**
   * Set common storage fields into the given properties object.
   * Should be overloaded if additional fields need to be set.
   * 
   * @param props the Properties object to write into
   */
  protected void setPropertiesFromFields(Properties props, 
                                         StorageDirectory sd)
      throws IOException {
    props.setProperty("layoutVersion", String.valueOf(layoutVersion));
    props.setProperty("storageType", storageType.toString());
    props.setProperty("namespaceID", String.valueOf(namespaceID));
    // Set clusterID in version with federation support
    if (versionSupportsFederation(getServiceLayoutFeatureMap())) {
      props.setProperty("clusterID", clusterID);
    }
    props.setProperty("cTime", String.valueOf(cTime));
  }

  /**
   * Write properties to the VERSION file in the given storage directory.
   */
  public void writeProperties(StorageDirectory sd) throws IOException {
    writeProperties(sd.getVersionFile(), sd);
  }
  
  public void writeProperties(File to, StorageDirectory sd) throws IOException {
    Properties props = new Properties();
    setPropertiesFromFields(props, sd);
    writeProperties(to, sd, props);
  }

  public static void writeProperties(File to, StorageDirectory sd,
      Properties props) throws IOException {
    RandomAccessFile file = new RandomAccessFile(to, "rws");
    FileOutputStream out = null;
    try {
      file.seek(0);
      out = new FileOutputStream(file.getFD());
      /*
       * If server is interrupted before this line, 
       * the version file will remain unchanged.
       */
      props.store(out, null);
      /*
       * Now the new fields are flushed to the head of the file, but file 
       * length can still be larger then required and therefore the file can 
       * contain whole or corrupted fields from its old contents in the end.
       * If server is interrupted here and restarted later these extra fields
       * either should not effect server behavior or should be handled
       * by the server correctly.
       */
      file.setLength(out.getChannel().position());
    } finally {
      if (out != null) {
        out.close();
      }
      file.close();
    }
  }

  public static void rename(File from, File to) throws IOException {
    try {
      NativeIO.renameTo(from, to);
    } catch (NativeIOException e) {
      throw new IOException("Failed to rename " + from.getCanonicalPath()
        + " to " + to.getCanonicalPath() + " due to failure in native rename. "
        + e.toString());
    }
  }

  /**
   * Copies a file (usually large) to a new location using native unbuffered IO.
   * <p>
   * This method copies the contents of the specified source file
   * to the specified destination file using OS specific unbuffered IO.
   * The goal is to avoid churning the file system buffer cache when copying
   * large files.
   *
   * We can't use FileUtils#copyFile from apache-commons-io because it
   * is a buffered IO based on FileChannel#transferFrom, which uses MmapByteBuffer
   * internally.
   *
   * The directory holding the destination file is created if it does not exist.
   * If the destination file exists, then this method will delete it first.
   * <p>
   * <strong>Note:</strong> Setting <code>preserveFileDate</code> to
   * {@code true} tries to preserve the file's last modified
   * date/times using {@link File#setLastModified(long)}, however it is
   * not guaranteed that the operation will succeed.
   * If the modification operation fails, no indication is provided.
   *
   * @param srcFile  an existing file to copy, must not be {@code null}
   * @param destFile  the new file, must not be {@code null}
   * @param preserveFileDate  true if the file date of the copy
   *  should be the same as the original
   *
   * @throws NullPointerException if source or destination is {@code null}
   * @throws IOException if source or destination is invalid
   * @throws IOException if an IO error occurs during copying
   */
  public static void nativeCopyFileUnbuffered(File srcFile, File destFile,
      boolean preserveFileDate) throws IOException {
    if (srcFile == null) {
      throw new NullPointerException("Source must not be null");
    }
    if (destFile == null) {
      throw new NullPointerException("Destination must not be null");
    }
    if (srcFile.exists() == false) {
      throw new FileNotFoundException("Source '" + srcFile + "' does not exist");
    }
    if (srcFile.isDirectory()) {
      throw new IOException("Source '" + srcFile + "' exists but is a directory");
    }
    if (srcFile.getCanonicalPath().equals(destFile.getCanonicalPath())) {
      throw new IOException("Source '" + srcFile + "' and destination '" +
          destFile + "' are the same");
    }
    File parentFile = destFile.getParentFile();
    if (parentFile != null) {
      if (!parentFile.mkdirs() && !parentFile.isDirectory()) {
        throw new IOException("Destination '" + parentFile
            + "' directory cannot be created");
      }
    }
    if (destFile.exists()) {
      if (FileUtil.canWrite(destFile) == false) {
        throw new IOException("Destination '" + destFile
            + "' exists but is read-only");
      } else {
        if (destFile.delete() == false) {
          throw new IOException("Destination '" + destFile
              + "' exists but cannot be deleted");
        }
      }
    }
    try {
      NativeIO.copyFileUnbuffered(srcFile, destFile);
    } catch (NativeIOException e) {
      throw new IOException("Failed to copy " + srcFile.getCanonicalPath()
          + " to " + destFile.getCanonicalPath()
          + " due to failure in NativeIO#copyFileUnbuffered(). "
          + e.toString());
    }
    if (srcFile.length() != destFile.length()) {
      throw new IOException("Failed to copy full contents from '" + srcFile
          + "' to '" + destFile + "'");
    }
    if (preserveFileDate) {
      if (destFile.setLastModified(srcFile.lastModified()) == false) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Failed to preserve last modified date from'" + srcFile
            + "' to '" + destFile + "'");
        }
      }
    }
  }

  /**
   * Recursively delete all the content of the directory first and then 
   * the directory itself from the local filesystem.
   * @param dir The directory to delete
   * @throws IOException
   */
  public static void deleteDir(File dir) throws IOException {
    if (!FileUtil.fullyDelete(dir))
      throw new IOException("Failed to delete " + dir.getCanonicalPath());
  }
  
  /**
   * Write all data storage files.
   * @throws IOException
   */
  public void writeAll() throws IOException {
    this.layoutVersion = getServiceLayoutVersion();
    for (Iterator<StorageDirectory> it = storageDirs.iterator(); it.hasNext();) {
      writeProperties(it.next());
    }
  }

  /**
   * Unlock all storage directories.
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

  public static String getRegistrationID(StorageInfo storage) {
    return "NS-" + Integer.toString(storage.getNamespaceID())
      + "-" + storage.getClusterID()
      + "-" + Long.toString(storage.getCTime());
  }
  
  public static boolean is203LayoutVersion(int layoutVersion) {
    for (int lv203 : LAYOUT_VERSIONS_203) {
      if (lv203 == layoutVersion) {
        return true;
      }
    }
    return false;
  }
}
