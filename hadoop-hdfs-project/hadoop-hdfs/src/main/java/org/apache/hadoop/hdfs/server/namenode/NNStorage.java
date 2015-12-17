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
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.util.PersistentLongFile;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * NNStorage is responsible for management of the StorageDirectories used by
 * the NameNode.
 */
@InterfaceAudience.Private
public class NNStorage extends Storage implements Closeable,
    StorageErrorReporter {
  static final String DEPRECATED_MESSAGE_DIGEST_PROPERTY = "imageMD5Digest";
  static final String LOCAL_URI_SCHEME = "file";

  //
  // The filenames used for storing the images
  //
  public enum NameNodeFile {
    IMAGE     ("fsimage"),
    TIME      ("fstime"), // from "old" pre-HDFS-1073 format
    SEEN_TXID ("seen_txid"),
    EDITS     ("edits"),
    IMAGE_NEW ("fsimage.ckpt"),
    IMAGE_ROLLBACK("fsimage_rollback"),
    EDITS_NEW ("edits.new"), // from "old" pre-HDFS-1073 format
    EDITS_INPROGRESS ("edits_inprogress"),
    EDITS_TMP ("edits_tmp"),
    IMAGE_LEGACY_OIV ("fsimage_legacy_oiv");  // For pre-PB format

    private String fileName = null;
    private NameNodeFile(String name) { this.fileName = name; }
    @VisibleForTesting
    public String getName() { return fileName; }
  }

  /**
   * Implementation of StorageDirType specific to namenode storage
   * A Storage directory could be of type IMAGE which stores only fsimage,
   * or of type EDITS which stores edits or of type IMAGE_AND_EDITS which
   * stores both fsimage and edits.
   */
  @VisibleForTesting
  public static enum NameNodeDirType implements StorageDirType {
    UNDEFINED,
    IMAGE,
    EDITS,
    IMAGE_AND_EDITS;

    @Override
    public StorageDirType getStorageDirType() {
      return this;
    }

    @Override
    public boolean isOfType(StorageDirType type) {
      if ((this == IMAGE_AND_EDITS) && (type == IMAGE || type == EDITS))
        return true;
      return this == type;
    }
  }

  protected String blockpoolID = ""; // id of the block pool
  
  /**
   * flag that controls if we try to restore failed storages
   */
  private boolean restoreFailedStorage = false;
  private final Object restorationLock = new Object();
  private boolean disablePreUpgradableLayoutCheck = false;


  /**
   * TxId of the last transaction that was included in the most
   * recent fsimage file. This does not include any transactions
   * that have since been written to the edit log.
   */
  protected volatile long mostRecentCheckpointTxId = HdfsConstants.INVALID_TXID;
  
  /**
   * Time of the last checkpoint, in milliseconds since the epoch.
   */
  private long mostRecentCheckpointTime = 0;

  /**
   * list of failed (and thus removed) storages
   */
  final protected List<StorageDirectory> removedStorageDirs
    = new CopyOnWriteArrayList<StorageDirectory>();

  /**
   * Properties from old layout versions that may be needed
   * during upgrade only.
   */
  private HashMap<String, String> deprecatedProperties;

  /**
   * Construct the NNStorage.
   * @param conf Namenode configuration.
   * @param imageDirs Directories the image can be stored in.
   * @param editsDirs Directories the editlog can be stored in.
   * @throws IOException if any directories are inaccessible.
   */
  public NNStorage(Configuration conf, 
                   Collection<URI> imageDirs, Collection<URI> editsDirs) 
      throws IOException {
    super(NodeType.NAME_NODE);

    storageDirs = new CopyOnWriteArrayList<StorageDirectory>();
    
    // this may modify the editsDirs, so copy before passing in
    setStorageDirectories(imageDirs, 
                          Lists.newArrayList(editsDirs),
                          FSNamesystem.getSharedEditsDirs(conf));
  }

  @Override // Storage
  public boolean isPreUpgradableLayout(StorageDirectory sd) throws IOException {
    if (disablePreUpgradableLayoutCheck) {
      return false;
    }

    File oldImageDir = new File(sd.getRoot(), "image");
    if (!oldImageDir.exists()) {
      return false;
    }
    // check the layout version inside the image file
    File oldF = new File(oldImageDir, "fsimage");
    RandomAccessFile oldFile = new RandomAccessFile(oldF, "rws");
    try {
      oldFile.seek(0);
      int oldVersion = oldFile.readInt();
      oldFile.close();
      oldFile = null;
      if (oldVersion < LAST_PRE_UPGRADE_LAYOUT_VERSION)
        return false;
    } finally {
      IOUtils.cleanup(LOG, oldFile);
    }
    return true;
  }

  @Override // Closeable
  public void close() throws IOException {
    unlockAll();
    storageDirs.clear();
  }

  /**
   * Set flag whether an attempt should be made to restore failed storage
   * directories at the next available oppurtuinity.
   *
   * @param val Whether restoration attempt should be made.
   */
  void setRestoreFailedStorage(boolean val) {
    LOG.warn("set restore failed storage to " + val);
    restoreFailedStorage=val;
  }

  /**
   * @return Whether failed storage directories are to be restored.
   */
  boolean getRestoreFailedStorage() {
    return restoreFailedStorage;
  }

  /**
   * See if any of removed storages is "writable" again, and can be returned
   * into service.
   */
  void attemptRestoreRemovedStorage() {
    // if directory is "alive" - copy the images there...
    if(!restoreFailedStorage || removedStorageDirs.size() == 0)
      return; //nothing to restore

    /* We don't want more than one thread trying to restore at a time */
    synchronized (this.restorationLock) {
      LOG.info("NNStorage.attemptRestoreRemovedStorage: check removed(failed) "+
               "storarge. removedStorages size = " + removedStorageDirs.size());
      for(Iterator<StorageDirectory> it
            = this.removedStorageDirs.iterator(); it.hasNext();) {
        StorageDirectory sd = it.next();
        File root = sd.getRoot();
        LOG.info("currently disabled dir " + root.getAbsolutePath() +
                 "; type="+sd.getStorageDirType() 
                 + ";canwrite="+FileUtil.canWrite(root));
        if(root.exists() && FileUtil.canWrite(root)) {
          LOG.info("restoring dir " + sd.getRoot().getAbsolutePath());
          this.addStorageDir(sd); // restore
          this.removedStorageDirs.remove(sd);
        }
      }
    }
  }

  /**
   * @return A list of storage directories which are in the errored state.
   */
  List<StorageDirectory> getRemovedStorageDirs() {
    return this.removedStorageDirs;
  }
  
  /**
   * See {@link NNStorage#setStorageDirectories(Collection, Collection, Collection)}
   */
  @VisibleForTesting
  synchronized void setStorageDirectories(Collection<URI> fsNameDirs,
                                          Collection<URI> fsEditsDirs)
      throws IOException {
    setStorageDirectories(fsNameDirs, fsEditsDirs, new ArrayList<URI>());
  }

  /**
   * Set the storage directories which will be used. This should only ever be
   * called from inside NNStorage. However, it needs to remain package private
   * for testing, as StorageDirectories need to be reinitialised after using
   * Mockito.spy() on this class, as Mockito doesn't work well with inner
   * classes, such as StorageDirectory in this case.
   *
   * Synchronized due to initialization of storageDirs and removedStorageDirs.
   *
   * @param fsNameDirs Locations to store images.
   * @param fsEditsDirs Locations to store edit logs.
   * @throws IOException
   */
  @VisibleForTesting
  synchronized void setStorageDirectories(Collection<URI> fsNameDirs,
                                          Collection<URI> fsEditsDirs,
                                          Collection<URI> sharedEditsDirs)
      throws IOException {
    this.storageDirs.clear();
    this.removedStorageDirs.clear();

   // Add all name dirs with appropriate NameNodeDirType
    for (URI dirName : fsNameDirs) {
      checkSchemeConsistency(dirName);
      boolean isAlsoEdits = false;
      for (URI editsDirName : fsEditsDirs) {
        if (editsDirName.compareTo(dirName) == 0) {
          isAlsoEdits = true;
          fsEditsDirs.remove(editsDirName);
          break;
        }
      }
      NameNodeDirType dirType = (isAlsoEdits) ?
                          NameNodeDirType.IMAGE_AND_EDITS :
                          NameNodeDirType.IMAGE;
      // Add to the list of storage directories, only if the
      // URI is of type file://
      if(dirName.getScheme().compareTo("file") == 0) {
        this.addStorageDir(new StorageDirectory(new File(dirName.getPath()),
            dirType,
            sharedEditsDirs.contains(dirName))); // Don't lock the dir if it's shared.
      }
    }

    // Add edits dirs if they are different from name dirs
    for (URI dirName : fsEditsDirs) {
      checkSchemeConsistency(dirName);
      // Add to the list of storage directories, only if the
      // URI is of type file://
      if(dirName.getScheme().compareTo("file") == 0)
        this.addStorageDir(new StorageDirectory(new File(dirName.getPath()),
                    NameNodeDirType.EDITS, sharedEditsDirs.contains(dirName)));
    }
  }

  /**
   * Return the storage directory corresponding to the passed URI
   * @param uri URI of a storage directory
   * @return The matching storage directory or null if none found
   */
  StorageDirectory getStorageDirectory(URI uri) {
    try {
      uri = Util.fileAsURI(new File(uri));
      Iterator<StorageDirectory> it = dirIterator();
      for (; it.hasNext(); ) {
        StorageDirectory sd = it.next();
        if (Util.fileAsURI(sd.getRoot()).equals(uri)) {
          return sd;
        }
      }
    } catch (IOException ioe) {
      LOG.warn("Error converting file to URI", ioe);
    }
    return null;
  }

  /**
   * Checks the consistency of a URI, in particular if the scheme
   * is specified 
   * @param u URI whose consistency is being checked.
   */
  private static void checkSchemeConsistency(URI u) throws IOException {
    String scheme = u.getScheme();
    // the URI should have a proper scheme
    if(scheme == null) {
      throw new IOException("Undefined scheme for " + u);
    }
  }

  /**
   * Retrieve current directories of type IMAGE
   * @return Collection of URI representing image directories
   * @throws IOException in case of URI processing error
   */
  Collection<URI> getImageDirectories() throws IOException {
    return getDirectories(NameNodeDirType.IMAGE);
  }

  /**
   * Retrieve current directories of type EDITS
   * @return Collection of URI representing edits directories
   * @throws IOException in case of URI processing error
   */
  Collection<URI> getEditsDirectories() throws IOException {
    return getDirectories(NameNodeDirType.EDITS);
  }

  /**
   * Return number of storage directories of the given type.
   * @param dirType directory type
   * @return number of storage directories of type dirType
   */
  int getNumStorageDirs(NameNodeDirType dirType) {
    if(dirType == null)
      return getNumStorageDirs();
    Iterator<StorageDirectory> it = dirIterator(dirType);
    int numDirs = 0;
    for(; it.hasNext(); it.next())
      numDirs++;
    return numDirs;
  }

  /**
   * Return the list of locations being used for a specific purpose.
   * i.e. Image or edit log storage.
   *
   * @param dirType Purpose of locations requested.
   * @throws IOException
   */
  Collection<URI> getDirectories(NameNodeDirType dirType)
      throws IOException {
    ArrayList<URI> list = new ArrayList<URI>();
    Iterator<StorageDirectory> it = (dirType == null) ? dirIterator() :
                                    dirIterator(dirType);
    for ( ;it.hasNext(); ) {
      StorageDirectory sd = it.next();
      try {
        list.add(Util.fileAsURI(sd.getRoot()));
      } catch (IOException e) {
        throw new IOException("Exception while processing " +
            "StorageDirectory " + sd.getRoot(), e);
      }
    }
    return list;
  }
  
  /**
   * Determine the last transaction ID noted in this storage directory.
   * This txid is stored in a special seen_txid file since it might not
   * correspond to the latest image or edit log. For example, an image-only
   * directory will have this txid incremented when edits logs roll, even
   * though the edits logs are in a different directory.
   *
   * @param sd StorageDirectory to check
   * @return If file exists and can be read, last recorded txid. If not, 0L.
   * @throws IOException On errors processing file pointed to by sd
   */
  static long readTransactionIdFile(StorageDirectory sd) throws IOException {
    File txidFile = getStorageFile(sd, NameNodeFile.SEEN_TXID);
    return PersistentLongFile.readFile(txidFile, 0);
  }
  
  /**
   * Write last checkpoint time into a separate file.
   * @param sd storage directory
   * @throws IOException
   */
  void writeTransactionIdFile(StorageDirectory sd, long txid) throws IOException {
    Preconditions.checkArgument(txid >= 0, "bad txid: " + txid);
    
    File txIdFile = getStorageFile(sd, NameNodeFile.SEEN_TXID);
    PersistentLongFile.writeFile(txIdFile, txid);
  }

  /**
   * Set the transaction ID and time of the last checkpoint
   * 
   * @param txid transaction id of the last checkpoint
   * @param time time of the last checkpoint, in millis since the epoch
   */
  void setMostRecentCheckpointInfo(long txid, long time) {
    this.mostRecentCheckpointTxId = txid;
    this.mostRecentCheckpointTime = time;
  }

  /**
   * @return the transaction ID of the last checkpoint.
   */
  public long getMostRecentCheckpointTxId() {
    return mostRecentCheckpointTxId;
  }
  
  /**
   * @return the time of the most recent checkpoint in millis since the epoch.
   */
  long getMostRecentCheckpointTime() {
    return mostRecentCheckpointTime;
  }

  /**
   * Write a small file in all available storage directories that
   * indicates that the namespace has reached some given transaction ID.
   * 
   * This is used when the image is loaded to avoid accidental rollbacks
   * in the case where an edit log is fully deleted but there is no
   * checkpoint. See TestNameEditsConfigs.testNameEditsConfigsFailure()
   * @param txid the txid that has been reached
   */
  public void writeTransactionIdFileToStorage(long txid) {
    writeTransactionIdFileToStorage(txid, null);
  }

  /**
   * Write a small file in all available storage directories that
   * indicates that the namespace has reached some given transaction ID.
   *
   * This is used when the image is loaded to avoid accidental rollbacks
   * in the case where an edit log is fully deleted but there is no
   * checkpoint. See TestNameEditsConfigs.testNameEditsConfigsFailure()
   * @param txid the txid that has been reached
   * @param type the type of directory
   */
  public void writeTransactionIdFileToStorage(long txid,
      NameNodeDirType type) {
    // Write txid marker in all storage directories
    for (Iterator<StorageDirectory> it = dirIterator(type); it.hasNext();) {
      StorageDirectory sd = it.next();
      try {
        writeTransactionIdFile(sd, txid);
      } catch(IOException e) {
        // Close any edits stream associated with this dir and remove directory
        LOG.warn("writeTransactionIdToStorage failed on " + sd,
            e);
        reportErrorsOnDirectory(sd);
      }
    }
  }

  /**
   * Return the name of the image file that is uploaded by periodic
   * checkpointing
   *
   * @return List of filenames to save checkpoints to.
   */
  public File[] getFsImageNameCheckpoint(long txid) {
    ArrayList<File> list = new ArrayList<File>();
    for (Iterator<StorageDirectory> it =
                 dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      list.add(getStorageFile(it.next(), NameNodeFile.IMAGE_NEW, txid));
    }
    return list.toArray(new File[list.size()]);
  }

  /**
   * @return The first image file with the given txid and image type.
   */
  public File getFsImageName(long txid, NameNodeFile nnf) {
    for (Iterator<StorageDirectory> it = dirIterator(NameNodeDirType.IMAGE);
        it.hasNext();) {
      StorageDirectory sd = it.next();
      File fsImage = getStorageFile(sd, nnf, txid);
      if (FileUtil.canRead(sd.getRoot()) && fsImage.exists()) {
        return fsImage;
      }
    }
    return null;
  }

  /**
   * @return The first image file whose txid is the same with the given txid and
   * image type is one of the given types.
   */
  public File getFsImage(long txid, EnumSet<NameNodeFile> nnfs) {
    for (Iterator<StorageDirectory> it = dirIterator(NameNodeDirType.IMAGE);
        it.hasNext();) {
      StorageDirectory sd = it.next();
      for (NameNodeFile nnf : nnfs) {
        File fsImage = getStorageFile(sd, nnf, txid);
        if (FileUtil.canRead(sd.getRoot()) && fsImage.exists()) {
          return fsImage;
        }
      }
    }
    return null;
  }

  public File getFsImageName(long txid) {
    return getFsImageName(txid, NameNodeFile.IMAGE);
  }

  public File getHighestFsImageName() {
    return getFsImageName(getMostRecentCheckpointTxId());
  }

  /** Create new dfs name directory.  Caution: this destroys all files
   * in this filesystem. */
  private void format(StorageDirectory sd) throws IOException {
    sd.clearDirectory(); // create currrent dir
    writeProperties(sd);
    writeTransactionIdFile(sd, 0);

    LOG.info("Storage directory " + sd.getRoot()
             + " has been successfully formatted.");
  }

  /**
   * Format all available storage directories.
   */
  public void format(NamespaceInfo nsInfo) throws IOException {
    Preconditions.checkArgument(nsInfo.getLayoutVersion() == 0 ||
        nsInfo.getLayoutVersion() == HdfsConstants.NAMENODE_LAYOUT_VERSION,
        "Bad layout version: %s", nsInfo.getLayoutVersion());
    
    this.setStorageInfo(nsInfo);
    this.blockpoolID = nsInfo.getBlockPoolID();
    for (Iterator<StorageDirectory> it =
                           dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      format(sd);
    }
  }
  
  public static NamespaceInfo newNamespaceInfo()
      throws UnknownHostException {
    return new NamespaceInfo(newNamespaceID(), newClusterID(),
        newBlockPoolID(), 0L);
  }
  
  public void format() throws IOException {
    this.layoutVersion = HdfsConstants.NAMENODE_LAYOUT_VERSION;
    for (Iterator<StorageDirectory> it =
                           dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      format(sd);
    }
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
  private static int newNamespaceID() {
    int newID = 0;
    while(newID == 0)
      newID = DFSUtil.getRandom().nextInt(0x7FFFFFFF);  // use 31 bits only
    return newID;
  }

  @Override // Storage
  protected void setFieldsFromProperties(
      Properties props, StorageDirectory sd) throws IOException {
    super.setFieldsFromProperties(props, sd);
    if (layoutVersion == 0) {
      throw new IOException("NameNode directory "
                            + sd.getRoot() + " is not formatted.");
    }

    // Set Block pool ID in version with federation support
    if (NameNodeLayoutVersion.supports(
        LayoutVersion.Feature.FEDERATION, getLayoutVersion())) {
      String sbpid = props.getProperty("blockpoolID");
      setBlockPoolID(sd.getRoot(), sbpid);
    }
    setDeprecatedPropertiesForUpgrade(props);
  }

  void readProperties(StorageDirectory sd, StartupOption startupOption)
      throws IOException {
    Properties props = readPropertiesFile(sd.getVersionFile());
    if (HdfsServerConstants.RollingUpgradeStartupOption.ROLLBACK.matches
        (startupOption)) {
      int lv = Integer.parseInt(getProperty(props, sd, "layoutVersion"));
      if (lv > getServiceLayoutVersion()) {
        // we should not use a newer version for rollingUpgrade rollback
        throw new IncorrectVersionException(getServiceLayoutVersion(), lv,
            "storage directory " + sd.getRoot().getAbsolutePath());
      }
      props.setProperty("layoutVersion",
          Integer.toString(HdfsConstants.NAMENODE_LAYOUT_VERSION));
    }
    setFieldsFromProperties(props, sd);
  }

  /**
   * Pull any properties out of the VERSION file that are from older
   * versions of HDFS and only necessary during upgrade.
   */
  private void setDeprecatedPropertiesForUpgrade(Properties props) {
    deprecatedProperties = new HashMap<String, String>();
    String md5 = props.getProperty(DEPRECATED_MESSAGE_DIGEST_PROPERTY);
    if (md5 != null) {
      deprecatedProperties.put(DEPRECATED_MESSAGE_DIGEST_PROPERTY, md5);
    }
  }
  
  /**
   * Return a property that was stored in an earlier version of HDFS.
   * 
   * This should only be used during upgrades.
   */
  String getDeprecatedProperty(String prop) {
    assert getLayoutVersion() > HdfsConstants.NAMENODE_LAYOUT_VERSION :
      "getDeprecatedProperty should only be done when loading " +
      "storage from past versions during upgrade.";
    return deprecatedProperties.get(prop);
  }

  /**
   * Write version file into the storage directory.
   *
   * The version file should always be written last.
   * Missing or corrupted version file indicates that
   * the checkpoint is not valid.
   *
   * @param sd storage directory
   * @throws IOException
   */
  @Override // Storage
  protected void setPropertiesFromFields(Properties props,
                           StorageDirectory sd
                           ) throws IOException {
    super.setPropertiesFromFields(props, sd);
    // Set blockpoolID in version with federation support
    if (NameNodeLayoutVersion.supports(
        LayoutVersion.Feature.FEDERATION, getLayoutVersion())) {
      props.setProperty("blockpoolID", blockpoolID);
    }
  }
  
  static File getStorageFile(StorageDirectory sd, NameNodeFile type, long imageTxId) {
    return new File(sd.getCurrentDir(),
                    String.format("%s_%019d", type.getName(), imageTxId));
  }
  
  /**
   * Get a storage file for one of the files that doesn't need a txid associated
   * (e.g version, seen_txid)
   */
  static File getStorageFile(StorageDirectory sd, NameNodeFile type) {
    return new File(sd.getCurrentDir(), type.getName());
  }

  @VisibleForTesting
  public static String getCheckpointImageFileName(long txid) {
    return getNameNodeFileName(NameNodeFile.IMAGE_NEW, txid);
  }

  @VisibleForTesting
  public static String getImageFileName(long txid) {
    return getNameNodeFileName(NameNodeFile.IMAGE, txid);
  }

  @VisibleForTesting
  public static String getRollbackImageFileName(long txid) {
    return getNameNodeFileName(NameNodeFile.IMAGE_ROLLBACK, txid);
  }

  public static String getLegacyOIVImageFileName(long txid) {
    return getNameNodeFileName(NameNodeFile.IMAGE_LEGACY_OIV, txid);
  }

  private static String getNameNodeFileName(NameNodeFile nnf, long txid) {
    return String.format("%s_%019d", nnf.getName(), txid);
  }

  @VisibleForTesting
  public static String getInProgressEditsFileName(long startTxId) {
    return getNameNodeFileName(NameNodeFile.EDITS_INPROGRESS, startTxId);
  }
  
  static File getInProgressEditsFile(StorageDirectory sd, long startTxId) {
    return new File(sd.getCurrentDir(), getInProgressEditsFileName(startTxId));
  }
  
  static File getFinalizedEditsFile(StorageDirectory sd,
      long startTxId, long endTxId) {
    return new File(sd.getCurrentDir(),
        getFinalizedEditsFileName(startTxId, endTxId));
  }

  static File getTemporaryEditsFile(StorageDirectory sd,
      long startTxId, long endTxId, long timestamp) {
    return new File(sd.getCurrentDir(),
        getTemporaryEditsFileName(startTxId, endTxId, timestamp));
  }

  static File getImageFile(StorageDirectory sd, NameNodeFile nnf, long txid) {
    return new File(sd.getCurrentDir(), getNameNodeFileName(nnf, txid));
  }

  @VisibleForTesting
  public static String getFinalizedEditsFileName(long startTxId, long endTxId) {
    return String.format("%s_%019d-%019d", NameNodeFile.EDITS.getName(),
                         startTxId, endTxId);
  }

  public static String getTemporaryEditsFileName(long startTxId, long endTxId,
      long timestamp) {
    return String.format("%s_%019d-%019d_%019d", NameNodeFile.EDITS_TMP.getName(),
                         startTxId, endTxId, timestamp);
  }
  
  /**
   * Return the first readable finalized edits file for the given txid.
   */
  File findFinalizedEditsFile(long startTxId, long endTxId)
  throws IOException {
    File ret = findFile(NameNodeDirType.EDITS,
        getFinalizedEditsFileName(startTxId, endTxId));
    if (ret == null) {
      throw new IOException(
          "No edits file for txid " + startTxId + "-" + endTxId + " exists!");
    }
    return ret;
  }
    
  /**
   * Return the first readable image file for the given txid and image type, or
   * null if no such image can be found
   */
  File findImageFile(NameNodeFile nnf, long txid) {
    return findFile(NameNodeDirType.IMAGE,
        getNameNodeFileName(nnf, txid));
  }

  /**
   * Return the first readable storage file of the given name
   * across any of the 'current' directories in SDs of the
   * given type, or null if no such file exists.
   */
  private File findFile(NameNodeDirType dirType, String name) {
    for (StorageDirectory sd : dirIterable(dirType)) {
      File candidate = new File(sd.getCurrentDir(), name);
      if (FileUtil.canRead(sd.getCurrentDir()) &&
          candidate.exists()) {
        return candidate;
      }
    }
    return null;
  }

  /**
   * Disable the check for pre-upgradable layouts. Needed for BackupImage.
   * @param val Whether to disable the preupgradeable layout check.
   */
  void setDisablePreUpgradableLayoutCheck(boolean val) {
    disablePreUpgradableLayoutCheck = val;
  }

  /**
   * Marks a list of directories as having experienced an error.
   *
   * @param sds A list of storage directories to mark as errored.
   */
  void reportErrorsOnDirectories(List<StorageDirectory> sds) {
    for (StorageDirectory sd : sds) {
      reportErrorsOnDirectory(sd);
    }
  }

  /**
   * Reports that a directory has experienced an error.
   * Notifies listeners that the directory is no longer
   * available.
   *
   * @param sd A storage directory to mark as errored.
   */
  private void reportErrorsOnDirectory(StorageDirectory sd) {
    LOG.error("Error reported on storage directory " + sd);

    String lsd = listStorageDirectories();
    LOG.debug("current list of storage dirs:" + lsd);

    LOG.warn("About to remove corresponding storage: "
             + sd.getRoot().getAbsolutePath());
    try {
      sd.unlock();
    } catch (Exception e) {
      LOG.warn("Unable to unlock bad storage directory: "
               +  sd.getRoot().getPath(), e);
    }

    if (this.storageDirs.remove(sd)) {
      this.removedStorageDirs.add(sd);
    }
    
    lsd = listStorageDirectories();
    LOG.debug("at the end current list of storage dirs:" + lsd);
  }
  
  /** 
   * Processes the startup options for the clusterid and blockpoolid 
   * for the upgrade. 
   * @param startOpt Startup options 
   * @param layoutVersion Layout version for the upgrade 
   * @throws IOException
   */
  void processStartupOptionsForUpgrade(StartupOption startOpt, int layoutVersion)
      throws IOException {
    if (startOpt == StartupOption.UPGRADE || startOpt == StartupOption.UPGRADEONLY) {
      // If upgrade from a release that does not support federation,
      // if clusterId is provided in the startupOptions use it.
      // Else generate a new cluster ID      
      if (!NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.FEDERATION, layoutVersion)) {
        if (startOpt.getClusterId() == null) {
          startOpt.setClusterId(newClusterID());
        }
        setClusterID(startOpt.getClusterId());
        setBlockPoolID(newBlockPoolID());
      } else {
        // Upgrade from one version of federation to another supported
        // version of federation doesn't require clusterID.
        // Warn the user if the current clusterid didn't match with the input
        // clusterid.
        if (startOpt.getClusterId() != null
            && !startOpt.getClusterId().equals(getClusterID())) {
          LOG.warn("Clusterid mismatch - current clusterid: " + getClusterID()
              + ", Ignoring given clusterid: " + startOpt.getClusterId());
        }
      }
      LOG.info("Using clusterid: " + getClusterID());
    }
  }
  
  /**
   * Report that an IOE has occurred on some file which may
   * or may not be within one of the NN image storage directories.
   */
  @Override
  public void reportErrorOnFile(File f) {
    // We use getAbsolutePath here instead of getCanonicalPath since we know
    // that there is some IO problem on that drive.
    // getCanonicalPath may need to call stat() or readlink() and it's likely
    // those calls would fail due to the same underlying IO problem.
    String absPath = f.getAbsolutePath();
    for (StorageDirectory sd : storageDirs) {
      String dirPath = sd.getRoot().getAbsolutePath();
      if (!dirPath.endsWith(File.separator)) {
        dirPath += File.separator;
      }
      if (absPath.startsWith(dirPath)) {
        reportErrorsOnDirectory(sd);
        return;
      }
    }
    
  }
  
  /**
   * Generate new clusterID.
   * 
   * clusterID is a persistent attribute of the cluster.
   * It is generated when the cluster is created and remains the same
   * during the life cycle of the cluster.  When a new name node is formated, if 
   * this is a new cluster, a new clusterID is geneated and stored.  Subsequent 
   * name node must be given the same ClusterID during its format to be in the 
   * same cluster.
   * When a datanode register it receive the clusterID and stick with it.
   * If at any point, name node or data node tries to join another cluster, it 
   * will be rejected.
   * 
   * @return new clusterID
   */ 
  public static String newClusterID() {
    return "CID-" + UUID.randomUUID().toString();
  }

  void setClusterID(String cid) {
    clusterID = cid;
  }

  /**
   * try to find current cluster id in the VERSION files
   * returns first cluster id found in any VERSION file
   * null in case none found
   * @return clusterId or null in case no cluster id found
   */
  public String determineClusterId() {
    String cid = null;
    Iterator<StorageDirectory> sdit = dirIterator(NameNodeDirType.IMAGE);
    while(sdit.hasNext()) {
      StorageDirectory sd = sdit.next();
      try {
        Properties props = readPropertiesFile(sd.getVersionFile());
        cid = props.getProperty("clusterID");
        LOG.info("current cluster id for sd="+sd.getCurrentDir() + 
            ";lv=" + layoutVersion + ";cid=" + cid);
        
        if(cid != null && !cid.equals(""))
          return cid;
      } catch (Exception e) {
        LOG.warn("this sd not available: " + e.getLocalizedMessage());
      } //ignore
    }
    LOG.warn("couldn't find any VERSION file containing valid ClusterId");
    return null;
  }

  /**
   * Generate new blockpoolID.
   * 
   * @return new blockpoolID
   */ 
  static String newBlockPoolID() throws UnknownHostException{
    String ip = "unknownIP";
    try {
      ip = DNS.getDefaultIP("default");
    } catch (UnknownHostException e) {
      LOG.warn("Could not find ip address of \"default\" inteface.");
      throw e;
    }
    
    int rand = DFSUtil.getSecureRandom().nextInt(Integer.MAX_VALUE);
    String bpid = "BP-" + rand + "-"+ ip + "-" + Time.now();
    return bpid;
  }

  /** Validate and set block pool ID */
  public void setBlockPoolID(String bpid) {
    blockpoolID = bpid;
  }

  /** Validate and set block pool ID */
  private void setBlockPoolID(File storage, String bpid)
      throws InconsistentFSStateException {
    if (bpid == null || bpid.equals("")) {
      throw new InconsistentFSStateException(storage, "file "
          + Storage.STORAGE_FILE_VERSION + " has no block pool Id.");
    }
    
    if (!blockpoolID.equals("") && !blockpoolID.equals(bpid)) {
      throw new InconsistentFSStateException(storage,
          "Unexepcted blockpoolID " + bpid + " . Expected " + blockpoolID);
    }
    setBlockPoolID(bpid);
  }
  
  public String getBlockPoolID() {
    return blockpoolID;
  }

  /**
   * Iterate over all current storage directories, inspecting them
   * with the given inspector.
   */
  void inspectStorageDirs(FSImageStorageInspector inspector)
      throws IOException {

    // Process each of the storage directories to find the pair of
    // newest image file and edit file
    for (Iterator<StorageDirectory> it = dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      inspector.inspectDirectory(sd);
    }
  }

  /**
   * Iterate over all of the storage dirs, reading their contents to determine
   * their layout versions. Returns an FSImageStorageInspector which has
   * inspected each directory.
   * 
   * <b>Note:</b> this can mutate the storage info fields (ctime, version, etc).
   * @throws IOException if no valid storage dirs are found or no valid layout version
   */
  FSImageStorageInspector readAndInspectDirs(EnumSet<NameNodeFile> fileTypes,
      StartupOption startupOption) throws IOException {
    Integer layoutVersion = null;
    boolean multipleLV = false;
    StringBuilder layoutVersions = new StringBuilder();

    // First determine what range of layout versions we're going to inspect
    for (Iterator<StorageDirectory> it = dirIterator(false);
         it.hasNext();) {
      StorageDirectory sd = it.next();
      if (!sd.getVersionFile().exists()) {
        FSImage.LOG.warn("Storage directory " + sd + " contains no VERSION file. Skipping...");
        continue;
      }
      readProperties(sd, startupOption); // sets layoutVersion
      int lv = getLayoutVersion();
      if (layoutVersion == null) {
        layoutVersion = Integer.valueOf(lv);
      } else if (!layoutVersion.equals(lv)) {
        multipleLV = true;
      }
      layoutVersions.append("(").append(sd.getRoot()).append(", ").append(lv).append(") ");
    }
    
    if (layoutVersion == null) {
      throw new IOException("No storage directories contained VERSION information");
    }
    if (multipleLV) {            
      throw new IOException(
          "Storage directories contain multiple layout versions: "
              + layoutVersions);
    }
    // If the storage directories are with the new layout version
    // (ie edits_<txnid>) then use the new inspector, which will ignore
    // the old format dirs.
    FSImageStorageInspector inspector;
    if (NameNodeLayoutVersion.supports(
        LayoutVersion.Feature.TXID_BASED_LAYOUT, getLayoutVersion())) {
      inspector = new FSImageTransactionalStorageInspector(fileTypes);
    } else {
      inspector = new FSImagePreTransactionalStorageInspector();
    }
    
    inspectStorageDirs(inspector);
    return inspector;
  }

  public NamespaceInfo getNamespaceInfo() {
    return new NamespaceInfo(
        getNamespaceID(),
        getClusterID(),
        getBlockPoolID(),
        getCTime());
  }
}
