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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.LightWeightResizableGSet;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class is used by datanodes to maintain meta data of its replicas.
 * It provides a general interface for meta information of a replica.
 */
@InterfaceAudience.Private
abstract public class ReplicaInfo extends Block
    implements Replica, LightWeightResizableGSet.LinkedElement {

  /** For implementing {@link LightWeightResizableGSet.LinkedElement} interface */
  private LightWeightResizableGSet.LinkedElement next;

  /** volume where the replica belongs */
  private FsVolumeSpi volume;
  
  /** directory where block & meta files belong */
  
  /**
   * Base directory containing numerically-identified sub directories and
   * possibly blocks.
   */
  private File baseDir;
  
  /**
   * Whether or not this replica's parent directory includes subdirs, in which
   * case we can generate them based on the replica's block ID
   */
  private boolean hasSubdirs;
  
  private static final Map<String, File> internedBaseDirs = new HashMap<String, File>();

  /** This is used by some tests and FsDatasetUtil#computeChecksum. */
  private static final FileIoProvider DEFAULT_FILE_IO_PROVIDER =
      new FileIoProvider(null, null);

  /**
   * Constructor
   * @param block a block
   * @param vol volume where replica is located
   * @param dir directory path where block and meta files are located
   */
  ReplicaInfo(Block block, FsVolumeSpi vol, File dir) {
    this(block.getBlockId(), block.getNumBytes(), 
        block.getGenerationStamp(), vol, dir);
  }
  
  /**
   * Constructor
   * @param blockId block id
   * @param len replica length
   * @param genStamp replica generation stamp
   * @param vol volume where replica is located
   * @param dir directory path where block and meta files are located
   */
  ReplicaInfo(long blockId, long len, long genStamp,
      FsVolumeSpi vol, File dir) {
    super(blockId, len, genStamp);
    this.volume = vol;
    setDirInternal(dir);
  }

  /**
   * Copy constructor.
   * @param from where to copy from
   */
  ReplicaInfo(ReplicaInfo from) {
    this(from, from.getVolume(), from.getDir());
  }
  
  /**
   * Get the full path of this replica's data file
   * @return the full path of this replica's data file
   */
  public File getBlockFile() {
    return new File(getDir(), getBlockName());
  }
  
  /**
   * Get the full path of this replica's meta file
   * @return the full path of this replica's meta file
   */
  public File getMetaFile() {
    return new File(getDir(),
        DatanodeUtil.getMetaName(getBlockName(), getGenerationStamp()));
  }
  
  /**
   * Get the volume where this replica is located on disk
   * @return the volume where this replica is located on disk
   */
  public FsVolumeSpi getVolume() {
    return volume;
  }

  /**
   * Get the {@link FileIoProvider} for disk IO operations.
   */
  public FileIoProvider getFileIoProvider() {
    // In tests and when invoked via FsDatasetUtil#computeChecksum, the
    // target volume for this replica may be unknown and hence null.
    // Use the DEFAULT_FILE_IO_PROVIDER with no-op hooks.
    return (volume != null) ? volume.getFileIoProvider()
        : DEFAULT_FILE_IO_PROVIDER;
  }

  /**
   * Set the volume where this replica is located on disk
   */
  void setVolume(FsVolumeSpi vol) {
    this.volume = vol;
  }

  /**
   * Get the storageUuid of the volume that stores this replica.
   */
  @Override
  public String getStorageUuid() {
    return volume.getStorageID();
  }
  
  /**
   * Return the parent directory path where this replica is located
   * @return the parent directory path where this replica is located
   */
  File getDir() {
    return hasSubdirs ? DatanodeUtil.idToBlockDir(baseDir,
        getBlockId()) : baseDir;
  }

  /**
   * Set the parent directory where this replica is located
   * @param dir the parent directory where the replica is located
   */
  public void setDir(File dir) {
    setDirInternal(dir);
  }

  private void setDirInternal(File dir) {
    if (dir == null) {
      baseDir = null;
      return;
    }

    ReplicaDirInfo dirInfo = parseBaseDir(dir);
    this.hasSubdirs = dirInfo.hasSubidrs;
    
    synchronized (internedBaseDirs) {
      if (!internedBaseDirs.containsKey(dirInfo.baseDirPath)) {
        // Create a new String path of this file and make a brand new File object
        // to guarantee we drop the reference to the underlying char[] storage.
        File baseDir = new File(dirInfo.baseDirPath);
        internedBaseDirs.put(dirInfo.baseDirPath, baseDir);
      }
      this.baseDir = internedBaseDirs.get(dirInfo.baseDirPath);
    }
  }

  @VisibleForTesting
  public static class ReplicaDirInfo {
    public String baseDirPath;
    public boolean hasSubidrs;

    public ReplicaDirInfo (String baseDirPath, boolean hasSubidrs) {
      this.baseDirPath = baseDirPath;
      this.hasSubidrs = hasSubidrs;
    }
  }
  
  @VisibleForTesting
  public static ReplicaDirInfo parseBaseDir(File dir) {
    
    File currentDir = dir;
    boolean hasSubdirs = false;
    while (currentDir.getName().startsWith(DataStorage.BLOCK_SUBDIR_PREFIX)) {
      hasSubdirs = true;
      currentDir = currentDir.getParentFile();
    }
    
    return new ReplicaDirInfo(currentDir.getAbsolutePath(), hasSubdirs);
  }

  /**
   * Number of bytes reserved for this replica on disk.
   */
  public long getBytesReserved() {
    return 0;
  }

  /**
   * Number of bytes originally reserved for this replica. The actual
   * reservation is adjusted as data is written to disk.
   *
   * @return the number of bytes originally reserved for this replica.
   */
  public long getOriginalBytesReserved() {
    return 0;
  }

  /**
   * Copy specified file into a temporary file. Then rename the
   * temporary file to the original name. This will cause any
   * hardlinks to the original file to be removed. The temporary
   * files are created in the same directory. The temporary files will
   * be recovered (especially on Windows) on datanode restart.
   */
  private void breakHardlinks(File file, Block b) throws IOException {
    final FileIoProvider fileIoProvider = getFileIoProvider();
    final File tmpFile = DatanodeUtil.createFileWithExistsCheck(
        getVolume(), b, DatanodeUtil.getUnlinkTmpFile(file), fileIoProvider);
    try {
      try (FileInputStream in = fileIoProvider.getFileInputStream(
        getVolume(), file)) {
        try (FileOutputStream out = fileIoProvider.getFileOutputStream(
            getVolume(), tmpFile)) {
          IOUtils.copyBytes(in, out, 16 * 1024);
        }
        if (file.length() != tmpFile.length()) {
          throw new IOException("Copy of file " + file + " size "
              + file.length() + " into file " + tmpFile
              + " resulted in a size of " + tmpFile.length());
        }
      }
      fileIoProvider.replaceFile(getVolume(), tmpFile, file);
    } catch (IOException e) {
      DataNode.LOG.error("Cannot breakHardlinks for file " + file, e);
      if (!fileIoProvider.delete(getVolume(), tmpFile)) {
        DataNode.LOG.info("detachFile failed to delete temporary file " +
            tmpFile);
      }
      throw e;
    }
  }

  /**
   * This function "breaks hardlinks" to the current replica file.
   *
   * When doing a DataNode upgrade, we create a bunch of hardlinks to each block
   * file.  This cleverly ensures that both the old and the new storage
   * directories can contain the same block file, without using additional space
   * for the data.
   *
   * However, when we want to append to the replica file, we need to "break" the
   * hardlink to ensure that the old snapshot continues to contain the old data
   * length.  If we failed to do that, we could roll back to the previous/
   * directory during a downgrade, and find that the block contents were longer
   * than they were at the time of upgrade.
   *
   * @return true only if data was copied.
   * @throws IOException
   */
  public boolean breakHardLinksIfNeeded() throws IOException {
    File file = getBlockFile();
    if (file == null || getVolume() == null) {
      throw new IOException("detachBlock:Block not found. " + this);
    }
    File meta = getMetaFile();

    int linkCount = getHardLinkCount(file);
    if (linkCount > 1) {
      DataNode.LOG.info("Breaking hardlink for " + linkCount + "x-linked " +
          "block " + this);
      breakHardlinks(file, this);
    }
    if (getHardLinkCount(meta) > 1) {
      breakHardlinks(meta, this);
    }
    return true;
  }

  @Override  //Object
  public String toString() {
    return getClass().getSimpleName()
        + ", " + super.toString()
        + ", " + getState()
        + "\n  getNumBytes()     = " + getNumBytes()
        + "\n  getBytesOnDisk()  = " + getBytesOnDisk()
        + "\n  getVisibleLength()= " + getVisibleLength()
        + "\n  getVolume()       = " + getVolume()
        + "\n  getBlockFile()    = " + getBlockFile();
  }

  @Override
  public boolean isOnTransientStorage() {
    return volume.isTransientStorage();
  }

  @Override
  public LightWeightResizableGSet.LinkedElement getNext() {
    return next;
  }

  @Override
  public void setNext(LightWeightResizableGSet.LinkedElement next) {
    this.next = next;
  }

  int getHardLinkCount(File fileName) throws IOException {
    return getFileIoProvider().getHardLinkCount(getVolume(), fileName);
  }
}
