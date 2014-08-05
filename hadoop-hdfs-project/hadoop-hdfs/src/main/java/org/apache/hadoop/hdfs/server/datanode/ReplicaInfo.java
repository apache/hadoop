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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.HardLink;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.io.IOUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class is used by datanodes to maintain meta data of its replicas.
 * It provides a general interface for meta information of a replica.
 */
@InterfaceAudience.Private
abstract public class ReplicaInfo extends Block implements Replica {
  
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

  /**
   * Constructor for a zero length replica
   * @param blockId block id
   * @param genStamp replica generation stamp
   * @param vol volume where replica is located
   * @param dir directory path where block and meta files are located
   */
  ReplicaInfo(long blockId, long genStamp, FsVolumeSpi vol, File dir) {
    this( blockId, 0L, genStamp, vol, dir);
  }
  
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
   * check if this replica has already been unlinked.
   * @return true if the replica has already been unlinked 
   *         or no need to be detached; false otherwise
   */
  public boolean isUnlinked() {
    return true;                // no need to be unlinked
  }

  /**
   * set that this replica is unlinked
   */
  public void setUnlinked() {
    // no need to be unlinked
  }
  
   /**
   * Copy specified file into a temporary file. Then rename the
   * temporary file to the original name. This will cause any
   * hardlinks to the original file to be removed. The temporary
   * files are created in the same directory. The temporary files will
   * be recovered (especially on Windows) on datanode restart.
   */
  private void unlinkFile(File file, Block b) throws IOException {
    File tmpFile = DatanodeUtil.createTmpFile(b, DatanodeUtil.getUnlinkTmpFile(file));
    try {
      FileInputStream in = new FileInputStream(file);
      try {
        FileOutputStream out = new FileOutputStream(tmpFile);
        try {
          IOUtils.copyBytes(in, out, 16*1024);
        } finally {
          out.close();
        }
      } finally {
        in.close();
      }
      if (file.length() != tmpFile.length()) {
        throw new IOException("Copy of file " + file + " size " + file.length()+
                              " into file " + tmpFile +
                              " resulted in a size of " + tmpFile.length());
      }
      FileUtil.replaceFile(tmpFile, file);
    } catch (IOException e) {
      boolean done = tmpFile.delete();
      if (!done) {
        DataNode.LOG.info("detachFile failed to delete temporary file " +
                          tmpFile);
      }
      throw e;
    }
  }

  /**
   * Remove a hard link by copying the block to a temporary place and 
   * then moving it back
   * @param numLinks number of hard links
   * @return true if copy is successful; 
   *         false if it is already detached or no need to be detached
   * @throws IOException if there is any copy error
   */
  public boolean unlinkBlock(int numLinks) throws IOException {
    if (isUnlinked()) {
      return false;
    }
    File file = getBlockFile();
    if (file == null || getVolume() == null) {
      throw new IOException("detachBlock:Block not found. " + this);
    }
    File meta = getMetaFile();

    if (HardLink.getLinkCount(file) > numLinks) {
      DataNode.LOG.info("CopyOnWrite for block " + this);
      unlinkFile(file, this);
    }
    if (HardLink.getLinkCount(meta) > numLinks) {
      unlinkFile(meta, this);
    }
    setUnlinked();
    return true;
  }

  /**
   * Set this replica's generation stamp to be a newer one
   * @param newGS new generation stamp
   * @throws IOException is the new generation stamp is not greater than the current one
   */
  void setNewerGenerationStamp(long newGS) throws IOException {
    long curGS = getGenerationStamp();
    if (newGS <= curGS) {
      throw new IOException("New generation stamp (" + newGS 
          + ") must be greater than current one (" + curGS + ")");
    }
    setGenerationStamp(newGS);
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
}
