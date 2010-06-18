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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.DU;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.ReplicaState;
import org.apache.hadoop.io.IOUtils;

/**************************************************
 * FSDataset manages a set of data blocks.  Each block
 * has a unique name and an extent on disk.
 *
 ***************************************************/
@InterfaceAudience.Private
public class FSDataset implements FSConstants, FSDatasetInterface {


  /**
   * A node type that can be built into a tree reflecting the
   * hierarchy of blocks on the local disk.
   */
  class FSDir {
    File dir;
    int numBlocks = 0;
    FSDir children[];
    int lastChildIdx = 0;
    /**
     */
    public FSDir(File dir) 
      throws IOException {
      this.dir = dir;
      this.children = null;
      if (!dir.exists()) {
        if (!dir.mkdirs()) {
          throw new IOException("Mkdirs failed to create " + 
                                dir.toString());
        }
      } else {
        File[] files = dir.listFiles();
        int numChildren = 0;
        for (int idx = 0; idx < files.length; idx++) {
          if (files[idx].isDirectory()) {
            numChildren++;
          } else if (Block.isBlockFilename(files[idx])) {
            numBlocks++;
          }
        }
        if (numChildren > 0) {
          children = new FSDir[numChildren];
          int curdir = 0;
          for (int idx = 0; idx < files.length; idx++) {
            if (files[idx].isDirectory()) {
              children[curdir] = new FSDir(files[idx]);
              curdir++;
            }
          }
        }
      }
    }
        
    public File addBlock(Block b, File src) throws IOException {
      //First try without creating subdirectories
      File file = addBlock(b, src, false, false);          
      return (file != null) ? file : addBlock(b, src, true, true);
    }

    private File addBlock(Block b, File src, boolean createOk, 
                          boolean resetIdx) throws IOException {
      if (numBlocks < maxBlocksPerDir) {
        File dest = new File(dir, b.getBlockName());
        File metaData = getMetaFile( src, b );
        File newmeta = getMetaFile(dest, b);
        if ( ! metaData.renameTo( newmeta ) ||
            ! src.renameTo( dest ) ) {
          throw new IOException( "could not move files for " + b +
                                 " from " + src + " to " + 
                                 dest.getAbsolutePath() + " or from"
                                 + metaData + " to " + newmeta);
        }
        if (DataNode.LOG.isDebugEnabled()) {
          DataNode.LOG.debug("addBlock: Moved " + metaData + " to " + newmeta);
          DataNode.LOG.debug("addBlock: Moved " + src + " to " + dest);
        }

        numBlocks += 1;
        return dest;
      }
            
      if (lastChildIdx < 0 && resetIdx) {
        //reset so that all children will be checked
        lastChildIdx = random.nextInt(children.length);              
      }
            
      if (lastChildIdx >= 0 && children != null) {
        //Check if any child-tree has room for a block.
        for (int i=0; i < children.length; i++) {
          int idx = (lastChildIdx + i)%children.length;
          File file = children[idx].addBlock(b, src, false, resetIdx);
          if (file != null) {
            lastChildIdx = idx;
            return file; 
          }
        }
        lastChildIdx = -1;
      }
            
      if (!createOk) {
        return null;
      }
            
      if (children == null || children.length == 0) {
        children = new FSDir[maxBlocksPerDir];
        for (int idx = 0; idx < maxBlocksPerDir; idx++) {
          children[idx] = new FSDir(new File(dir, DataStorage.BLOCK_SUBDIR_PREFIX+idx));
        }
      }
            
      //now pick a child randomly for creating a new set of subdirs.
      lastChildIdx = random.nextInt(children.length);
      return children[ lastChildIdx ].addBlock(b, src, true, false); 
    }

    void getVolumeMap(ReplicasMap volumeMap, FSVolume volume) 
    throws IOException {
      if (children != null) {
        for (int i = 0; i < children.length; i++) {
          children[i].getVolumeMap(volumeMap, volume);
        }
      }

      recoverTempUnlinkedBlock();
      volume.addToReplicasMap(volumeMap, dir, true);
    }
        
    /**
     * Recover unlinked tmp files on datanode restart. If the original block
     * does not exist, then the tmp file is renamed to be the
     * original file name; otherwise the tmp file is deleted.
     */
    private void recoverTempUnlinkedBlock() throws IOException {
      File files[] = dir.listFiles();
      for (File file : files) {
        if (!FSDataset.isUnlinkTmpFile(file)) {
          continue;
        }
        File blockFile = getOrigFile(file);
        if (blockFile.exists()) {
          //
          // If the original block file still exists, then no recovery
          // is needed.
          //
          if (!file.delete()) {
            throw new IOException("Unable to cleanup unlinked tmp file " +
                file);
          }
        } else {
          if (!file.renameTo(blockFile)) {
            throw new IOException("Unable to cleanup detached file " +
                file);
          }
        }
      }
    }
    
    /**
     * check if a data diretory is healthy
     * @throws DiskErrorException
     */
    public void checkDirTree() throws DiskErrorException {
      DiskChecker.checkDir(dir);
            
      if (children != null) {
        for (int i = 0; i < children.length; i++) {
          children[i].checkDirTree();
        }
      }
    }
        
    void clearPath(File f) {
      String root = dir.getAbsolutePath();
      String dir = f.getAbsolutePath();
      if (dir.startsWith(root)) {
        String[] dirNames = dir.substring(root.length()).
          split(File.separator + "subdir");
        if (clearPath(f, dirNames, 1))
          return;
      }
      clearPath(f, null, -1);
    }
        
    /*
     * dirNames is an array of string integers derived from
     * usual directory structure data/subdirN/subdirXY/subdirM ...
     * If dirName array is non-null, we only check the child at 
     * the children[dirNames[idx]]. This avoids iterating over
     * children in common case. If directory structure changes 
     * in later versions, we need to revisit this.
     */
    private boolean clearPath(File f, String[] dirNames, int idx) {
      if ((dirNames == null || idx == dirNames.length) &&
          dir.compareTo(f) == 0) {
        numBlocks--;
        return true;
      }
          
      if (dirNames != null) {
        //guess the child index from the directory name
        if (idx > (dirNames.length - 1) || children == null) {
          return false;
        }
        int childIdx; 
        try {
          childIdx = Integer.parseInt(dirNames[idx]);
        } catch (NumberFormatException ignored) {
          // layout changed? we could print a warning.
          return false;
        }
        return (childIdx >= 0 && childIdx < children.length) ?
          children[childIdx].clearPath(f, dirNames, idx+1) : false;
      }

      //guesses failed. back to blind iteration.
      if (children != null) {
        for(int i=0; i < children.length; i++) {
          if (children[i].clearPath(f, null, -1)){
            return true;
          }
        }
      }
      return false;
    }
        
    public String toString() {
      return "FSDir{" +
        "dir=" + dir +
        ", children=" + (children == null ? null : Arrays.asList(children)) +
        "}";
    }
  }

  class FSVolume {
    private File currentDir;
    private FSDir dataDir;      // directory store Finalized replica
    private File rbwDir;        // directory store RBW replica
    private File tmpDir;        // directory store Temporary replica
    private DF usage;
    private DU dfsUsage;
    private long reserved;

    
    FSVolume(File currentDir, Configuration conf) throws IOException {
      this.reserved = conf.getLong("dfs.datanode.du.reserved", 0);
      this.currentDir = currentDir; 
      File parent = currentDir.getParentFile();
      final File finalizedDir = new File(
          currentDir, DataStorage.STORAGE_DIR_FINALIZED);

      // Files that were being written when the datanode was last shutdown
      // are now moved back to the data directory. It is possible that
      // in the future, we might want to do some sort of datanode-local
      // recovery for these blocks. For example, crc validation.
      //
      this.tmpDir = new File(parent, "tmp");
      if (tmpDir.exists()) {
        FileUtil.fullyDelete(tmpDir);
      }
      this.rbwDir = new File(currentDir, DataStorage.STORAGE_DIR_RBW);
      if (rbwDir.exists() && !supportAppends) {
        FileUtil.fullyDelete(rbwDir);
      }
      this.dataDir = new FSDir(finalizedDir);
      if (!rbwDir.mkdirs()) {  // create rbw directory if not exist
        if (!rbwDir.isDirectory()) {
          throw new IOException("Mkdirs failed to create " + rbwDir.toString());
        }
      }
      if (!tmpDir.mkdirs()) {
        if (!tmpDir.isDirectory()) {
          throw new IOException("Mkdirs failed to create " + tmpDir.toString());
        }
      }
      this.usage = new DF(parent, conf);
      this.dfsUsage = new DU(parent, conf);
      this.dfsUsage.start();
    }

    File getCurrentDir() {
      return currentDir;
    }
    
    void decDfsUsed(long value) {
      // The caller to this method (BlockFileDeleteTask.run()) does
      // not have locked FSDataset.this yet.
      synchronized(FSDataset.this) {
        dfsUsage.decDfsUsed(value);
      }
    }
    
    long getDfsUsed() throws IOException {
      return dfsUsage.getUsed();
    }
    
    /**
     * Calculate the capacity of the filesystem, after removing any
     * reserved capacity.
     * @return the unreserved number of bytes left in this filesystem. May be zero.
     */
    long getCapacity() throws IOException {
      long remaining = usage.getCapacity() - reserved;
      return remaining > 0 ? remaining : 0;
    }
      
    long getAvailable() throws IOException {
      long remaining = getCapacity()-getDfsUsed();
      long available = usage.getAvailable();
      if (remaining>available) {
        remaining = available;
      }
      return (remaining > 0) ? remaining : 0;
    }
      
    String getMount() throws IOException {
      return usage.getMount();
    }
      
    File getDir() {
      return dataDir.dir;
    }
    
    /**
     * Temporary files. They get moved to the finalized block directory when
     * the block is finalized.
     */
    File createTmpFile(Block b) throws IOException {
      File f = new File(tmpDir, b.getBlockName());
      return FSDataset.createTmpFile(b, f);
    }

    /**
     * RBW files. They get moved to the finalized block directory when
     * the block is finalized.
     */
    File createRbwFile(Block b) throws IOException {
      File f = new File(rbwDir, b.getBlockName());
      return FSDataset.createTmpFile(b, f);
    }

    File addBlock(Block b, File f) throws IOException {
      File blockFile = dataDir.addBlock(b, f);
      File metaFile = getMetaFile( blockFile , b);
      dfsUsage.incDfsUsed(b.getNumBytes()+metaFile.length());
      return blockFile;
    }
      
    void checkDirs() throws DiskErrorException {
      dataDir.checkDirTree();
      DiskChecker.checkDir(tmpDir);
      DiskChecker.checkDir(rbwDir);
    }
      
    void getVolumeMap(ReplicasMap volumeMap) throws IOException {
      // add finalized replicas
      dataDir.getVolumeMap(volumeMap, this);
      // add rbw replicas
      addToReplicasMap(volumeMap, rbwDir, false);
    }

    /**
     * Add replicas under the given directory to the volume map
     * @param volumeMap the replicas map
     * @param dir an input directory
     * @param isFinalized true if the directory has finalized replicas;
     *                    false if the directory has rbw replicas
     */
    private void addToReplicasMap(ReplicasMap volumeMap, 
        File dir, boolean isFinalized) {
      File blockFiles[] = dir.listFiles();
      for (File blockFile : blockFiles) {
        if (!Block.isBlockFilename(blockFile))
          continue;
        
        long genStamp = getGenerationStampFromFile(blockFiles, blockFile);
        long blockId = Block.filename2id(blockFile.getName());
        ReplicaInfo newReplica = null;
        if (isFinalized) {
          newReplica = new FinalizedReplica(blockId, 
              blockFile.length(), genStamp, this, blockFile.getParentFile());
        } else {
          newReplica = new ReplicaWaitingToBeRecovered(blockId,
              validateIntegrity(blockFile, genStamp), 
              genStamp, this, blockFile.getParentFile());
        }

        ReplicaInfo oldReplica = volumeMap.add(newReplica);
        if (oldReplica != null) {
          DataNode.LOG.warn("Two block files with the same block id exist " +
              "on disk: " + oldReplica.getBlockFile() +
              " and " + blockFile );
        }
      }
    }
    
    /**
     * Find out the number of bytes in the block that match its crc.
     * 
     * This algorithm assumes that data corruption caused by unexpected 
     * datanode shutdown occurs only in the last crc chunk. So it checks
     * only the last chunk.
     * 
     * @param blockFile the block file
     * @param genStamp generation stamp of the block
     * @return the number of valid bytes
     */
    private long validateIntegrity(File blockFile, long genStamp) {
      DataInputStream checksumIn = null;
      InputStream blockIn = null;
      try {
        File metaFile = new File(getMetaFileName(blockFile.toString(), genStamp));
        long blockFileLen = blockFile.length();
        long metaFileLen = metaFile.length();
        int crcHeaderLen = DataChecksum.getChecksumHeaderSize();
        if (!blockFile.exists() || blockFileLen == 0 ||
            !metaFile.exists() || metaFileLen < (long)crcHeaderLen) {
          return 0;
        }
        checksumIn = new DataInputStream(
            new BufferedInputStream(new FileInputStream(metaFile),
                BUFFER_SIZE));

        // read and handle the common header here. For now just a version
        BlockMetadataHeader header = BlockMetadataHeader.readHeader(checksumIn);
        short version = header.getVersion();
        if (version != FSDataset.METADATA_VERSION) {
          DataNode.LOG.warn("Wrong version (" + version + ") for metadata file "
              + metaFile + " ignoring ...");
        }
        DataChecksum checksum = header.getChecksum();
        int bytesPerChecksum = checksum.getBytesPerChecksum();
        int checksumSize = checksum.getChecksumSize();
        long numChunks = Math.min(
            (blockFileLen + bytesPerChecksum - 1)/bytesPerChecksum, 
            (metaFileLen - crcHeaderLen)/checksumSize);
        if (numChunks == 0) {
          return 0;
        }
        IOUtils.skipFully(checksumIn, (numChunks-1)*checksumSize);
        blockIn = new FileInputStream(blockFile);
        long lastChunkStartPos = (numChunks-1)*bytesPerChecksum;
        IOUtils.skipFully(blockIn, lastChunkStartPos);
        int lastChunkSize = (int)Math.min(
            bytesPerChecksum, blockFileLen-lastChunkStartPos);
        byte[] buf = new byte[lastChunkSize+checksumSize];
        checksumIn.readFully(buf, lastChunkSize, checksumSize);
        IOUtils.readFully(blockIn, buf, 0, lastChunkSize);

        checksum.update(buf, 0, lastChunkSize);
        if (checksum.compare(buf, lastChunkSize)) { // last chunk matches crc
          return lastChunkStartPos + lastChunkSize;
        } else { // last chunck is corrupt
          return lastChunkStartPos;
        }
      } catch (IOException e) {
        DataNode.LOG.warn(e);
        return 0;
      } finally {
        IOUtils.closeStream(checksumIn);
        IOUtils.closeStream(blockIn);
      }
    }
      
    void clearPath(File f) {
      dataDir.clearPath(f);
    }
      
    public String toString() {
      return getDir().getAbsolutePath();
    }
  }
    
  static class FSVolumeSet {
    FSVolume[] volumes = null;
    int curVolume = 0;
      
    FSVolumeSet(FSVolume[] volumes) {
      this.volumes = volumes;
    }
    
    private int numberOfVolumes() {
      return volumes.length;
    }
      
    synchronized FSVolume getNextVolume(long blockSize) throws IOException {
      
      if(volumes.length < 1) {
        throw new DiskOutOfSpaceException("No more available volumes");
      }
      
      // since volumes could've been removed because of the failure
      // make sure we are not out of bounds
      if(curVolume >= volumes.length) {
        curVolume = 0;
      }
      
      int startVolume = curVolume;
      
      while (true) {
        FSVolume volume = volumes[curVolume];
        curVolume = (curVolume + 1) % volumes.length;
        if (volume.getAvailable() > blockSize) { return volume; }
        if (curVolume == startVolume) {
          throw new DiskOutOfSpaceException("Insufficient space for an additional block");
        }
      }
    }
      
    long getDfsUsed() throws IOException {
      long dfsUsed = 0L;
      for (int idx = 0; idx < volumes.length; idx++) {
        dfsUsed += volumes[idx].getDfsUsed();
      }
      return dfsUsed;
    }

    long getCapacity() throws IOException {
      long capacity = 0L;
      for (int idx = 0; idx < volumes.length; idx++) {
        capacity += volumes[idx].getCapacity();
      }
      return capacity;
    }
      
    long getRemaining() throws IOException {
      long remaining = 0L;
      for (int idx = 0; idx < volumes.length; idx++) {
        remaining += volumes[idx].getAvailable();
      }
      return remaining;
    }
      
    synchronized void getVolumeMap(ReplicasMap volumeMap) throws IOException {
      for (int idx = 0; idx < volumes.length; idx++) {
        volumes[idx].getVolumeMap(volumeMap);
      }
    }
      
    /**
     * goes over all the volumes and checkDir eachone of them
     * if one throws DiskErrorException - removes from the list of active 
     * volumes. 
     * @return list of all the removed volumes
     */
    synchronized List<FSVolume> checkDirs() {
      
      ArrayList<FSVolume> removed_vols = null;  
      
      for (int idx = 0; idx < volumes.length; idx++) {
        FSVolume fsv = volumes[idx];
        try {
          fsv.checkDirs();
        } catch (DiskErrorException e) {
          DataNode.LOG.warn("Removing failed volume " + fsv + ": ",e);
          if(removed_vols == null) {
            removed_vols = new ArrayList<FSVolume>(1);
          }
          removed_vols.add(volumes[idx]);
          volumes[idx] = null; //remove the volume
        }
      }
      
      // repair array - copy non null elements
      int removed_size = (removed_vols==null)? 0 : removed_vols.size();
      if(removed_size > 0) {
        FSVolume fsvs[] = new FSVolume [volumes.length-removed_size];
        for(int idx=0,idy=0; idx<volumes.length; idx++) {
          if(volumes[idx] != null) {
            fsvs[idy] = volumes[idx];
            idy++;
          }
        }
        volumes = fsvs; // replace array of volumes
        DataNode.LOG.info("Completed FSVolumeSet.checkDirs. Removed "
            + removed_vols.size() + " volumes. List of current volumes: "
            + this);
      }

      return removed_vols;
    }
      
    public String toString() {
      StringBuilder sb = new StringBuilder();
      for (int idx = 0; idx < volumes.length; idx++) {
        sb.append(volumes[idx].toString());
        if (idx != volumes.length - 1) { sb.append(","); }
      }
      return sb.toString();
    }

    public boolean isValid(FSVolume volume) {
      for (int idx = 0; idx < volumes.length; idx++) {
        if (volumes[idx] == volume) {
          return true;
        }
      }
      return false;
    }
  }
  
  //////////////////////////////////////////////////////
  //
  // FSDataSet
  //
  //////////////////////////////////////////////////////

  //Find better place?
  public static final String METADATA_EXTENSION = ".meta";
  public static final short METADATA_VERSION = 1;
  static final String UNLINK_BLOCK_SUFFIX = ".unlinked";

  private static boolean isUnlinkTmpFile(File f) {
    String name = f.getName();
    return name.endsWith(UNLINK_BLOCK_SUFFIX);
  }
  
  static File getUnlinkTmpFile(File f) {
    return new File(f.getParentFile(), f.getName()+UNLINK_BLOCK_SUFFIX);
  }
  
  private static File getOrigFile(File unlinkTmpFile) {
    String fileName = unlinkTmpFile.getName();
    return new File(unlinkTmpFile.getParentFile(),
        fileName.substring(0, fileName.length()-UNLINK_BLOCK_SUFFIX.length()));
  }
  
  static String getMetaFileName(String blockFileName, long genStamp) {
    return blockFileName + "_" + genStamp + METADATA_EXTENSION;
  }
  
  static File getMetaFile(File f , Block b) {
    return new File(getMetaFileName(f.getAbsolutePath(),
                                    b.getGenerationStamp())); 
  }
  protected File getMetaFile(Block b) throws IOException {
    return getMetaFile(getBlockFile(b), b);
  }

  /** Find the metadata file for the specified block file.
   * Return the generation stamp from the name of the metafile.
   */
  private static long getGenerationStampFromFile(File[] listdir, File blockFile) {
    String blockName = blockFile.getName();
    for (int j = 0; j < listdir.length; j++) {
      String path = listdir[j].getName();
      if (!path.startsWith(blockName)) {
        continue;
      }
      if (blockFile == listdir[j]) {
        continue;
      }
      return Block.getGenerationStamp(listdir[j].getName());
    }
    DataNode.LOG.warn("Block " + blockFile + 
                      " does not have a metafile!");
    return GenerationStamp.GRANDFATHER_GENERATION_STAMP;
  }

  /** Find the corresponding meta data file from a given block file */
  private static File findMetaFile(final File blockFile) throws IOException {
    final String prefix = blockFile.getName() + "_";
    final File parent = blockFile.getParentFile();
    File[] matches = parent.listFiles(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return dir.equals(parent)
            && name.startsWith(prefix) && name.endsWith(METADATA_EXTENSION);
      }
    });

    if (matches == null || matches.length == 0) {
      throw new IOException("Meta file not found, blockFile=" + blockFile);
    }
    else if (matches.length > 1) {
      throw new IOException("Found more than one meta files: " 
          + Arrays.asList(matches));
    }
    return matches[0];
  }
  
  /** Find the corresponding meta data file from a given block file */
  private static long parseGenerationStamp(File blockFile, File metaFile
      ) throws IOException {
    String metaname = metaFile.getName();
    String gs = metaname.substring(blockFile.getName().length() + 1,
        metaname.length() - METADATA_EXTENSION.length());
    try {
      return Long.parseLong(gs);
    } catch(NumberFormatException nfe) {
      throw (IOException)new IOException("blockFile=" + blockFile
          + ", metaFile=" + metaFile).initCause(nfe);
    }
  }

  /** Return the block file for the given ID */ 
  public File findBlockFile(long blockId) {
    return getFile(blockId);
  }

  /** {@inheritDoc} */
  public synchronized Block getStoredBlock(long blkid) throws IOException {
    File blockfile = findBlockFile(blkid);
    if (blockfile == null) {
      return null;
    }
    File metafile = findMetaFile(blockfile);
    return new Block(blkid, blockfile.length(),
        parseGenerationStamp(blockfile, metafile));
  }

  /**
   * Returns a clone of a replica stored in data-node memory.
   * Should be primarily used for testing.
   * @param blockId
   * @return
   */
  synchronized ReplicaInfo fetchReplicaInfo(long blockId) {
    ReplicaInfo r = volumeMap.get(blockId);
    if(r == null)
      return null;
    switch(r.getState()) {
    case FINALIZED:
      return new FinalizedReplica((FinalizedReplica)r);
    case RBW:
      return new ReplicaBeingWritten((ReplicaBeingWritten)r);
    case RWR:
      return new ReplicaWaitingToBeRecovered((ReplicaWaitingToBeRecovered)r);
    case RUR:
      return new ReplicaUnderRecovery((ReplicaUnderRecovery)r);
    case TEMPORARY:
      return new ReplicaInPipeline((ReplicaInPipeline)r);
    }
    return null;
  }

  public boolean metaFileExists(Block b) throws IOException {
    return getMetaFile(b).exists();
  }
  
  public long getMetaDataLength(Block b) throws IOException {
    File checksumFile = getMetaFile( b );
    return checksumFile.length();
  }

  public MetaDataInputStream getMetaDataInputStream(Block b)
      throws IOException {
    File checksumFile = getMetaFile( b );
    return new MetaDataInputStream(new FileInputStream(checksumFile),
                                                    checksumFile.length());
  }

  static File createTmpFile(Block b, File f) throws IOException {
    if (f.exists()) {
      throw new IOException("Unexpected problem in creating temporary file for "+
                            b + ".  File " + f + " should not be present, but is.");
    }
    // Create the zero-length temp file
    //
    boolean fileCreated = false;
    try {
      fileCreated = f.createNewFile();
    } catch (IOException ioe) {
      throw (IOException)new IOException(DISK_ERROR +f).initCause(ioe);
    }
    if (!fileCreated) {
      throw new IOException("Unexpected problem in creating temporary file for "+
                            b + ".  File " + f + " should be creatable, but is already present.");
    }
    return f;
  }
    
  FSVolumeSet volumes;
  private int maxBlocksPerDir = 0;
  ReplicasMap volumeMap = new ReplicasMap();
  static  Random random = new Random();
  FSDatasetAsyncDiskService asyncDiskService;
  private int validVolsRequired;

  // Used for synchronizing access to usage stats
  private Object statsLock = new Object();

  boolean supportAppends = true;

  /**
   * An FSDataset has a directory where it loads its data files.
   */
  public FSDataset(DataStorage storage, Configuration conf) throws IOException {
    this.maxBlocksPerDir = conf.getInt("dfs.datanode.numblocks", 64);
    this.supportAppends = conf.getBoolean(DFSConfigKeys.DFS_SUPPORT_APPEND_KEY,
                                      DFSConfigKeys.DFS_SUPPORT_APPEND_DEFAULT);
    // The number of volumes required for operation is the total number 
    // of volumes minus the number of failed volumes we can tolerate.
    final int volFailuresTolerated =
      conf.getInt(DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY,
                  DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_DEFAULT);
    this.validVolsRequired = storage.getNumStorageDirs() - volFailuresTolerated; 
    if (validVolsRequired < 1 ||
        validVolsRequired > storage.getNumStorageDirs()) {
      DataNode.LOG.error("Invalid value " + volFailuresTolerated + " for " +
          DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY);
    }
    FSVolume[] volArray = new FSVolume[storage.getNumStorageDirs()];
    for (int idx = 0; idx < storage.getNumStorageDirs(); idx++) {
      volArray[idx] = new FSVolume(storage.getStorageDir(idx).getCurrentDir(), conf);
    }
    volumes = new FSVolumeSet(volArray);
    volumes.getVolumeMap(volumeMap);
    File[] roots = new File[storage.getNumStorageDirs()];
    for (int idx = 0; idx < storage.getNumStorageDirs(); idx++) {
      roots[idx] = storage.getStorageDir(idx).getCurrentDir();
    }
    asyncDiskService = new FSDatasetAsyncDiskService(roots);
    registerMBean(storage.getStorageID());
  }

  /**
   * Return the total space used by dfs datanode
   */
  public long getDfsUsed() throws IOException {
    synchronized(statsLock) {
      return volumes.getDfsUsed();
    }
  }

  /**
   * Return true - if there are still valid volumes on the DataNode. 
   */
  public boolean hasEnoughResource() {
    return volumes.numberOfVolumes() >= validVolsRequired; 
  }

  /**
   * Return total capacity, used and unused
   */
  public long getCapacity() throws IOException {
    synchronized(statsLock) {
      return volumes.getCapacity();
    }
  }

  /**
   * Return how many bytes can still be stored in the FSDataset
   */
  public long getRemaining() throws IOException {
    synchronized(statsLock) {
      return volumes.getRemaining();
    }
  }

  /**
   * Find the block's on-disk length
   */
  public long getLength(Block b) throws IOException {
    return getBlockFile(b).length();
  }

  /**
   * Get File name for a given block.
   */
  public synchronized File getBlockFile(Block b) throws IOException {
    File f = validateBlockFile(b);
    if(f == null) {
      if (InterDatanodeProtocol.LOG.isDebugEnabled()) {
        InterDatanodeProtocol.LOG.debug("b=" + b + ", volumeMap=" + volumeMap);
      }
      throw new IOException("Block " + b + " is not valid.");
    }
    return f;
  }
  
  public synchronized InputStream getBlockInputStream(Block b) throws IOException {
    return new FileInputStream(getBlockFile(b));
  }

  public synchronized InputStream getBlockInputStream(Block b, long seekOffset) throws IOException {

    File blockFile = getBlockFile(b);
    RandomAccessFile blockInFile = new RandomAccessFile(blockFile, "r");
    if (seekOffset > 0) {
      blockInFile.seek(seekOffset);
    }
    return new FileInputStream(blockInFile.getFD());
  }

  /**
   * Get the meta info of a block stored in volumeMap
   * @param b block
   * @return the meta replica information
   * @throws IOException if no entry is in the map or 
   *                        there is a generation stamp mismatch
   */
  private ReplicaInfo getReplicaInfo(Block b) throws IOException {
    ReplicaInfo info = volumeMap.get(b);
    if (info == null) {
      throw new IOException("Block " + b + " does not exist in volumeMap.");
    }
    return info;
  }
  
  /**
   * Returns handles to the block file and its metadata file
   */
  public synchronized BlockInputStreams getTmpInputStreams(Block b, 
                          long blkOffset, long ckoff) throws IOException {

    ReplicaInfo info = getReplicaInfo(b);
    File blockFile = info.getBlockFile();
    RandomAccessFile blockInFile = new RandomAccessFile(blockFile, "r");
    if (blkOffset > 0) {
      blockInFile.seek(blkOffset);
    }
    File metaFile = info.getMetaFile();
    RandomAccessFile metaInFile = new RandomAccessFile(metaFile, "r");
    if (ckoff > 0) {
      metaInFile.seek(ckoff);
    }
    return new BlockInputStreams(new FileInputStream(blockInFile.getFD()),
                                new FileInputStream(metaInFile.getFD()));
  }
    
  /**
   * Make a copy of the block if this block is linked to an existing
   * snapshot. This ensures that modifying this block does not modify
   * data in any existing snapshots.
   * @param block Block
   * @param numLinks Unlink if the number of links exceed this value
   * @throws IOException
   * @return - true if the specified block was unlinked or the block
   *           is not in any snapshot.
   */
  public boolean unlinkBlock(Block block, int numLinks) throws IOException {
    ReplicaInfo info = null;

    synchronized (this) {
      info = getReplicaInfo(block);
    }
   return info.unlinkBlock(numLinks);
  }

  static private void truncateBlock(File blockFile, File metaFile,
      long oldlen, long newlen) throws IOException {
    DataNode.LOG.info("truncateBlock: blockFile=" + blockFile
        + ", metaFile=" + metaFile
        + ", oldlen=" + oldlen
        + ", newlen=" + newlen);

    if (newlen == oldlen) {
      return;
    }
    if (newlen > oldlen) {
      throw new IOException("Cannout truncate block to from oldlen (=" + oldlen
          + ") to newlen (=" + newlen + ")");
    }

    DataChecksum dcs = BlockMetadataHeader.readHeader(metaFile).getChecksum(); 
    int checksumsize = dcs.getChecksumSize();
    int bpc = dcs.getBytesPerChecksum();
    long n = (newlen - 1)/bpc + 1;
    long newmetalen = BlockMetadataHeader.getHeaderSize() + n*checksumsize;
    long lastchunkoffset = (n - 1)*bpc;
    int lastchunksize = (int)(newlen - lastchunkoffset); 
    byte[] b = new byte[Math.max(lastchunksize, checksumsize)]; 

    RandomAccessFile blockRAF = new RandomAccessFile(blockFile, "rw");
    try {
      //truncate blockFile 
      blockRAF.setLength(newlen);
 
      //read last chunk
      blockRAF.seek(lastchunkoffset);
      blockRAF.readFully(b, 0, lastchunksize);
    } finally {
      blockRAF.close();
    }

    //compute checksum
    dcs.update(b, 0, lastchunksize);
    dcs.writeValue(b, 0, false);

    //update metaFile 
    RandomAccessFile metaRAF = new RandomAccessFile(metaFile, "rw");
    try {
      metaRAF.setLength(newmetalen);
      metaRAF.seek(newmetalen - checksumsize);
      metaRAF.write(b, 0, checksumsize);
    } finally {
      metaRAF.close();
    }
  }

  private final static String DISK_ERROR = "Possible disk error on file creation: ";
  /** Get the cause of an I/O exception if caused by a possible disk error
   * @param ioe an I/O exception
   * @return cause if the I/O exception is caused by a possible disk error;
   *         null otherwise.
   */ 
  static IOException getCauseIfDiskError(IOException ioe) {
    if (ioe.getMessage()!=null && ioe.getMessage().startsWith(DISK_ERROR)) {
      return (IOException)ioe.getCause();
    } else {
      return null;
    }
  }

  @Override  // FSDatasetInterface
  public synchronized ReplicaInPipelineInterface append(Block b,
      long newGS, long expectedBlockLen) throws IOException {
    // If the block was successfully finalized because all packets
    // were successfully processed at the Datanode but the ack for
    // some of the packets were not received by the client. The client 
    // re-opens the connection and retries sending those packets.
    // The other reason is that an "append" is occurring to this block.
    
    // check the validity of the parameter
    if (newGS < b.getGenerationStamp()) {
      throw new IOException("The new generation stamp " + newGS + 
          " should be greater than the replica " + b + "'s generation stamp");
    }
    ReplicaInfo replicaInfo = volumeMap.get(b);
    if (replicaInfo == null) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.NON_EXISTENT_REPLICA + b);
    }  
    DataNode.LOG.info("Appending to replica " + replicaInfo);
    if (replicaInfo.getState() != ReplicaState.FINALIZED) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.UNFINALIZED_REPLICA + b);
    }
    if (replicaInfo.getNumBytes() != expectedBlockLen) {
      throw new IOException("Corrupted replica " + replicaInfo + 
          " with a length of " + replicaInfo.getNumBytes() + 
          " expected length is " + expectedBlockLen);
    }

    return append((FinalizedReplica)replicaInfo, newGS, b.getNumBytes());
  }
  
  /** Append to a finalized replica
   * Change a finalized replica to be a RBW replica and 
   * bump its generation stamp to be the newGS
   * 
   * @param replicaInfo a finalized replica
   * @param newGS new generation stamp
   * @param estimateBlockLen estimate generation stamp
   * @return a RBW replica
   * @throws IOException if moving the replica from finalized directory 
   *         to rbw directory fails
   */
  private synchronized ReplicaBeingWritten append(FinalizedReplica replicaInfo, 
      long newGS, long estimateBlockLen) throws IOException {
    // unlink the finalized replica
    replicaInfo.unlinkBlock(1);
    
    // construct a RBW replica with the new GS
    File blkfile = replicaInfo.getBlockFile();
    FSVolume v = replicaInfo.getVolume();
    if (v.getAvailable() < estimateBlockLen - replicaInfo.getNumBytes()) {
      throw new DiskOutOfSpaceException("Insufficient space for appending to "
          + replicaInfo);
    }
    File newBlkFile = new File(v.rbwDir, replicaInfo.getBlockName());
    File oldmeta = replicaInfo.getMetaFile();
    ReplicaBeingWritten newReplicaInfo = new ReplicaBeingWritten(
        replicaInfo.getBlockId(), replicaInfo.getNumBytes(), newGS,
        v, newBlkFile.getParentFile(), Thread.currentThread());
    File newmeta = newReplicaInfo.getMetaFile();

    // rename meta file to rbw directory
    if (DataNode.LOG.isDebugEnabled()) {
      DataNode.LOG.debug("Renaming " + oldmeta + " to " + newmeta);
    }
    if (!oldmeta.renameTo(newmeta)) {
      throw new IOException("Block " + replicaInfo + " reopen failed. " +
                            " Unable to move meta file  " + oldmeta +
                            " to rbw dir " + newmeta);
    }

    // rename block file to rbw directory
    if (DataNode.LOG.isDebugEnabled()) {
      DataNode.LOG.debug("Renaming " + blkfile + " to " + newBlkFile);
      DataNode.LOG.debug("Old block file length is " + blkfile.length());
    }
    if (!blkfile.renameTo(newBlkFile)) {
      if (!newmeta.renameTo(oldmeta)) {  // restore the meta file
        DataNode.LOG.warn("Cannot move meta file " + newmeta + 
            "back to the finalized directory " + oldmeta);
      }
      throw new IOException("Block " + replicaInfo + " reopen failed. " +
                              " Unable to move block file " + blkfile +
                              " to rbw dir " + newBlkFile);
    }
    
    // Replace finalized replica by a RBW replica in replicas map
    volumeMap.add(newReplicaInfo);
    
    return newReplicaInfo;
  }

  private ReplicaInfo recoverCheck(Block b, long newGS, 
      long expectedBlockLen) throws IOException {
    ReplicaInfo replicaInfo = volumeMap.get(b.getBlockId());
    if (replicaInfo == null) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.NON_EXISTENT_REPLICA + b);
    }
    
    // check state
    if (replicaInfo.getState() != ReplicaState.FINALIZED &&
        replicaInfo.getState() != ReplicaState.RBW) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.UNFINALIZED_AND_NONRBW_REPLICA + replicaInfo);
    }

    // check generation stamp
    long replicaGenerationStamp = replicaInfo.getGenerationStamp();
    if (replicaGenerationStamp < b.getGenerationStamp() ||
        replicaGenerationStamp > newGS) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.UNEXPECTED_GS_REPLICA + replicaGenerationStamp
          + ". Expected GS range is [" + b.getGenerationStamp() + ", " + 
          newGS + "].");
    }
    
    // stop the previous writer before check a replica's length
    long replicaLen = replicaInfo.getNumBytes();
    if (replicaInfo.getState() == ReplicaState.RBW) {
      ReplicaBeingWritten rbw = (ReplicaBeingWritten)replicaInfo;
      // kill the previous writer
      rbw.stopWriter();
      rbw.setWriter(Thread.currentThread());
      // check length: bytesRcvd, bytesOnDisk, and bytesAcked should be the same
      if (replicaLen != rbw.getBytesOnDisk() 
          || replicaLen != rbw.getBytesAcked()) {
        throw new ReplicaAlreadyExistsException("RBW replica " + replicaInfo + 
            "bytesRcvd(" + rbw.getNumBytes() + "), bytesOnDisk(" + 
            rbw.getBytesOnDisk() + "), and bytesAcked(" + rbw.getBytesAcked() +
            ") are not the same.");
      }
    }
    
    // check block length
    if (replicaLen != expectedBlockLen) {
      throw new IOException("Corrupted replica " + replicaInfo + 
          " with a length of " + replicaLen + 
          " expected length is " + expectedBlockLen);
    }
    
    return replicaInfo;
  }
  @Override  // FSDatasetInterface
  public synchronized ReplicaInPipelineInterface recoverAppend(Block b,
      long newGS, long expectedBlockLen) throws IOException {
    DataNode.LOG.info("Recover failed append to " + b);

    ReplicaInfo replicaInfo = recoverCheck(b, newGS, expectedBlockLen);

    // change the replica's state/gs etc.
    if (replicaInfo.getState() == ReplicaState.FINALIZED ) {
      return append((FinalizedReplica)replicaInfo, newGS, b.getNumBytes());
    } else { //RBW
      bumpReplicaGS(replicaInfo, newGS);
      return (ReplicaBeingWritten)replicaInfo;
    }
  }

  @Override
  public void recoverClose(Block b, long newGS,
      long expectedBlockLen) throws IOException {
    DataNode.LOG.info("Recover failed close " + b);
    // check replica's state
    ReplicaInfo replicaInfo = recoverCheck(b, newGS, expectedBlockLen);
    // bump the replica's GS
    bumpReplicaGS(replicaInfo, newGS);
    // finalize the replica if RBW
    if (replicaInfo.getState() == ReplicaState.RBW) {
      finalizeBlock(replicaInfo);
    }
  }
  
  /**
   * Bump a replica's generation stamp to a new one.
   * Its on-disk meta file name is renamed to be the new one too.
   * 
   * @param replicaInfo a replica
   * @param newGS new generation stamp
   * @throws IOException if rename fails
   */
  private void bumpReplicaGS(ReplicaInfo replicaInfo, 
      long newGS) throws IOException { 
    long oldGS = replicaInfo.getGenerationStamp();
    File oldmeta = replicaInfo.getMetaFile();
    replicaInfo.setGenerationStamp(newGS);
    File newmeta = replicaInfo.getMetaFile();

    // rename meta file to new GS
    if (DataNode.LOG.isDebugEnabled()) {
      DataNode.LOG.debug("Renaming " + oldmeta + " to " + newmeta);
    }
    if (!oldmeta.renameTo(newmeta)) {
      replicaInfo.setGenerationStamp(oldGS); // restore old GS
      throw new IOException("Block " + (Block)replicaInfo + " reopen failed. " +
                            " Unable to move meta file  " + oldmeta +
                            " to " + newmeta);
    }
  }

  @Override
  public synchronized ReplicaInPipelineInterface createRbw(Block b)
      throws IOException {
    ReplicaInfo replicaInfo = volumeMap.get(b.getBlockId());
    if (replicaInfo != null) {
      throw new ReplicaAlreadyExistsException("Block " + b +
      " already exists in state " + replicaInfo.getState() +
      " and thus cannot be created.");
    }
    // create a new block
    FSVolume v = volumes.getNextVolume(b.getNumBytes());
    // create a rbw file to hold block in the designated volume
    File f = v.createRbwFile(b);
    ReplicaBeingWritten newReplicaInfo = new ReplicaBeingWritten(b.getBlockId(), 
        b.getGenerationStamp(), v, f.getParentFile());
    volumeMap.add(newReplicaInfo);
    return newReplicaInfo;
  }
  
  @Override
  public synchronized ReplicaInPipelineInterface recoverRbw(Block b,
      long newGS, long minBytesRcvd, long maxBytesRcvd)
      throws IOException {
    DataNode.LOG.info("Recover the RBW replica " + b);

    ReplicaInfo replicaInfo = volumeMap.get(b.getBlockId());
    if (replicaInfo == null) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.NON_EXISTENT_REPLICA + b);
    }
    
    // check the replica's state
    if (replicaInfo.getState() != ReplicaState.RBW) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.NON_RBW_REPLICA + replicaInfo);
    }
    ReplicaBeingWritten rbw = (ReplicaBeingWritten)replicaInfo;
    
    DataNode.LOG.info("Recovering replica " + rbw);

    // Stop the previous writer
    rbw.stopWriter();
    rbw.setWriter(Thread.currentThread());

    // check generation stamp
    long replicaGenerationStamp = rbw.getGenerationStamp();
    if (replicaGenerationStamp < b.getGenerationStamp() ||
        replicaGenerationStamp > newGS) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.UNEXPECTED_GS_REPLICA + b +
          ". Expected GS range is [" + b.getGenerationStamp() + ", " + 
          newGS + "].");
    }
    
    // check replica length
    if (rbw.getBytesAcked() < minBytesRcvd || rbw.getNumBytes() > maxBytesRcvd){
      throw new ReplicaNotFoundException("Unmatched length replica " + 
          replicaInfo + ": BytesAcked = " + rbw.getBytesAcked() + 
          " BytesRcvd = " + rbw.getNumBytes() + " are not in the range of [" + 
          minBytesRcvd + ", " + maxBytesRcvd + "].");
    }

    // bump the replica's generation stamp to newGS
    bumpReplicaGS(rbw, newGS);
    
    return rbw;
  }
  
  @Override
  public synchronized ReplicaInPipelineInterface createTemporary(Block b)
      throws IOException {
    ReplicaInfo replicaInfo = volumeMap.get(b.getBlockId());
    if (replicaInfo != null) {
      throw new ReplicaAlreadyExistsException("Block " + b +
          " already exists in state " + replicaInfo.getState() +
          " and thus cannot be created.");
    }
    
    FSVolume v = volumes.getNextVolume(b.getNumBytes());
    // create a temporary file to hold block in the designated volume
    File f = v.createTmpFile(b);
    ReplicaInPipeline newReplicaInfo = new ReplicaInPipeline(b.getBlockId(), 
        b.getGenerationStamp(), v, f.getParentFile());
    volumeMap.add(newReplicaInfo);
    
    return newReplicaInfo;
  }

  /**
   * Sets the offset in the meta file so that the
   * last checksum will be overwritten.
   */
  public void adjustCrcChannelPosition(Block b, BlockWriteStreams streams, 
      int checksumSize) throws IOException {
    FileOutputStream file = (FileOutputStream) streams.checksumOut;
    FileChannel channel = file.getChannel();
    long oldPos = channel.position();
    long newPos = oldPos - checksumSize;
    DataNode.LOG.info("Changing meta file offset of block " + b + " from " + 
        oldPos + " to " + newPos);
    channel.position(newPos);
  }

  synchronized File createTmpFile( FSVolume vol, Block blk ) throws IOException {
    if ( vol == null ) {
      vol = getReplicaInfo( blk ).getVolume();
      if ( vol == null ) {
        throw new IOException("Could not find volume for block " + blk);
      }
    }
    return vol.createTmpFile(blk);
  }

  //
  // REMIND - mjc - eventually we should have a timeout system
  // in place to clean up block files left by abandoned clients.
  // We should have some timer in place, so that if a blockfile
  // is created but non-valid, and has been idle for >48 hours,
  // we can GC it safely.
  //

  /**
   * Complete the block write!
   */
  public synchronized void finalizeBlock(Block b) throws IOException {
    ReplicaInfo replicaInfo = getReplicaInfo(b);
    if (replicaInfo.getState() == ReplicaState.FINALIZED) {
      // this is legal, when recovery happens on a file that has
      // been opened for append but never modified
      return;
    }
    finalizeReplica(replicaInfo);
  }
  
  private synchronized FinalizedReplica finalizeReplica(ReplicaInfo replicaInfo)
  throws IOException {
    FinalizedReplica newReplicaInfo = null;
    if (replicaInfo.getState() == ReplicaState.RUR &&
       ((ReplicaUnderRecovery)replicaInfo).getOrignalReplicaState() == 
         ReplicaState.FINALIZED) {
      newReplicaInfo = (FinalizedReplica)
             ((ReplicaUnderRecovery)replicaInfo).getOriginalReplica();
    } else {
      FSVolume v = replicaInfo.getVolume();
      File f = replicaInfo.getBlockFile();
      if (v == null) {
        throw new IOException("No volume for temporary file " + f + 
            " for block " + replicaInfo);
      }

      File dest = v.addBlock(replicaInfo, f);
      newReplicaInfo = new FinalizedReplica(replicaInfo, v, dest.getParentFile());
    }
    volumeMap.add(newReplicaInfo);
    return newReplicaInfo;
  }

  /**
   * Remove the temporary block file (if any)
   */
  public synchronized void unfinalizeBlock(Block b) throws IOException {
    ReplicaInfo replicaInfo = volumeMap.get(b);
    if (replicaInfo != null && replicaInfo.getState() == ReplicaState.TEMPORARY) {
      // remove from volumeMap
      volumeMap.remove(b);
      
      // delete the on-disk temp file
      if (delBlockFromDisk(replicaInfo.getBlockFile(), 
          replicaInfo.getMetaFile(), b)) {
        DataNode.LOG.warn("Block " + b + " unfinalized and removed. " );
      }
    }
  }

  /**
   * Remove a block from disk
   * @param blockFile block file
   * @param metaFile block meta file
   * @param b a block
   * @return true if on-disk files are deleted; false otherwise
   */
  private boolean delBlockFromDisk(File blockFile, File metaFile, Block b) {
    if (blockFile == null) {
      DataNode.LOG.warn("No file exists for block: " + b);
      return true;
    }
    
    if (!blockFile.delete()) {
      DataNode.LOG.warn("Not able to delete the block file: " + blockFile);
      return false;
    } else { // remove the meta file
      if (metaFile != null && !metaFile.delete()) {
        DataNode.LOG.warn(
            "Not able to delete the meta block file: " + metaFile);
        return false;
      }
    }
    return true;
  }

  /**
   * Generates a block report from the in-memory block map.
   */
  public BlockListAsLongs getBlockReport() {
    ArrayList<ReplicaInfo> finalized =
      new ArrayList<ReplicaInfo>(volumeMap.size());
    ArrayList<ReplicaInfo> uc = new ArrayList<ReplicaInfo>();
    synchronized(this) {
      for (ReplicaInfo b : volumeMap.replicas()) {
        switch(b.getState()) {
        case FINALIZED:
          finalized.add(b);
          break;
        case RBW:
        case RWR:
          uc.add(b);
          break;
        case RUR:
          ReplicaUnderRecovery rur = (ReplicaUnderRecovery)b;
          uc.add(rur.getOriginalReplica());
          break;
        case TEMPORARY:
          break;
        default:
          assert false : "Illegal ReplicaInfo state.";
        }
      }
      return new BlockListAsLongs(finalized, uc);
    }
  }

  /**
   * Get the block list from in-memory blockmap. Note if <deepcopy>
   * is false, reference to the block in the volumeMap is returned. This block
   * should not be changed. Suitable synchronization using {@link FSDataset}
   * is needed to handle concurrent modification to the block.
   */
  synchronized Block[] getBlockList(boolean deepcopy) {
    Block[] list = volumeMap.replicas().toArray(new Block[volumeMap.size()]);
    if (deepcopy) {
      for (int i = 0; i < list.length; i++) {
        list[i] = new Block(list[i]);
      }
    }
    return list;
  }

  /**
   * Get the list of finalized blocks from in-memory blockmap.
   */
  synchronized List<Block> getFinalizedBlocks() {
    ArrayList<Block> finalized = new ArrayList<Block>(volumeMap.size());
    for (ReplicaInfo b : volumeMap.replicas()) {
      if(b.getState() == ReplicaState.FINALIZED) {
        finalized.add(new Block(b));
      }
    }
    return finalized;
  }

  /**
   * Check whether the given block is a valid one.
   * valid means finalized
   */
  public boolean isValidBlock(Block b) {
    ReplicaInfo replicaInfo = volumeMap.get(b);
    if (replicaInfo == null || 
        replicaInfo.getState() != ReplicaState.FINALIZED) {
      return false;
    }
    return replicaInfo.getBlockFile().exists();
  }

  /**
   * Find the file corresponding to the block and return it if it exists.
   */
  File validateBlockFile(Block b) throws IOException {
    //Should we check for metadata file too?
    File f = getFile(b);
    
    if(f != null ) {
      if(f.exists())
        return f;
   
      // if file is not null, but doesn't exist - possibly disk failed
      DataNode datanode = DataNode.getDataNode();
      datanode.checkDiskError();
    }
    
    if (InterDatanodeProtocol.LOG.isDebugEnabled()) {
      InterDatanodeProtocol.LOG.debug("b=" + b + ", f=" + f);
    }
    return null;
  }

  /** Check the files of a replica. */
  static void checkReplicaFiles(final ReplicaInfo r) throws IOException {
    //check replica's file
    final File f = r.getBlockFile();
    if (!f.exists()) {
      throw new FileNotFoundException("File " + f + " not found, r=" + r);
    }
    if (r.getBytesOnDisk() != f.length()) {
      throw new IOException("File length mismatched.  The length of "
          + f + " is " + f.length() + " but r=" + r);
    }

    //check replica's meta file
    final File metafile = getMetaFile(f, r);
    if (!metafile.exists()) {
      throw new IOException("Metafile " + metafile + " does not exist, r=" + r);
    }
    if (metafile.length() == 0) {
      throw new IOException("Metafile " + metafile + " is empty, r=" + r);
    }
  }

  /**
   * We're informed that a block is no longer valid.  We
   * could lazily garbage-collect the block, but why bother?
   * just get rid of it.
   */
  public void invalidate(Block invalidBlks[]) throws IOException {
    boolean error = false;
    for (int i = 0; i < invalidBlks.length; i++) {
      File f = null;
      FSVolume v;
      synchronized (this) {
        f = getFile(invalidBlks[i]);
        ReplicaInfo dinfo = volumeMap.get(invalidBlks[i]);
        if (dinfo == null || 
            dinfo.getGenerationStamp() != invalidBlks[i].getGenerationStamp()) {
          DataNode.LOG.warn("Unexpected error trying to delete block "
                           + invalidBlks[i] + 
                           ". BlockInfo not found in volumeMap.");
          error = true;
          continue;
        }
        v = dinfo.getVolume();
        if (f == null) {
          DataNode.LOG.warn("Unexpected error trying to delete block "
                            + invalidBlks[i] + 
                            ". Block not found in blockMap." +
                            ((v == null) ? " " : " Block found in volumeMap."));
          error = true;
          continue;
        }
        if (v == null) {
          DataNode.LOG.warn("Unexpected error trying to delete block "
                            + invalidBlks[i] + 
                            ". No volume for this block." +
                            " Block found in blockMap. " + f + ".");
          error = true;
          continue;
        }
        File parent = f.getParentFile();
        if (parent == null) {
          DataNode.LOG.warn("Unexpected error trying to delete block "
                            + invalidBlks[i] + 
                            ". Parent not found for file " + f + ".");
          error = true;
          continue;
        }
        ReplicaState replicaState = dinfo.getState();
        if (replicaState == ReplicaState.FINALIZED || 
            (replicaState == ReplicaState.RUR && 
                ((ReplicaUnderRecovery)dinfo).getOrignalReplicaState() == 
                  ReplicaState.FINALIZED)) {
          v.clearPath(parent);
        }
        volumeMap.remove(invalidBlks[i]);
      }
      File metaFile = getMetaFile( f, invalidBlks[i] );
      long dfsBytes = f.length() + metaFile.length();
      
      // Delete the block asynchronously to make sure we can do it fast enough
      asyncDiskService.deleteAsync(v, f, metaFile, dfsBytes, invalidBlks[i].toString());
    }
    if (error) {
      throw new IOException("Error in deleting blocks.");
    }
  }

  /**
   * Turn the block identifier into a filename; ignore generation stamp!!!
   */
  public synchronized File getFile(Block b) {
    return getFile(b.getBlockId());
  }

  /**
   * Turn the block identifier into a filename
   * @param blockId a block's id
   * @return on disk data file path; null if the replica does not exist
   */
  private File getFile(long blockId) {
    ReplicaInfo info = volumeMap.get(blockId);
    if (info != null) {
      return info.getBlockFile();
    }
    return null;    
  }
  /**
   * check if a data directory is healthy
   * if some volumes failed - make sure to remove all the blocks that belong
   * to these volumes
   * @throws DiskErrorException
   */
  public void checkDataDir() throws DiskErrorException {
    long total_blocks=0, removed_blocks=0;
    List<FSVolume> failed_vols =  volumes.checkDirs();
    
    //if there no failed volumes return
    if(failed_vols == null) 
      return;
    
    // else 
    // remove related blocks
    long mlsec = System.currentTimeMillis();
    synchronized (this) {
      Iterator<ReplicaInfo> ib = volumeMap.replicas().iterator();
      while(ib.hasNext()) {
        ReplicaInfo b = ib.next();
        total_blocks ++;
        // check if the volume block belongs to still valid
        FSVolume vol = b.getVolume();
        for(FSVolume fv: failed_vols) {
          if(vol == fv) {
            DataNode.LOG.warn("removing block " + b.getBlockId() + " from vol " 
                + vol.dataDir.dir.getAbsolutePath());
            ib.remove();
            removed_blocks++;
            break;
          }
        }
      }
    } // end of sync
    mlsec = System.currentTimeMillis() - mlsec;
    DataNode.LOG.warn("Removed " + removed_blocks + " out of " + total_blocks +
        "(took " + mlsec + " millisecs)");

    // report the error
    StringBuilder sb = new StringBuilder();
    for(FSVolume fv : failed_vols) {
      sb.append(fv.dataDir.dir.getAbsolutePath() + ";");
    }

    throw  new DiskErrorException("DataNode failed volumes:" + sb);
  
  }
    

  public String toString() {
    return "FSDataset{dirpath='"+volumes+"'}";
  }

  private ObjectName mbeanName;
  private Random rand = new Random();
  
  /**
   * Register the FSDataset MBean using the name
   *        "hadoop:service=DataNode,name=FSDatasetState-<storageid>"
   */
  void registerMBean(final String storageId) {
    // We wrap to bypass standard mbean naming convetion.
    // This wraping can be removed in java 6 as it is more flexible in 
    // package naming for mbeans and their impl.
    StandardMBean bean;
    String storageName;
    if (storageId == null || storageId.equals("")) {// Temp fix for the uninitialized storage
      storageName = "UndefinedStorageId" + rand.nextInt();
    } else {
      storageName = storageId;
    }
    try {
      bean = new StandardMBean(this,FSDatasetMBean.class);
      mbeanName = MBeanUtil.registerMBean("DataNode", "FSDatasetState-" + storageName, bean);
    } catch (NotCompliantMBeanException e) {
      e.printStackTrace();
    }
 
    DataNode.LOG.info("Registered FSDatasetStatusMBean");
  }

  public void shutdown() {
    if (mbeanName != null)
      MBeanUtil.unregisterMBean(mbeanName);
    
    if (asyncDiskService != null) {
      asyncDiskService.shutdown();
    }
    
    if(volumes != null) {
      for (FSVolume volume : volumes.volumes) {
        if(volume != null) {
          volume.dfsUsage.shutdown();
        }
      }
    }
  }

  public String getStorageInfo() {
    return toString();
  }

  /**
   * Reconcile the difference between blocks on the disk and blocks in
   * volumeMap
   *
   * Check the given block for inconsistencies. Look at the
   * current state of the block and reconcile the differences as follows:
   * <ul>
   * <li>If the block file is missing, delete the block from volumeMap</li>
   * <li>If the block file exists and the block is missing in volumeMap,
   * add the block to volumeMap <li>
   * <li>If generation stamp does not match, then update the block with right
   * generation stamp</li>
   * <li>If the block length in memory does not match the actual block file length
   * then mark the block as corrupt and update the block length in memory</li>
   * <li>If the file in {@link ReplicaInfo} does not match the file on
   * the disk, update {@link ReplicaInfo} with the correct file</li>
   * </ul>
   *
   * @param blockId Block that differs
   * @param diskFile Block file on the disk
   * @param diskMetaFile Metadata file from on the disk
   * @param vol Volume of the block file
   */
  public void checkAndUpdate(long blockId, File diskFile,
      File diskMetaFile, FSVolume vol) {
    DataNode datanode = DataNode.getDataNode();
    Block corruptBlock = null;
    ReplicaInfo memBlockInfo;
    synchronized (this) {
      memBlockInfo = volumeMap.get(blockId);
      if (memBlockInfo != null && memBlockInfo.getState() != ReplicaState.FINALIZED) {
        // Block is not finalized - ignore the difference
        return;
      }

      final long diskGS = diskMetaFile != null && diskMetaFile.exists() ?
          Block.getGenerationStamp(diskMetaFile.getName()) :
            GenerationStamp.GRANDFATHER_GENERATION_STAMP;

      if (diskFile == null || !diskFile.exists()) {
        if (memBlockInfo == null) {
          // Block file does not exist and block does not exist in memory
          // If metadata file exists then delete it
          if (diskMetaFile != null && diskMetaFile.exists()
              && diskMetaFile.delete()) {
            DataNode.LOG.warn("Deleted a metadata file without a block "
                + diskMetaFile.getAbsolutePath());
          }
          return;
        }
        if (!memBlockInfo.getBlockFile().exists()) {
          // Block is in memory and not on the disk
          // Remove the block from volumeMap
          volumeMap.remove(blockId);
          if (datanode.blockScanner != null) {
            datanode.blockScanner.deleteBlock(new Block(blockId));
          }
          DataNode.LOG.warn("Removed block " + blockId
              + " from memory with missing block file on the disk");
          // Finally remove the metadata file
          if (diskMetaFile != null && diskMetaFile.exists()
              && diskMetaFile.delete()) {
            DataNode.LOG.warn("Deleted a metadata file for the deleted block "
                + diskMetaFile.getAbsolutePath());
          }
        }
        return;
      }
      /*
       * Block file exists on the disk
       */
      if (memBlockInfo == null) {
        // Block is missing in memory - add the block to volumeMap
        ReplicaInfo diskBlockInfo = new FinalizedReplica(blockId, 
            diskFile.length(), diskGS, vol, diskFile.getParentFile());
        volumeMap.add(diskBlockInfo);
        if (datanode.blockScanner != null) {
          datanode.blockScanner.addBlock(diskBlockInfo);
        }
        DataNode.LOG.warn("Added missing block to memory " + (Block)diskBlockInfo);
        return;
      }
      /*
       * Block exists in volumeMap and the block file exists on the disk
       */
      // Compare block files
      File memFile = memBlockInfo.getBlockFile();
      if (memFile.exists()) {
        if (memFile.compareTo(diskFile) != 0) {
          DataNode.LOG.warn("Block file " + memFile.getAbsolutePath()
              + " does not match file found by scan "
              + diskFile.getAbsolutePath());
          // TODO: Should the diskFile be deleted?
        }
      } else {
        // Block refers to a block file that does not exist.
        // Update the block with the file found on the disk. Since the block
        // file and metadata file are found as a pair on the disk, update
        // the block based on the metadata file found on the disk
        DataNode.LOG.warn("Block file in volumeMap "
            + memFile.getAbsolutePath()
            + " does not exist. Updating it to the file found during scan "
            + diskFile.getAbsolutePath());
        memBlockInfo.setDir(diskFile.getParentFile());
        memFile = diskFile;

        DataNode.LOG.warn("Updating generation stamp for block " + blockId
            + " from " + memBlockInfo.getGenerationStamp() + " to " + diskGS);
        memBlockInfo.setGenerationStamp(diskGS);
      }

      // Compare generation stamp
      if (memBlockInfo.getGenerationStamp() != diskGS) {
        File memMetaFile = getMetaFile(diskFile, memBlockInfo);
        if (memMetaFile.exists()) {
          if (memMetaFile.compareTo(diskMetaFile) != 0) {
            DataNode.LOG.warn("Metadata file in memory "
                + memMetaFile.getAbsolutePath()
                + " does not match file found by scan "
                + diskMetaFile.getAbsolutePath());
          }
        } else {
          // Metadata file corresponding to block in memory is missing
          // If metadata file found during the scan is on the same directory
          // as the block file, then use the generation stamp from it
          long gs = diskMetaFile != null && diskMetaFile.exists()
              && diskMetaFile.getParent().equals(memFile.getParent()) ? diskGS
              : GenerationStamp.GRANDFATHER_GENERATION_STAMP;

          DataNode.LOG.warn("Updating generation stamp for block " + blockId
              + " from " + memBlockInfo.getGenerationStamp() + " to " + gs);

          memBlockInfo.setGenerationStamp(gs);
        }
      }

      // Compare block size
      if (memBlockInfo.getNumBytes() != memFile.length()) {
        // Update the length based on the block file
        corruptBlock = new Block(memBlockInfo);
        DataNode.LOG.warn("Updating size of block " + blockId + " from "
            + memBlockInfo.getNumBytes() + " to " + memFile.length());
        memBlockInfo.setNumBytes(memFile.length());
      }
    }

    // Send corrupt block report outside the lock
    if (corruptBlock != null) {
      DatanodeInfo[] dnArr = { new DatanodeInfo(datanode.dnRegistration) };
      LocatedBlock[] blocks = { new LocatedBlock(corruptBlock, dnArr) };
      try {
        datanode.namenode.reportBadBlocks(blocks);
        DataNode.LOG.warn("Reporting the block " + corruptBlock
            + " as corrupt due to length mismatch");
      } catch (IOException e) {
        DataNode.LOG.warn("Failed to repot bad block " + corruptBlock
            + "Exception:" + StringUtils.stringifyException(e));
      }
    }
  }

  /**
   * @deprecated use {@link #fetchReplicaInfo(long)} instead.
   */
  @Override
  @Deprecated
  public ReplicaInfo getReplica(long blockId) {
    assert(Thread.holdsLock(this));
    return volumeMap.get(blockId);
  }

  @Override // FSDatasetInterface
  public synchronized ReplicaRecoveryInfo initReplicaRecovery(
      RecoveringBlock rBlock) throws IOException {
    return initReplicaRecovery(
        volumeMap, rBlock.getBlock(), rBlock.getNewGenerationStamp());
  }

  /** static version of {@link #initReplicaRecovery(Block, long)}. */
  static ReplicaRecoveryInfo initReplicaRecovery(
      ReplicasMap map, Block block, long recoveryId) throws IOException {
    final ReplicaInfo replica = map.get(block.getBlockId());
    DataNode.LOG.info("initReplicaRecovery: block=" + block
        + ", recoveryId=" + recoveryId
        + ", replica=" + replica);

    //check replica
    if (replica == null) {
      return null;
    }

    //stop writer if there is any
    if (replica instanceof ReplicaInPipeline) {
      final ReplicaInPipeline rip = (ReplicaInPipeline)replica;
      rip.stopWriter();

      //check replica bytes on disk.
      if (rip.getBytesOnDisk() < rip.getVisibleLength()) {
        throw new IOException("THIS IS NOT SUPPOSED TO HAPPEN:"
            + " getBytesOnDisk() < getVisibleLength(), rip=" + rip);
      }

      //check the replica's files
      checkReplicaFiles(rip);
    }

    //check generation stamp
    if (replica.getGenerationStamp() < block.getGenerationStamp()) {
      throw new IOException(
          "replica.getGenerationStamp() < block.getGenerationStamp(), block="
          + block + ", replica=" + replica);
    }

    //check recovery id
    if (replica.getGenerationStamp() >= recoveryId) {
      throw new IOException("THIS IS NOT SUPPOSED TO HAPPEN:"
          + " replica.getGenerationStamp() >= recoveryId = " + recoveryId
          + ", block=" + block + ", replica=" + replica);
    }

    //check RUR
    final ReplicaUnderRecovery rur;
    if (replica.getState() == ReplicaState.RUR) {
      rur = (ReplicaUnderRecovery)replica;
      if (rur.getRecoveryID() >= recoveryId) {
        throw new RecoveryInProgressException(
            "rur.getRecoveryID() >= recoveryId = " + recoveryId
            + ", block=" + block + ", rur=" + rur);
      }
      final long oldRecoveryID = rur.getRecoveryID();
      rur.setRecoveryID(recoveryId);
      DataNode.LOG.info("initReplicaRecovery: update recovery id for " + block
          + " from " + oldRecoveryID + " to " + recoveryId);
    }
    else {
      rur = new ReplicaUnderRecovery(replica, recoveryId);
      map.add(rur);
      DataNode.LOG.info("initReplicaRecovery: changing replica state for "
          + block + " from " + replica.getState()
          + " to " + rur.getState());
    }
    return rur.createInfo();
  }

  @Override // FSDatasetInterface
  public synchronized ReplicaInfo updateReplicaUnderRecovery(
                                    final Block oldBlock,
                                    final long recoveryId,
                                    final long newlength) throws IOException {
    //get replica
    final ReplicaInfo replica = volumeMap.get(oldBlock.getBlockId());
    DataNode.LOG.info("updateReplica: block=" + oldBlock
        + ", recoveryId=" + recoveryId
        + ", length=" + newlength
        + ", replica=" + replica);

    //check replica
    if (replica == null) {
      throw new ReplicaNotFoundException(oldBlock);
    }

    //check replica state
    if (replica.getState() != ReplicaState.RUR) {
      throw new IOException("replica.getState() != " + ReplicaState.RUR
          + ", replica=" + replica);
    }

    //check replica's byte on disk
    if (replica.getBytesOnDisk() != oldBlock.getNumBytes()) {
      throw new IOException("THIS IS NOT SUPPOSED TO HAPPEN:"
          + " replica.getBytesOnDisk() != block.getNumBytes(), block="
          + oldBlock + ", replica=" + replica);
    }

    //check replica files before update
    checkReplicaFiles(replica);

    //update replica
    final FinalizedReplica finalized = updateReplicaUnderRecovery(
        (ReplicaUnderRecovery)replica, recoveryId, newlength);

    //check replica files after update
    checkReplicaFiles(finalized);
    return finalized;
  }

  private FinalizedReplica updateReplicaUnderRecovery(
                                          ReplicaUnderRecovery rur,
                                          long recoveryId,
                                          long newlength) throws IOException {
    //check recovery id
    if (rur.getRecoveryID() != recoveryId) {
      throw new IOException("rur.getRecoveryID() != recoveryId = " + recoveryId
          + ", rur=" + rur);
    }

    // bump rur's GS to be recovery id
    bumpReplicaGS(rur, recoveryId);

    //update length
    final File replicafile = rur.getBlockFile();
    if (rur.getNumBytes() < newlength) {
      throw new IOException("rur.getNumBytes() < newlength = " + newlength
          + ", rur=" + rur);
    }
    if (rur.getNumBytes() > newlength) {
      rur.unlinkBlock(1);
      truncateBlock(replicafile, rur.getMetaFile(), rur.getNumBytes(), newlength);
      // update RUR with the new length
      rur.setNumBytes(newlength);
   }

    // finalize the block
    return finalizeReplica(rur);
  }

  @Override // FSDatasetInterface
  public synchronized long getReplicaVisibleLength(final Block block)
  throws IOException {
    final Replica replica = volumeMap.get(block.getBlockId());
    if (replica == null) {
      throw new ReplicaNotFoundException(block);
    }
    if (replica.getGenerationStamp() < block.getGenerationStamp()) {
      throw new IOException(
          "replica.getGenerationStamp() < block.getGenerationStamp(), block="
          + block + ", replica=" + replica);
    }
    return replica.getVisibleLength();
  }
}
