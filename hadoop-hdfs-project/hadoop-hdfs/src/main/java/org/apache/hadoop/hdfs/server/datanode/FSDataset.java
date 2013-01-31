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
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.DU;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.util.ReflectionUtils;

/**************************************************
 * FSDataset manages a set of data blocks.  Each block
 * has a unique name and an extent on disk.
 *
 ***************************************************/
@InterfaceAudience.Private
class FSDataset implements FSDatasetInterface {
  /**
   * A factory for creating FSDataset objects.
   */
  static class Factory extends FSDatasetInterface.Factory {
    @Override
    public FSDatasetInterface createFSDatasetInterface(DataNode datanode,
        DataStorage storage, Configuration conf) throws IOException {
      return new FSDataset(datanode, storage, conf);
    }
  }

  /**
   * A node type that can be built into a tree reflecting the
   * hierarchy of blocks on the local disk.
   */
  private class FSDir {
    final File dir;
    int numBlocks = 0;
    FSDir children[];
    int lastChildIdx = 0;

    private FSDir(File dir) 
      throws IOException {
      this.dir = dir;
      this.children = null;
      if (!dir.exists()) {
        if (!dir.mkdirs()) {
          throw new IOException("Mkdirs failed to create " + 
                                dir.toString());
        }
      } else {
        File[] files = FileUtil.listFiles(dir); 
        List<FSDir> dirList = new ArrayList<FSDir>();
        for (int idx = 0; idx < files.length; idx++) {
          if (files[idx].isDirectory()) {
            dirList.add(new FSDir(files[idx]));
          } else if (Block.isBlockFilename(files[idx])) {
            numBlocks++;
          }
        }
        if (dirList.size() > 0) {
          children = dirList.toArray(new FSDir[dirList.size()]);
        }
      }
    }
        
    private File addBlock(Block b, File src) throws IOException {
      //First try without creating subdirectories
      File file = addBlock(b, src, false, false);          
      return (file != null) ? file : addBlock(b, src, true, true);
    }

    private File addBlock(Block b, File src, boolean createOk, 
                          boolean resetIdx) throws IOException {
      if (numBlocks < maxBlocksPerDir) {
        final File dest = moveBlockFiles(b, src, dir);
        numBlocks += 1;
        return dest;
      }
            
      if (lastChildIdx < 0 && resetIdx) {
        //reset so that all children will be checked
        lastChildIdx = DFSUtil.getRandom().nextInt(children.length);              
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
      lastChildIdx = DFSUtil.getRandom().nextInt(children.length);
      return children[ lastChildIdx ].addBlock(b, src, true, false); 
    }

    private void getVolumeMap(String bpid, ReplicasMap volumeMap, FSVolume volume) 
    throws IOException {
      if (children != null) {
        for (int i = 0; i < children.length; i++) {
          children[i].getVolumeMap(bpid, volumeMap, volume);
        }
      }

      recoverTempUnlinkedBlock();
      volume.addToReplicasMap(bpid, volumeMap, dir, true);
    }
        
    /**
     * Recover unlinked tmp files on datanode restart. If the original block
     * does not exist, then the tmp file is renamed to be the
     * original file name; otherwise the tmp file is deleted.
     */
    private void recoverTempUnlinkedBlock() throws IOException {
      File files[] = FileUtil.listFiles(dir);
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
    private void checkDirTree() throws DiskErrorException {
      DiskChecker.checkDir(dir);
            
      if (children != null) {
        for (int i = 0; i < children.length; i++) {
          children[i].checkDirTree();
        }
      }
    }
        
    private void clearPath(File f) {
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

    @Override
    public String toString() {
      return "FSDir{" +
        "dir=" + dir +
        ", children=" + (children == null ? null : Arrays.asList(children)) +
        "}";
    }
  }

  /**
   * A BlockPoolSlice represents a portion of a BlockPool stored on a volume.  
   * Taken together, all BlockPoolSlices sharing a block pool ID across a 
   * cluster represent a single block pool.
   */
  private class BlockPoolSlice {
    private final String bpid;
    private final FSVolume volume; // volume to which this BlockPool belongs to
    private final File currentDir; // StorageDirectory/current/bpid/current
    private final FSDir finalizedDir; // directory store Finalized replica
    private final File rbwDir; // directory store RBW replica
    private final File tmpDir; // directory store Temporary replica
    
    // TODO:FEDERATION scalability issue - a thread per DU is needed
    private final DU dfsUsage;

    /**
     * 
     * @param bpid Block pool Id
     * @param volume {@link FSVolume} to which this BlockPool belongs to
     * @param bpDir directory corresponding to the BlockPool
     * @param conf
     * @throws IOException
     */
    BlockPoolSlice(String bpid, FSVolume volume, File bpDir, Configuration conf)
        throws IOException {
      this.bpid = bpid;
      this.volume = volume;
      this.currentDir = new File(bpDir, DataStorage.STORAGE_DIR_CURRENT); 
      final File finalizedDir = new File(
          currentDir, DataStorage.STORAGE_DIR_FINALIZED);

      // Files that were being written when the datanode was last shutdown
      // are now moved back to the data directory. It is possible that
      // in the future, we might want to do some sort of datanode-local
      // recovery for these blocks. For example, crc validation.
      //
      this.tmpDir = new File(bpDir, DataStorage.STORAGE_DIR_TMP);
      if (tmpDir.exists()) {
        FileUtil.fullyDelete(tmpDir);
      }
      this.rbwDir = new File(currentDir, DataStorage.STORAGE_DIR_RBW);
      if (rbwDir.exists() && !supportAppends) {
        FileUtil.fullyDelete(rbwDir);
      }
      this.finalizedDir = new FSDir(finalizedDir);
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
      this.dfsUsage = new DU(bpDir, conf);
      this.dfsUsage.start();
    }

    File getDirectory() {
      return currentDir.getParentFile();
    }

    File getFinalizedDir() {
      return finalizedDir.dir;
    }
    
    File getRbwDir() {
      return rbwDir;
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
     * Temporary files. They get moved to the finalized block directory when
     * the block is finalized.
     */
    File createTmpFile(Block b) throws IOException {
      File f = new File(tmpDir, b.getBlockName());
      return DatanodeUtil.createTmpFile(b, f);
    }

    /**
     * RBW files. They get moved to the finalized block directory when
     * the block is finalized.
     */
    File createRbwFile(Block b) throws IOException {
      File f = new File(rbwDir, b.getBlockName());
      return DatanodeUtil.createTmpFile(b, f);
    }

    File addBlock(Block b, File f) throws IOException {
      File blockFile = finalizedDir.addBlock(b, f);
      File metaFile = DatanodeUtil.getMetaFile(blockFile, b.getGenerationStamp());
      dfsUsage.incDfsUsed(b.getNumBytes()+metaFile.length());
      return blockFile;
    }
      
    void checkDirs() throws DiskErrorException {
      finalizedDir.checkDirTree();
      DiskChecker.checkDir(tmpDir);
      DiskChecker.checkDir(rbwDir);
    }
      
    void getVolumeMap(ReplicasMap volumeMap) throws IOException {
      // add finalized replicas
      finalizedDir.getVolumeMap(bpid, volumeMap, volume);
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
    private void addToReplicasMap(ReplicasMap volumeMap, File dir,
        boolean isFinalized) throws IOException {
      File blockFiles[] = FileUtil.listFiles(dir);
      for (File blockFile : blockFiles) {
        if (!Block.isBlockFilename(blockFile))
          continue;
        
        long genStamp = getGenerationStampFromFile(blockFiles, blockFile);
        long blockId = Block.filename2id(blockFile.getName());
        ReplicaInfo newReplica = null;
        if (isFinalized) {
          newReplica = new FinalizedReplica(blockId, 
              blockFile.length(), genStamp, volume, blockFile.getParentFile());
        } else {
          newReplica = new ReplicaWaitingToBeRecovered(blockId,
              validateIntegrity(blockFile, genStamp), 
              genStamp, volume, blockFile.getParentFile());
        }

        ReplicaInfo oldReplica = volumeMap.add(bpid, newReplica);
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
        final File metaFile = DatanodeUtil.getMetaFile(blockFile, genStamp);
        long blockFileLen = blockFile.length();
        long metaFileLen = metaFile.length();
        int crcHeaderLen = DataChecksum.getChecksumHeaderSize();
        if (!blockFile.exists() || blockFileLen == 0 ||
            !metaFile.exists() || metaFileLen < (long)crcHeaderLen) {
          return 0;
        }
        checksumIn = new DataInputStream(
            new BufferedInputStream(new FileInputStream(metaFile),
                HdfsConstants.IO_FILE_BUFFER_SIZE));

        // read and handle the common header here. For now just a version
        BlockMetadataHeader header = BlockMetadataHeader.readHeader(checksumIn);
        short version = header.getVersion();
        if (version != BlockMetadataHeader.VERSION) {
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
      finalizedDir.clearPath(f);
    }
      
    public String toString() {
      return currentDir.getAbsolutePath();
    }
    
    public void shutdown() {
      dfsUsage.shutdown();
    }
  }
  
  class FSVolume implements FSVolumeInterface {
    private final Map<String, BlockPoolSlice> map = new HashMap<String, BlockPoolSlice>();
    private final File currentDir;    // <StorageDirectory>/current
    private final DF usage;           
    private final long reserved;
    
    FSVolume(File currentDir, Configuration conf) throws IOException {
      this.reserved = conf.getLong(DFSConfigKeys.DFS_DATANODE_DU_RESERVED_KEY,
                                   DFSConfigKeys.DFS_DATANODE_DU_RESERVED_DEFAULT);
      this.currentDir = currentDir; 
      File parent = currentDir.getParentFile();
      this.usage = new DF(parent, conf);
    }
    
    File getCurrentDir() {
      return currentDir;
    }
    
    File getRbwDir(String bpid) throws IOException {
      BlockPoolSlice bp = getBlockPoolSlice(bpid);
      return bp.getRbwDir();
    }
    
    void decDfsUsed(String bpid, long value) {
      // The caller to this method (BlockFileDeleteTask.run()) does
      // not have locked FSDataset.this yet.
      synchronized(FSDataset.this) {
        BlockPoolSlice bp = map.get(bpid);
        if (bp != null) {
          bp.decDfsUsed(value);
        }
      }
    }
    
    long getDfsUsed() throws IOException {
      // TODO valid synchronization
      long dfsUsed = 0;
      Set<Entry<String, BlockPoolSlice>> set = map.entrySet();
      for (Entry<String, BlockPoolSlice> entry : set) {
        dfsUsed += entry.getValue().getDfsUsed();
      }
      return dfsUsed;
    }
    
    long getBlockPoolUsed(String bpid) throws IOException {
      return getBlockPoolSlice(bpid).getDfsUsed();
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

    @Override
    public long getAvailable() throws IOException {
      long remaining = getCapacity()-getDfsUsed();
      long available = usage.getAvailable();
      if (remaining>available) {
        remaining = available;
      }
      return (remaining > 0) ? remaining : 0;
    }
      
    long getReserved(){
      return reserved;
    }
    
    String getMount() throws IOException {
      return usage.getMount();
    }

    private BlockPoolSlice getBlockPoolSlice(String bpid) throws IOException {
      BlockPoolSlice bp = map.get(bpid);
      if (bp == null) {
        throw new IOException("block pool " + bpid + " is not found");
      }
      return bp;
    }

    @Override
    public File getDirectory(String bpid) throws IOException {
      return getBlockPoolSlice(bpid).getDirectory();
    }

    @Override
    public File getFinalizedDir(String bpid) throws IOException {
      return getBlockPoolSlice(bpid).getFinalizedDir();
    }

    /**
     * Make a deep copy of the list of currently active BPIDs
     */
    @Override
    public String[] getBlockPoolList() {
      synchronized(FSDataset.this) {
        return map.keySet().toArray(new String[map.keySet().size()]);   
      }
    }
      
    /**
     * Temporary files. They get moved to the finalized block directory when
     * the block is finalized.
     */
    File createTmpFile(String bpid, Block b) throws IOException {
      BlockPoolSlice bp = getBlockPoolSlice(bpid);
      return bp.createTmpFile(b);
    }

    /**
     * RBW files. They get moved to the finalized block directory when
     * the block is finalized.
     */
    File createRbwFile(String bpid, Block b) throws IOException {
      BlockPoolSlice bp = getBlockPoolSlice(bpid);
      return bp.createRbwFile(b);
    }

    File addBlock(String bpid, Block b, File f) throws IOException {
      BlockPoolSlice bp = getBlockPoolSlice(bpid);
      return bp.addBlock(b, f);
    }
      
    void checkDirs() throws DiskErrorException {
      // TODO:FEDERATION valid synchronization
      Set<Entry<String, BlockPoolSlice>> set = map.entrySet();
      for (Entry<String, BlockPoolSlice> entry : set) {
        entry.getValue().checkDirs();
      }
    }
      
    void getVolumeMap(ReplicasMap volumeMap) throws IOException {
      Set<Entry<String, BlockPoolSlice>> set = map.entrySet();
      for (Entry<String, BlockPoolSlice> entry : set) {
        entry.getValue().getVolumeMap(volumeMap);
      }
    }
    
    void getVolumeMap(String bpid, ReplicasMap volumeMap) throws IOException {
      BlockPoolSlice bp = getBlockPoolSlice(bpid);
      bp.getVolumeMap(volumeMap);
    }
    
    /**
     * Add replicas under the given directory to the volume map
     * @param volumeMap the replicas map
     * @param dir an input directory
     * @param isFinalized true if the directory has finalized replicas;
     *                    false if the directory has rbw replicas
     * @throws IOException 
     */
    private void addToReplicasMap(String bpid, ReplicasMap volumeMap, 
        File dir, boolean isFinalized) throws IOException {
      BlockPoolSlice bp = getBlockPoolSlice(bpid);
      // TODO move this up
      // dfsUsage.incDfsUsed(b.getNumBytes()+metaFile.length());
      bp.addToReplicasMap(volumeMap, dir, isFinalized);
    }
    
    void clearPath(String bpid, File f) throws IOException {
      BlockPoolSlice bp = getBlockPoolSlice(bpid);
      bp.clearPath(f);
    }

    @Override
    public String toString() {
      return currentDir.getAbsolutePath();
    }

    public void shutdown() {
      Set<Entry<String, BlockPoolSlice>> set = map.entrySet();
      for (Entry<String, BlockPoolSlice> entry : set) {
        entry.getValue().shutdown();
      }
    }

    public void addBlockPool(String bpid, Configuration conf)
        throws IOException {
      File bpdir = new File(currentDir, bpid);
      BlockPoolSlice bp = new BlockPoolSlice(bpid, this, bpdir, conf);
      map.put(bpid, bp);
    }
    
    public void shutdownBlockPool(String bpid) {
      BlockPoolSlice bp = map.get(bpid);
      if (bp!=null) {
        bp.shutdown();
      }
      map.remove(bpid);
    }

    private boolean isBPDirEmpty(String bpid)
        throws IOException {
      File volumeCurrentDir = this.getCurrentDir();
      File bpDir = new File(volumeCurrentDir, bpid);
      File bpCurrentDir = new File(bpDir, DataStorage.STORAGE_DIR_CURRENT);
      File finalizedDir = new File(bpCurrentDir,
          DataStorage.STORAGE_DIR_FINALIZED);
      File rbwDir = new File(bpCurrentDir, DataStorage.STORAGE_DIR_RBW);
      if (finalizedDir.exists() && FileUtil.list(finalizedDir).length != 0) {
        return false;
      }
      if (rbwDir.exists() && FileUtil.list(rbwDir).length != 0) {
        return false;
      }
      return true;
    }
    
    private void deleteBPDirectories(String bpid, boolean force)
        throws IOException {
      File volumeCurrentDir = this.getCurrentDir();
      File bpDir = new File(volumeCurrentDir, bpid);
      if (!bpDir.isDirectory()) {
        // nothing to be deleted
        return;
      }
      File tmpDir = new File(bpDir, DataStorage.STORAGE_DIR_TMP);
      File bpCurrentDir = new File(bpDir, DataStorage.STORAGE_DIR_CURRENT);
      File finalizedDir = new File(bpCurrentDir,
          DataStorage.STORAGE_DIR_FINALIZED);
      File rbwDir = new File(bpCurrentDir, DataStorage.STORAGE_DIR_RBW);
      if (force) {
        FileUtil.fullyDelete(bpDir);
      } else {
        if (!rbwDir.delete()) {
          throw new IOException("Failed to delete " + rbwDir);
        }
        if (!finalizedDir.delete()) {
          throw new IOException("Failed to delete " + finalizedDir);
        }
        FileUtil.fullyDelete(tmpDir);
        for (File f : FileUtil.listFiles(bpCurrentDir)) {
          if (!f.delete()) {
            throw new IOException("Failed to delete " + f);
          }
        }
        if (!bpCurrentDir.delete()) {
          throw new IOException("Failed to delete " + bpCurrentDir);
        }
        for (File f : FileUtil.listFiles(bpDir)) {
          if (!f.delete()) {
            throw new IOException("Failed to delete " + f);
          }
        }
        if (!bpDir.delete()) {
          throw new IOException("Failed to delete " + bpDir);
        }
      }
    }
  }
    
  static class FSVolumeSet {
    /*
     * Read access to this unmodifiable list is not synchronized.
     * This list is replaced on modification holding "this" lock.
     */
    private volatile List<FSVolumeInterface> volumes = null;

    BlockVolumeChoosingPolicy blockChooser;
    int numFailedVolumes;

    FSVolumeSet(List<FSVolumeInterface> volumes, int failedVols,
        BlockVolumeChoosingPolicy blockChooser) {
      this.volumes = Collections.unmodifiableList(volumes);
      this.blockChooser = blockChooser;
      this.numFailedVolumes = failedVols;
    }
    
    private int numberOfFailedVolumes() {
      return numFailedVolumes;
    }
    
    /** 
     * Get next volume. Synchronized to ensure {@link #curVolume} is updated
     * by a single thread and next volume is chosen with no concurrent
     * update to {@link #volumes}.
     * @param blockSize free space needed on the volume
     * @return next volume to store the block in.
     */
    synchronized FSVolume getNextVolume(long blockSize) throws IOException {
      return (FSVolume)blockChooser.chooseVolume(volumes, blockSize);
    }
      
    private long getDfsUsed() throws IOException {
      long dfsUsed = 0L;
      for (FSVolumeInterface v : volumes) {
        dfsUsed += ((FSVolume)v).getDfsUsed();
      }
      return dfsUsed;
    }

    private long getBlockPoolUsed(String bpid) throws IOException {
      long dfsUsed = 0L;
      for (FSVolumeInterface v : volumes) {
        dfsUsed += ((FSVolume)v).getBlockPoolUsed(bpid);
      }
      return dfsUsed;
    }

    private long getCapacity() throws IOException {
      long capacity = 0L;
      for (FSVolumeInterface v : volumes) {
        capacity += ((FSVolume)v).getCapacity();
      }
      return capacity;
    }
      
    private long getRemaining() throws IOException {
      long remaining = 0L;
      for (FSVolumeInterface vol : volumes) {
        remaining += vol.getAvailable();
      }
      return remaining;
    }
      
    private void getVolumeMap(ReplicasMap volumeMap)
        throws IOException {
      for (FSVolumeInterface v : volumes) {
        ((FSVolume)v).getVolumeMap(volumeMap);
      }
    }
    
    private void getVolumeMap(String bpid, ReplicasMap volumeMap)
        throws IOException {
      for (FSVolumeInterface v : volumes) {
        ((FSVolume)v).getVolumeMap(bpid, volumeMap);
      }
    }
      
    /**
     * Calls {@link FSVolume#checkDirs()} on each volume, removing any
     * volumes from the active list that result in a DiskErrorException.
     * 
     * This method is synchronized to allow only one instance of checkDirs() 
     * call
     * @return list of all the removed volumes.
     */
    private synchronized List<FSVolume> checkDirs() {
      ArrayList<FSVolume> removedVols = null;
      
      // Make a copy of volumes for performing modification 
      final List<FSVolumeInterface> volumeList = new ArrayList<FSVolumeInterface>(volumes);
      
      for (int idx = 0; idx < volumeList.size(); idx++) {
        FSVolume fsv = (FSVolume)volumeList.get(idx);
        try {
          fsv.checkDirs();
        } catch (DiskErrorException e) {
          DataNode.LOG.warn("Removing failed volume " + fsv + ": ",e);
          if (removedVols == null) {
            removedVols = new ArrayList<FSVolume>(1);
          }
          removedVols.add(fsv);
          fsv.shutdown(); 
          volumeList.set(idx, null); // Remove the volume
          numFailedVolumes++;
        }
      }
      
      // Remove null volumes from the volumes array
      if (removedVols != null && removedVols.size() > 0) {
        List<FSVolumeInterface> newVols = new ArrayList<FSVolumeInterface>();
        for (FSVolumeInterface vol : volumeList) {
          if (vol != null) {
            newVols.add(vol);
          }
        }
        volumes = Collections.unmodifiableList(newVols); // Replace volume list
        DataNode.LOG.info("Completed FSVolumeSet.checkDirs. Removed "
            + removedVols.size() + " volumes. List of current volumes: "
            + this);
      }

      return removedVols;
    }

    @Override
    public String toString() {
      return volumes.toString();
    }


    private void addBlockPool(String bpid, Configuration conf)
        throws IOException {
      for (FSVolumeInterface v : volumes) {
        ((FSVolume)v).addBlockPool(bpid, conf);
      }
    }
    
    private void removeBlockPool(String bpid) {
      for (FSVolumeInterface v : volumes) {
        ((FSVolume)v).shutdownBlockPool(bpid);
      }
    }

    private void shutdown() {
      for (FSVolumeInterface volume : volumes) {
        if(volume != null) {
          ((FSVolume)volume).shutdown();
        }
      }
    }
  }
  
  //////////////////////////////////////////////////////
  //
  // FSDataSet
  //
  //////////////////////////////////////////////////////

  private static boolean isUnlinkTmpFile(File f) {
    String name = f.getName();
    return name.endsWith(DatanodeUtil.UNLINK_BLOCK_SUFFIX);
  }
  
  private static File getOrigFile(File unlinkTmpFile) {
    String fileName = unlinkTmpFile.getName();
    return new File(unlinkTmpFile.getParentFile(),
        fileName.substring(0,
            fileName.length() - DatanodeUtil.UNLINK_BLOCK_SUFFIX.length()));
  }
  
  protected File getMetaFile(ExtendedBlock b) throws IOException {
    return DatanodeUtil.getMetaFile(getBlockFile(b), b.getGenerationStamp());
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
  private static long parseGenerationStamp(File blockFile, File metaFile
      ) throws IOException {
    String metaname = metaFile.getName();
    String gs = metaname.substring(blockFile.getName().length() + 1,
        metaname.length() - DatanodeUtil.METADATA_EXTENSION.length());
    try {
      return Long.parseLong(gs);
    } catch(NumberFormatException nfe) {
      throw (IOException)new IOException("blockFile=" + blockFile
          + ", metaFile=" + metaFile).initCause(nfe);
    }
  }

  @Override // FSDatasetInterface
  public List<FSVolumeInterface> getVolumes() {
    return volumes.volumes;
  }

  @Override // FSDatasetInterface
  public synchronized Block getStoredBlock(String bpid, long blkid)
      throws IOException {
    File blockfile = getFile(bpid, blkid);
    if (blockfile == null) {
      return null;
    }
    final File metafile = DatanodeUtil.findMetaFile(blockfile);
    return new Block(blkid, blockfile.length(),
        parseGenerationStamp(blockfile, metafile));
  }

  /**
   * Returns a clone of a replica stored in data-node memory.
   * Should be primarily used for testing.
   * @param blockId
   * @return
   */
  ReplicaInfo fetchReplicaInfo(String bpid, long blockId) {
    ReplicaInfo r = volumeMap.get(bpid, blockId);
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

  @Override // FSDatasetInterface
  public boolean metaFileExists(ExtendedBlock b) throws IOException {
    return getMetaFile(b).exists();
  }
  
  @Override // FSDatasetInterface
  public long getMetaDataLength(ExtendedBlock b) throws IOException {
    File checksumFile = getMetaFile(b);
    return checksumFile.length();
  }

  @Override // FSDatasetInterface
  public MetaDataInputStream getMetaDataInputStream(ExtendedBlock b)
      throws IOException {
    File checksumFile = getMetaFile(b);
    return new MetaDataInputStream(new FileInputStream(checksumFile),
                                                    checksumFile.length());
  }
    
  private final DataNode datanode;
  final FSVolumeSet volumes;
  private final int maxBlocksPerDir;
  final ReplicasMap volumeMap;
  final FSDatasetAsyncDiskService asyncDiskService;
  private final int validVolsRequired;

  // Used for synchronizing access to usage stats
  private final Object statsLock = new Object();

  final boolean supportAppends;

  /**
   * An FSDataset has a directory where it loads its data files.
   */
  private FSDataset(DataNode datanode, DataStorage storage, Configuration conf
      ) throws IOException {
    this.datanode = datanode;
    this.maxBlocksPerDir = 
      conf.getInt(DFSConfigKeys.DFS_DATANODE_NUMBLOCKS_KEY,
                  DFSConfigKeys.DFS_DATANODE_NUMBLOCKS_DEFAULT);
    this.supportAppends = 
      conf.getBoolean(DFSConfigKeys.DFS_SUPPORT_APPEND_KEY,
                      DFSConfigKeys.DFS_SUPPORT_APPEND_DEFAULT);
    // The number of volumes required for operation is the total number 
    // of volumes minus the number of failed volumes we can tolerate.
    final int volFailuresTolerated =
      conf.getInt(DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY,
                  DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_DEFAULT);

    String[] dataDirs = conf.getTrimmedStrings(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY);

    int volsConfigured = (dataDirs == null) ? 0 : dataDirs.length;
    int volsFailed = volsConfigured - storage.getNumStorageDirs();
    this.validVolsRequired = volsConfigured - volFailuresTolerated;

    if (volFailuresTolerated < 0 || volFailuresTolerated >= volsConfigured) {
      throw new DiskErrorException("Invalid volume failure "
          + " config value: " + volFailuresTolerated);
    }
    if (volsFailed > volFailuresTolerated) {
      throw new DiskErrorException("Too many failed volumes - "
          + "current valid volumes: " + storage.getNumStorageDirs() 
          + ", volumes configured: " + volsConfigured 
          + ", volumes failed: " + volsFailed
          + ", volume failures tolerated: " + volFailuresTolerated);
    }

    final List<FSVolumeInterface> volArray = new ArrayList<FSVolumeInterface>(
        storage.getNumStorageDirs());
    for (int idx = 0; idx < storage.getNumStorageDirs(); idx++) {
      final File dir = storage.getStorageDir(idx).getCurrentDir();
      volArray.add(new FSVolume(dir, conf));
      DataNode.LOG.info("FSDataset added volume - " + dir);
    }
    volumeMap = new ReplicasMap(this);

    BlockVolumeChoosingPolicy blockChooserImpl =
      (BlockVolumeChoosingPolicy) ReflectionUtils.newInstance(
        conf.getClass(DFSConfigKeys.DFS_DATANODE_BLOCKVOLUMECHOICEPOLICY,
            RoundRobinVolumesPolicy.class,
            BlockVolumeChoosingPolicy.class),
        conf);
    volumes = new FSVolumeSet(volArray, volsFailed, blockChooserImpl);
    volumes.getVolumeMap(volumeMap);

    File[] roots = new File[storage.getNumStorageDirs()];
    for (int idx = 0; idx < storage.getNumStorageDirs(); idx++) {
      roots[idx] = storage.getStorageDir(idx).getCurrentDir();
    }
    asyncDiskService = new FSDatasetAsyncDiskService(this, roots);
    registerMBean(storage.getStorageID());
  }

  /**
   * Return the total space used by dfs datanode
   */
  @Override // FSDatasetMBean
  public long getDfsUsed() throws IOException {
    synchronized(statsLock) {
      return volumes.getDfsUsed();
    }
  }

  /**
   * Return the total space used by dfs datanode
   */
  @Override // FSDatasetMBean
  public long getBlockPoolUsed(String bpid) throws IOException {
    synchronized(statsLock) {
      return volumes.getBlockPoolUsed(bpid);
    }
  }
  
  /**
   * Return true - if there are still valid volumes on the DataNode. 
   */
  @Override // FSDatasetInterface
  public boolean hasEnoughResource() {
    return getVolumes().size() >= validVolsRequired; 
  }

  /**
   * Return total capacity, used and unused
   */
  @Override // FSDatasetMBean
  public long getCapacity() throws IOException {
    synchronized(statsLock) {
      return volumes.getCapacity();
    }
  }

  /**
   * Return how many bytes can still be stored in the FSDataset
   */
  @Override // FSDatasetMBean
  public long getRemaining() throws IOException {
    synchronized(statsLock) {
      return volumes.getRemaining();
    }
  }

  /**
   * Return the number of failed volumes in the FSDataset.
   */
  public int getNumFailedVolumes() {
    return volumes.numberOfFailedVolumes();
  }

  /**
   * Find the block's on-disk length
   */
  @Override // FSDatasetInterface
  public long getLength(ExtendedBlock b) throws IOException {
    return getBlockFile(b).length();
  }

  /**
   * Get File name for a given block.
   */
  private File getBlockFile(ExtendedBlock b) throws IOException {
    return getBlockFile(b.getBlockPoolId(), b.getLocalBlock());
  }
  
  /**
   * Get File name for a given block.
   */
  File getBlockFile(String bpid, Block b) throws IOException {
    File f = validateBlockFile(bpid, b);
    if(f == null) {
      if (InterDatanodeProtocol.LOG.isDebugEnabled()) {
        InterDatanodeProtocol.LOG.debug("b=" + b + ", volumeMap=" + volumeMap);
      }
      throw new IOException("Block " + b + " is not valid.");
    }
    return f;
  }
  
  @Override // FSDatasetInterface
  public InputStream getBlockInputStream(ExtendedBlock b)
      throws IOException {
    File f = getBlockFileNoExistsCheck(b);
    try {
      return new FileInputStream(f);
    } catch (FileNotFoundException fnfe) {
      throw new IOException("Block " + b + " is not valid. " +
          "Expected block file at " + f + " does not exist.");
    }
  }
  
  /**
   * Return the File associated with a block, without first
   * checking that it exists. This should be used when the
   * next operation is going to open the file for read anyway,
   * and thus the exists check is redundant.
   */
  private File getBlockFileNoExistsCheck(ExtendedBlock b)
      throws IOException {
    final File f;
    synchronized(this) {
      f = getFile(b.getBlockPoolId(), b.getLocalBlock().getBlockId());
    }
    if (f == null) {
      throw new IOException("Block " + b + " is not valid");
    }
    return f;
  }

  @Override // FSDatasetInterface
  public InputStream getBlockInputStream(ExtendedBlock b,
      long seekOffset) throws IOException {
    File blockFile = getBlockFileNoExistsCheck(b);
    RandomAccessFile blockInFile;
    try {
      blockInFile = new RandomAccessFile(blockFile, "r");
    } catch (FileNotFoundException fnfe) {
      throw new IOException("Block " + b + " is not valid. " +
          "Expected block file at " + blockFile + " does not exist.");
    }

    if (seekOffset > 0) {
      blockInFile.seek(seekOffset);
    }
    return new FileInputStream(blockInFile.getFD());
  }

  /**
   * Get the meta info of a block stored in volumeMap. To find a block,
   * block pool Id, block Id and generation stamp must match.
   * @param b extended block
   * @return the meta replica information; null if block was not found
   * @throws ReplicaNotFoundException if no entry is in the map or 
   *                        there is a generation stamp mismatch
   */
  ReplicaInfo getReplicaInfo(ExtendedBlock b)
      throws ReplicaNotFoundException {
    ReplicaInfo info = volumeMap.get(b.getBlockPoolId(), b.getLocalBlock());
    if (info == null) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.NON_EXISTENT_REPLICA + b);
    }
    return info;
  }
  
  /**
   * Get the meta info of a block stored in volumeMap. Block is looked up
   * without matching the generation stamp.
   * @param bpid block pool Id
   * @param blkid block Id
   * @return the meta replica information; null if block was not found
   * @throws ReplicaNotFoundException if no entry is in the map or 
   *                        there is a generation stamp mismatch
   */
  private ReplicaInfo getReplicaInfo(String bpid, long blkid)
      throws ReplicaNotFoundException {
    ReplicaInfo info = volumeMap.get(bpid, blkid);
    if (info == null) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.NON_EXISTENT_REPLICA + bpid + ":" + blkid);
    }
    return info;
  }
  
  /**
   * Returns handles to the block file and its metadata file
   */
  @Override // FSDatasetInterface
  public synchronized BlockInputStreams getTmpInputStreams(ExtendedBlock b, 
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
  public boolean unlinkBlock(ExtendedBlock block, int numLinks) throws IOException {
    ReplicaInfo info = getReplicaInfo(block);
    return info.unlinkBlock(numLinks);
  }

  private static File moveBlockFiles(Block b, File srcfile, File destdir
      ) throws IOException {
    final File dstfile = new File(destdir, b.getBlockName());
    final File srcmeta = DatanodeUtil.getMetaFile(srcfile, b.getGenerationStamp());
    final File dstmeta = DatanodeUtil.getMetaFile(dstfile, b.getGenerationStamp());
    if (!srcmeta.renameTo(dstmeta)) {
      throw new IOException("Failed to move meta file for " + b
          + " from " + srcmeta + " to " + dstmeta);
    }
    if (!srcfile.renameTo(dstfile)) {
      throw new IOException("Failed to move block file for " + b
          + " from " + srcfile + " to " + dstfile.getAbsolutePath());
    }
    if (DataNode.LOG.isDebugEnabled()) {
      DataNode.LOG.debug("addBlock: Moved " + srcmeta + " to " + dstmeta);
      DataNode.LOG.debug("addBlock: Moved " + srcfile + " to " + dstfile);
    }
    return dstfile;
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


  @Override  // FSDatasetInterface
  public synchronized ReplicaInPipelineInterface append(ExtendedBlock b,
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
    ReplicaInfo replicaInfo = getReplicaInfo(b);
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

    return append(b.getBlockPoolId(), (FinalizedReplica)replicaInfo, newGS,
        b.getNumBytes());
  }
  
  /** Append to a finalized replica
   * Change a finalized replica to be a RBW replica and 
   * bump its generation stamp to be the newGS
   * 
   * @param bpid block pool Id
   * @param replicaInfo a finalized replica
   * @param newGS new generation stamp
   * @param estimateBlockLen estimate generation stamp
   * @return a RBW replica
   * @throws IOException if moving the replica from finalized directory 
   *         to rbw directory fails
   */
  private synchronized ReplicaBeingWritten append(String bpid,
      FinalizedReplica replicaInfo, long newGS, long estimateBlockLen)
      throws IOException {
    // unlink the finalized replica
    replicaInfo.unlinkBlock(1);
    
    // construct a RBW replica with the new GS
    File blkfile = replicaInfo.getBlockFile();
    FSVolume v = (FSVolume)replicaInfo.getVolume();
    if (v.getAvailable() < estimateBlockLen - replicaInfo.getNumBytes()) {
      throw new DiskOutOfSpaceException("Insufficient space for appending to "
          + replicaInfo);
    }
    File newBlkFile = new File(v.getRbwDir(bpid), replicaInfo.getBlockName());
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
    volumeMap.add(bpid, newReplicaInfo);
    
    return newReplicaInfo;
  }

  private ReplicaInfo recoverCheck(ExtendedBlock b, long newGS, 
      long expectedBlockLen) throws IOException {
    ReplicaInfo replicaInfo = getReplicaInfo(b.getBlockPoolId(), b.getBlockId());
    
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
  public synchronized ReplicaInPipelineInterface recoverAppend(ExtendedBlock b,
      long newGS, long expectedBlockLen) throws IOException {
    DataNode.LOG.info("Recover failed append to " + b);

    ReplicaInfo replicaInfo = recoverCheck(b, newGS, expectedBlockLen);

    // change the replica's state/gs etc.
    if (replicaInfo.getState() == ReplicaState.FINALIZED ) {
      return append(b.getBlockPoolId(), (FinalizedReplica) replicaInfo, newGS, 
          b.getNumBytes());
    } else { //RBW
      bumpReplicaGS(replicaInfo, newGS);
      return (ReplicaBeingWritten)replicaInfo;
    }
  }

  @Override // FSDatasetInterface
  public void recoverClose(ExtendedBlock b, long newGS,
      long expectedBlockLen) throws IOException {
    DataNode.LOG.info("Recover failed close " + b);
    // check replica's state
    ReplicaInfo replicaInfo = recoverCheck(b, newGS,
        expectedBlockLen);
    // bump the replica's GS
    bumpReplicaGS(replicaInfo, newGS);
    // finalize the replica if RBW
    if (replicaInfo.getState() == ReplicaState.RBW) {
      finalizeReplica(b.getBlockPoolId(), replicaInfo);
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

  @Override // FSDatasetInterface
  public synchronized ReplicaInPipelineInterface createRbw(ExtendedBlock b)
      throws IOException {
    ReplicaInfo replicaInfo = volumeMap.get(b.getBlockPoolId(), 
        b.getBlockId());
    if (replicaInfo != null) {
      throw new ReplicaAlreadyExistsException("Block " + b +
      " already exists in state " + replicaInfo.getState() +
      " and thus cannot be created.");
    }
    // create a new block
    FSVolume v = volumes.getNextVolume(b.getNumBytes());
    // create a rbw file to hold block in the designated volume
    File f = v.createRbwFile(b.getBlockPoolId(), b.getLocalBlock());
    ReplicaBeingWritten newReplicaInfo = new ReplicaBeingWritten(b.getBlockId(), 
        b.getGenerationStamp(), v, f.getParentFile());
    volumeMap.add(b.getBlockPoolId(), newReplicaInfo);
    return newReplicaInfo;
  }
  
  @Override // FSDatasetInterface
  public synchronized ReplicaInPipelineInterface recoverRbw(ExtendedBlock b,
      long newGS, long minBytesRcvd, long maxBytesRcvd)
      throws IOException {
    DataNode.LOG.info("Recover the RBW replica " + b);

    ReplicaInfo replicaInfo = getReplicaInfo(b.getBlockPoolId(), b.getBlockId());
    
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
  
  @Override // FSDatasetInterface
  public synchronized ReplicaInPipelineInterface convertTemporaryToRbw(
      final ExtendedBlock b) throws IOException {
    final long blockId = b.getBlockId();
    final long expectedGs = b.getGenerationStamp();
    final long visible = b.getNumBytes();
    DataNode.LOG.info("Convert replica " + b
        + " from Temporary to RBW, visible length=" + visible);

    final ReplicaInPipeline temp;
    {
      // get replica
      final ReplicaInfo r = volumeMap.get(b.getBlockPoolId(), blockId);
      if (r == null) {
        throw new ReplicaNotFoundException(
            ReplicaNotFoundException.NON_EXISTENT_REPLICA + b);
      }
      // check the replica's state
      if (r.getState() != ReplicaState.TEMPORARY) {
        throw new ReplicaAlreadyExistsException(
            "r.getState() != ReplicaState.TEMPORARY, r=" + r);
      }
      temp = (ReplicaInPipeline)r;
    }
    // check generation stamp
    if (temp.getGenerationStamp() != expectedGs) {
      throw new ReplicaAlreadyExistsException(
          "temp.getGenerationStamp() != expectedGs = " + expectedGs
          + ", temp=" + temp);
    }

    // TODO: check writer?
    // set writer to the current thread
    // temp.setWriter(Thread.currentThread());

    // check length
    final long numBytes = temp.getNumBytes();
    if (numBytes < visible) {
      throw new IOException(numBytes + " = numBytes < visible = "
          + visible + ", temp=" + temp);
    }
    // check volume
    final FSVolume v = (FSVolume)temp.getVolume();
    if (v == null) {
      throw new IOException("r.getVolume() = null, temp="  + temp);
    }
    
    // move block files to the rbw directory
    BlockPoolSlice bpslice = v.getBlockPoolSlice(b.getBlockPoolId());
    final File dest = moveBlockFiles(b.getLocalBlock(), temp.getBlockFile(), 
        bpslice.getRbwDir());
    // create RBW
    final ReplicaBeingWritten rbw = new ReplicaBeingWritten(
        blockId, numBytes, expectedGs,
        v, dest.getParentFile(), Thread.currentThread());
    rbw.setBytesAcked(visible);
    // overwrite the RBW in the volume map
    volumeMap.add(b.getBlockPoolId(), rbw);
    return rbw;
  }

  @Override // FSDatasetInterface
  public synchronized ReplicaInPipelineInterface createTemporary(ExtendedBlock b)
      throws IOException {
    ReplicaInfo replicaInfo = volumeMap.get(b.getBlockPoolId(), b.getBlockId());
    if (replicaInfo != null) {
      throw new ReplicaAlreadyExistsException("Block " + b +
          " already exists in state " + replicaInfo.getState() +
          " and thus cannot be created.");
    }
    
    FSVolume v = volumes.getNextVolume(b.getNumBytes());
    // create a temporary file to hold block in the designated volume
    File f = v.createTmpFile(b.getBlockPoolId(), b.getLocalBlock());
    ReplicaInPipeline newReplicaInfo = new ReplicaInPipeline(b.getBlockId(), 
        b.getGenerationStamp(), v, f.getParentFile());
    volumeMap.add(b.getBlockPoolId(), newReplicaInfo);
    
    return newReplicaInfo;
  }

  /**
   * Sets the offset in the meta file so that the
   * last checksum will be overwritten.
   */
  @Override // FSDatasetInterface
  public void adjustCrcChannelPosition(ExtendedBlock b, BlockWriteStreams streams, 
      int checksumSize) throws IOException {
    FileOutputStream file = (FileOutputStream) streams.checksumOut;
    FileChannel channel = file.getChannel();
    long oldPos = channel.position();
    long newPos = oldPos - checksumSize;
    if (DataNode.LOG.isDebugEnabled()) {
      DataNode.LOG.debug("Changing meta file offset of block " + b + " from " +
          oldPos + " to " + newPos);
    }
    channel.position(newPos);
  }

  synchronized File createTmpFile(FSVolume vol, String bpid, Block blk) throws IOException {
    if ( vol == null ) {
      ReplicaInfo replica = volumeMap.get(bpid, blk);
      if (replica != null) {
        vol = (FSVolume)volumeMap.get(bpid, blk).getVolume();
      }
      if ( vol == null ) {
        throw new IOException("Could not find volume for block " + blk);
      }
    }
    return vol.createTmpFile(bpid, blk);
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
  @Override // FSDatasetInterface
  public synchronized void finalizeBlock(ExtendedBlock b) throws IOException {
    if (Thread.interrupted()) {
      // Don't allow data modifications from interrupted threads
      throw new IOException("Cannot finalize block from Interrupted Thread");
    }
    ReplicaInfo replicaInfo = getReplicaInfo(b);
    if (replicaInfo.getState() == ReplicaState.FINALIZED) {
      // this is legal, when recovery happens on a file that has
      // been opened for append but never modified
      return;
    }
    finalizeReplica(b.getBlockPoolId(), replicaInfo);
  }
  
  private synchronized FinalizedReplica finalizeReplica(String bpid,
      ReplicaInfo replicaInfo) throws IOException {
    FinalizedReplica newReplicaInfo = null;
    if (replicaInfo.getState() == ReplicaState.RUR &&
       ((ReplicaUnderRecovery)replicaInfo).getOrignalReplicaState() == 
         ReplicaState.FINALIZED) {
      newReplicaInfo = (FinalizedReplica)
             ((ReplicaUnderRecovery)replicaInfo).getOriginalReplica();
    } else {
      FSVolume v = (FSVolume)replicaInfo.getVolume();
      File f = replicaInfo.getBlockFile();
      if (v == null) {
        throw new IOException("No volume for temporary file " + f + 
            " for block " + replicaInfo);
      }

      File dest = v.addBlock(bpid, replicaInfo, f);
      newReplicaInfo = new FinalizedReplica(replicaInfo, v, dest.getParentFile());
    }
    volumeMap.add(bpid, newReplicaInfo);
    return newReplicaInfo;
  }

  /**
   * Remove the temporary block file (if any)
   */
  @Override // FSDatasetInterface
  public synchronized void unfinalizeBlock(ExtendedBlock b) throws IOException {
    ReplicaInfo replicaInfo = volumeMap.get(b.getBlockPoolId(), 
        b.getLocalBlock());
    if (replicaInfo != null && replicaInfo.getState() == ReplicaState.TEMPORARY) {
      // remove from volumeMap
      volumeMap.remove(b.getBlockPoolId(), b.getLocalBlock());
      
      // delete the on-disk temp file
      if (delBlockFromDisk(replicaInfo.getBlockFile(), 
          replicaInfo.getMetaFile(), b.getLocalBlock())) {
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
  @Override // FSDatasetInterface
  public BlockListAsLongs getBlockReport(String bpid) {
    int size =  volumeMap.size(bpid);
    ArrayList<ReplicaInfo> finalized = new ArrayList<ReplicaInfo>(size);
    ArrayList<ReplicaInfo> uc = new ArrayList<ReplicaInfo>();
    if (size == 0) {
      return new BlockListAsLongs(finalized, uc);
    }
    
    synchronized(this) {
      for (ReplicaInfo b : volumeMap.replicas(bpid)) {
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
   * Get the list of finalized blocks from in-memory blockmap for a block pool.
   */
  @Override
  public synchronized List<Block> getFinalizedBlocks(String bpid) {
    ArrayList<Block> finalized = new ArrayList<Block>(volumeMap.size(bpid));
    for (ReplicaInfo b : volumeMap.replicas(bpid)) {
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
  @Override // FSDatasetInterface
  public boolean isValidBlock(ExtendedBlock b) {
    return isValid(b, ReplicaState.FINALIZED);
  }

  /**
   * Check whether the given block is a valid RBW.
   */
  @Override // {@link FSDatasetInterface}
  public boolean isValidRbw(final ExtendedBlock b) {
    return isValid(b, ReplicaState.RBW);
  }

  /** Does the block exist and have the given state? */
  private boolean isValid(final ExtendedBlock b, final ReplicaState state) {
    final ReplicaInfo replicaInfo = volumeMap.get(b.getBlockPoolId(), 
        b.getLocalBlock());
    return replicaInfo != null
        && replicaInfo.getState() == state
        && replicaInfo.getBlockFile().exists();
  }

  /**
   * Find the file corresponding to the block and return it if it exists.
   */
  File validateBlockFile(String bpid, Block b) throws IOException {
    //Should we check for metadata file too?
    final File f;
    synchronized(this) {
      f = getFile(bpid, b.getBlockId());
    }
    
    if(f != null ) {
      if(f.exists())
        return f;
   
      // if file is not null, but doesn't exist - possibly disk failed
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
    final File metafile = DatanodeUtil.getMetaFile(f, r.getGenerationStamp());
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
  @Override // FSDatasetInterface
  public void invalidate(String bpid, Block invalidBlks[]) throws IOException {
    boolean error = false;
    for (int i = 0; i < invalidBlks.length; i++) {
      File f = null;
      FSVolume v;
      synchronized (this) {
        f = getFile(bpid, invalidBlks[i].getBlockId());
        ReplicaInfo dinfo = volumeMap.get(bpid, invalidBlks[i]);
        if (dinfo == null || 
            dinfo.getGenerationStamp() != invalidBlks[i].getGenerationStamp()) {
          DataNode.LOG.warn("Unexpected error trying to delete block "
                           + invalidBlks[i] + 
                           ". BlockInfo not found in volumeMap.");
          error = true;
          continue;
        }
        v = (FSVolume)dinfo.getVolume();
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
          v.clearPath(bpid, parent);
        }
        volumeMap.remove(bpid, invalidBlks[i]);
      }
      File metaFile = DatanodeUtil.getMetaFile(f, invalidBlks[i].getGenerationStamp());

      // Delete the block asynchronously to make sure we can do it fast enough
      asyncDiskService.deleteAsync(v, f, metaFile,
          new ExtendedBlock(bpid, invalidBlks[i]));
    }
    if (error) {
      throw new IOException("Error in deleting blocks.");
    }
  }
  
  public void notifyNamenodeDeletedBlock(ExtendedBlock block){
    datanode.notifyNamenodeDeletedBlock(block);
  }

  @Override // {@link FSDatasetInterface}
  public synchronized boolean contains(final ExtendedBlock block) {
    final long blockId = block.getLocalBlock().getBlockId();
    return getFile(block.getBlockPoolId(), blockId) != null;
  }

  /**
   * Turn the block identifier into a filename
   * @param bpid Block pool Id
   * @param blockId a block's id
   * @return on disk data file path; null if the replica does not exist
   */
  File getFile(final String bpid, final long blockId) {
    ReplicaInfo info = volumeMap.get(bpid, blockId);
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
  @Override // FSDatasetInterface
  public void checkDataDir() throws DiskErrorException {
    long totalBlocks=0, removedBlocks=0;
    List<FSVolume> failedVols =  volumes.checkDirs();
    
    // If there no failed volumes return
    if (failedVols == null) { 
      return;
    }
    
    // Otherwise remove blocks for the failed volumes
    long mlsec = System.currentTimeMillis();
    synchronized (this) {
      for (FSVolume fv: failedVols) {
        for (String bpid : fv.map.keySet()) {
          Iterator<ReplicaInfo> ib = volumeMap.replicas(bpid).iterator();
          while(ib.hasNext()) {
            ReplicaInfo b = ib.next();
            totalBlocks++;
            // check if the volume block belongs to still valid
            if(b.getVolume() == fv) {
              DataNode.LOG.warn("Removing replica " + bpid + ":" + b.getBlockId()
                  + " on failed volume " + fv.currentDir.getAbsolutePath());
              ib.remove();
              removedBlocks++;
            }
          }
        }
      }
    } // end of sync
    mlsec = System.currentTimeMillis() - mlsec;
    DataNode.LOG.warn("Removed " + removedBlocks + " out of " + totalBlocks +
        "(took " + mlsec + " millisecs)");

    // report the error
    StringBuilder sb = new StringBuilder();
    for (FSVolume fv : failedVols) {
      sb.append(fv.currentDir.getAbsolutePath() + ";");
    }

    throw  new DiskErrorException("DataNode failed volumes:" + sb);
  }
    

  @Override // FSDatasetInterface
  public String toString() {
    return "FSDataset{dirpath='"+volumes+"'}";
  }

  private ObjectName mbeanName;
  
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
      storageName = "UndefinedStorageId" + DFSUtil.getRandom().nextInt();
    } else {
      storageName = storageId;
    }
    try {
      bean = new StandardMBean(this,FSDatasetMBean.class);
      mbeanName = MBeans.register("DataNode", "FSDatasetState-" + storageName, bean);
    } catch (NotCompliantMBeanException e) {
      DataNode.LOG.warn("Error registering FSDatasetState MBean", e);
    }
    DataNode.LOG.info("Registered FSDatasetState MBean");
  }

  @Override // FSDatasetInterface
  public void shutdown() {
    if (mbeanName != null)
      MBeans.unregister(mbeanName);
    
    if (asyncDiskService != null) {
      asyncDiskService.shutdown();
    }
    
    if(volumes != null) {
      volumes.shutdown();
    }
  }

  @Override // FSDatasetMBean
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
  @Override
  public void checkAndUpdate(String bpid, long blockId, File diskFile,
      File diskMetaFile, FSVolumeInterface vol) {
    Block corruptBlock = null;
    ReplicaInfo memBlockInfo;
    synchronized (this) {
      memBlockInfo = volumeMap.get(bpid, blockId);
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
          volumeMap.remove(bpid, blockId);
          if (datanode.blockScanner != null) {
            datanode.blockScanner.deleteBlock(bpid, new Block(blockId));
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
        volumeMap.add(bpid, diskBlockInfo);
        if (datanode.blockScanner != null) {
          datanode.blockScanner.addBlock(new ExtendedBlock(bpid, diskBlockInfo));
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
        File memMetaFile = DatanodeUtil.getMetaFile(diskFile, 
            memBlockInfo.getGenerationStamp());
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
      DataNode.LOG.warn("Reporting the block " + corruptBlock
          + " as corrupt due to length mismatch");
      try {
        datanode.reportBadBlocks(new ExtendedBlock(bpid, corruptBlock));  
      } catch (IOException e) {
        DataNode.LOG.warn("Failed to repot bad block " + corruptBlock, e);
      }
    }
  }

  /**
   * @deprecated use {@link #fetchReplicaInfo(String, long)} instead.
   */
  @Override // FSDatasetInterface
  @Deprecated
  public ReplicaInfo getReplica(String bpid, long blockId) {
    return volumeMap.get(bpid, blockId);
  }

  @Override 
  public synchronized String getReplicaString(String bpid, long blockId) {
    final Replica r = volumeMap.get(bpid, blockId);
    return r == null? "null": r.toString();
  }

  @Override // FSDatasetInterface
  public synchronized ReplicaRecoveryInfo initReplicaRecovery(
      RecoveringBlock rBlock) throws IOException {
    return initReplicaRecovery(rBlock.getBlock().getBlockPoolId(),
        volumeMap, rBlock.getBlock().getLocalBlock(), rBlock.getNewGenerationStamp());
  }

  /** static version of {@link #initReplicaRecovery(Block, long)}. */
  static ReplicaRecoveryInfo initReplicaRecovery(String bpid,
      ReplicasMap map, Block block, long recoveryId) throws IOException {
    final ReplicaInfo replica = map.get(bpid, block.getBlockId());
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
      map.add(bpid, rur);
      DataNode.LOG.info("initReplicaRecovery: changing replica state for "
          + block + " from " + replica.getState()
          + " to " + rur.getState());
    }
    return rur.createInfo();
  }

  @Override // FSDatasetInterface
  public synchronized ReplicaInfo updateReplicaUnderRecovery(
                                    final ExtendedBlock oldBlock,
                                    final long recoveryId,
                                    final long newlength) throws IOException {
    //get replica
    final ReplicaInfo replica = volumeMap.get(oldBlock.getBlockPoolId(), 
        oldBlock.getBlockId());
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
    final FinalizedReplica finalized = updateReplicaUnderRecovery(oldBlock
        .getBlockPoolId(), (ReplicaUnderRecovery) replica, recoveryId, newlength);

    //check replica files after update
    checkReplicaFiles(finalized);
    return finalized;
  }

  private FinalizedReplica updateReplicaUnderRecovery(
                                          String bpid,
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
    return finalizeReplica(bpid, rur);
  }

  @Override // FSDatasetInterface
  public synchronized long getReplicaVisibleLength(final ExtendedBlock block)
  throws IOException {
    final Replica replica = getReplicaInfo(block.getBlockPoolId(), 
        block.getBlockId());
    if (replica.getGenerationStamp() < block.getGenerationStamp()) {
      throw new IOException(
          "replica.getGenerationStamp() < block.getGenerationStamp(), block="
          + block + ", replica=" + replica);
    }
    return replica.getVisibleLength();
  }
  
  public synchronized void addBlockPool(String bpid, Configuration conf)
      throws IOException {
    DataNode.LOG.info("Adding block pool " + bpid);
    volumes.addBlockPool(bpid, conf);
    volumeMap.initBlockPool(bpid);
    volumes.getVolumeMap(bpid, volumeMap);
  }
  
  public synchronized void shutdownBlockPool(String bpid) {
    DataNode.LOG.info("Removing block pool " + bpid);
    volumeMap.cleanUpBlockPool(bpid);
    volumes.removeBlockPool(bpid);
  }
  
  @Override
  public String[] getBlockPoolList() {
    return volumeMap.getBlockPoolList();
  }
  
  /**
   * Class for representing the Datanode volume information
   */
  private static class VolumeInfo {
    final String directory;
    final long usedSpace;
    final long freeSpace;
    final long reservedSpace;

    VolumeInfo(String dir, long usedSpace, long freeSpace, long reservedSpace) {
      this.directory = dir;
      this.usedSpace = usedSpace;
      this.freeSpace = freeSpace;
      this.reservedSpace = reservedSpace;
    }
  }  

  private Collection<VolumeInfo> getVolumeInfo() {
    Collection<VolumeInfo> info = new ArrayList<VolumeInfo>();
    for (FSVolumeInterface v : volumes.volumes) {
      final FSVolume volume = (FSVolume)v;
      long used = 0;
      long free = 0;
      try {
        used = volume.getDfsUsed();
        free = volume.getAvailable();
      } catch (IOException e) {
        DataNode.LOG.warn(e.getMessage());
        used = 0;
        free = 0;
      }
      
      info.add(new VolumeInfo(volume.toString(), used, free, 
          volume.getReserved()));
    }
    return info;
  }

  @Override
  public Map<String, Object> getVolumeInfoMap() {
    final Map<String, Object> info = new HashMap<String, Object>();
    Collection<VolumeInfo> volumes = getVolumeInfo();
    for (VolumeInfo v : volumes) {
      final Map<String, Object> innerInfo = new HashMap<String, Object>();
      innerInfo.put("usedSpace", v.usedSpace);
      innerInfo.put("freeSpace", v.freeSpace);
      innerInfo.put("reservedSpace", v.reservedSpace);
      info.put(v.directory, innerInfo);
    }
    return info;
  }

  @Override //FSDatasetInterface
  public synchronized void deleteBlockPool(String bpid, boolean force)
      throws IOException {
    if (!force) {
      for (FSVolumeInterface volume : volumes.volumes) {
        if (!((FSVolume)volume).isBPDirEmpty(bpid)) {
          DataNode.LOG.warn(bpid
              + " has some block files, cannot delete unless forced");
          throw new IOException("Cannot delete block pool, "
              + "it contains some block files");
        }
      }
    }
    for (FSVolumeInterface volume : volumes.volumes) {
      ((FSVolume)volume).deleteBPDirectories(bpid, force);
    }
  }
  
  @Override // FSDatasetInterface
  public BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock block)
      throws IOException {
    File datafile = getBlockFile(block);
    File metafile = DatanodeUtil.getMetaFile(datafile, block.getGenerationStamp());
    BlockLocalPathInfo info = new BlockLocalPathInfo(block,
        datafile.getAbsolutePath(), metafile.getAbsolutePath());
    return info;
  }
}
