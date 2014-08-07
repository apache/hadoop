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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * The underlying volume used to store replica.
 * 
 * It uses the {@link FsDatasetImpl} object for synchronization.
 */
@InterfaceAudience.Private
class FsVolumeImpl implements FsVolumeSpi {
  private final FsDatasetImpl dataset;
  private final String storageID;
  private final StorageType storageType;
  private final Map<String, BlockPoolSlice> bpSlices
      = new ConcurrentHashMap<String, BlockPoolSlice>();
  private final File currentDir;    // <StorageDirectory>/current
  private final DF usage;           
  private final long reserved;
  /**
   * Per-volume worker pool that processes new blocks to cache.
   * The maximum number of workers per volume is bounded (configurable via
   * dfs.datanode.fsdatasetcache.max.threads.per.volume) to limit resource
   * contention.
   */
  private final ThreadPoolExecutor cacheExecutor;
  
  FsVolumeImpl(FsDatasetImpl dataset, String storageID, File currentDir,
      Configuration conf, StorageType storageType) throws IOException {
    this.dataset = dataset;
    this.storageID = storageID;
    this.reserved = conf.getLong(
        DFSConfigKeys.DFS_DATANODE_DU_RESERVED_KEY,
        DFSConfigKeys.DFS_DATANODE_DU_RESERVED_DEFAULT);
    this.currentDir = currentDir; 
    File parent = currentDir.getParentFile();
    this.usage = new DF(parent, conf);
    this.storageType = storageType;
    final int maxNumThreads = dataset.datanode.getConf().getInt(
        DFSConfigKeys.DFS_DATANODE_FSDATASETCACHE_MAX_THREADS_PER_VOLUME_KEY,
        DFSConfigKeys.DFS_DATANODE_FSDATASETCACHE_MAX_THREADS_PER_VOLUME_DEFAULT
        );
    ThreadFactory workerFactory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("FsVolumeImplWorker-" + parent.toString() + "-%d")
        .build();
    cacheExecutor = new ThreadPoolExecutor(
        1, maxNumThreads,
        60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(),
        workerFactory);
    cacheExecutor.allowCoreThreadTimeOut(true);
  }
  
  File getCurrentDir() {
    return currentDir;
  }
  
  File getRbwDir(String bpid) throws IOException {
    return getBlockPoolSlice(bpid).getRbwDir();
  }
  
  void decDfsUsed(String bpid, long value) {
    synchronized(dataset) {
      BlockPoolSlice bp = bpSlices.get(bpid);
      if (bp != null) {
        bp.decDfsUsed(value);
      }
    }
  }
  
  long getDfsUsed() throws IOException {
    long dfsUsed = 0;
    synchronized(dataset) {
      for(BlockPoolSlice s : bpSlices.values()) {
        dfsUsed += s.getDfsUsed();
      }
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
  long getCapacity() {
    long remaining = usage.getCapacity() - reserved;
    return remaining > 0 ? remaining : 0;
  }

  @Override
  public long getAvailable() throws IOException {
    long remaining = getCapacity()-getDfsUsed();
    long available = usage.getAvailable();
    if (remaining > available) {
      remaining = available;
    }
    return (remaining > 0) ? remaining : 0;
  }
    
  long getReserved(){
    return reserved;
  }

  BlockPoolSlice getBlockPoolSlice(String bpid) throws IOException {
    BlockPoolSlice bp = bpSlices.get(bpid);
    if (bp == null) {
      throw new IOException("block pool " + bpid + " is not found");
    }
    return bp;
  }

  @Override
  public String getBasePath() {
    return currentDir.getParent();
  }
  
  @Override
  public String getPath(String bpid) throws IOException {
    return getBlockPoolSlice(bpid).getDirectory().getAbsolutePath();
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
    return bpSlices.keySet().toArray(new String[bpSlices.keySet().size()]);   
  }
    
  /**
   * Temporary files. They get moved to the finalized block directory when
   * the block is finalized.
   */
  File createTmpFile(String bpid, Block b) throws IOException {
    return getBlockPoolSlice(bpid).createTmpFile(b);
  }

  /**
   * RBW files. They get moved to the finalized block directory when
   * the block is finalized.
   */
  File createRbwFile(String bpid, Block b) throws IOException {
    return getBlockPoolSlice(bpid).createRbwFile(b);
  }

  File addBlock(String bpid, Block b, File f) throws IOException {
    return getBlockPoolSlice(bpid).addBlock(b, f);
  }

  Executor getCacheExecutor() {
    return cacheExecutor;
  }

  void checkDirs() throws DiskErrorException {
    // TODO:FEDERATION valid synchronization
    for(BlockPoolSlice s : bpSlices.values()) {
      s.checkDirs();
    }
  }
    
  void getVolumeMap(ReplicaMap volumeMap) throws IOException {
    for(BlockPoolSlice s : bpSlices.values()) {
      s.getVolumeMap(volumeMap);
    }
  }
  
  void getVolumeMap(String bpid, ReplicaMap volumeMap) throws IOException {
    getBlockPoolSlice(bpid).getVolumeMap(volumeMap);
  }
  
  /**
   * Add replicas under the given directory to the volume map
   * @param volumeMap the replicas map
   * @param dir an input directory
   * @param isFinalized true if the directory has finalized replicas;
   *                    false if the directory has rbw replicas
   * @throws IOException 
   */
  void addToReplicasMap(String bpid, ReplicaMap volumeMap, 
      File dir, boolean isFinalized) throws IOException {
    BlockPoolSlice bp = getBlockPoolSlice(bpid);
    // TODO move this up
    // dfsUsage.incDfsUsed(b.getNumBytes()+metaFile.length());
    bp.addToReplicasMap(volumeMap, dir, isFinalized);
  }

  @Override
  public String toString() {
    return currentDir.getAbsolutePath();
  }

  void shutdown() {
    cacheExecutor.shutdown();
    Set<Entry<String, BlockPoolSlice>> set = bpSlices.entrySet();
    for (Entry<String, BlockPoolSlice> entry : set) {
      entry.getValue().shutdown();
    }
  }

  void addBlockPool(String bpid, Configuration conf) throws IOException {
    File bpdir = new File(currentDir, bpid);
    BlockPoolSlice bp = new BlockPoolSlice(bpid, this, bpdir, conf);
    bpSlices.put(bpid, bp);
  }
  
  void shutdownBlockPool(String bpid) {
    BlockPoolSlice bp = bpSlices.get(bpid);
    if (bp != null) {
      bp.shutdown();
    }
    bpSlices.remove(bpid);
  }

  boolean isBPDirEmpty(String bpid) throws IOException {
    File volumeCurrentDir = this.getCurrentDir();
    File bpDir = new File(volumeCurrentDir, bpid);
    File bpCurrentDir = new File(bpDir, DataStorage.STORAGE_DIR_CURRENT);
    File finalizedDir = new File(bpCurrentDir,
        DataStorage.STORAGE_DIR_FINALIZED);
    File rbwDir = new File(bpCurrentDir, DataStorage.STORAGE_DIR_RBW);
    if (finalizedDir.exists() && !DatanodeUtil.dirNoFilesRecursive(
        finalizedDir)) {
      return false;
    }
    if (rbwDir.exists() && FileUtil.list(rbwDir).length != 0) {
      return false;
    }
    return true;
  }
  
  void deleteBPDirectories(String bpid, boolean force) throws IOException {
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
      if (!DatanodeUtil.dirNoFilesRecursive(finalizedDir) ||
          !FileUtil.fullyDelete(finalizedDir)) {
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

  @Override
  public String getStorageID() {
    return storageID;
  }
  
  @Override
  public StorageType getStorageType() {
    return storageType;
  }
  
  DatanodeStorage toDatanodeStorage() {
    return new DatanodeStorage(storageID, DatanodeStorage.State.NORMAL, storageType);
  }

}

