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
package org.apache.hadoop.hdfs.server.namenode.syncservice;

import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.ProvidedVolumeInfo;
import org.apache.hadoop.hdfs.server.namenode.FSDirUtil;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Cache evictor for provided storage writeBack mount.
 */
public abstract class WriteCacheEvictor {
  private static final Logger LOG =
      LoggerFactory.getLogger(WriteCacheEvictor.class);
  protected Queue<Long> evictQueue;
  protected ScheduledExecutorService evictExecutor;
  protected FSNamesystem fsNamesystem;
  private static WriteCacheEvictor writeCacheEvictor = null;
  protected long writeCacheQuota;
  private boolean isRunning;

  protected WriteCacheEvictor(Configuration conf, FSNamesystem fsNamesystem) {
    // Access by multi-thread.
    this.evictQueue = new ConcurrentLinkedQueue<>();
    this.fsNamesystem = fsNamesystem;
    this.evictExecutor = Executors.newSingleThreadScheduledExecutor();
    this.writeCacheQuota = conf.getLong(
        DFSConfigKeys.DFS_PROVIDED_WRITE_CACHE_QUOTA_SIZE,
        DFSConfigKeys.DFS_PROVIDED_WRITE_CACHE_QUOTA_SIZE_DEFAULT);
  }

  /**
   * This method is just for test use.
   */
  @VisibleForTesting
  void setFsNamesystem(FSNamesystem fsNamesystem) {
    this.fsNamesystem = fsNamesystem;
  }

  public static WriteCacheEvictor getInstance(Configuration conf,
      FSNamesystem fsNamesystem) {
    if (writeCacheEvictor == null) {
      String evictorClassName =
          conf.get(DFSConfigKeys.DFS_PROVIDED_WRITE_CACHE_EVICTOR_CLASS,
              DFSConfigKeys.DFS_PROVIDED_WRITE_CACHE_EVICTOR_CLASS_DEFAULT);
      try {
        Class evictorClass =
            ClassLoader.getSystemClassLoader().loadClass(evictorClassName);
        writeCacheEvictor = (WriteCacheEvictor) evictorClass.getConstructor(
            Configuration.class, FSNamesystem.class).newInstance(
            conf, fsNamesystem);
        LOG.info(evictorClass.getSimpleName() +
            " will be used to evict provided write cache.");
      } catch (Throwable t) {
        LOG.warn("Failed to load given class or create new instance. " +
            "Trying to directly instantiate " +
            FIFOWriteCacheEvictor.class.getSimpleName());
        writeCacheEvictor = new FIFOWriteCacheEvictor(conf, fsNamesystem);
      }
    }
    return writeCacheEvictor;
  }

  public long getWriteCacheQuota() {
    return writeCacheQuota;
  }

  public long getWriteCacheUsed() {
    List<ProvidedVolumeInfo> providedVols = fsNamesystem
        .getMountManager()
        .getSyncMountManager()
        .getWriteBackMounts();
    List<String> writeBackPaths = providedVols.stream()
        .map(providedVol -> providedVol.getMountPath())
        .collect(Collectors.toList());
    return writeBackPaths
        .stream()
        .collect(Collectors.summingLong(this::getSpaceUsed));
  }

  /**
   * Get storage space used, all replicas belong to files under src
   * are covered in the calculation. Block on provided storage will
   * be excluded.
   * {@link FSNamesystem#getQuotaUsage#getSpaceUsed getSpaceUsed} will
   * consider data in snapshot, which doesn't meet our purpose.
   */
  public long getSpaceUsed(String src) {
    try {
      INode mountPointINode = fsNamesystem
          .getFSDirectory()
          .getINode(src, FSDirectory.DirOp.READ);
      INodeDirectory iNodeDir = mountPointINode.asDirectory();
      return computeSpaceUsed(iNodeDir);
    } catch (IOException e) {
      LOG.warn("Failed to get space consumed for " + src +
          " due to " + e.getMessage());
      return 0L;
    }
  }

  public long computeSpaceUsed(INodeDirectory iNodeDir) {
    List<INode> children =
        Lists.newArrayList(iNodeDir.getChildrenList(Snapshot.CURRENT_STATE_ID));
    long size = 0L;
    for (INode iNode : children) {
      if (iNode.isFile()) {
        INodeFile iNodeFile = iNode.asFile();
        if (hasNormalStorage(iNodeFile)) {
          size += iNodeFile.computeFileSize() *
              iNodeFile.getFileReplication();
        }
      } else if (iNode.isDirectory()) {
        size += computeSpaceUsed(iNode.asDirectory());
      }
    }
    return size;
  }

  /**
   * Check the last block to see whether the given file has
   * normal storage. If so, we think the blocks on normal
   * storage are not evicted.
   */
  public boolean hasNormalStorage(INodeFile iNodeFile) {
    Iterator<DatanodeStorageInfo> iter =
        iNodeFile.getLastBlock().getStorageInfos();
    while (iter.hasNext()) {
      if (iter.next().getStorageType() != StorageType.PROVIDED) {
        return true;
      }
    }
    return false;
  }

  @VisibleForTesting
  public static void destroyInstance() {
    writeCacheEvictor = null;
  }

  public void start() {
    isRunning = true;
    restore();
    // Storage space will be freed in async way after some blocks are deleted.
    // So the period should not be too small.
    evictExecutor.scheduleAtFixedRate(
        this::evict, 10, 60, TimeUnit.SECONDS);
  }

  public void stop() {
    LOG.info("Shutting down cache eviction service...");
    evictExecutor.shutdown();
    isRunning = false;
  }

  public boolean isRunning() {
    return isRunning;
  }

  /**
   * Considering cluster restart case, we need to restore evict queue for
   * write back mount. Blocks already synced to provided storage, but not
   * evicted from local storage needs to be put into evict queue.
   */
  protected void restore() {
    List<ProvidedVolumeInfo> providedVolumes =
        fsNamesystem.getMountManager().getSyncMountManager()
            .getWriteBackMounts();
    for (ProvidedVolumeInfo providedVolume : providedVolumes) {
      String mountPoint = providedVolume.getMountPath();
      try {
        INode mountPointINode = fsNamesystem
            .getFSDirectory()
            .getINode(mountPoint, FSDirectory.DirOp.READ);
        INodeDirectory iNodeDir = mountPointINode.asDirectory();
        addBlocksToEvictQueue(iNodeDir);
      } catch (IOException e) {
        LOG.warn("Exception in restore evict queue for provided storage " +
            "write back mount." + e.getMessage());
      }
    }
  }

  /**
   * Assume a file's all blocks are in same eviction state. So we only pick
   * one block to see whether this file's local blocks can be evicted.
   */
  private void addBlocksToEvictQueue(INodeDirectory iNodeDir) {
    List<INode> children =
        Lists.newArrayList(iNodeDir.getChildrenList(Snapshot.CURRENT_STATE_ID));
    for (INode iNode : children) {
      if (iNode.isFile()) {
        INodeFile iNodeFile = iNode.asFile();
        // update storage used
        BlockInfo[] blocks = iNodeFile.getBlocks();
        // Skip empty file
        if (blocks.length == 0) {
          continue;
        }
        BlockInfo blockInfo = blocks[0];
        Optional<BlockInfo> blockInfoOpt = Optional.of(blockInfo);
        blockInfoOpt.filter(block -> isSyncedButNotEvicted(block)).ifPresent(
            block -> add(block.getBlockCollectionId()));
      } else if (iNode.isDirectory()) {
        addBlocksToEvictQueue(iNode.asDirectory());
      }
    }
  }

  /**
   * Check the block's storage type and return true if the block has both
   * provided storage and local HDFS storage.
   */
  public boolean isSyncedButNotEvicted(BlockInfo blockInfo) {
    Iterator<DatanodeStorageInfo> storageInfos = blockInfo.getStorageInfos();
    boolean isSynced = false;
    boolean isNotEvicted = false;
    while (storageInfos.hasNext()) {
      StorageType storageType = storageInfos.next().getStorageType();
      if (storageType == StorageType.PROVIDED) {
        isSynced = true;
      } else {
        isNotEvicted = true;
      }
      if (isSynced && isNotEvicted) {
        return true;
      }
    }
    return false;
  }

  protected void add(long blkCollectId) {
    if (!isRunning) {
      LOG.warn("Cache evictor is not running, ignore the request for " +
          "evicting blocks [blockCollectionId=" + blkCollectId + "]");
      return;
    }
    this.evictQueue.add(blkCollectId);
  }

  /**
   * Evict the cache according to a specific policy.
   */
  abstract void evict();

  public List<Long> removeCacheUnderMount(String mountPath) throws IOException {
    List<Long> blkCollectId = new LinkedList<>();
    List<INodeFile> iNodeFiles =
        FSDirUtil.getInodeFiles(fsNamesystem, mountPath);
    for (INodeFile iNodeFile : iNodeFiles) {
      if (evictQueue.remove(iNodeFile.getId())) {
        blkCollectId.add(iNodeFile.getId());
      }
      removeLocalBlocks(iNodeFile.getBlocks());
    }
    return blkCollectId;
  }

  protected void removeLocalBlocks(BlockInfo[] blocks) {
    BlockManager blockManager = fsNamesystem.getBlockManager();
    for (BlockInfo block : blocks) {
      Iterator<DatanodeStorageInfo> iter = block.getStorageInfos();
      while (iter.hasNext()) {
        DatanodeStorageInfo storageInfo = iter.next();
        // Skip provided storage
        if (storageInfo.getStorageType() == StorageType.PROVIDED) {
          continue;
        }
//        blockManager.processExtraRedundancyBlock(block,
//            blockManager.getExpectedRedundancyNum(block),
//            storageInfo.getDatanodeDescriptor(), null);
        blockManager.removeLocalBlock(block, storageInfo);
      }
    }
  }
}
