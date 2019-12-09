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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.datanode.FileIoProvider;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.DataNodeVolumeMetrics;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.FinalizedReplica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBeingWritten;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.hdfs.server.datanode.LocalReplica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBuilder;
import org.apache.hadoop.hdfs.server.datanode.LocalReplicaInPipeline;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipeline;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.DirectoryScanner.BlockDirFilter;
import org.apache.hadoop.hdfs.server.datanode.DirectoryScanner.ReportCompiler;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.RamDiskReplicaTracker.RamDiskReplica;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.util.CloseableReferenceCount;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * The underlying volume used to store replica.
 * 
 * It uses the {@link FsDatasetImpl} object for synchronization.
 */
@InterfaceAudience.Private
@VisibleForTesting
public class FsVolumeImpl implements FsVolumeSpi {
  public static final Logger LOG =
      LoggerFactory.getLogger(FsVolumeImpl.class);
  private static final ObjectWriter WRITER =
      new ObjectMapper().writerWithDefaultPrettyPrinter();
  private static final ObjectReader READER =
      new ObjectMapper().readerFor(BlockIteratorState.class);

  private final FsDatasetImpl dataset;
  private final String storageID;
  private final StorageType storageType;
  private final Map<String, BlockPoolSlice> bpSlices
      = new ConcurrentHashMap<String, BlockPoolSlice>();

  // Refers to the base StorageLocation used to construct this volume
  // (i.e., does not include STORAGE_DIR_CURRENT in
  // <location>/STORAGE_DIR_CURRENT/)
  private final StorageLocation storageLocation;

  private final File currentDir;    // <StorageDirectory>/current
  private final DF usage;
  private final ReservedSpaceCalculator reserved;
  private CloseableReferenceCount reference = new CloseableReferenceCount();

  // Disk space reserved for blocks (RBW or Re-replicating) open for write.
  private AtomicLong reservedForReplicas;
  private long recentReserved = 0;
  private final Configuration conf;
  // Capacity configured. This is useful when we want to
  // limit the visible capacity for tests. If negative, then we just
  // query from the filesystem.
  protected volatile long configuredCapacity;
  private final FileIoProvider fileIoProvider;
  private final DataNodeVolumeMetrics metrics;

  /**
   * Per-volume worker pool that processes new blocks to cache.
   * The maximum number of workers per volume is bounded (configurable via
   * dfs.datanode.fsdatasetcache.max.threads.per.volume) to limit resource
   * contention.
   */
  protected ThreadPoolExecutor cacheExecutor;

  FsVolumeImpl(FsDatasetImpl dataset, String storageID, StorageDirectory sd,
      FileIoProvider fileIoProvider, Configuration conf) throws IOException {
    // outside tests, usage created in ReservedSpaceCalculator.Builder
    this(dataset, storageID, sd, fileIoProvider, conf, null);
  }

  FsVolumeImpl(FsDatasetImpl dataset, String storageID, StorageDirectory sd,
      FileIoProvider fileIoProvider, Configuration conf, DF usage)
      throws IOException {

    if (sd.getStorageLocation() == null) {
      throw new IOException("StorageLocation specified for storage directory " +
          sd + " is null");
    }
    this.dataset = dataset;
    this.storageID = storageID;
    this.reservedForReplicas = new AtomicLong(0L);
    this.storageLocation = sd.getStorageLocation();
    this.currentDir = sd.getCurrentDir();
    this.storageType = storageLocation.getStorageType();
    this.configuredCapacity = -1;
    this.usage = usage;
    if (this.usage != null) {
      reserved = new ReservedSpaceCalculator.Builder(conf)
          .setUsage(this.usage).setStorageType(storageType).build();
    } else {
      reserved = null;
      LOG.warn("Setting reserved to null as usage is null");
    }
    if (currentDir != null) {
      File parent = currentDir.getParentFile();
      cacheExecutor = initializeCacheExecutor(parent);
      this.metrics = DataNodeVolumeMetrics.create(conf, parent.getPath());
    } else {
      cacheExecutor = null;
      this.metrics = null;
    }
    this.conf = conf;
    this.fileIoProvider = fileIoProvider;
  }

  protected ThreadPoolExecutor initializeCacheExecutor(File parent) {
    if (storageType.isTransient()) {
      return null;
    }
    if (dataset.datanode == null) {
      // FsVolumeImpl is used in test.
      return null;
    }

    final int maxNumThreads = dataset.datanode.getConf().getInt(
        DFSConfigKeys.DFS_DATANODE_FSDATASETCACHE_MAX_THREADS_PER_VOLUME_KEY,
        DFSConfigKeys.DFS_DATANODE_FSDATASETCACHE_MAX_THREADS_PER_VOLUME_DEFAULT);

    ThreadFactory workerFactory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("FsVolumeImplWorker-" + parent.toString() + "-%d")
        .build();
    ThreadPoolExecutor executor = new ThreadPoolExecutor(
        1, maxNumThreads,
        60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(),
        workerFactory);
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  private void printReferenceTraceInfo(String op) {
    StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    for (StackTraceElement ste : stack) {
      switch (ste.getMethodName()) {
      case "getDfsUsed":
      case "getBlockPoolUsed":
      case "getAvailable":
      case "getVolumeMap":
        return;
      default:
        break;
      }
    }
    FsDatasetImpl.LOG.trace("Reference count: " + op + " " + this + ": " +
        this.reference.getReferenceCount());
    FsDatasetImpl.LOG.trace(
        Joiner.on("\n").join(Thread.currentThread().getStackTrace()));
  }

  /**
   * Increase the reference count. The caller must increase the reference count
   * before issuing IOs.
   *
   * @throws IOException if the volume is already closed.
   */
  private void reference() throws ClosedChannelException {
    this.reference.reference();
    if (FsDatasetImpl.LOG.isTraceEnabled()) {
      printReferenceTraceInfo("incr");
    }
  }

  /**
   * Decrease the reference count.
   */
  private void unreference() {
    if (FsDatasetImpl.LOG.isTraceEnabled()) {
      printReferenceTraceInfo("desc");
    }
    if (FsDatasetImpl.LOG.isDebugEnabled()) {
      if (reference.getReferenceCount() <= 0) {
        FsDatasetImpl.LOG.debug("Decrease reference count <= 0 on " + this +
          Joiner.on("\n").join(Thread.currentThread().getStackTrace()));
      }
    }
    checkReference();
    this.reference.unreference();
  }

  private static class FsVolumeReferenceImpl implements FsVolumeReference {
    private FsVolumeImpl volume;

    FsVolumeReferenceImpl(FsVolumeImpl volume) throws ClosedChannelException {
      this.volume = volume;
      volume.reference();
    }

    /**
     * Decreases the reference count.
     * @throws IOException it never throws IOException.
     */
    @Override
    public void close() throws IOException {
      if (volume != null) {
        volume.unreference();
        volume = null;
      }
    }

    @Override
    public FsVolumeSpi getVolume() {
      return this.volume;
    }
  }

  @Override
  public FsVolumeReference obtainReference() throws ClosedChannelException {
    return new FsVolumeReferenceImpl(this);
  }

  private void checkReference() {
    Preconditions.checkState(reference.getReferenceCount() > 0);
  }

  @VisibleForTesting
  int getReferenceCount() {
    return this.reference.getReferenceCount();
  }

  /**
   * Close this volume.
   * @throws IOException if the volume is closed.
   */
  void setClosed() throws IOException {
    try {
      this.reference.setClosed();
      dataset.stopAllDataxceiverThreads(this);
    } catch (ClosedChannelException e) {
      throw new IOException("The volume has already closed.", e);
    }
  }

  /**
   * Check whether this volume has successfully been closed.
   */
  boolean checkClosed() {
    if (this.reference.getReferenceCount() > 0) {
      if (FsDatasetImpl.LOG.isDebugEnabled()) {
        FsDatasetImpl.LOG.debug(String.format(
            "The reference count for %s is %d, wait to be 0.",
            this, reference.getReferenceCount()));
      }
      return false;
    }
    return true;
  }

  @VisibleForTesting
  File getCurrentDir() {
    return currentDir;
  }
  
  protected File getRbwDir(String bpid) throws IOException {
    return getBlockPoolSlice(bpid).getRbwDir();
  }

  protected File getLazyPersistDir(String bpid) throws IOException {
    return getBlockPoolSlice(bpid).getLazypersistDir();
  }

  protected File getTmpDir(String bpid) throws IOException {
    return getBlockPoolSlice(bpid).getTmpDir();
  }

  void onBlockFileDeletion(String bpid, long value) {
    decDfsUsedAndNumBlocks(bpid, value, true);
    if (isTransientStorage()) {
      dataset.releaseLockedMemory(value, true);
    }
  }

  void onMetaFileDeletion(String bpid, long value) {
    decDfsUsedAndNumBlocks(bpid, value, false);
  }

  private void decDfsUsedAndNumBlocks(String bpid, long value,
                                      boolean blockFileDeleted) {
    // BlockPoolSlice map is thread safe, and update the space used or
    // number of blocks are atomic operations, so it doesn't require to
    // hold the dataset lock.
    BlockPoolSlice bp = bpSlices.get(bpid);
    if (bp != null) {
      bp.decDfsUsed(value);
      if (blockFileDeleted) {
        bp.decrNumBlocks();
      }
    }
  }

  void incDfsUsedAndNumBlocks(String bpid, long value) {
    BlockPoolSlice bp = bpSlices.get(bpid);
    if (bp != null) {
      bp.incDfsUsed(value);
      bp.incrNumBlocks();
    }
  }

  void incDfsUsed(String bpid, long value) {
    BlockPoolSlice bp = bpSlices.get(bpid);
    if (bp != null) {
      bp.incDfsUsed(value);
    }
  }

  @VisibleForTesting
  public long getDfsUsed() throws IOException {
    long dfsUsed = 0;
    for(BlockPoolSlice s : bpSlices.values()) {
      dfsUsed += s.getDfsUsed();
    }
    return dfsUsed;
  }

  long getBlockPoolUsed(String bpid) throws IOException {
    return getBlockPoolSlice(bpid).getDfsUsed();
  }
  
  /**
   * Return either the configured capacity of the file system if configured; or
   * the capacity of the file system excluding space reserved for non-HDFS.
   * 
   * @return the unreserved number of bytes left in this filesystem. May be
   *         zero.
   */
  @VisibleForTesting
  public long getCapacity() {
    if (configuredCapacity < 0) {
      long remaining = usage.getCapacity() - getReserved();
      return remaining > 0 ? remaining : 0;
    }

    return configuredCapacity;
  }

  /**
   * This function MUST NOT be used outside of tests.
   *
   * @param capacity
   */
  @VisibleForTesting
  public void setCapacityForTesting(long capacity) {
    this.configuredCapacity = capacity;
  }

  /*
   * Calculate the available space of the filesystem, excluding space reserved
   * for non-HDFS and space reserved for RBW
   * 
   * @return the available number of bytes left in this filesystem. May be zero.
   */
  @Override
  public long getAvailable() throws IOException {
    long remaining = getCapacity() - getDfsUsed() - getReservedForReplicas();
    long available = usage.getAvailable()  - getRemainingReserved()
        - getReservedForReplicas();
    if (remaining > available) {
      remaining = available;
    }
    return (remaining > 0) ? remaining : 0;
  }

  long getActualNonDfsUsed() throws IOException {
    return usage.getUsed() - getDfsUsed();
  }

  private long getRemainingReserved() throws IOException {
    long actualNonDfsUsed = getActualNonDfsUsed();
    long actualReserved = getReserved();
    if (actualNonDfsUsed < actualReserved) {
      return actualReserved - actualNonDfsUsed;
    }
    return 0L;
  }

  /**
   * Unplanned Non-DFS usage, i.e. Extra usage beyond reserved.
   *
   * @return Disk usage excluding space used by HDFS and excluding space
   * reserved for blocks open for write.
   * @throws IOException
   */
  public long getNonDfsUsed() throws IOException {
    long actualNonDfsUsed = getActualNonDfsUsed();
    long actualReserved = getReserved();
    if (actualNonDfsUsed < actualReserved) {
      return 0L;
    }
    return actualNonDfsUsed - actualReserved;
  }

  @VisibleForTesting
  long getDfAvailable() {
    return usage.getAvailable();
  }

  @VisibleForTesting
  public long getReservedForReplicas() {
    return reservedForReplicas.get();
  }

  @VisibleForTesting
  long getRecentReserved() {
    return recentReserved;
  }

  long getReserved(){
    return reserved != null ? reserved.getReserved() : 0;
  }

  @VisibleForTesting
  BlockPoolSlice getBlockPoolSlice(String bpid) throws IOException {
    BlockPoolSlice bp = bpSlices.get(bpid);
    if (bp == null) {
      throw new IOException("block pool " + bpid + " is not found");
    }
    return bp;
  }

  @Override
  public URI getBaseURI() {
    return new File(currentDir.getParent()).toURI();
  }

  @Override
  public DF getUsageStats(Configuration conf) {
    if (currentDir != null) {
      try {
        return new DF(new File(currentDir.getParent()), conf);
      } catch (IOException e) {
        LOG.error("Unable to get disk statistics for volume " + this);
      }
    }
    return null;
  }

  @Override
  public StorageLocation getStorageLocation() {
    return storageLocation;
  }

  @Override
  public boolean isTransientStorage() {
    return storageType.isTransient();
  }

  @VisibleForTesting
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
    checkReference();
    reserveSpaceForReplica(b.getNumBytes());
    try {
      return getBlockPoolSlice(bpid).createTmpFile(b);
    } catch (IOException exception) {
      releaseReservedSpace(b.getNumBytes());
      throw exception;
    }
  }

  @Override
  public void reserveSpaceForReplica(long bytesToReserve) {
    if (bytesToReserve != 0) {
      reservedForReplicas.addAndGet(bytesToReserve);
      recentReserved = bytesToReserve;
    }
  }

  @Override
  public void releaseReservedSpace(long bytesToRelease) {
    if (bytesToRelease != 0) {

      long oldReservation, newReservation;
      do {
        oldReservation = reservedForReplicas.get();
        newReservation = oldReservation - bytesToRelease;
        if (newReservation < 0) {
          // Failsafe, this should never occur in practice, but if it does we
          // don't want to start advertising more space than we have available.
          newReservation = 0;
        }
      } while (!reservedForReplicas.compareAndSet(oldReservation,
          newReservation));
    }
  }

  @Override
  public void releaseLockedMemory(long bytesToRelease) {
    if (isTransientStorage()) {
      dataset.releaseLockedMemory(bytesToRelease, false);
    }
  }

  private enum SubdirFilter implements FilenameFilter {
    INSTANCE;

    @Override
    public boolean accept(File dir, String name) {
      return name.startsWith("subdir");
    }
  }

  private enum BlockFileFilter implements FilenameFilter {
    INSTANCE;

    @Override
    public boolean accept(File dir, String name) {
      return !name.endsWith(".meta") &&
              name.startsWith(Block.BLOCK_FILE_PREFIX);
    }
  }

  @VisibleForTesting
  public static String nextSorted(List<String> arr, String prev) {
    int res = 0;
    if (prev != null) {
      res = Collections.binarySearch(arr, prev);
      if (res < 0) {
        res = -1 - res;
      } else {
        res++;
      }
    }
    if (res >= arr.size()) {
      return null;
    }
    return arr.get(res);
  }

  private static class BlockIteratorState {
    BlockIteratorState() {
      lastSavedMs = iterStartMs = Time.now();
      curFinalizedDir = null;
      curFinalizedSubDir = null;
      curEntry = null;
      atEnd = false;
    }

    // The wall-clock ms since the epoch at which this iterator was last saved.
    @JsonProperty
    private long lastSavedMs;

    // The wall-clock ms since the epoch at which this iterator was created.
    @JsonProperty
    private long iterStartMs;

    @JsonProperty
    private String curFinalizedDir;

    @JsonProperty
    private String curFinalizedSubDir;

    @JsonProperty
    private String curEntry;

    @JsonProperty
    private boolean atEnd;
  }

  /**
   * A BlockIterator implementation for FsVolumeImpl.
   */
  private class BlockIteratorImpl implements FsVolumeSpi.BlockIterator {
    private final File bpidDir;
    private final String name;
    private final String bpid;
    private long maxStalenessMs = 0;

    private List<String> cache;
    private long cacheMs;

    private BlockIteratorState state;

    BlockIteratorImpl(String bpid, String name) {
      this.bpidDir = new File(currentDir, bpid);
      this.name = name;
      this.bpid = bpid;
      rewind();
    }

    /**
     * Get the next subdirectory within the block pool slice.
     *
     * @return         The next subdirectory within the block pool slice, or
     *                   null if there are no more.
     */
    private String getNextSubDir(String prev, File dir)
          throws IOException {
      List<String> children = fileIoProvider.listDirectory(
          FsVolumeImpl.this, dir, SubdirFilter.INSTANCE);
      cache = null;
      cacheMs = 0;
      if (children.size() == 0) {
        LOG.trace("getNextSubDir({}, {}): no subdirectories found in {}",
            storageID, bpid, dir.getAbsolutePath());
        return null;
      }
      Collections.sort(children);
      String nextSubDir = nextSorted(children, prev);
      if (nextSubDir == null) {
        LOG.trace("getNextSubDir({}, {}): no more subdirectories found in {}",
            storageID, bpid, dir.getAbsolutePath());
      } else {
        LOG.trace("getNextSubDir({}, {}): picking next subdirectory {} " +
            "within {}", storageID, bpid, nextSubDir, dir.getAbsolutePath());
      }
      return nextSubDir;
    }

    private String getNextFinalizedDir() throws IOException {
      File dir = Paths.get(
          bpidDir.getAbsolutePath(), "current", "finalized").toFile();
      return getNextSubDir(state.curFinalizedDir, dir);
    }

    private String getNextFinalizedSubDir() throws IOException {
      if (state.curFinalizedDir == null) {
        return null;
      }
      File dir = Paths.get(
          bpidDir.getAbsolutePath(), "current", "finalized",
              state.curFinalizedDir).toFile();
      return getNextSubDir(state.curFinalizedSubDir, dir);
    }

    private List<String> getSubdirEntries() throws IOException {
      if (state.curFinalizedSubDir == null) {
        return null; // There are no entries in the null subdir.
      }
      long now = Time.monotonicNow();
      if (cache != null) {
        long delta = now - cacheMs;
        if (delta < maxStalenessMs) {
          return cache;
        } else {
          LOG.trace("getSubdirEntries({}, {}): purging entries cache for {} " +
            "after {} ms.", storageID, bpid, state.curFinalizedSubDir, delta);
          cache = null;
        }
      }
      File dir = Paths.get(bpidDir.getAbsolutePath(), "current", "finalized",
                    state.curFinalizedDir, state.curFinalizedSubDir).toFile();
      List<String> entries = fileIoProvider.listDirectory(
          FsVolumeImpl.this, dir, BlockFileFilter.INSTANCE);
      if (entries.size() == 0) {
        entries = null;
      } else {
        Collections.sort(entries);
      }
      if (entries == null) {
        LOG.trace("getSubdirEntries({}, {}): no entries found in {}",
            storageID, bpid, dir.getAbsolutePath());
      } else {
        LOG.trace("getSubdirEntries({}, {}): listed {} entries in {}", 
            storageID, bpid, entries.size(), dir.getAbsolutePath());
      }
      cache = entries;
      cacheMs = now;
      return cache;
    }

    /**
     * Get the next block.<p/>
     *
     * Each volume has a hierarchical structure.<p/>
     *
     * <code>
     * BPID B0
     *   finalized/
     *     subdir0
     *       subdir0
     *         blk_000
     *         blk_001
     *       ...
     *     subdir1
     *       subdir0
     *         ...
     *   rbw/
     * </code>
     *
     * When we run out of entries at one level of the structure, we search
     * progressively higher levels.  For example, when we run out of blk_
     * entries in a subdirectory, we search for the next subdirectory.
     * And so on.
     */
    @Override
    public ExtendedBlock nextBlock() throws IOException {
      if (state.atEnd) {
        return null;
      }
      try {
        while (true) {
          List<String> entries = getSubdirEntries();
          if (entries != null) {
            state.curEntry = nextSorted(entries, state.curEntry);
            if (state.curEntry == null) {
              LOG.trace("nextBlock({}, {}): advancing from {} to next " +
                  "subdirectory.", storageID, bpid, state.curFinalizedSubDir);
            } else {
              ExtendedBlock block =
                  new ExtendedBlock(bpid, Block.filename2id(state.curEntry));
              File expectedBlockDir = DatanodeUtil.idToBlockDir(
                  new File("."), block.getBlockId());
              File actualBlockDir = Paths.get(".",
                  state.curFinalizedDir, state.curFinalizedSubDir).toFile();
              if (!expectedBlockDir.equals(actualBlockDir)) {
                LOG.error("nextBlock({}, {}): block id {} found in invalid " +
                    "directory.  Expected directory: {}.  " +
                    "Actual directory: {}", storageID, bpid,
                    block.getBlockId(), expectedBlockDir.getPath(),
                    actualBlockDir.getPath());
                continue;
              }

              File blkFile = getBlockFile(bpid, block);
              File metaFile = FsDatasetUtil.findMetaFile(blkFile);
              block.setGenerationStamp(
                  Block.getGenerationStamp(metaFile.getName()));
              block.setNumBytes(blkFile.length());

              LOG.trace("nextBlock({}, {}): advancing to {}",
                  storageID, bpid, block);
              return block;
            }
          }
          state.curFinalizedSubDir = getNextFinalizedSubDir();
          if (state.curFinalizedSubDir == null) {
            state.curFinalizedDir = getNextFinalizedDir();
            if (state.curFinalizedDir == null) {
              state.atEnd = true;
              return null;
            }
          }
        }
      } catch (IOException e) {
        state.atEnd = true;
        LOG.error("nextBlock({}, {}): I/O error", storageID, bpid, e);
        throw e;
      }
    }

    private File getBlockFile(String bpid, ExtendedBlock blk)
        throws IOException {
      return new File(DatanodeUtil.idToBlockDir(getFinalizedDir(bpid),
          blk.getBlockId()).toString() + "/" + blk.getBlockName());
    }

    @Override
    public boolean atEnd() {
      return state.atEnd;
    }

    @Override
    public void rewind() {
      cache = null;
      cacheMs = 0;
      state = new BlockIteratorState();
    }

    @Override
    public void save() throws IOException {
      state.lastSavedMs = Time.now();
      boolean success = false;
      try (BufferedWriter writer = new BufferedWriter(
          new OutputStreamWriter(fileIoProvider.getFileOutputStream(
              FsVolumeImpl.this, getTempSaveFile()), "UTF-8"))) {
        WRITER.writeValue(writer, state);
        success = true;
      } finally {
        if (!success) {
          fileIoProvider.delete(FsVolumeImpl.this, getTempSaveFile());
        }
      }
      fileIoProvider.move(FsVolumeImpl.this,
          getTempSaveFile().toPath(), getSaveFile().toPath(),
          StandardCopyOption.ATOMIC_MOVE);
      if (LOG.isTraceEnabled()) {
        LOG.trace("save({}, {}): saved {}", storageID, bpid,
            WRITER.writeValueAsString(state));
      }
    }

    public void load() throws IOException {
      File file = getSaveFile();
      this.state = READER.readValue(file);
      LOG.trace("load({}, {}): loaded iterator {} from {}: {}", storageID,
          bpid, name, file.getAbsoluteFile(),
          WRITER.writeValueAsString(state));
    }

    File getSaveFile() {
      return new File(bpidDir, name + ".cursor");
    }

    File getTempSaveFile() {
      return new File(bpidDir, name + ".cursor.tmp");
    }

    @Override
    public void setMaxStalenessMs(long maxStalenessMs) {
      this.maxStalenessMs = maxStalenessMs;
    }

    @Override
    public void close() throws IOException {
      // No action needed for this volume implementation.
    }

    @Override
    public long getIterStartMs() {
      return state.iterStartMs;
    }

    @Override
    public long getLastSavedMs() {
      return state.lastSavedMs;
    }

    @Override
    public String getBlockPoolId() {
      return bpid;
    }
  }

  @Override
  public BlockIterator newBlockIterator(String bpid, String name) {
    return new BlockIteratorImpl(bpid, name);
  }

  @Override
  public BlockIterator loadBlockIterator(String bpid, String name)
      throws IOException {
    BlockIteratorImpl iter = new BlockIteratorImpl(bpid, name);
    iter.load();
    return iter;
  }

  @Override
  public FsDatasetSpi<? extends FsVolumeSpi> getDataset() {
    return dataset;
  }

  /**
   * RBW files. They get moved to the finalized block directory when
   * the block is finalized.
   */
  File createRbwFile(String bpid, Block b) throws IOException {
    checkReference();
    reserveSpaceForReplica(b.getNumBytes());
    try {
      return getBlockPoolSlice(bpid).createRbwFile(b);
    } catch (IOException exception) {
      releaseReservedSpace(b.getNumBytes());
      throw exception;
    }
  }

  /**
   *
   * @param bytesReserved Space that was reserved during
   *     block creation. Now that the block is being finalized we
   *     can free up this space.
   * @return
   * @throws IOException
   */
  ReplicaInfo addFinalizedBlock(String bpid, Block b, ReplicaInfo replicaInfo,
      long bytesReserved) throws IOException {
    releaseReservedSpace(bytesReserved);
    File dest = getBlockPoolSlice(bpid).addFinalizedBlock(b, replicaInfo);
    byte[] checksum = null;
    // copy the last partial checksum if the replica is originally
    // in finalized or rbw state.
    if (replicaInfo.getState() == ReplicaState.FINALIZED) {
      FinalizedReplica finalized = (FinalizedReplica)replicaInfo;
      checksum = finalized.getLastPartialChunkChecksum();
    } else if (replicaInfo.getState() == ReplicaState.RBW) {
      ReplicaBeingWritten rbw = (ReplicaBeingWritten)replicaInfo;
      checksum = rbw.getLastChecksumAndDataLen().getChecksum();
    }

    return new ReplicaBuilder(ReplicaState.FINALIZED)
        .setBlock(replicaInfo)
        .setFsVolume(this)
        .setDirectoryToUse(dest.getParentFile())
        .setLastPartialChunkChecksum(checksum)
        .build();
  }

  Executor getCacheExecutor() {
    return cacheExecutor;
  }

  @Override
  public VolumeCheckResult check(VolumeCheckContext ignored)
      throws DiskErrorException {
    // TODO:FEDERATION valid synchronization
    for(BlockPoolSlice s : bpSlices.values()) {
      s.checkDirs();
    }
    return VolumeCheckResult.HEALTHY;
  }
    
  void getVolumeMap(ReplicaMap volumeMap,
                    final RamDiskReplicaTracker ramDiskReplicaMap)
      throws IOException {
    for(BlockPoolSlice s : bpSlices.values()) {
      s.getVolumeMap(volumeMap, ramDiskReplicaMap);
    }
  }
  
  void getVolumeMap(String bpid, ReplicaMap volumeMap,
                    final RamDiskReplicaTracker ramDiskReplicaMap)
      throws IOException {
    getBlockPoolSlice(bpid).getVolumeMap(volumeMap, ramDiskReplicaMap);
  }

  long getNumBlocks() {
    long numBlocks = 0;
    for (BlockPoolSlice s : bpSlices.values()) {
      numBlocks += s.getNumOfBlocks();
    }
    return numBlocks;
  }

  @Override
  public String toString() {
    return currentDir != null ? currentDir.getParent() : "NULL";
  }

  void shutdown() {
    if (cacheExecutor != null) {
      cacheExecutor.shutdown();
    }
    Set<Entry<String, BlockPoolSlice>> set = bpSlices.entrySet();
    for (Entry<String, BlockPoolSlice> entry : set) {
      entry.getValue().shutdown(null);
    }
    if (metrics != null) {
      metrics.unRegister();
    }
  }

  void addBlockPool(String bpid, Configuration c) throws IOException {
    addBlockPool(bpid, c, null);
  }

  void addBlockPool(String bpid, Configuration c, Timer timer)
      throws IOException {
    File bpdir = new File(currentDir, bpid);
    BlockPoolSlice bp;
    if (timer == null) {
      bp = new BlockPoolSlice(bpid, this, bpdir, c, new Timer());
    } else {
      bp = new BlockPoolSlice(bpid, this, bpdir, c, timer);
    }
    bpSlices.put(bpid, bp);
  }
  
  void shutdownBlockPool(String bpid, BlockListAsLongs blocksListsAsLongs) {
    BlockPoolSlice bp = bpSlices.get(bpid);
    if (bp != null) {
      bp.shutdown(blocksListsAsLongs);
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
    if (fileIoProvider.exists(this, finalizedDir) &&
        !DatanodeUtil.dirNoFilesRecursive(this, finalizedDir, fileIoProvider)) {
      return false;
    }
    if (fileIoProvider.exists(this, rbwDir) &&
        fileIoProvider.list(this, rbwDir).length != 0) {
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
    File lazypersistDir = new File(bpCurrentDir,
        DataStorage.STORAGE_DIR_LAZY_PERSIST);
    File rbwDir = new File(bpCurrentDir, DataStorage.STORAGE_DIR_RBW);
    if (force) {
      fileIoProvider.fullyDelete(this, bpDir);
    } else {
      if (!fileIoProvider.delete(this, rbwDir)) {
        throw new IOException("Failed to delete " + rbwDir);
      }
      if (!DatanodeUtil.dirNoFilesRecursive(
              this, finalizedDir, fileIoProvider) ||
          !fileIoProvider.fullyDelete(
              this, finalizedDir)) {
        throw new IOException("Failed to delete " + finalizedDir);
      }
      if (lazypersistDir.exists() &&
          ((!DatanodeUtil.dirNoFilesRecursive(
              this, lazypersistDir, fileIoProvider) ||
              !fileIoProvider.fullyDelete(this, lazypersistDir)))) {
        throw new IOException("Failed to delete " + lazypersistDir);
      }
      fileIoProvider.fullyDelete(this, tmpDir);
      for (File f : fileIoProvider.listFiles(this, bpCurrentDir)) {
        if (!fileIoProvider.delete(this, f)) {
          throw new IOException("Failed to delete " + f);
        }
      }
      if (!fileIoProvider.delete(this, bpCurrentDir)) {
        throw new IOException("Failed to delete " + bpCurrentDir);
      }
      for (File f : fileIoProvider.listFiles(this, bpDir)) {
        if (!fileIoProvider.delete(this, f)) {
          throw new IOException("Failed to delete " + f);
        }
      }
      if (!fileIoProvider.delete(this, bpDir)) {
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


  @Override
  public byte[] loadLastPartialChunkChecksum(
      File blockFile, File metaFile) throws IOException {
    // readHeader closes the temporary FileInputStream.
    DataChecksum dcs;
    try (FileInputStream fis = fileIoProvider.getFileInputStream(
        this, metaFile)) {
      dcs = BlockMetadataHeader.readHeader(fis).getChecksum();
    }
    final int checksumSize = dcs.getChecksumSize();
    final long onDiskLen = blockFile.length();
    final int bytesPerChecksum = dcs.getBytesPerChecksum();

    if (onDiskLen % bytesPerChecksum == 0) {
      // the last chunk is a complete one. No need to preserve its checksum
      // because it will not be modified.
      return null;
    }

    long offsetInChecksum = BlockMetadataHeader.getHeaderSize() +
        (onDiskLen / bytesPerChecksum) * checksumSize;
    byte[] lastChecksum = new byte[checksumSize];
    try (RandomAccessFile raf = fileIoProvider.getRandomAccessFile(
        this, metaFile, "r")) {
      raf.seek(offsetInChecksum);
      int readBytes = raf.read(lastChecksum, 0, checksumSize);
      if (readBytes == -1) {
        throw new IOException("Expected to read " + checksumSize +
            " bytes from offset " + offsetInChecksum +
            " but reached end of file.");
      } else if (readBytes != checksumSize) {
        throw new IOException("Expected to read " + checksumSize +
            " bytes from offset " + offsetInChecksum + " but read " +
            readBytes + " bytes.");
      }
    }
    return lastChecksum;
  }

  public ReplicaInPipeline append(String bpid, ReplicaInfo replicaInfo,
      long newGS, long estimateBlockLen) throws IOException {

    long bytesReserved = estimateBlockLen - replicaInfo.getNumBytes();
    if (getAvailable() < bytesReserved) {
      throw new DiskOutOfSpaceException("Insufficient space for appending to "
          + replicaInfo);
    }

    assert replicaInfo.getVolume() == this:
      "The volume of the replica should be the same as this volume";

    // construct a RBW replica with the new GS
    File newBlkFile = new File(getRbwDir(bpid), replicaInfo.getBlockName());
    LocalReplicaInPipeline newReplicaInfo = new ReplicaBuilder(ReplicaState.RBW)
        .setBlockId(replicaInfo.getBlockId())
        .setLength(replicaInfo.getNumBytes())
        .setGenerationStamp(newGS)
        .setFsVolume(this)
        .setDirectoryToUse(newBlkFile.getParentFile())
        .setWriterThread(Thread.currentThread())
        .setBytesToReserve(bytesReserved)
        .buildLocalReplicaInPipeline();

    // Only a finalized replica can be appended.
    FinalizedReplica finalized = (FinalizedReplica)replicaInfo;
    // load last checksum and datalen
    newReplicaInfo.setLastChecksumAndDataLen(
        finalized.getVisibleLength(), finalized.getLastPartialChunkChecksum());

    // rename meta file to rbw directory
    // rename block file to rbw directory
    newReplicaInfo.moveReplicaFrom(replicaInfo, newBlkFile);

    reserveSpaceForReplica(bytesReserved);
    return newReplicaInfo;
  }

  public ReplicaInPipeline createRbw(ExtendedBlock b) throws IOException {

    File f = createRbwFile(b.getBlockPoolId(), b.getLocalBlock());
    LocalReplicaInPipeline newReplicaInfo = new ReplicaBuilder(ReplicaState.RBW)
        .setBlockId(b.getBlockId())
        .setGenerationStamp(b.getGenerationStamp())
        .setFsVolume(this)
        .setDirectoryToUse(f.getParentFile())
        .setBytesToReserve(b.getNumBytes())
        .buildLocalReplicaInPipeline();
    return newReplicaInfo;
  }

  public ReplicaInPipeline convertTemporaryToRbw(ExtendedBlock b,
      ReplicaInfo temp) throws IOException {

    final long blockId = b.getBlockId();
    final long expectedGs = b.getGenerationStamp();
    final long visible = b.getNumBytes();
    final long numBytes = temp.getNumBytes();

    // move block files to the rbw directory
    BlockPoolSlice bpslice = getBlockPoolSlice(b.getBlockPoolId());
    final File dest = FsDatasetImpl.moveBlockFiles(b.getLocalBlock(), temp,
        bpslice.getRbwDir());
    // create RBW
    final LocalReplicaInPipeline rbw = new ReplicaBuilder(ReplicaState.RBW)
        .setBlockId(blockId)
        .setLength(numBytes)
        .setGenerationStamp(expectedGs)
        .setFsVolume(this)
        .setDirectoryToUse(dest.getParentFile())
        .setWriterThread(Thread.currentThread())
        .setBytesToReserve(0)
        .buildLocalReplicaInPipeline();
    rbw.setBytesAcked(visible);

    // load last checksum and datalen
    final File destMeta = FsDatasetUtil.getMetaFile(dest,
        b.getGenerationStamp());
    byte[] lastChunkChecksum = loadLastPartialChunkChecksum(dest, destMeta);
    rbw.setLastChecksumAndDataLen(numBytes, lastChunkChecksum);
    return rbw;
  }

  public ReplicaInPipeline createTemporary(ExtendedBlock b) throws IOException {
    // create a temporary file to hold block in the designated volume
    File f = createTmpFile(b.getBlockPoolId(), b.getLocalBlock());
    LocalReplicaInPipeline newReplicaInfo =
        new ReplicaBuilder(ReplicaState.TEMPORARY)
          .setBlockId(b.getBlockId())
          .setGenerationStamp(b.getGenerationStamp())
          .setDirectoryToUse(f.getParentFile())
          .setBytesToReserve(b.getLocalBlock().getNumBytes())
          .setFsVolume(this)
          .buildLocalReplicaInPipeline();
    return newReplicaInfo;
  }

  public ReplicaInPipeline updateRURCopyOnTruncate(ReplicaInfo rur,
      String bpid, long newBlockId, long recoveryId, long newlength)
      throws IOException {

    rur.breakHardLinksIfNeeded();
    File[] copiedReplicaFiles =
        copyReplicaWithNewBlockIdAndGS(rur, bpid, newBlockId, recoveryId);
    File blockFile = copiedReplicaFiles[1];
    File metaFile = copiedReplicaFiles[0];
    LocalReplica.truncateBlock(rur.getVolume(), blockFile, metaFile,
        rur.getNumBytes(), newlength, fileIoProvider);

    LocalReplicaInPipeline newReplicaInfo = new ReplicaBuilder(ReplicaState.RBW)
        .setBlockId(newBlockId)
        .setGenerationStamp(recoveryId)
        .setFsVolume(this)
        .setDirectoryToUse(blockFile.getParentFile())
        .setBytesToReserve(newlength)
        .buildLocalReplicaInPipeline();
    // In theory, this rbw replica needs to reload last chunk checksum,
    // but it is immediately converted to finalized state within the same lock,
    // so no need to update it.
    return newReplicaInfo;
  }

  private File[] copyReplicaWithNewBlockIdAndGS(
      ReplicaInfo replicaInfo, String bpid, long newBlkId, long newGS)
      throws IOException {
    String blockFileName = Block.BLOCK_FILE_PREFIX + newBlkId;
    FsVolumeImpl v = (FsVolumeImpl) replicaInfo.getVolume();
    final File tmpDir = v.getBlockPoolSlice(bpid).getTmpDir();
    final File destDir = DatanodeUtil.idToBlockDir(tmpDir, newBlkId);
    final File dstBlockFile = new File(destDir, blockFileName);
    final File dstMetaFile = FsDatasetUtil.getMetaFile(dstBlockFile, newGS);
    return FsDatasetImpl.copyBlockFiles(replicaInfo, dstMetaFile,
        dstBlockFile, true, DFSUtilClient.getSmallBufferSize(conf), conf);
  }

  @Override
  public LinkedList<ScanInfo> compileReport(String bpid,
      LinkedList<ScanInfo> report, ReportCompiler reportCompiler)
      throws InterruptedException, IOException {
    return compileReport(getFinalizedDir(bpid),
        getFinalizedDir(bpid), report, reportCompiler);
  }

  @Override
  public FileIoProvider getFileIoProvider() {
    return fileIoProvider;
  }

  @Override
  public DataNodeVolumeMetrics getMetrics() {
    return metrics;
  }

  private LinkedList<ScanInfo> compileReport(File bpFinalizedDir,
      File dir, LinkedList<ScanInfo> report, ReportCompiler reportCompiler)
        throws InterruptedException {

    reportCompiler.throttle();

    List <String> fileNames;
    try {
      fileNames = fileIoProvider.listDirectory(
          this, dir, BlockDirFilter.INSTANCE);
    } catch (IOException ioe) {
      LOG.warn("Exception occurred while compiling report: ", ioe);
      // Volume error check moved to FileIoProvider.
      // Ignore this directory and proceed.
      return report;
    }
    Collections.sort(fileNames);

    /*
     * Assumption: In the sorted list of files block file appears immediately
     * before block metadata file. This is true for the current naming
     * convention for block file blk_<blockid> and meta file
     * blk_<blockid>_<genstamp>.meta
     */
    for (int i = 0; i < fileNames.size(); i++) {
      // Make sure this thread can make a timely exit. With a low throttle
      // rate, completing a run can take a looooong time.
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }

      File file = new File(dir, fileNames.get(i));
      if (file.isDirectory()) {
        compileReport(bpFinalizedDir, file, report, reportCompiler);
        continue;
      }
      if (!Block.isBlockFilename(file)) {
        if (isBlockMetaFile(Block.BLOCK_FILE_PREFIX, file.getName())) {
          long blockId = Block.getBlockId(file.getName());
          verifyFileLocation(file, bpFinalizedDir,
              blockId);
          report.add(new ScanInfo(blockId, null, file, this));
        }
        continue;
      }
      File blockFile = file;
      long blockId = Block.filename2id(file.getName());
      File metaFile = null;

      // Skip all the files that start with block name until
      // getting to the metafile for the block
      while (i + 1 < fileNames.size()) {
        File blkMetaFile = new File(dir, fileNames.get(i + 1));
        if (!(blkMetaFile.isFile()
            && blkMetaFile.getName().startsWith(blockFile.getName()))) {
          break;
        }
        i++;
        if (isBlockMetaFile(blockFile.getName(), blkMetaFile.getName())) {
          metaFile = blkMetaFile;
          break;
        }
      }
      verifyFileLocation(blockFile, bpFinalizedDir, blockId);
      report.add(new ScanInfo(blockId, blockFile, metaFile, this));
    }
    return report;
  }

  /**
   * Helper method to determine if a file name is consistent with a block.
   * meta-data file
   *
   * @param blockId the block ID
   * @param metaFile the file to check
   * @return whether the file name is a block meta-data file name
   */
  private static boolean isBlockMetaFile(String blockId, String metaFile) {
    return metaFile.startsWith(blockId)
        && metaFile.endsWith(Block.METADATA_EXTENSION);
  }

  /**
   * Verify whether the actual directory location of block file has the
   * expected directory path computed using its block ID.
   */
  private void verifyFileLocation(File actualBlockFile,
      File bpFinalizedDir, long blockId) {
    File expectedBlockDir =
        DatanodeUtil.idToBlockDir(bpFinalizedDir, blockId);
    File actualBlockDir = actualBlockFile.getParentFile();
    if (actualBlockDir.compareTo(expectedBlockDir) != 0) {
      LOG.warn("Block: " + blockId +
          " found in invalid directory.  Expected directory: " +
          expectedBlockDir + ".  Actual directory: " + actualBlockDir);
    }
  }

  public ReplicaInfo moveBlockToTmpLocation(ExtendedBlock block,
      ReplicaInfo replicaInfo,
      int smallBufferSize,
      Configuration conf) throws IOException {

    File[] blockFiles = FsDatasetImpl.copyBlockFiles(block.getBlockId(),
        block.getGenerationStamp(), replicaInfo,
        getTmpDir(block.getBlockPoolId()),
        replicaInfo.isOnTransientStorage(), smallBufferSize, conf);

    ReplicaInfo newReplicaInfo = new ReplicaBuilder(ReplicaState.TEMPORARY)
        .setBlockId(replicaInfo.getBlockId())
        .setGenerationStamp(replicaInfo.getGenerationStamp())
        .setFsVolume(this)
        .setDirectoryToUse(blockFiles[0].getParentFile())
        .setBytesToReserve(0)
        .build();
    newReplicaInfo.setNumBytes(blockFiles[1].length());
    return newReplicaInfo;
  }

  public File[] copyBlockToLazyPersistLocation(String bpId, long blockId,
      long genStamp,
      ReplicaInfo replicaInfo,
      int smallBufferSize,
      Configuration conf) throws IOException {

    File lazyPersistDir  = getLazyPersistDir(bpId);
    if (!lazyPersistDir.exists() && !lazyPersistDir.mkdirs()) {
      FsDatasetImpl.LOG.warn("LazyWriter failed to create " + lazyPersistDir);
      throw new IOException("LazyWriter fail to find or " +
          "create lazy persist dir: " + lazyPersistDir.toString());
    }

    // No FsDatasetImpl lock for the file copy
    File[] targetFiles = FsDatasetImpl.copyBlockFiles(
        blockId, genStamp, replicaInfo, lazyPersistDir, true,
        smallBufferSize, conf);
    return targetFiles;
  }

  public void incrNumBlocks(String bpid) throws IOException {
    getBlockPoolSlice(bpid).incrNumBlocks();
  }

  public void resolveDuplicateReplicas(String bpid, ReplicaInfo memBlockInfo,
      ReplicaInfo diskBlockInfo, ReplicaMap volumeMap) throws IOException {
    getBlockPoolSlice(bpid).resolveDuplicateReplicas(
        memBlockInfo, diskBlockInfo, volumeMap);
  }

  public ReplicaInfo activateSavedReplica(String bpid,
      ReplicaInfo replicaInfo, RamDiskReplica replicaState) throws IOException {
    return getBlockPoolSlice(bpid).activateSavedReplica(replicaInfo,
        replicaState);
  }
}
