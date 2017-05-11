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
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.ClosedChannelException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.util.CloseableReferenceCount;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.StringUtils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.util.Time;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private final FsDatasetImpl dataset;
  private final String storageID;
  private final StorageType storageType;
  private final Map<String, BlockPoolSlice> bpSlices
      = new ConcurrentHashMap<String, BlockPoolSlice>();
  private final File currentDir;    // <StorageDirectory>/current
  private final DF usage;           
  private final long reserved;
  private CloseableReferenceCount reference = new CloseableReferenceCount();

  // Disk space reserved for open blocks.
  private AtomicLong reservedForRbw;

  // Capacity configured. This is useful when we want to
  // limit the visible capacity for tests. If negative, then we just
  // query from the filesystem.
  protected volatile long configuredCapacity;

  /**
   * Per-volume worker pool that processes new blocks to cache.
   * The maximum number of workers per volume is bounded (configurable via
   * dfs.datanode.fsdatasetcache.max.threads.per.volume) to limit resource
   * contention.
   */
  protected ThreadPoolExecutor cacheExecutor;
  
  FsVolumeImpl(FsDatasetImpl dataset, String storageID, File currentDir,
      Configuration conf, StorageType storageType) throws IOException {
    this.dataset = dataset;
    this.storageID = storageID;
    this.reserved = conf.getLong(DFSConfigKeys.DFS_DATANODE_DU_RESERVED_KEY
        + "." + StringUtils.toLowerCase(storageType.toString()), conf.getLong(
        DFSConfigKeys.DFS_DATANODE_DU_RESERVED_KEY,
        DFSConfigKeys.DFS_DATANODE_DU_RESERVED_DEFAULT));
    this.reservedForRbw = new AtomicLong(0L);
    this.currentDir = currentDir; 
    File parent = currentDir.getParentFile();
    this.usage = new DF(parent, conf);
    this.storageType = storageType;
    this.configuredCapacity = -1;
    cacheExecutor = initializeCacheExecutor(parent);
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
    private final FsVolumeImpl volume;

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
      volume.unreference();
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
   * Close this volume and wait all other threads to release the reference count
   * on this volume.
   * @throws IOException if the volume is closed or the waiting is interrupted.
   */
  void closeAndWait() throws IOException {
    try {
      this.reference.setClosed();
      dataset.stopAllDataxceiverThreads(this);
    } catch (ClosedChannelException e) {
      throw new IOException("The volume has already closed.", e);
    }
    final int SLEEP_MILLIS = 500;
    while (this.reference.getReferenceCount() > 0) {
      if (FsDatasetImpl.LOG.isDebugEnabled()) {
        FsDatasetImpl.LOG.debug(String.format(
            "The reference count for %s is %d, wait to be 0.",
            this, reference.getReferenceCount()));
      }
      try {
        Thread.sleep(SLEEP_MILLIS);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
  }

  File getCurrentDir() {
    return currentDir;
  }
  
  File getRbwDir(String bpid) throws IOException {
    return getBlockPoolSlice(bpid).getRbwDir();
  }

  File getLazyPersistDir(String bpid) throws IOException {
    return getBlockPoolSlice(bpid).getLazypersistDir();
  }

  File getTmpDir(String bpid) throws IOException {
    return getBlockPoolSlice(bpid).getTmpDir();
  }

  void decDfsUsed(String bpid, long value) {
    synchronized(dataset) {
      BlockPoolSlice bp = bpSlices.get(bpid);
      if (bp != null) {
        bp.decDfsUsed(value);
      }
    }
  }

  void incDfsUsed(String bpid, long value) {
    synchronized(dataset) {
      BlockPoolSlice bp = bpSlices.get(bpid);
      if (bp != null) {
        bp.incDfsUsed(value);
      }
    }
  }

  @VisibleForTesting
  public long getDfsUsed() throws IOException {
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
   * Return either the configured capacity of the file system if configured; or
   * the capacity of the file system excluding space reserved for non-HDFS.
   * 
   * @return the unreserved number of bytes left in this filesystem. May be
   *         zero.
   */
  @VisibleForTesting
  public long getCapacity() {
    if (configuredCapacity < 0) {
      long remaining = usage.getCapacity() - reserved;
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
    long remaining = getCapacity() - getDfsUsed() - getReservedForRbw();
    long available =
        usage.getAvailable() - getRemainingReserved() - getReservedForRbw();
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
    if (actualNonDfsUsed < reserved) {
      return reserved - actualNonDfsUsed;
    }
    return 0L;
  }

  /**
   * Unplanned Non-DFS usage, i.e. Extra usage beyond reserved.
   *
   * @return
   * @throws IOException
   */
  public long getNonDfsUsed() throws IOException {
    long actualNonDfsUsed = getActualNonDfsUsed();
    if (actualNonDfsUsed < reserved) {
      return 0L;
    }
    return actualNonDfsUsed - reserved;
  }

  @VisibleForTesting
  long getDfAvailable() {
    return usage.getAvailable();
  }

  @VisibleForTesting
  public long getReservedForRbw() {
    return reservedForRbw.get();
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
  public boolean isTransientStorage() {
    return storageType.isTransient();
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
    checkReference();
    return getBlockPoolSlice(bpid).createTmpFile(b);
  }

  @Override
  public void reserveSpaceForRbw(long bytesToReserve) {
    if (bytesToReserve != 0) {
      reservedForRbw.addAndGet(bytesToReserve);
    }
  }

  @Override
  public void releaseReservedSpace(long bytesToRelease) {
    if (bytesToRelease != 0) {

      long oldReservation, newReservation;
      do {
        oldReservation = reservedForRbw.get();
        newReservation = oldReservation - bytesToRelease;
        if (newReservation < 0) {
          // Failsafe, this should never occur in practice, but if it does we don't
          // want to start advertising more space than we have available.
          newReservation = 0;
        }
      } while (!reservedForRbw.compareAndSet(oldReservation, newReservation));
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
      List<String> children =
          IOUtils.listDirectory(dir, SubdirFilter.INSTANCE);
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
      List<String> entries =
          IOUtils.listDirectory(dir, BlockFileFilter.INSTANCE);
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
      ObjectMapper mapper = new ObjectMapper();
      try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(getTempSaveFile(), false), "UTF-8"))) {
        mapper.writerWithDefaultPrettyPrinter().writeValue(writer, state);
        success = true;
      } finally {
        if (!success) {
          if (getTempSaveFile().delete()) {
            LOG.debug("save({}, {}): error deleting temporary file.",
                storageID, bpid);
          }
        }
      }
      Files.move(getTempSaveFile().toPath(), getSaveFile().toPath(),
          StandardCopyOption.ATOMIC_MOVE);
      if (LOG.isTraceEnabled()) {
        LOG.trace("save({}, {}): saved {}", storageID, bpid,
            mapper.writerWithDefaultPrettyPrinter().writeValueAsString(state));
      }
    }

    public void load() throws IOException {
      ObjectMapper mapper = new ObjectMapper();
      File file = getSaveFile();
      this.state = mapper.reader(BlockIteratorState.class).readValue(file);
      LOG.trace("load({}, {}): loaded iterator {} from {}: {}", storageID,
          bpid, name, file.getAbsoluteFile(),
          mapper.writerWithDefaultPrettyPrinter().writeValueAsString(state));
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
  public FsDatasetSpi getDataset() {
    return dataset;
  }

  /**
   * RBW files. They get moved to the finalized block directory when
   * the block is finalized.
   */
  File createRbwFile(String bpid, Block b) throws IOException {
    checkReference();
    reserveSpaceForRbw(b.getNumBytes());
    try {
      return getBlockPoolSlice(bpid).createRbwFile(b);
    } catch (IOException exception) {
      releaseReservedSpace(b.getNumBytes());
      throw exception;
    }
  }

  /**
   *
   * @param bytesReservedForRbw Space that was reserved during
   *     block creation. Now that the block is being finalized we
   *     can free up this space.
   * @return
   * @throws IOException
   */
  File addFinalizedBlock(String bpid, Block b,
                         File f, long bytesReservedForRbw)
      throws IOException {
    releaseReservedSpace(bytesReservedForRbw);
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
  
  @Override
  public String toString() {
    return currentDir.getAbsolutePath();
  }

  void shutdown() {
    if (cacheExecutor != null) {
      cacheExecutor.shutdown();
    }
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
    File lazypersistDir = new File(bpCurrentDir,
        DataStorage.STORAGE_DIR_LAZY_PERSIST);
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
      if (lazypersistDir.exists() &&
        ((!DatanodeUtil.dirNoFilesRecursive(lazypersistDir) ||
          !FileUtil.fullyDelete(lazypersistDir)))) {
        throw new IOException("Failed to delete " + lazypersistDir);
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


  @Override
  public byte[] loadLastPartialChunkChecksum(
      File blockFile, File metaFile) throws IOException {
    // readHeader closes the temporary FileInputStream.
    DataChecksum dcs = BlockMetadataHeader
        .readHeader(metaFile).getChecksum();
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
    try (RandomAccessFile raf = new RandomAccessFile(metaFile, "r")) {
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
}

