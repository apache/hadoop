/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.impl.prefetch;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nullable;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.util.DurationInfo;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.impl.prefetch.Validate.checkNotNull;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.stubDurationTrackerFactory;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_FILE_CACHE_EVICTION;
import static org.apache.hadoop.util.Preconditions.checkArgument;
import static org.apache.hadoop.util.Preconditions.checkState;

/**
 * Provides functionality necessary for caching blocks of data read from FileSystem.
 * Each cache block is stored on the local disk as a separate file.
 */
public class SingleFilePerBlockCache implements BlockCache {
  private static final Logger LOG = LoggerFactory.getLogger(SingleFilePerBlockCache.class);

  /**
   * Blocks stored in this cache.
   * A concurrent hash map is used here, but it is still important for cache operations to
   * be thread safe.
   */
  private final Map<Integer, Entry> blocks = new ConcurrentHashMap<>();

  /**
   * Total max blocks count, to be considered as baseline for LRU cache eviction.
   */
  private final int maxBlocksCount;

  /**
   * The lock to be shared by LRU based linked list updates.
   */
  private final ReentrantReadWriteLock blocksLock = new ReentrantReadWriteLock();

  /**
   * Head of the linked list.
   */
  private Entry head;

  /**
   * Tail of the linked list.
   */
  private Entry tail;

  /**
   * Total size of the linked list.
   */
  private int entryListSize;

  /**
   * Number of times a block was read from this cache.
   * Used for determining cache utilization factor.
   */
  private final AtomicInteger numGets = new AtomicInteger();

  private final AtomicBoolean closed = new AtomicBoolean(false);

  private final PrefetchingStatistics prefetchingStatistics;

  /**
   * Duration tracker factory required to track the duration of some operations.
   */
  private final DurationTrackerFactory trackerFactory;

  /**
   * File attributes attached to any intermediate temporary file created during index creation.
   */
  private static final Set<PosixFilePermission> TEMP_FILE_ATTRS =
      ImmutableSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);

  /**
   * Cache entry.
   * Each block is stored as a separate file.
   */
  private static final class Entry {
    private final int blockNumber;
    private final Path path;
    private final int size;
    private final long checksum;
    private final ReentrantReadWriteLock lock;
    private enum LockType {
      READ,
      WRITE
    }
    private Entry previous;
    private Entry next;

    Entry(int blockNumber, Path path, int size, long checksum) {
      this.blockNumber = blockNumber;
      this.path = path;
      this.size = size;
      this.checksum = checksum;
      this.lock = new ReentrantReadWriteLock();
      this.previous = null;
      this.next = null;
    }

    @Override
    public String toString() {
      return String.format(
          "([%03d] %s: size = %,d, checksum = %d)",
          blockNumber, path, size, checksum);
    }

    /**
     * Take the read or write lock.
     *
     * @param lockType type of the lock.
     */
    private void takeLock(LockType lockType) {
      if (LockType.READ == lockType) {
        lock.readLock().lock();
      } else if (LockType.WRITE == lockType) {
        lock.writeLock().lock();
      }
    }

    /**
     * Release the read or write lock.
     *
     * @param lockType type of the lock.
     */
    private void releaseLock(LockType lockType) {
      if (LockType.READ == lockType) {
        lock.readLock().unlock();
      } else if (LockType.WRITE == lockType) {
        lock.writeLock().unlock();
      }
    }

    /**
     * Try to take the read or write lock within the given timeout.
     *
     * @param lockType type of the lock.
     * @param timeout the time to wait for the given lock.
     * @param unit the time unit of the timeout argument.
     * @return true if the lock of the given lock type was acquired.
     */
    private boolean takeLock(LockType lockType, long timeout, TimeUnit unit) {
      try {
        if (LockType.READ == lockType) {
          return lock.readLock().tryLock(timeout, unit);
        } else if (LockType.WRITE == lockType) {
          return lock.writeLock().tryLock(timeout, unit);
        }
      } catch (InterruptedException e) {
        LOG.warn("Thread interrupted while trying to acquire {} lock", lockType, e);
        Thread.currentThread().interrupt();
      }
      return false;
    }

    private Entry getPrevious() {
      return previous;
    }

    private void setPrevious(Entry previous) {
      this.previous = previous;
    }

    private Entry getNext() {
      return next;
    }

    private void setNext(Entry next) {
      this.next = next;
    }
  }

  /**
   * Constructs an instance of a {@code SingleFilePerBlockCache}.
   *
   * @param prefetchingStatistics statistics for this stream.
   * @param maxBlocksCount max blocks count to be kept in cache at any time.
   * @param trackerFactory tracker with statistics to update
   */
  public SingleFilePerBlockCache(PrefetchingStatistics prefetchingStatistics,
      int maxBlocksCount,
      @Nullable DurationTrackerFactory trackerFactory) {
    this.prefetchingStatistics = requireNonNull(prefetchingStatistics);
    checkArgument(maxBlocksCount > 0, "maxBlocksCount should be more than 0");
    this.maxBlocksCount = maxBlocksCount;
    this.trackerFactory = trackerFactory != null
        ? trackerFactory : stubDurationTrackerFactory();
  }

  /**
   * Indicates whether the given block is in this cache.
   */
  @Override
  public boolean containsBlock(int blockNumber) {
    return blocks.containsKey(blockNumber);
  }

  /**
   * Gets the blocks in this cache.
   */
  @Override
  public synchronized Iterable<Integer> blocks() {
    return Collections.unmodifiableList(new ArrayList<>(blocks.keySet()));
  }

  /**
   * Gets the number of blocks in this cache.
   */
  @Override
  public int size() {
    return blocks.size();
  }

  @Override
  public synchronized boolean get(int blockNumber, ByteBuffer buffer) throws IOException {
    if (closed.get()) {
      return false;
    }

    checkNotNull(buffer, "buffer");

    if (!blocks.containsKey(blockNumber)) {
      // no block found
      return false;
    }

    // block found. read it.
    Entry entry = getEntry(blockNumber);
    entry.takeLock(Entry.LockType.READ);
    try {
      buffer.clear();
      readFile(entry.path, buffer);
      buffer.rewind();
      validateEntry(entry, buffer);
    } finally {
      entry.releaseLock(Entry.LockType.READ);
    }
    return true;
  }

  /**
   * Read the contents of a file into a bytebuffer.
   * @param path local path
   * @param buffer destination.
   * @return bytes read.
   * @throws IOException read failure.
   */
  protected int readFile(Path path, ByteBuffer buffer) throws IOException {
    int numBytesRead = 0;
    int numBytes;
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
      while ((numBytes = channel.read(buffer)) > 0) {
        numBytesRead += numBytes;
      }
      buffer.limit(buffer.position());
    }
    return numBytesRead;
  }

  /**
   * Get an entry in the cache.
   * Increases the value of {@link #numGets}
   * @param blockNumber block number
   * @return the entry.
   */
  private synchronized Entry getEntry(int blockNumber) {
    Validate.checkNotNegative(blockNumber, "blockNumber");

    Entry entry = blocks.get(blockNumber);
    checkState(entry != null, "block %d not found in cache", blockNumber);
    numGets.getAndIncrement();
    addToLinkedListHead(entry);
    return entry;
  }

  /**
   * Add the given entry to the head of the linked list if
   * is not already there.
   * Locks {@link #blocksLock} first.
   * @param entry Block entry to add.
   */
  private void addToLinkedListHead(Entry entry) {
    blocksLock.writeLock().lock();
    try {
      maybePushToHeadOfBlockList(entry);
    } finally {
      blocksLock.writeLock().unlock();
    }
  }

  /**
   * Maybe Add the given entry to the head of the block list.
   * No-op if the block is already in the list.
   * @param entry Block entry to add.
   * @return true if the block was added.
   */
  private boolean maybePushToHeadOfBlockList(Entry entry) {
    if (head == null) {
      head = entry;
      tail = entry;
    }
    LOG.debug(
        "Block {} to be added to the head. Current head block {} and tail block {}; {}",
        entry.blockNumber, head.blockNumber, tail.blockNumber, entry);
    if (entry != head) {
      Entry prev = entry.getPrevious();
      Entry next = entry.getNext();
      // no-op if the block is already block list
      if (!blocks.containsKey(entry.blockNumber)) {
        LOG.debug("Block {} is already in block list", entry.blockNumber);
        return false;
      }
      if (prev != null) {
        prev.setNext(next);
      }
      if (next != null) {
        next.setPrevious(prev);
      }
      entry.setPrevious(null);
      entry.setNext(head);
      head.setPrevious(entry);
      head = entry;
      if (prev != null && prev.getNext() == null) {
        tail = prev;
      }
    }
    return true;
  }

  /**
   * Puts the given block in this cache.
   *
   * @param blockNumber the block number, used as a key for blocks map.
   * @param buffer buffer contents of the given block to be added to this cache.
   * @param conf the configuration.
   * @param localDirAllocator the local dir allocator instance.
   * @throws IOException if either local dir allocator fails to allocate file or if IO error
   * occurs while writing the buffer content to the file.
   * @throws IllegalArgumentException if buffer is null, or if buffer.limit() is zero or negative.
   * @throws IllegalStateException if the cache file exists and is not empty
   */
  @Override
  public synchronized void put(int blockNumber, ByteBuffer buffer, Configuration conf,
      LocalDirAllocator localDirAllocator) throws IOException {
    if (closed.get()) {
      return;
    }

    checkNotNull(buffer, "buffer");

    if (blocks.containsKey(blockNumber)) {
      // this block already exists.
      // verify the checksum matches
      Entry entry = blocks.get(blockNumber);
      entry.takeLock(Entry.LockType.READ);
      try {
        validateEntry(entry, buffer);
      } finally {
        entry.releaseLock(Entry.LockType.READ);
      }
      addToLinkedListHead(entry);
      return;
    }

    Validate.checkPositiveInteger(buffer.limit(), "buffer.limit()");

    String blockInfo = String.format("-block-%04d", blockNumber);
    Path blockFilePath = getCacheFilePath(conf, localDirAllocator, blockInfo, buffer.limit());
    long size = Files.size(blockFilePath);
    checkState(size == 0, "[%d] temp file already has data. %s (%d)",
          blockNumber, blockFilePath, size);

    writeFile(blockFilePath, buffer);
    long checksum = BufferData.getChecksum(buffer);
    Entry entry = new Entry(blockNumber, blockFilePath, buffer.limit(), checksum);
    blocks.put(blockNumber, entry);
    // Update stream_read_blocks_in_cache stats only after blocks map is updated with new file
    // entry to avoid any discrepancy related to the value of stream_read_blocks_in_cache.
    // If stream_read_blocks_in_cache is updated before updating the blocks map here, closing of
    // the input stream can lead to the removal of the cache file even before blocks is added
    // with the new cache file, leading to incorrect value of stream_read_blocks_in_cache.
    prefetchingStatistics.blockAddedToFileCache();
    addToLinkedListAndEvictIfRequired(entry);
  }

  /**
   * Add the given entry to the head of the linked list and if the LRU cache size
   * exceeds the max limit, evict tail of the LRU linked list.
   *
   * @param entry Block entry to add.
   */
  private void addToLinkedListAndEvictIfRequired(Entry entry) {
    blocksLock.writeLock().lock();
    try {
      if (maybePushToHeadOfBlockList(entry)) {
        entryListSize++;
      }
      if (entryListSize > maxBlocksCount && !closed.get()) {
        Entry elementToPurge = tail;
        tail = tail.getPrevious();
        if (tail == null) {
          tail = head;
        }
        tail.setNext(null);
        elementToPurge.setPrevious(null);
        deleteBlockFileAndEvictCache(elementToPurge);
      }
    } finally {
      blocksLock.writeLock().unlock();
    }
  }

  /**
   * Delete cache file as part of the block cache LRU eviction.
   *
   * @param elementToPurge Block entry to evict.
   */
  private void deleteBlockFileAndEvictCache(Entry elementToPurge) {
    LOG.debug("Evicting block {} from cache: {}", elementToPurge.blockNumber, elementToPurge);
    try (DurationTracker ignored = trackerFactory.trackDuration(STREAM_FILE_CACHE_EVICTION)) {
      boolean lockAcquired = elementToPurge.takeLock(Entry.LockType.WRITE,
          PrefetchConstants.PREFETCH_WRITE_LOCK_TIMEOUT,
          PrefetchConstants.PREFETCH_WRITE_LOCK_TIMEOUT_UNIT);
      if (!lockAcquired) {
        LOG.warn("Cache file {} deletion would not be attempted as write lock could not"
                + " be acquired within {} {}", elementToPurge.path,
            PrefetchConstants.PREFETCH_WRITE_LOCK_TIMEOUT,
            PrefetchConstants.PREFETCH_WRITE_LOCK_TIMEOUT_UNIT);
      } else {
        try {
          if (Files.deleteIfExists(elementToPurge.path)) {
            entryListSize--;
            prefetchingStatistics.blockRemovedFromFileCache();
            blocks.remove(elementToPurge.blockNumber);
            prefetchingStatistics.blockEvictedFromFileCache();
          } else {
            LOG.debug("Cache file {} not found for deletion: {}", elementToPurge.path, elementToPurge);
          }
        } catch (IOException e) {
          LOG.warn("Failed to delete cache file {} for {}", elementToPurge.path, elementToPurge, e);
        } finally {
          elementToPurge.releaseLock(Entry.LockType.WRITE);
        }
      }
    }
  }

  private static final Set<? extends OpenOption> CREATE_OPTIONS =
      EnumSet.of(StandardOpenOption.WRITE,
          StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING);

  /**
   * Write the contents of the buffer to the path.
   * @param path file to create.
   * @param buffer source buffer.
   * @throws IOException
   */
  protected void writeFile(Path path, ByteBuffer buffer) throws IOException {
    buffer.rewind();
    try (WritableByteChannel writeChannel = Files.newByteChannel(path, CREATE_OPTIONS);
             DurationInfo d = new DurationInfo(LOG, "save %d bytes to %s",
                 buffer.remaining(), path)) {
      while (buffer.hasRemaining()) {
        writeChannel.write(buffer);
      }
    }
  }

  /**
   * Return temporary file created based on the file path retrieved from local dir allocator.
   * @param conf The configuration object.
   * @param localDirAllocator Local dir allocator instance.
   * @param blockInfo info about block to use in filename
   * @param fileSize size or -1 if unknown
   * @return Path of the temporary file created.
   * @throws IOException if IO error occurs while local dir allocator tries to retrieve path
   * from local FS or file creation fails or permission set fails.
   */
  protected Path getCacheFilePath(final Configuration conf,
      final LocalDirAllocator localDirAllocator,
      final String blockInfo,
      final long fileSize)
      throws IOException {
    return getTempFilePath(conf, localDirAllocator, blockInfo, fileSize);
  }

  @Override
  public void close() throws IOException {
    if (closed.compareAndSet(false, true)) {
      LOG.debug(getStats());
      deleteCacheFiles();
    }
  }

  /**
   * Delete cache files as part of the close call.
   */
  private void deleteCacheFiles() {
    int numFilesDeleted = 0;
    LOG.debug("Prefetch cache close: Deleting {} cache files", blocks.size());
    for (Entry entry : blocks.values()) {
      boolean lockAcquired =
          entry.takeLock(Entry.LockType.WRITE, PrefetchConstants.PREFETCH_WRITE_LOCK_TIMEOUT,
              PrefetchConstants.PREFETCH_WRITE_LOCK_TIMEOUT_UNIT);
      if (!lockAcquired) {
        LOG.error("Cache file {} deletion would not be attempted as write lock could not"
                + " be acquired within {} {}", entry.path,
            PrefetchConstants.PREFETCH_WRITE_LOCK_TIMEOUT,
            PrefetchConstants.PREFETCH_WRITE_LOCK_TIMEOUT_UNIT);
        continue;
      }
      try {
        if (Files.deleteIfExists(entry.path)) {
          prefetchingStatistics.blockRemovedFromFileCache();
          numFilesDeleted++;
        }
      } catch (IOException e) {
        LOG.warn("Failed to delete cache file {}", entry.path, e);
      } finally {
        entry.releaseLock(Entry.LockType.WRITE);
      }
    }
    LOG.debug("Prefetch cache close: Deleted {} cache files", numFilesDeleted);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("stats: ");
    sb.append(getStats());
    sb.append(", blocks:[");
    sb.append(getIntList(blocks()));
    sb.append("]");
    return sb.toString();
  }

  /**
   * Validate a block entry against a buffer, including checksum comparison.
   * @param entry block entry
   * @param buffer buffer
   * @throws IllegalStateException if invalid.
   */
  private void validateEntry(Entry entry, ByteBuffer buffer) {
    checkState(entry.size == buffer.limit(),
        "[%d] entry.size(%d) != buffer.limit(%d)",
        entry.blockNumber, entry.size, buffer.limit());

    long checksum = BufferData.getChecksum(buffer);
    checkState(entry.checksum == checksum,
        "[%d] entry.checksum(%d) != buffer checksum(%d)",
        entry.blockNumber, entry.checksum, checksum);
  }

  /**
   * Produces a human readable list of blocks for the purpose of logging.
   * This method minimizes the length of returned list by converting
   * a contiguous list of blocks into a range.
   * for example,
   * 1, 3, 4, 5, 6, 8 becomes 1, 3~6, 8
   */
  private String getIntList(Iterable<Integer> nums) {
    List<String> numList = new ArrayList<>();
    List<Integer> numbers = new ArrayList<Integer>();
    for (Integer n : nums) {
      numbers.add(n);
    }
    Collections.sort(numbers);

    int index = 0;
    while (index < numbers.size()) {
      int start = numbers.get(index);
      int prev = start;
      int end = start;
      while ((++index < numbers.size()) && ((end = numbers.get(index)) == prev + 1)) {
        prev = end;
      }

      if (start == prev) {
        numList.add(Integer.toString(start));
      } else {
        numList.add(String.format("%d~%d", start, prev));
      }
    }

    return String.join(", ", numList);
  }

  private String getStats() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format(
        "#entries = %d, #gets = %d",
        blocks.size(), numGets));
    return sb.toString();
  }

  /**
   *  Prefix for cache files: {@value}.
   */
  @VisibleForTesting
  public static final String CACHE_FILE_PREFIX = "fs-cache-";

  // The suffix (file extension) of each serialized index file.
  private static final String BINARY_FILE_SUFFIX = ".bin";

  /**
   * Create temporary file based on the file path retrieved from local dir allocator
   * instance. The file is created with .bin suffix. The created file has been granted
   * posix file permissions available in TEMP_FILE_ATTRS.
   * @param conf the configuration.
   * @param localDirAllocator the local dir allocator instance.
   * @param blockInfo info about block to use in filename
   * @param fileSize size or -1 if unknown
   * @return path of the file created.
   * @throws IOException if IO error occurs while local dir allocator tries to retrieve path
   * from local FS or file creation fails or permission set fails.
   */
  private static Path getTempFilePath(final Configuration conf,
      final LocalDirAllocator localDirAllocator,
      final String blockInfo,
      final long fileSize) throws IOException {
    org.apache.hadoop.fs.Path path =
        localDirAllocator.getLocalPathForWrite(CACHE_FILE_PREFIX, fileSize, conf);
    File dir = new File(path.getParent().toUri().getPath());
    String prefix = path.getName();
    File tmpFile = File.createTempFile(prefix, blockInfo + BINARY_FILE_SUFFIX, dir);
    Path tmpFilePath = Paths.get(tmpFile.toURI());
    return Files.setPosixFilePermissions(tmpFilePath, TEMP_FILE_ATTRS);
  }
}
