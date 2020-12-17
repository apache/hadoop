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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manage the persistent memory volumes.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class PmemVolumeManager {

  /**
   * Counts used bytes for persistent memory.
   */
  private static class UsedBytesCount {
    private long maxBytes;
    private final AtomicLong usedBytes = new AtomicLong(0);

    UsedBytesCount(long maxBytes) {
      this.maxBytes = maxBytes;
    }

    /**
     * Try to reserve more bytes.
     *
     * @param bytesCount    The number of bytes to add.
     *
     * @return              The new number of usedBytes if we succeeded;
     *                      -1 if we failed.
     */
    long reserve(long bytesCount) {
      while (true) {
        long cur = usedBytes.get();
        long next = cur + bytesCount;
        if (next > maxBytes) {
          return -1;
        }
        if (usedBytes.compareAndSet(cur, next)) {
          return next;
        }
      }
    }

    /**
     * Release some bytes that we're using.
     *
     * @param bytesCount    The number of bytes to release.
     *
     * @return              The new number of usedBytes.
     */
    long release(long bytesCount) {
      return usedBytes.addAndGet(-bytesCount);
    }

    long getUsedBytes() {
      return usedBytes.get();
    }

    long getMaxBytes() {
      return maxBytes;
    }

    long getAvailableBytes() {
      return maxBytes - usedBytes.get();
    }

    void setMaxBytes(long maxBytes) {
      this.maxBytes = maxBytes;
    }
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(PmemVolumeManager.class);
  public static final String CACHE_DIR = "hdfs_pmem_cache";
  private static PmemVolumeManager pmemVolumeManager = null;
  private final ArrayList<String> pmemVolumes = new ArrayList<>();
  // Maintain which pmem volume a block is cached to.
  private final Map<ExtendedBlockId, Byte> blockKeyToVolume =
      new ConcurrentHashMap<>();
  private final List<UsedBytesCount> usedBytesCounts = new ArrayList<>();
  private boolean cacheRecoveryEnabled;

  /**
   * The total cache capacity in bytes of persistent memory.
   */
  private long cacheCapacity;
  private static long maxBytesPerPmem = -1;
  private int count = 0;
  private byte nextIndex = 0;

  private PmemVolumeManager(String[] pmemVolumesConfig,
                            boolean cacheRecoveryEnabled) throws IOException {
    if (pmemVolumesConfig == null || pmemVolumesConfig.length == 0) {
      throw new IOException("The persistent memory volume, " +
          DFSConfigKeys.DFS_DATANODE_PMEM_CACHE_DIRS_KEY +
          " is not configured!");
    }
    this.cacheRecoveryEnabled = cacheRecoveryEnabled;
    this.loadVolumes(pmemVolumesConfig);
    cacheCapacity = 0L;
    for (UsedBytesCount counter : usedBytesCounts) {
      cacheCapacity += counter.getMaxBytes();
    }
  }

  public synchronized static void init(
      String[] pmemVolumesConfig, boolean cacheRecoveryEnabled)
      throws IOException {
    if (pmemVolumeManager == null) {
      pmemVolumeManager = new PmemVolumeManager(pmemVolumesConfig,
          cacheRecoveryEnabled);
    }
  }

  public static PmemVolumeManager getInstance() {
    if (pmemVolumeManager == null) {
      throw new RuntimeException(
          "The pmemVolumeManager should be instantiated!");
    }
    return pmemVolumeManager;
  }

  @VisibleForTesting
  public static void reset() {
    pmemVolumeManager = null;
  }

  @VisibleForTesting
  public static void setMaxBytes(long maxBytes) {
    maxBytesPerPmem = maxBytes;
  }

  public long getCacheUsed() {
    long usedBytes = 0L;
    for (UsedBytesCount counter : usedBytesCounts) {
      usedBytes += counter.getUsedBytes();
    }
    return usedBytes;
  }

  public long getCacheCapacity() {
    return cacheCapacity;
  }

  /**
   * Try to reserve more bytes on persistent memory.
   *
   * @param key           The ExtendedBlockId for a block.
   *
   * @param bytesCount    The number of bytes to add.
   *
   * @return              The new number of usedBytes if we succeeded;
   *                      -1 if we failed.
   */
  synchronized long reserve(ExtendedBlockId key, long bytesCount) {
    try {
      byte index = chooseVolume(bytesCount);
      long usedBytes = usedBytesCounts.get(index).reserve(bytesCount);
      // Put the entry into blockKeyToVolume if reserving bytes succeeded.
      if (usedBytes > 0) {
        blockKeyToVolume.put(key, index);
      }
      return usedBytes;
    } catch (IOException e) {
      LOG.warn(e.getMessage());
      return -1L;
    }
  }

  /**
   * Release some bytes that we're using on persistent memory.
   *
   * @param key           The ExtendedBlockId for a block.
   *
   * @param bytesCount    The number of bytes to release.
   *
   * @return              The new number of usedBytes.
   */
  long release(ExtendedBlockId key, long bytesCount) {
    Byte index = blockKeyToVolume.remove(key);
    return usedBytesCounts.get(index).release(bytesCount);
  }

  /**
   * Load and verify the configured pmem volumes.
   *
   * @throws IOException   If there is no available pmem volume.
   */
  private void loadVolumes(String[] volumes)
      throws IOException {
    // Check whether the volume exists
    for (byte n = 0; n < volumes.length; n++) {
      try {
        File pmemDir = new File(volumes[n]);
        File realPmemDir = verifyIfValidPmemVolume(pmemDir);
        if (!cacheRecoveryEnabled) {
          // Clean up the cache left before, if any.
          cleanup(realPmemDir);
        }
        this.pmemVolumes.add(realPmemDir.getPath());
        long maxBytes;
        if (maxBytesPerPmem == -1) {
          maxBytes = realPmemDir.getUsableSpace();
        } else {
          maxBytes = maxBytesPerPmem;
        }
        UsedBytesCount usedBytesCount = new UsedBytesCount(maxBytes);
        this.usedBytesCounts.add(usedBytesCount);
        LOG.info("Added persistent memory - {} with size={}",
            volumes[n], maxBytes);
      } catch (IllegalArgumentException e) {
        LOG.error("Failed to parse persistent memory volume " + volumes[n], e);
        continue;
      } catch (IOException e) {
        LOG.error("Bad persistent memory volume: " + volumes[n], e);
        continue;
      }
    }
    count = pmemVolumes.size();
    if (count == 0) {
      throw new IOException(
          "At least one valid persistent memory volume is required!");
    }
  }

  void cleanup(File realPmemDir) {
    try {
      FileUtils.cleanDirectory(realPmemDir);
    } catch (IOException e) {
      LOG.error("Failed to clean up " + realPmemDir.getPath(), e);
    }
  }

  void cleanup() {
    // Remove all files under the volume.
    for (String pmemVolume : pmemVolumes) {
      cleanup(new File(pmemVolume));
    }
  }

  /**
   * Recover cache from the cached files in the configured pmem volumes.
   */
  public Map<ExtendedBlockId, MappableBlock> recoverCache(
      String bpid, MappableBlockLoader cacheLoader) throws IOException {
    final Map<ExtendedBlockId, MappableBlock> keyToMappableBlock
        = new ConcurrentHashMap<>();
    for (byte volumeIndex = 0; volumeIndex < pmemVolumes.size();
         volumeIndex++) {
      long maxBytes = usedBytesCounts.get(volumeIndex).getMaxBytes();
      long usedBytes = 0;
      File cacheDir = new File(pmemVolumes.get(volumeIndex), bpid);
      Collection<File> cachedFileList = FileUtils.listFiles(cacheDir,
          TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
      // Scan the cached files in pmem volumes for cache recovery.
      for (File cachedFile : cachedFileList) {
        MappableBlock mappableBlock = cacheLoader.
            getRecoveredMappableBlock(cachedFile, bpid, volumeIndex);
        ExtendedBlockId key = mappableBlock.getKey();
        keyToMappableBlock.put(key, mappableBlock);
        usedBytes += cachedFile.length();
      }
      // Update maxBytes and cache capacity according to cache space
      // used by recovered cached files.
      usedBytesCounts.get(volumeIndex).setMaxBytes(maxBytes + usedBytes);
      cacheCapacity += usedBytes;
      usedBytesCounts.get(volumeIndex).reserve(usedBytes);
    }
    return keyToMappableBlock;
  }

  public void recoverBlockKeyToVolume(ExtendedBlockId key, byte volumeIndex) {
    blockKeyToVolume.put(key, volumeIndex);
  }

  @VisibleForTesting
  static File verifyIfValidPmemVolume(File pmemDir)
      throws IOException {
    if (!pmemDir.exists()) {
      final String message = pmemDir + " does not exist";
      throw new IOException(message);
    }
    if (!pmemDir.isDirectory()) {
      final String message = pmemDir + " is not a directory";
      throw new IllegalArgumentException(message);
    }

    File realPmemDir = new File(getRealPmemDir(pmemDir.getPath()));
    if (!realPmemDir.exists() && !realPmemDir.mkdir()) {
      throw new IOException("Failed to create " + realPmemDir.getPath());
    }

    String uuidStr = UUID.randomUUID().toString();
    String testFilePath = realPmemDir.getPath() + "/.verify.pmem." + uuidStr;
    byte[] contents = uuidStr.getBytes("UTF-8");
    RandomAccessFile testFile = null;
    MappedByteBuffer out = null;
    try {
      testFile = new RandomAccessFile(testFilePath, "rw");
      out = testFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0,
          contents.length);
      if (out == null) {
        throw new IOException(
            "Failed to map the test file under " + realPmemDir);
      }
      out.put(contents);
      // Forces to write data to storage device containing the mapped file
      out.force();
      return realPmemDir;
    } catch (IOException e) {
      throw new IOException(
          "Exception while writing data to persistent storage dir: " +
              realPmemDir, e);
    } finally {
      if (out != null) {
        out.clear();
      }
      if (testFile != null) {
        IOUtils.closeQuietly(testFile);
        NativeIO.POSIX.munmap(out);
        try {
          FsDatasetUtil.deleteMappedFile(testFilePath);
        } catch (IOException e) {
          LOG.warn("Failed to delete test file " + testFilePath +
              " from persistent memory", e);
        }
      }
    }
  }

  /**
   * Create cache subdirectory specified with blockPoolId.
   */
  public void createBlockPoolDir(String bpid) throws IOException {
    for (String volume : pmemVolumes) {
      File cacheDir = new File(volume, bpid);
      if (!cacheDir.exists() && !cacheDir.mkdir()) {
        throw new IOException("Failed to create " + cacheDir.getPath());
      }
    }
  }

  public static String getRealPmemDir(String rawPmemDir) {
    return new File(rawPmemDir, CACHE_DIR).getAbsolutePath();
  }

  /**
   * Choose a persistent memory volume based on a specific algorithm.
   * Currently it is a round-robin policy.
   *
   * TODO: Refine volume selection policy by considering storage utilization.
   */
  synchronized Byte chooseVolume(long bytesCount) throws IOException {
    if (count == 0) {
      throw new IOException("No usable persistent memory is found");
    }
    int k = 0;
    long maxAvailableSpace = 0L;
    while (k++ != count) {
      if (nextIndex == count) {
        nextIndex = 0;
      }
      byte index = nextIndex++;
      long availableBytes = usedBytesCounts.get(index).getAvailableBytes();
      if (availableBytes >= bytesCount) {
        return index;
      }
      if (availableBytes > maxAvailableSpace) {
        maxAvailableSpace = availableBytes;
      }
    }
    throw new IOException("There is no enough persistent memory space " +
        "for caching. The current max available space is " +
        maxAvailableSpace + ", but " + bytesCount + "is required.");
  }

  @VisibleForTesting
  String getVolumeByIndex(Byte index) {
    return pmemVolumes.get(index);
  }

  ArrayList<String> getVolumes() {
    return pmemVolumes;
  }

  /**
   * A cache file is named after the corresponding BlockId.
   * Thus, cache file name can be inferred according to BlockId.
   */
  public String idToCacheFileName(ExtendedBlockId key) {
    return String.valueOf(key.getBlockId());
  }

  /**
   * Create and get the directory where a cache file with this key and
   * volumeIndex should be stored. Use hierarchical strategy of storing
   * blocks to avoid keeping cache files under one directory.
   *
   * @param volumeIndex   The index of pmem volume where a replica will be
   *                      cached to or has been cached to.
   *
   * @param key           The replica's ExtendedBlockId.
   *
   * @return              A path to which the block replica is mapped.
   */
  public String idToCacheFilePath(Byte volumeIndex, ExtendedBlockId key)
      throws IOException {
    final String cacheSubdirPrefix = "subdir";
    long blockId = key.getBlockId();
    String bpid = key.getBlockPoolId();
    int d1 = (int) ((blockId >> 16) & 0x1F);
    int d2 = (int) ((blockId >> 8) & 0x1F);
    String parentDir = pmemVolumes.get(volumeIndex) + "/" + bpid;
    String subDir = cacheSubdirPrefix + d1 + "/" + cacheSubdirPrefix + d2;
    File filePath = new File(parentDir, subDir);
    if (!filePath.exists() && !filePath.mkdirs()) {
      throw new IOException("Failed to create " + filePath.getPath());
    }
    return filePath.getAbsolutePath() + "/" + idToCacheFileName(key);
  }

  /**
   * The cache file path is pmemVolume/BlockPoolId/subdir#/subdir#/BlockId.
   */
  public String getCachePath(ExtendedBlockId key) throws IOException {
    Byte volumeIndex = blockKeyToVolume.get(key);
    if (volumeIndex == null) {
      return  null;
    }
    return idToCacheFilePath(volumeIndex, key);
  }

  @VisibleForTesting
  Map<ExtendedBlockId, Byte> getBlockKeyToVolume() {
    return blockKeyToVolume;
  }
}
