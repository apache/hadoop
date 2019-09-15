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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
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
    private final long maxBytes;
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

  /**
   * The total cache capacity in bytes of persistent memory.
   */
  private long cacheCapacity;
  private static long maxBytesPerPmem = -1;
  private int count = 0;
  private byte nextIndex = 0;

  private PmemVolumeManager(String[] pmemVolumesConfig) throws IOException {
    if (pmemVolumesConfig == null || pmemVolumesConfig.length == 0) {
      throw new IOException("The persistent memory volume, " +
          DFSConfigKeys.DFS_DATANODE_CACHE_PMEM_DIRS_KEY +
          " is not configured!");
    }
    this.loadVolumes(pmemVolumesConfig);
    cacheCapacity = 0L;
    for (UsedBytesCount counter : usedBytesCounts) {
      cacheCapacity += counter.getMaxBytes();
    }
  }

  public synchronized static void init(String[] pmemVolumesConfig)
      throws IOException {
    if (pmemVolumeManager == null) {
      pmemVolumeManager = new PmemVolumeManager(pmemVolumesConfig);
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
        // Clean up the cache left before, if any.
        cleanup(realPmemDir);
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

  /**
   * The cache file is named as BlockPoolId-BlockId.
   * So its name can be inferred by BlockPoolId and BlockId.
   */
  public String getCacheFileName(ExtendedBlockId key) {
    return key.getBlockPoolId() + "-" + key.getBlockId();
  }

  /**
   * Considering the pmem volume size is below TB level currently,
   * it is tolerable to keep cache files under one directory.
   * The strategy will be optimized, especially if one pmem volume
   * has huge cache capacity.
   *
   * @param volumeIndex   The index of pmem volume where a replica will be
   *                      cached to or has been cached to.
   *
   * @param key           The replica's ExtendedBlockId.
   *
   * @return              A path to which the block replica is mapped.
   */
  public String inferCacheFilePath(Byte volumeIndex, ExtendedBlockId key) {
    return pmemVolumes.get(volumeIndex) + "/" + getCacheFileName(key);
  }

  /**
   * The cache file path is pmemVolume/BlockPoolId-BlockId.
   */
  public String getCachePath(ExtendedBlockId key) {
    Byte volumeIndex = blockKeyToVolume.get(key);
    if (volumeIndex == null) {
      return  null;
    }
    return inferCacheFilePath(volumeIndex, key);
  }

  @VisibleForTesting
  Map<ExtendedBlockId, Byte> getBlockKeyToVolume() {
    return blockKeyToVolume;
  }
}
