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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manage the persistent memory volumes.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PmemVolumeManager {

  /**
   * Counts used bytes for persistent memory.
   */
  private class UsedBytesCount {
    private final AtomicLong usedBytes = new AtomicLong(0);

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
        if (next > cacheCapacity) {
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

    long get() {
      return usedBytes.get();
    }
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(PmemVolumeManager.class);
  private final ArrayList<String> pmemVolumes = new ArrayList<>();
  // Maintain which pmem volume a block is cached to.
  private final Map<ExtendedBlockId, Byte> blockKeyToVolume =
      new ConcurrentHashMap<>();
  private final UsedBytesCount usedBytesCount;

  /**
   * The total cache capacity in bytes of persistent memory.
   * It is 0L if the specific mappableBlockLoader couldn't cache data to pmem.
   */
  private final long cacheCapacity;
  private int count = 0;
  // Strict atomic operation is not guaranteed for the performance sake.
  private int i = 0;

  PmemVolumeManager(long maxBytes, String[] pmemVolumesConfigured)
      throws IOException {
    if (pmemVolumesConfigured == null || pmemVolumesConfigured.length == 0) {
      throw new IOException("The persistent memory volume, " +
          DFSConfigKeys.DFS_DATANODE_CACHE_PMEM_DIRS_KEY +
          " is not configured!");
    }
    this.loadVolumes(pmemVolumesConfigured);
    this.usedBytesCount = new UsedBytesCount();
    this.cacheCapacity = maxBytes;
  }

  public long getCacheUsed() {
    return usedBytesCount.get();
  }

  public long getCacheCapacity() {
    return cacheCapacity;
  }

  /**
   * Try to reserve more bytes on persistent memory.
   *
   * @param bytesCount    The number of bytes to add.
   *
   * @return              The new number of usedBytes if we succeeded;
   *                      -1 if we failed.
   */
  long reserve(long bytesCount) {
    return usedBytesCount.reserve(bytesCount);
  }

  /**
   * Release some bytes that we're using on persistent memory.
   *
   * @param bytesCount    The number of bytes to release.
   *
   * @return              The new number of usedBytes.
   */
  long release(long bytesCount) {
    return usedBytesCount.release(bytesCount);
  }

  /**
   * Load and verify the configured pmem volumes.
   *
   * @throws IOException   If there is no available pmem volume.
   */
  private void loadVolumes(String[] volumes) throws IOException {
    // Check whether the volume exists
    for (String volume: volumes) {
      try {
        File pmemDir = new File(volume);
        verifyIfValidPmemVolume(pmemDir);
        // Remove all files under the volume.
        FileUtils.cleanDirectory(pmemDir);
      } catch (IllegalArgumentException e) {
        LOG.error("Failed to parse persistent memory volume " + volume, e);
        continue;
      } catch (IOException e) {
        LOG.error("Bad persistent memory volume: " + volume, e);
        continue;
      }
      pmemVolumes.add(volume);
      LOG.info("Added persistent memory - " + volume);
    }
    count = pmemVolumes.size();
    if (count == 0) {
      throw new IOException(
          "At least one valid persistent memory volume is required!");
    }
  }

  @VisibleForTesting
  static void verifyIfValidPmemVolume(File pmemDir)
      throws IOException {
    if (!pmemDir.exists()) {
      final String message = pmemDir + " does not exist";
      throw new IOException(message);
    }

    if (!pmemDir.isDirectory()) {
      final String message = pmemDir + " is not a directory";
      throw new IllegalArgumentException(message);
    }

    String uuidStr = UUID.randomUUID().toString();
    String testFilePath = pmemDir.getPath() + "/.verify.pmem." + uuidStr;
    byte[] contents = uuidStr.getBytes("UTF-8");
    RandomAccessFile testFile = null;
    MappedByteBuffer out = null;
    try {
      testFile = new RandomAccessFile(testFilePath, "rw");
      out = testFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0,
          contents.length);
      if (out == null) {
        throw new IOException("Failed to map the test file under " + pmemDir);
      }
      out.put(contents);
      // Forces to write data to storage device containing the mapped file
      out.force();
    } catch (IOException e) {
      throw new IOException(
          "Exception while writing data to persistent storage dir: " +
              pmemDir, e);
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
   * Choose a persistent memory volume based on a specific algorithm.
   * Currently it is a round-robin policy.
   *
   * TODO: Refine volume selection policy by considering storage utilization.
   */
  Byte getOneVolumeIndex() throws IOException {
    if (count != 0) {
      return (byte)(i++ % count);
    } else {
      throw new IOException("No usable persistent memory is found");
    }
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
  public String getCacheFilePath(ExtendedBlockId key) {
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

  /**
   * Add cached block's ExtendedBlockId and its cache volume index to a map
   * after cache.
   */
  public void afterCache(ExtendedBlockId key, Byte volumeIndex) {
    blockKeyToVolume.put(key, volumeIndex);
  }

  /**
   * Remove the record in blockKeyToVolume for uncached block after uncache.
   */
  public void afterUncache(ExtendedBlockId key) {
    blockKeyToVolume.remove(key);
  }
}
