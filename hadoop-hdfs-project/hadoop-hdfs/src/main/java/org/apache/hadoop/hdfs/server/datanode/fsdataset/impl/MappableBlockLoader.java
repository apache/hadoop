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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.ExtendedBlockId;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Maps block to DataNode cache region.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class MappableBlockLoader {

  /**
   * Initialize a specific MappableBlockLoader.
   */
  abstract void initialize(FsDatasetCache cacheManager) throws IOException;

  /**
   * Load the block.
   *
   * Map the block, and then verify its checksum.
   *
   * @param length         The current length of the block.
   * @param blockIn        The block input stream. Should be positioned at the
   *                       start. The caller must close this.
   * @param metaIn         The meta file input stream. Should be positioned at
   *                       the start. The caller must close this.
   * @param blockFileName  The block file name, for logging purposes.
   * @param key            The extended block ID.
   *
   * @throws IOException   If mapping block to cache region fails or checksum
   *                       fails.
   *
   * @return               The Mappable block.
   */
  abstract MappableBlock load(long length, FileInputStream blockIn,
                              FileInputStream metaIn, String blockFileName,
                              ExtendedBlockId key)
      throws IOException;

  /**
   * Try to reserve some given bytes.
   *
   * @param bytesCount    The number of bytes to add.
   *
   * @return              The new number of usedBytes if we succeeded;
   *                      -1 if we failed.
   */
  abstract long reserve(long bytesCount);

  /**
   * Release some bytes that we're using.
   *
   * @param bytesCount    The number of bytes to release.
   *
   * @return              The new number of usedBytes.
   */
  abstract long release(long bytesCount);

  /**
   * Get the config key of cache capacity.
   */
  abstract String getCacheCapacityConfigKey();

  /**
   * Get the approximate amount of cache space used.
   */
  abstract long getCacheUsed();

  /**
   * Get the maximum amount of cache bytes.
   */
  abstract long getCacheCapacity();

  /**
   * Check whether the cache is non-volatile.
   */
  abstract boolean isTransientCache();

  /**
   * Get a cache file path if applicable. Otherwise return null.
   */
  abstract String getCachedPath(ExtendedBlockId key);

  /**
   * Reads bytes into a buffer until EOF or the buffer's limit is reached.
   */
  protected int fillBuffer(FileChannel channel, ByteBuffer buf)
      throws IOException {
    int bytesRead = channel.read(buf);
    if (bytesRead < 0) {
      //EOF
      return bytesRead;
    }
    while (buf.remaining() > 0) {
      int n = channel.read(buf);
      if (n < 0) {
        //EOF
        return bytesRead;
      }
      bytesRead += n;
    }
    return bytesRead;
  }
}
