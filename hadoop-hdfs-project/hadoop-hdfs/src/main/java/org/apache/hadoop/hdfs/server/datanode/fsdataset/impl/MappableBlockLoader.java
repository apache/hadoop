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

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.DNConf;
import org.apache.hadoop.util.DataChecksum;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
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
  abstract CacheStats initialize(DNConf dnConf) throws IOException;

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
      FileInputStream metaIn, String blockFileName, ExtendedBlockId key)
      throws IOException;

  /**
   * Try to reserve some given bytes.
   *
   * @param key           The ExtendedBlockId for a block.
   *
   * @param bytesCount    The number of bytes to add.
   *
   * @return              The new number of usedBytes if we succeeded;
   *                      -1 if we failed.
   */
  abstract long reserve(ExtendedBlockId key, long bytesCount);

  /**
   * Release some bytes that we're using.
   *
   * @param key           The ExtendedBlockId for a block.
   *
   * @param bytesCount    The number of bytes to release.
   *
   * @return              The new number of usedBytes.
   */
  abstract long release(ExtendedBlockId key, long bytesCount);

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
   * Check whether this is a native pmem cache loader.
   */
  abstract boolean isNativeLoader();

  /**
   * Get mappableBlock recovered from persistent memory.
   */
  abstract MappableBlock getRecoveredMappableBlock(
      File cacheFile, String bpid, byte volumeIndex) throws IOException;

  /**
   * Clean up cache, can be used during DataNode shutdown.
   */
  void shutdown() {
    // Do nothing.
  }

  /**
   * Verifies the block's checksum. This is an I/O intensive operation.
   */
  protected void verifyChecksum(long length, FileInputStream metaIn,
      FileChannel blockChannel, String blockFileName) throws IOException {
    // Verify the checksum from the block's meta file
    // Get the DataChecksum from the meta file header
    BlockMetadataHeader header =
        BlockMetadataHeader.readHeader(new DataInputStream(
            new BufferedInputStream(metaIn, BlockMetadataHeader
                .getHeaderSize())));
    FileChannel metaChannel = null;
    try {
      metaChannel = metaIn.getChannel();
      if (metaChannel == null) {
        throw new IOException(
            "Block InputStream meta file has no FileChannel.");
      }
      DataChecksum checksum = header.getChecksum();
      final int bytesPerChecksum = checksum.getBytesPerChecksum();
      final int checksumSize = checksum.getChecksumSize();
      final int numChunks = (8 * 1024 * 1024) / bytesPerChecksum;
      ByteBuffer blockBuf = ByteBuffer.allocate(numChunks * bytesPerChecksum);
      ByteBuffer checksumBuf = ByteBuffer.allocate(numChunks * checksumSize);
      // Verify the checksum
      int bytesVerified = 0;
      while (bytesVerified < length) {
        Preconditions.checkState(bytesVerified % bytesPerChecksum == 0,
            "Unexpected partial chunk before EOF");
        assert bytesVerified % bytesPerChecksum == 0;
        int bytesRead = fillBuffer(blockChannel, blockBuf);
        if (bytesRead == -1) {
          throw new IOException("checksum verification failed: premature EOF");
        }
        blockBuf.flip();
        // Number of read chunks, including partial chunk at end
        int chunks = (bytesRead + bytesPerChecksum - 1) / bytesPerChecksum;
        checksumBuf.limit(chunks * checksumSize);
        fillBuffer(metaChannel, checksumBuf);
        checksumBuf.flip();
        checksum.verifyChunkedSums(blockBuf, checksumBuf, blockFileName,
            bytesVerified);
        // Success
        bytesVerified += bytesRead;
        blockBuf.clear();
        checksumBuf.clear();
      }
    } finally {
      IOUtils.closeQuietly(metaChannel);
    }
  }

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
