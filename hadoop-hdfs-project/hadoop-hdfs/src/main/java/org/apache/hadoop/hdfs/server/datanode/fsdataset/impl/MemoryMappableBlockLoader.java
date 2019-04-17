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

import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.DataChecksum;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Maps block to memory.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MemoryMappableBlockLoader extends MappableBlockLoader {

  private MemoryCacheStats memCacheStats;

  @Override
  void initialize(FsDatasetCache cacheManager) throws IOException {
    this.memCacheStats = cacheManager.getMemCacheStats();
  }

  /**
   * Load the block.
   *
   * mmap and mlock the block, and then verify its checksum.
   *
   * @param length         The current length of the block.
   * @param blockIn        The block input stream. Should be positioned at the
   *                       start. The caller must close this.
   * @param metaIn         The meta file input stream. Should be positioned at
   *                       the start. The caller must close this.
   * @param blockFileName  The block file name, for logging purposes.
   * @param key            The extended block ID.
   *
   * @throws IOException   If mapping block to memory fails or checksum fails.

   * @return               The Mappable block.
   */
  @Override
  MappableBlock load(long length, FileInputStream blockIn,
                            FileInputStream metaIn, String blockFileName,
                            ExtendedBlockId key)
      throws IOException {
    MemoryMappedBlock mappableBlock = null;
    MappedByteBuffer mmap = null;
    FileChannel blockChannel = null;
    try {
      blockChannel = blockIn.getChannel();
      if (blockChannel == null) {
        throw new IOException("Block InputStream has no FileChannel.");
      }
      mmap = blockChannel.map(FileChannel.MapMode.READ_ONLY, 0, length);
      NativeIO.POSIX.getCacheManipulator().mlock(blockFileName, mmap, length);
      verifyChecksum(length, metaIn, blockChannel, blockFileName);
      mappableBlock = new MemoryMappedBlock(mmap, length);
    } finally {
      IOUtils.closeQuietly(blockChannel);
      if (mappableBlock == null) {
        if (mmap != null) {
          NativeIO.POSIX.munmap(mmap); // unmapping also unlocks
        }
      }
    }
    return mappableBlock;
  }

  /**
   * Verifies the block's checksum. This is an I/O intensive operation.
   */
  private void verifyChecksum(long length, FileInputStream metaIn,
                             FileChannel blockChannel, String blockFileName)
      throws IOException {
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

  @Override
  public String getCacheCapacityConfigKey() {
    return DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY;
  }

  @Override
  public long getCacheUsed() {
    return memCacheStats.getCacheUsed();
  }

  @Override
  public long getCacheCapacity() {
    return memCacheStats.getCacheCapacity();
  }

  @Override
  long reserve(long bytesCount) {
    return memCacheStats.reserve(bytesCount);
  }

  @Override
  long release(long bytesCount) {
    return memCacheStats.release(bytesCount);
  }

  @Override
  public boolean isTransientCache() {
    return true;
  }

  @Override
  public String getCachedPath(ExtendedBlockId key) {
    return null;
  }
}
