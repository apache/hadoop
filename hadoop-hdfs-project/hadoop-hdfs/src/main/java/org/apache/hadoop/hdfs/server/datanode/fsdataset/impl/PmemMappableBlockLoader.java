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
import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.DNConf;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Maps block to persistent memory by using mapped byte buffer.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PmemMappableBlockLoader extends MappableBlockLoader {
  private static final Logger LOG =
      LoggerFactory.getLogger(PmemMappableBlockLoader.class);
  private PmemVolumeManager pmemVolumeManager;

  @Override
  void initialize(FsDatasetCache cacheManager) throws IOException {
    DNConf dnConf = cacheManager.getDnConf();
    this.pmemVolumeManager = new PmemVolumeManager(dnConf.getMaxLockedPmem(),
        dnConf.getPmemVolumes());
  }

  @VisibleForTesting
  PmemVolumeManager getPmemVolumeManager() {
    return pmemVolumeManager;
  }

  /**
   * Load the block.
   *
   * Map the block and verify its checksum.
   *
   * The block will be mapped to PmemDir/BlockPoolId-BlockId, in which PmemDir
   * is a persistent memory volume selected by getOneLocation() method.
   *
   * @param length         The current length of the block.
   * @param blockIn        The block input stream. Should be positioned at the
   *                       start. The caller must close this.
   * @param metaIn         The meta file input stream. Should be positioned at
   *                       the start. The caller must close this.
   * @param blockFileName  The block file name, for logging purposes.
   * @param key            The extended block ID.
   *
   * @throws IOException   If mapping block fails or checksum fails.
   *
   * @return               The Mappable block.
   */
  @Override
  MappableBlock load(long length, FileInputStream blockIn,
                            FileInputStream metaIn, String blockFileName,
                            ExtendedBlockId key)
      throws IOException {
    PmemMappedBlock mappableBlock = null;
    String filePath = null;

    FileChannel blockChannel = null;
    RandomAccessFile file = null;
    MappedByteBuffer out = null;
    try {
      blockChannel = blockIn.getChannel();
      if (blockChannel == null) {
        throw new IOException("Block InputStream has no FileChannel.");
      }

      Byte volumeIndex = pmemVolumeManager.getOneVolumeIndex();
      filePath = pmemVolumeManager.inferCacheFilePath(volumeIndex, key);
      file = new RandomAccessFile(filePath, "rw");
      out = file.getChannel().
          map(FileChannel.MapMode.READ_WRITE, 0, length);
      if (out == null) {
        throw new IOException("Failed to map the block " + blockFileName +
            " to persistent storage.");
      }
      verifyChecksumAndMapBlock(out, length, metaIn, blockChannel,
          blockFileName);
      mappableBlock = new PmemMappedBlock(
          length, volumeIndex, key, pmemVolumeManager);
      pmemVolumeManager.afterCache(key, volumeIndex);
      LOG.info("Successfully cached one replica:{} into persistent memory"
          + ", [cached path={}, length={}]", key, filePath, length);
    } finally {
      IOUtils.closeQuietly(blockChannel);
      if (out != null) {
        NativeIO.POSIX.munmap(out);
      }
      IOUtils.closeQuietly(file);
      if (mappableBlock == null) {
        FsDatasetUtil.deleteMappedFile(filePath);
      }
    }
    return mappableBlock;
  }

  /**
   * Verifies the block's checksum meanwhile maps block to persistent memory.
   * This is an I/O intensive operation.
   */
  private void verifyChecksumAndMapBlock(
      MappedByteBuffer out, long length, FileInputStream metaIn,
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
        throw new IOException("Cannot get FileChannel from " +
            "Block InputStream meta file.");
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
          throw new IOException(
              "Checksum verification failed for the block " + blockFileName +
                  ": premature EOF");
        }
        blockBuf.flip();
        // Number of read chunks, including partial chunk at end
        int chunks = (bytesRead + bytesPerChecksum - 1) / bytesPerChecksum;
        checksumBuf.limit(chunks * checksumSize);
        fillBuffer(metaChannel, checksumBuf);
        checksumBuf.flip();
        checksum.verifyChunkedSums(blockBuf, checksumBuf, blockFileName,
            bytesVerified);

        // / Copy data to persistent file
        out.put(blockBuf);
        // positioning the
        bytesVerified += bytesRead;

        // Clear buffer
        blockBuf.clear();
        checksumBuf.clear();
      }
      // Forces to write data to storage device containing the mapped file
      out.force();
    } finally {
      IOUtils.closeQuietly(metaChannel);
    }
  }

  @Override
  public String getCacheCapacityConfigKey() {
    return DFSConfigKeys.DFS_DATANODE_CACHE_PMEM_CAPACITY_KEY;
  }

  @Override
  public long getCacheUsed() {
    return pmemVolumeManager.getCacheUsed();
  }

  @Override
  public long getCacheCapacity() {
    return pmemVolumeManager.getCacheCapacity();
  }

  @Override
  long reserve(long bytesCount) {
    return pmemVolumeManager.reserve(bytesCount);
  }

  @Override
  long release(long bytesCount) {
    return pmemVolumeManager.release(bytesCount);
  }

  @Override
  public boolean isTransientCache() {
    return false;
  }

  @Override
  public String getCachedPath(ExtendedBlockId key) {
    return pmemVolumeManager.getCacheFilePath(key);
  }
}
