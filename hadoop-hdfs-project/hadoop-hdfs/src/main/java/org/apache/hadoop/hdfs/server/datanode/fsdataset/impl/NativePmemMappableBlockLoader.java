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
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIO.POSIX;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Map block to persistent memory with native PMDK libs.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class NativePmemMappableBlockLoader extends PmemMappableBlockLoader {
  private static final Logger LOG =
      LoggerFactory.getLogger(NativePmemMappableBlockLoader.class);

  @Override
  CacheStats initialize(DNConf dnConf) throws IOException {
    return super.initialize(dnConf);
  }

  /**
   * Load the block.
   *
   * Map the block and verify its checksum.
   *
   * The block will be mapped to PmemDir/BlockPoolId/subdir#/subdir#/BlockId,
   * in which PmemDir is a persistent memory volume chosen by PmemVolumeManager.
   *
   * @param length         The current length of the block.
   * @param blockIn        The block input stream. Should be positioned at the
   *                       start. The caller must close this.
   * @param metaIn         The meta file input stream. Should be positioned at
   *                       the start. The caller must close this.
   * @param blockFileName  The block file name, for logging purposes.
   * @param key            The extended block ID.
   *
   * @throws IOException   If mapping block to persistent memory fails or
   *                       checksum fails.
   *
   * @return               The Mappable block.
   */
  @Override
  public MappableBlock load(long length, FileInputStream blockIn,
      FileInputStream metaIn, String blockFileName,
      ExtendedBlockId key)
      throws IOException {
    NativePmemMappedBlock mappableBlock = null;
    POSIX.PmemMappedRegion region = null;
    String filePath = null;

    FileChannel blockChannel = null;
    try {
      blockChannel = blockIn.getChannel();
      if (blockChannel == null) {
        throw new IOException("Block InputStream has no FileChannel.");
      }

      assert NativeIO.isAvailable();
      filePath = PmemVolumeManager.getInstance().getCachePath(key);
      region = POSIX.Pmem.mapBlock(filePath, length, false);
      if (region == null) {
        throw new IOException("Failed to map the block " + blockFileName +
            " to persistent storage.");
      }
      verifyChecksumAndMapBlock(region, length, metaIn, blockChannel,
          blockFileName);
      mappableBlock = new NativePmemMappedBlock(region.getAddress(),
          region.getLength(), key);
      LOG.info("Successfully cached one replica:{} into persistent memory"
          + ", [cached path={}, address={}, length={}]", key, filePath,
          region.getAddress(), length);
    } finally {
      IOUtils.closeQuietly(blockChannel);
      if (mappableBlock == null) {
        if (region != null) {
          // unmap content from persistent memory
          POSIX.Pmem.unmapBlock(region.getAddress(),
              region.getLength());
          FsDatasetUtil.deleteMappedFile(filePath);
        }
      }
    }
    return mappableBlock;
  }

  /**
   * Verifies the block's checksum meanwhile map block to persistent memory.
   * This is an I/O intensive operation.
   */
  private void verifyChecksumAndMapBlock(POSIX.PmemMappedRegion region,
      long length, FileInputStream metaIn, FileChannel blockChannel,
      String blockFileName) throws IOException {
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
        throw new IOException("Cannot get FileChannel" +
            " from Block InputStream meta file.");
      }
      DataChecksum checksum = header.getChecksum();
      final int bytesPerChecksum = checksum.getBytesPerChecksum();
      final int checksumSize = checksum.getChecksumSize();
      final int numChunks = (8 * 1024 * 1024) / bytesPerChecksum;
      ByteBuffer blockBuf = ByteBuffer.allocate(numChunks * bytesPerChecksum);
      ByteBuffer checksumBuf = ByteBuffer.allocate(numChunks * checksumSize);
      // Verify the checksum
      int bytesVerified = 0;
      long mappedAddress = -1L;
      if (region != null) {
        mappedAddress = region.getAddress();
      }
      while (bytesVerified < length) {
        Preconditions.checkState(bytesVerified % bytesPerChecksum == 0,
            "Unexpected partial chunk before EOF.");
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
        // Success
        bytesVerified += bytesRead;
        // Copy data to persistent file
        POSIX.Pmem.memCopy(blockBuf.array(), mappedAddress,
            region.isPmem(), bytesRead);
        mappedAddress += bytesRead;
        // Clear buffer
        blockBuf.clear();
        checksumBuf.clear();
      }
      if (region != null) {
        POSIX.Pmem.memSync(region);
      }
    } finally {
      IOUtils.closeQuietly(metaChannel);
    }
  }

  @Override
  public boolean isNativeLoader() {
    return true;
  }

  @Override
  public MappableBlock getRecoveredMappableBlock(
      File cacheFile, String bpid, byte volumeIndex) throws IOException {
    NativeIO.POSIX.PmemMappedRegion region =
        NativeIO.POSIX.Pmem.mapBlock(cacheFile.getAbsolutePath(),
            cacheFile.length(), true);
    if (region == null) {
      throw new IOException("Failed to recover the block "
          + cacheFile.getName() + " in persistent storage.");
    }
    ExtendedBlockId key =
        new ExtendedBlockId(super.getBlockId(cacheFile), bpid);
    MappableBlock mappableBlock = new NativePmemMappedBlock(
        region.getAddress(), region.getLength(), key);
    PmemVolumeManager.getInstance().recoverBlockKeyToVolume(key, volumeIndex);

    String path = PmemVolumeManager.getInstance().getCachePath(key);
    long addr = mappableBlock.getAddress();
    long length = mappableBlock.getLength();
    LOG.info("Recovering persistent memory cache for block {}, " +
        "path = {}, address = {}, length = {}", key, path, addr, length);
    return mappableBlock;
  }
}
