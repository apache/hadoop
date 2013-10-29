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

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.DataChecksum;

import com.google.common.base.Preconditions;

/**
 * Low-level wrapper for a Block and its backing files that provides mmap,
 * mlock, and checksum verification operations.
 * 
 * This could be a private class of FsDatasetCache, not meant for other users.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class MappableBlock implements Closeable {

  private final String bpid;
  private final Block block;
  private final FsVolumeImpl volume;

  private final FileInputStream blockIn;
  private final FileInputStream metaIn;
  private final FileChannel blockChannel;
  private final FileChannel metaChannel;
  private final long blockSize;

  private boolean isMapped;
  private boolean isLocked;
  private boolean isChecksummed;

  private MappedByteBuffer blockMapped = null;

  public MappableBlock(String bpid, Block blk, FsVolumeImpl volume,
      FileInputStream blockIn, FileInputStream metaIn) throws IOException {
    this.bpid = bpid;
    this.block = blk;
    this.volume = volume;

    this.blockIn = blockIn;
    this.metaIn = metaIn;
    this.blockChannel = blockIn.getChannel();
    this.metaChannel = metaIn.getChannel();
    this.blockSize = blockChannel.size();

    this.isMapped = false;
    this.isLocked = false;
    this.isChecksummed = false;
  }

  public String getBlockPoolId() {
    return bpid;
  }

  public Block getBlock() {
    return block;
  }

  public FsVolumeImpl getVolume() {
    return volume;
  }

  public boolean isMapped() {
    return isMapped;
  }

  public boolean isLocked() {
    return isLocked;
  }

  public boolean isChecksummed() {
    return isChecksummed;
  }

  /**
   * Returns the number of bytes on disk for the block file
   */
  public long getNumBytes() {
    return blockSize;
  }

  /**
   * Maps the block into memory. See mmap(2).
   */
  public void map() throws IOException {
    if (isMapped) {
      return;
    }
    blockMapped = blockChannel.map(MapMode.READ_ONLY, 0, blockSize);
    isMapped = true;
  }

  /**
   * Unmaps the block from memory. See munmap(2).
   */
  public void unmap() {
    if (!isMapped) {
      return;
    }
    if (blockMapped instanceof sun.nio.ch.DirectBuffer) {
      sun.misc.Cleaner cleaner =
          ((sun.nio.ch.DirectBuffer)blockMapped).cleaner();
      cleaner.clean();
    }
    isMapped = false;
    isLocked = false;
    isChecksummed = false;
  }

  /**
   * Locks the block into memory. This prevents the block from being paged out.
   * See mlock(2).
   */
  public void lock() throws IOException {
    Preconditions.checkArgument(isMapped,
        "Block must be mapped before it can be locked!");
    if (isLocked) {
      return;
    }
    NativeIO.POSIX.mlock(blockMapped, blockSize);
    isLocked = true;
  }

  /**
   * Unlocks the block from memory, allowing it to be paged out. See munlock(2).
   */
  public void unlock() throws IOException {
    if (!isLocked || !isMapped) {
      return;
    }
    NativeIO.POSIX.munlock(blockMapped, blockSize);
    isLocked = false;
    isChecksummed = false;
  }

  /**
   * Reads bytes into a buffer until EOF or the buffer's limit is reached
   */
  private int fillBuffer(FileChannel channel, ByteBuffer buf)
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

  /**
   * Verifies the block's checksum. This is an I/O intensive operation.
   * @return if the block was successfully checksummed.
   */
  public void verifyChecksum() throws IOException, ChecksumException {
    Preconditions.checkArgument(isLocked && isMapped,
        "Block must be mapped and locked before checksum verification!");
    // skip if checksum has already been successfully verified
    if (isChecksummed) {
      return;
    }
    // Verify the checksum from the block's meta file
    // Get the DataChecksum from the meta file header
    metaChannel.position(0);
    BlockMetadataHeader header =
        BlockMetadataHeader.readHeader(new DataInputStream(
            new BufferedInputStream(metaIn, BlockMetadataHeader
                .getHeaderSize())));
    DataChecksum checksum = header.getChecksum();
    final int bytesPerChecksum = checksum.getBytesPerChecksum();
    final int checksumSize = checksum.getChecksumSize();
    final int numChunks = (8*1024*1024) / bytesPerChecksum;
    ByteBuffer blockBuf = ByteBuffer.allocate(numChunks*bytesPerChecksum);
    ByteBuffer checksumBuf = ByteBuffer.allocate(numChunks*checksumSize);
    // Verify the checksum
    int bytesVerified = 0;
    while (bytesVerified < blockChannel.size()) {
      Preconditions.checkState(bytesVerified % bytesPerChecksum == 0,
          "Unexpected partial chunk before EOF");
      assert bytesVerified % bytesPerChecksum == 0;
      int bytesRead = fillBuffer(blockChannel, blockBuf);
      if (bytesRead == -1) {
        throw new IOException("Premature EOF");
      }
      blockBuf.flip();
      // Number of read chunks, including partial chunk at end
      int chunks = (bytesRead+bytesPerChecksum-1) / bytesPerChecksum;
      checksumBuf.limit(chunks*checksumSize);
      fillBuffer(metaChannel, checksumBuf);
      checksumBuf.flip();
      checksum.verifyChunkedSums(blockBuf, checksumBuf, block.getBlockName(),
          bytesVerified);
      // Success
      bytesVerified += bytesRead;
      blockBuf.clear();
      checksumBuf.clear();
    }
    isChecksummed = true;
    // Can close the backing file since everything is safely in memory
    blockChannel.close();
  }

  @Override
  public void close() {
    unmap();
    IOUtils.closeQuietly(blockIn);
    IOUtils.closeQuietly(metaIn);
  }
}
