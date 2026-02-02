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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.fs.store.DataBlocks;

/**
 * Return activeBlock with blockId.
 */
public class AbfsBlock implements Closeable {

  private final DataBlocks.DataBlock activeBlock;
  private AbfsOutputStream outputStream;
  private final long offset;
  private BlockEntry blockEntry;

  /**
   * Gets the activeBlock and the blockId.
   * @param outputStream AbfsOutputStream Instance.
   * @param offset Used to generate blockId based on offset.
   * @throws IOException
   */
  AbfsBlock(AbfsOutputStream outputStream, long offset) throws IOException {
    this.outputStream = outputStream;
    this.offset = offset;
    DataBlocks.BlockFactory blockFactory = outputStream.getBlockManager().getBlockFactory();
    long blockCount = outputStream.getBlockManager().getBlockCount();
    int blockSize = outputStream.getBlockManager().getBlockSize();
    AbfsOutputStreamStatistics outputStreamStatistics = outputStream.getOutputStreamStatistics();
    this.activeBlock = blockFactory.create(blockCount, blockSize, outputStreamStatistics);
  }

  /**
   * Returns datasize for the block.
   * @return datasize.
   */
  public int dataSize() {
    return activeBlock.dataSize();
  }

  /**
   * Returns an instance of BlockUploadData.
   *
   * @return an instance of BlockUploadData.
   * @throws IOException if an I/O error occurs during the upload process.
   */
  public DataBlocks.BlockUploadData startUpload() throws IOException {
    return activeBlock.startUpload();
  }

  /**
   * Return the block has data or not.
   * @return block has data or not.
   */
  public boolean hasData() {
    return activeBlock.hasData();
  }

  /**
   * Writes a series of bytes from the buffer, starting from the specified offset.
   * Returns the number of bytes written. Only valid in the state Writing.
   * The base class verifies the state but does no writing.
   *
   * @param buffer the buffer containing the bytes to write.
   * @param offset the offset in the buffer to start writing from.
   * @param length the number of bytes to write.
   * @return the number of bytes written.
   * @throws IOException if an I/O error occurs.
   */
  public int write(byte[] buffer, int offset, int length) throws IOException {
    return activeBlock.write(buffer, offset, length);
  }

  /**
   * Returns remainingCapacity.
   * @return remainingCapacity.
   */
  public int remainingCapacity() {
    return activeBlock.remainingCapacity();
  }

  /**
   * Returns the offset of the block.
   *
   * @return the offset of the block.
   */
  public Long getOffset() {
    return offset;
  }

  @Override
  public void close() throws IOException {
    if (activeBlock != null) {
      activeBlock.close();
    }
  }

  /**
   * Returns blockId for the block.
   * @return blockId.
   */
  public String getBlockId() {
    throw new IllegalArgumentException("DFS client does not support blockId");
  }

  /**
   * Gets the AbfsOutputStream.
   *
   * @return the AbfsOutputStream.
   */
  public AbfsOutputStream getOutputStream() {
    return outputStream;
  }

  /**
   * Sets the AbfsOutputStream.
   *
   * @param outputStream the AbfsOutputStream to set.
   */
  public void setOutputStream(final AbfsOutputStream outputStream) {
    this.outputStream = outputStream;
  }

  /**
   * Returns the block entry.
   *
   * @return the block entry.
   */
  public BlockEntry getBlockEntry() {
    return blockEntry;
  }

  /**
   * Sets the block entry.
   *
   * @param blockEntry the block entry to set.
   */
  public void setBlockEntry(final BlockEntry blockEntry) {
    this.blockEntry = blockEntry;
  }
}
