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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.store.DataBlocks;

/**
 * Abstract base class for managing Azure Data Lake Storage (ADLS) blocks.
 */
public abstract class AzureBlockManager {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsOutputStream.class);

  /** Factory for blocks. */
  private final DataBlocks.BlockFactory blockFactory;

  /** Current data block. Null means none currently active. */
  private AbfsBlock activeBlock;

  /** Count of blocks uploaded. */
  private long blockCount = 0;

  /** The size of a single block. */
  private final int blockSize;

  private AbfsOutputStream abfsOutputStream;

  /**
   * Constructs an AzureBlockManager.
   *
   * @param abfsOutputStream the output stream associated with this block manager
   * @param blockFactory the factory to create blocks
   * @param blockSize the size of each block
   */
  protected AzureBlockManager(AbfsOutputStream abfsOutputStream,
      DataBlocks.BlockFactory blockFactory,
      final int blockSize) {
    this.abfsOutputStream = abfsOutputStream;
    this.blockFactory = blockFactory;
    this.blockSize = blockSize;
  }

  /**
   * Creates a new block at the given position.
   *
   * @param position the position in the output stream where the block should be created
   * @return the created block
   * @throws IOException if an I/O error occurs
   */
  protected final synchronized AbfsBlock createBlock(final long position)
      throws IOException {
    return createBlockInternal(position);
  }

  /**
   * Internal method to create a new block at the given position.
   *
   * @param position the position in the output stream where the block should be created.
   * @return the created block.
   * @throws IOException if an I/O error occurs.
   */
  protected abstract AbfsBlock createBlockInternal(long position)
      throws IOException;

  /**
   * Gets the active block.
   *
   * @return the active block
   */
  protected synchronized AbfsBlock getActiveBlock() {
    return activeBlock;
  }

  /**
   * Sets the active block.
   *
   * @param activeBlock the block to set as active
   */
  public synchronized void setActiveBlock(final AbfsBlock activeBlock) {
    this.activeBlock = activeBlock;
  }

  /**
   * Checks if there is an active block.
   *
   * @return true if there is an active block, false otherwise
   */
  protected synchronized boolean hasActiveBlock() {
    return activeBlock != null;
  }

  /**
   * Gets the block factory.
   *
   * @return the block factory
   */
  protected DataBlocks.BlockFactory getBlockFactory() {
    return blockFactory;
  }

  /**
   * Gets the count of blocks uploaded.
   *
   * @return the block count
   */
  protected long getBlockCount() {
    return blockCount;
  }

  /**
   * Sets the count of blocks uploaded.
   *
   * @param blockCount the count of blocks to set
   */
  public void setBlockCount(final long blockCount) {
    this.blockCount = blockCount;
  }

  /**
   * Gets the block size.
   *
   * @return the block size
   */
  protected int getBlockSize() {
    return blockSize;
  }

  /**
   * Gets the AbfsOutputStream associated with this block manager.
   *
   * @return the AbfsOutputStream
   */
  protected AbfsOutputStream getAbfsOutputStream() {
    return abfsOutputStream;
  }

  /**
   * Clears the active block.
   */
  void clearActiveBlock() {
    synchronized (this) {
      if (activeBlock != null) {
        LOG.debug("Clearing active block");
      }
      activeBlock = null;
    }
  }

  // Used to clear any resources used by the block manager.
  void close() {
    if (hasActiveBlock()) {
      clearActiveBlock();
    }
    LOG.debug("AzureBlockManager closed.");
  }
}
