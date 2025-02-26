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
 * Manages Azure Data Lake Storage (ADLS) blocks for append operations.
 */
public class AzureDFSBlockManager extends AzureBlockManager {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsOutputStream.class);

  /**
   * Constructs an AzureDFSBlockManager.
   *
   * @param abfsOutputStream the output stream associated with this block manager
   * @param blockFactory the factory to create blocks
   * @param blockSize the size of each block
   */
  public AzureDFSBlockManager(AbfsOutputStream abfsOutputStream,
      DataBlocks.BlockFactory blockFactory,
      int blockSize) {
    super(abfsOutputStream, blockFactory, blockSize);
    LOG.trace(
        "Created a new DFS Block Manager for AbfsOutputStream instance {} for path {}",
        abfsOutputStream.getStreamID(), abfsOutputStream.getPath());
  }

  /**
   * Creates a new block at the given position if none exists.
   *
   * @param position the position in the output stream where the block should be created
   * @return the created block
   * @throws IOException if an I/O error occurs
   */
  @Override
  protected synchronized AbfsBlock createBlockInternal(long position)
      throws IOException {
    if (getActiveBlock() == null) {
      setBlockCount(getBlockCount() + 1);
      AbfsBlock activeBlock = new AbfsBlock(getAbfsOutputStream(), position);
      setActiveBlock(activeBlock);
    }
    return getActiveBlock();
  }

  /**
   * Gets the active block.
   *
   * @return the active block
   */
  @Override
  protected synchronized AbfsBlock getActiveBlock() {
    return super.getActiveBlock();
  }

  /**
   * Checks if there is an active block.
   *
   * @return true if there is an active block, false otherwise
   */
  @Override
  protected synchronized boolean hasActiveBlock() {
    return super.hasActiveBlock();
  }
}
