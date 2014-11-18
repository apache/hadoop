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
package org.apache.hadoop.hdfs.server.blockmanagement;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;

import java.io.IOException;

/**
 * BlockIdManager allocates the generation stamps and the block ID. The
 * {@see FSNamesystem} is responsible for persisting the allocations in the
 * {@see EditLog}.
 */
public class BlockIdManager {
  /**
   * The global generation stamp for legacy blocks with randomly
   * generated block IDs.
   */
  private final GenerationStamp generationStampV1 = new GenerationStamp();
  /**
   * The global generation stamp for this file system.
   */
  private final GenerationStamp generationStampV2 = new GenerationStamp();
  /**
   * The value of the generation stamp when the first switch to sequential
   * block IDs was made. Blocks with generation stamps below this value
   * have randomly allocated block IDs. Blocks with generation stamps above
   * this value had sequentially allocated block IDs. Read from the fsImage
   * (or initialized as an offset from the V1 (legacy) generation stamp on
   * upgrade).
   */
  private long generationStampV1Limit;
  /**
   * The global block ID space for this file system.
   */
  private final SequentialBlockIdGenerator blockIdGenerator;

  public BlockIdManager(BlockManager blockManager) {
    this.generationStampV1Limit = GenerationStamp.GRANDFATHER_GENERATION_STAMP;
    this.blockIdGenerator = new SequentialBlockIdGenerator(blockManager);
  }

  /**
   * Upgrades the generation stamp for the filesystem
   * by reserving a sufficient range for all existing blocks.
   * Should be invoked only during the first upgrade to
   * sequential block IDs.
   */
  public long upgradeGenerationStampToV2() {
    Preconditions.checkState(generationStampV2.getCurrentValue() ==
      GenerationStamp.LAST_RESERVED_STAMP);
    generationStampV2.skipTo(generationStampV1.getCurrentValue() +
      HdfsConstants.RESERVED_GENERATION_STAMPS_V1);

    generationStampV1Limit = generationStampV2.getCurrentValue();
    return generationStampV2.getCurrentValue();
  }

  /**
   * Sets the generation stamp that delineates random and sequentially
   * allocated block IDs.
   *
   * @param stamp set generation stamp limit to this value
   */
  public void setGenerationStampV1Limit(long stamp) {
    Preconditions.checkState(generationStampV1Limit == GenerationStamp
      .GRANDFATHER_GENERATION_STAMP);
    generationStampV1Limit = stamp;
  }

  /**
   * Gets the value of the generation stamp that delineates sequential
   * and random block IDs.
   */
  public long getGenerationStampAtblockIdSwitch() {
    return generationStampV1Limit;
  }

  @VisibleForTesting
  SequentialBlockIdGenerator getBlockIdGenerator() {
    return blockIdGenerator;
  }

  /**
   * Sets the maximum allocated block ID for this filesystem. This is
   * the basis for allocating new block IDs.
   */
  public void setLastAllocatedBlockId(long blockId) {
    blockIdGenerator.skipTo(blockId);
  }

  /**
   * Gets the maximum sequentially allocated block ID for this filesystem
   */
  public long getLastAllocatedBlockId() {
    return blockIdGenerator.getCurrentValue();
  }

  /**
   * Sets the current generation stamp for legacy blocks
   */
  public void setGenerationStampV1(long stamp) {
    generationStampV1.setCurrentValue(stamp);
  }

  /**
   * Gets the current generation stamp for legacy blocks
   */
  public long getGenerationStampV1() {
    return generationStampV1.getCurrentValue();
  }

  /**
   * Gets the current generation stamp for this filesystem
   */
  public void setGenerationStampV2(long stamp) {
    generationStampV2.setCurrentValue(stamp);
  }

  public long getGenerationStampV2() {
    return generationStampV2.getCurrentValue();
  }

  /**
   * Increments, logs and then returns the stamp
   */
  public long nextGenerationStamp(boolean legacyBlock) throws IOException {
    return legacyBlock ? getNextGenerationStampV1() :
      getNextGenerationStampV2();
  }

  @VisibleForTesting
  long getNextGenerationStampV1() throws IOException {
    long genStampV1 = generationStampV1.nextValue();

    if (genStampV1 >= generationStampV1Limit) {
      // We ran out of generation stamps for legacy blocks. In practice, it
      // is extremely unlikely as we reserved 1T v1 generation stamps. The
      // result is that we can no longer append to the legacy blocks that
      // were created before the upgrade to sequential block IDs.
      throw new OutOfV1GenerationStampsException();
    }

    return genStampV1;
  }

  @VisibleForTesting
  long getNextGenerationStampV2() {
    return generationStampV2.nextValue();
  }

  public long getGenerationStampV1Limit() {
    return generationStampV1Limit;
  }

  /**
   * Determine whether the block ID was randomly generated (legacy) or
   * sequentially generated. The generation stamp value is used to
   * make the distinction.
   *
   * @return true if the block ID was randomly generated, false otherwise.
   */
  public boolean isLegacyBlock(Block block) {
    return block.getGenerationStamp() < getGenerationStampV1Limit();
  }

  /**
   * Increments, logs and then returns the block ID
   */
  public long nextBlockId() {
    return blockIdGenerator.nextValue();
  }

  public boolean isGenStampInFuture(Block block) {
    if (isLegacyBlock(block)) {
      return block.getGenerationStamp() > getGenerationStampV1();
    } else {
      return block.getGenerationStamp() > getGenerationStampV2();
    }
  }

  public void clear() {
    generationStampV1.setCurrentValue(GenerationStamp.LAST_RESERVED_STAMP);
    generationStampV2.setCurrentValue(GenerationStamp.LAST_RESERVED_STAMP);
    getBlockIdGenerator().setCurrentValue(SequentialBlockIdGenerator
      .LAST_RESERVED_BLOCK_ID);
    generationStampV1Limit = GenerationStamp.GRANDFATHER_GENERATION_STAMP;
  }
}