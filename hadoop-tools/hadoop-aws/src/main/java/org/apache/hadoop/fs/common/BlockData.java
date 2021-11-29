/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.common;

/**
 * Holds information about blocks of data in an S3 file.
 */
public class BlockData {
  // State of each block of data.
  enum State {
    // Data is not yet ready to be read.
    NOT_READY,

    // A read of this block has been queued.
    QUEUED,

    // This block is ready to be read.
    READY,

    // This block has been cached.
    CACHED
  }

  // State of all blocks in an S3 file.
  private State[] state;

  // The size of an S3 file.
  public final long fileSize;

  // The S3 file is divided into blocks of this size.
  public final int blockSize;

  // The S3 file has these many blocks.
  public final int numBlocks;

  /**
   * Constructs an instance of {@link BlockData}.
   *
   * @param fileSize the size of an S3 file.
   * @param blockSize the S3 file is divided into blocks of this size.
   */
  public BlockData(long fileSize, int blockSize) {
    Validate.checkNotNegative(fileSize, "fileSize");
    if (fileSize == 0) {
      Validate.checkNotNegative(blockSize, "blockSize");
    } else {
      Validate.checkPositiveInteger(blockSize, "blockSize");
    }

    this.fileSize = fileSize;
    this.blockSize = blockSize;
    this.numBlocks =
        (fileSize == 0) ? 0 : ((int) (fileSize / blockSize)) + (fileSize % blockSize > 0 ? 1 : 0);
    this.state = new State[this.numBlocks];
    for (int b = 0; b < this.numBlocks; b++) {
      this.setState(b, State.NOT_READY);
    }
  }

  public boolean isLastBlock(int blockNumber) {
    if (this.fileSize == 0) {
      return false;
    }

    throwIfInvalidBlockNumber(blockNumber);

    return blockNumber == (this.numBlocks - 1);
  }

  public int getBlockNumber(long offset) {
    throwIfInvalidOffset(offset);

    return (int) (offset / this.blockSize);
  }

  public int getSize(int blockNumber) {
    if (this.fileSize == 0) {
      return 0;
    }

    if (this.isLastBlock(blockNumber)) {
      return (int) (this.fileSize - (this.blockSize * (this.numBlocks - 1)));
    } else {
      return this.blockSize;
    }
  }

  public boolean isValidOffset(long offset) {
    return (offset >= 0) && (offset < this.fileSize);
  }

  public long getStartOffset(int blockNumber) {
    throwIfInvalidBlockNumber(blockNumber);

    return blockNumber * (long) this.blockSize;
  }

  public int getRelativeOffset(int blockNumber, long offset) {
    throwIfInvalidOffset(offset);

    return (int) (offset - this.getStartOffset(blockNumber));
  }

  public State getState(int blockNumber) {
    throwIfInvalidBlockNumber(blockNumber);

    return this.state[blockNumber];
  }

  public State setState(int blockNumber, State blockState) {
    throwIfInvalidBlockNumber(blockNumber);

    return this.state[blockNumber] = blockState;
  }

  // Debug helper.
  public String getStateString() {
    StringBuilder sb = new StringBuilder();
    int blockNumber = 0;
    while (blockNumber < this.numBlocks) {
      State state = this.getState(blockNumber);
      int endBlockNumber = blockNumber;
      while ((endBlockNumber < this.numBlocks) && (this.getState(endBlockNumber) == state)) {
        endBlockNumber++;
      }
      sb.append(String.format("[%03d ~ %03d] %s\n", blockNumber, endBlockNumber - 1, state));
      blockNumber = endBlockNumber;
    }
    return sb.toString();
  }

  private void throwIfInvalidBlockNumber(int blockNumber) {
    Validate.checkWithinRange(blockNumber, "blockNumber", 0, this.numBlocks - 1);
  }

  private void throwIfInvalidOffset(long offset) {
    Validate.checkWithinRange(offset, "offset", 0, this.fileSize - 1);
  }
}
