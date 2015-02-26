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
package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;

/**
 * Feature for file with striped blocks
 */
class FileWithStripedBlocksFeature implements INode.Feature {
  private BlockInfoStriped[] blocks;

  FileWithStripedBlocksFeature() {
    blocks = new BlockInfoStriped[0];
  }

  FileWithStripedBlocksFeature(BlockInfoStriped[] blocks) {
    Preconditions.checkArgument(blocks != null);
    this.blocks = blocks;
  }

  BlockInfoStriped[] getBlocks() {
    return this.blocks;
  }

  void setBlock(int index, BlockInfoStriped blk) {
    blocks[index] = blk;
  }

  BlockInfoStriped getLastBlock() {
    return blocks == null || blocks.length == 0 ?
        null : blocks[blocks.length - 1];
  }

  int numBlocks() {
    return blocks == null ? 0 : blocks.length;
  }

  void updateBlockCollection(INodeFile file) {
    if (blocks != null) {
      for (BlockInfoStriped blk : blocks) {
        blk.setBlockCollection(file);
      }
    }
  }

  private void setBlocks(BlockInfoStriped[] blocks) {
    this.blocks = blocks;
  }

  void addBlock(BlockInfoStriped newBlock) {
    if (this.blocks == null) {
      this.setBlocks(new BlockInfoStriped[]{newBlock});
    } else {
      int size = this.blocks.length;
      BlockInfoStriped[] newlist = new BlockInfoStriped[size + 1];
      System.arraycopy(this.blocks, 0, newlist, 0, size);
      newlist[size] = newBlock;
      this.setBlocks(newlist);
    }
  }

  boolean removeLastBlock(Block oldblock) {
    if (blocks == null || blocks.length == 0) {
      return false;
    }
    int newSize = blocks.length - 1;
    if (!blocks[newSize].equals(oldblock)) {
      return false;
    }

    //copy to a new list
    BlockInfoStriped[] newlist = new BlockInfoStriped[newSize];
    System.arraycopy(blocks, 0, newlist, 0, newSize);
    setBlocks(newlist);
    return true;
  }

  void truncateStripedBlocks(int n) {
    final BlockInfoStriped[] newBlocks;
    if (n == 0) {
      newBlocks = new BlockInfoStriped[0];
    } else {
      newBlocks = new BlockInfoStriped[n];
      System.arraycopy(getBlocks(), 0, newBlocks, 0, n);
    }
    // set new blocks
    setBlocks(newBlocks);
  }

  void clear() {
    this.blocks = null;
  }
}
