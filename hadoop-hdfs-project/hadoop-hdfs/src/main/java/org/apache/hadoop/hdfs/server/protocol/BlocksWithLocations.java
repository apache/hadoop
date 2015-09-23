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
package org.apache.hadoop.hdfs.server.protocol;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;

/**
 * Maintains an array of blocks and their corresponding storage IDs.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlocksWithLocations {

  /**
   * A class to keep track of a block and its locations
   */
  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  public static class BlockWithLocations {
    final Block block;
    final String[] datanodeUuids;
    final String[] storageIDs;
    final StorageType[] storageTypes;
    
    /** constructor */
    public BlockWithLocations(Block block, String[] datanodeUuids,
        String[] storageIDs, StorageType[] storageTypes) {
      this.block = block;
      this.datanodeUuids = datanodeUuids;
      this.storageIDs = storageIDs;
      this.storageTypes = storageTypes;
    }
    
    /** get the block */
    public Block getBlock() {
      return block;
    }
    
    /** get the block's datanode locations */
    public String[] getDatanodeUuids() {
      return datanodeUuids;
    }

    /** get the block's storage locations */
    public String[] getStorageIDs() {
      return storageIDs;
    }

    /** @return the storage types */
    public StorageType[] getStorageTypes() {
      return storageTypes;
    }

    @Override
    public String toString() {
      final StringBuilder b = new StringBuilder();
      b.append(block);
      if (datanodeUuids.length == 0) {
        return b.append("[]").toString();
      }
      
      appendString(0, b.append("["));
      for(int i = 1; i < datanodeUuids.length; i++) {
        appendString(i, b.append(","));
      }
      return b.append("]").toString();
    }
    
    private StringBuilder appendString(int i, StringBuilder b) {
      return b.append("[").append(storageTypes[i]).append("]")
              .append(storageIDs[i])
              .append("@").append(datanodeUuids[i]);
    }
  }

  public static class StripedBlockWithLocations extends BlockWithLocations {
    final byte[] indices;
    final short dataBlockNum;
    final int cellSize;

    public StripedBlockWithLocations(BlockWithLocations blk, byte[] indices,
         short dataBlockNum, int cellSize) {
      super(blk.getBlock(), blk.getDatanodeUuids(), blk.getStorageIDs(),
          blk.getStorageTypes());
      Preconditions.checkArgument(
          blk.getDatanodeUuids().length == indices.length);
      this.indices = indices;
      this.dataBlockNum = dataBlockNum;
      this.cellSize = cellSize;
    }

    public byte[] getIndices() {
      return indices;
    }

    public short getDataBlockNum() {
      return dataBlockNum;
    }

    public int getCellSize() {
      return cellSize;
    }
  }

  private final BlockWithLocations[] blocks;

  /** Constructor with one parameter */
  public BlocksWithLocations(BlockWithLocations[] blocks) {
    this.blocks = blocks;
  }

  /** getter */
  public BlockWithLocations[] getBlocks() {
    return blocks;
  }
}
