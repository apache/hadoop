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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;

class INodeFile extends INode {
  static final FsPermission UMASK = FsPermission.createImmutable((short)0111);

  protected BlockInfo blocks[] = null;
  protected short blockReplication;
  protected long preferredBlockSize;

  INodeFile(PermissionStatus permissions,
            int nrBlocks, short replication, long modificationTime,
            long atime, long preferredBlockSize) {
    this(permissions, new BlockInfo[nrBlocks], replication,
        modificationTime, atime, preferredBlockSize);
  }

  protected INodeFile() {
    blocks = null;
    blockReplication = 0;
    preferredBlockSize = 0;
  }

  protected INodeFile(PermissionStatus permissions, BlockInfo[] blklist,
                      short replication, long modificationTime,
                      long atime, long preferredBlockSize) {
    super(permissions, modificationTime, atime);
    this.blockReplication = replication;
    this.preferredBlockSize = preferredBlockSize;
    blocks = blklist;
  }

  /**
   * Set the {@link FsPermission} of this {@link INodeFile}.
   * Since this is a file,
   * the {@link FsAction#EXECUTE} action, if any, is ignored.
   */
  protected void setPermission(FsPermission permission) {
    super.setPermission(permission.applyUMask(UMASK));
  }

  public boolean isDirectory() {
    return false;
  }

  /**
   * Get block replication for the file 
   * @return block replication
   */
  public short getReplication() {
    return this.blockReplication;
  }

  void setReplication(short replication) {
    this.blockReplication = replication;
  }

  /**
   * Get file blocks 
   * @return file blocks
   */
  BlockInfo[] getBlocks() {
    return this.blocks;
  }

  /**
   * add a block to the block list
   */
  void addBlock(BlockInfo newblock) {
    if (this.blocks == null) {
      this.blocks = new BlockInfo[1];
      this.blocks[0] = newblock;
    } else {
      int size = this.blocks.length;
      BlockInfo[] newlist = new BlockInfo[size + 1];
      System.arraycopy(this.blocks, 0, newlist, 0, size);
      newlist[size] = newblock;
      this.blocks = newlist;
    }
  }

  /**
   * Set file block
   */
  void setBlock(int idx, BlockInfo blk) {
    this.blocks[idx] = blk;
  }

  int collectSubtreeBlocksAndClear(List<Block> v) {
    parent = null;
    for (Block blk : blocks) {
      v.add(blk);
    }
    blocks = null;
    return 1;
  }

  /** {@inheritDoc} */
  long[] computeContentSummary(long[] summary) {
    long bytes = 0;
    for(Block blk : blocks) {
      bytes += blk.getNumBytes();
    }
    summary[0] += bytes;
    summary[1]++;
    summary[3] += diskspaceConsumed();
    return summary;
  }

  

  @Override
  DirCounts spaceConsumedInTree(DirCounts counts) {
    counts.nsCount += 1;
    counts.dsCount += diskspaceConsumed();
    return counts;
  }

  long diskspaceConsumed() {
    return diskspaceConsumed(blocks);
  }
  
  long diskspaceConsumed(Block[] blkArr) {
    long size = 0;
    for (Block blk : blkArr) {
      if (blk != null) {
        size += blk.getNumBytes();
      }
    }
    /* If the last block is being written to, use prefferedBlockSize
     * rather than the actual block size.
     */
    if (blkArr.length > 0 && blkArr[blkArr.length-1] != null && 
        isUnderConstruction()) {
      size += preferredBlockSize - blocks[blocks.length-1].getNumBytes();
    }
    return size * blockReplication;
  }
  
  /**
   * Get the preferred block size of the file.
   * @return the number of bytes
   */
  public long getPreferredBlockSize() {
    return preferredBlockSize;
  }

  /**
   * Return the penultimate allocated block for this file.
   */
  Block getPenultimateBlock() {
    if (blocks == null || blocks.length <= 1) {
      return null;
    }
    return blocks[blocks.length - 2];
  }

  INodeFileUnderConstruction toINodeFileUnderConstruction(
      String clientName, String clientMachine, DatanodeDescriptor clientNode
      ) throws IOException {
    if (isUnderConstruction()) {
      return (INodeFileUnderConstruction)this;
    }
    return new INodeFileUnderConstruction(name,
        blockReplication, modificationTime, preferredBlockSize,
        blocks, getPermissionStatus(),
        clientName, clientMachine, clientNode);
  }
}
