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
   * append array of blocks to this.blocks
   */
  void appendBlocks(INodeFile [] inodes, int totalAddedBlocks) {
    int size = this.blocks.length;
    
    BlockInfo[] newlist = new BlockInfo[size + totalAddedBlocks];
    System.arraycopy(this.blocks, 0, newlist, 0, size);
    
    for(INodeFile in: inodes) {
      System.arraycopy(in.blocks, 0, newlist, size, in.blocks.length);
      size += in.blocks.length;
    }
    
    for(BlockInfo bi: this.blocks) {
      bi.setINode(this);
    }
    this.blocks = newlist;
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
    if(blocks != null && v != null) {
      for (BlockInfo blk : blocks) {
        v.add(blk);
        blk.setINode(null);
      }
    }
    blocks = null;
    return 1;
  }

  /** {@inheritDoc} */
  long[] computeContentSummary(long[] summary) {
    summary[0] += computeFileSize(true);
    summary[1]++;
    summary[3] += diskspaceConsumed();
    return summary;
  }

  /** Compute file size.
   * May or may not include BlockInfoUnderConstruction.
   */
  long computeFileSize(boolean includesBlockInfoUnderConstruction) {
    if (blocks == null || blocks.length == 0) {
      return 0;
    }
    final int last = blocks.length - 1;
    //check if the last block is BlockInfoUnderConstruction
    long bytes = blocks[last] instanceof BlockInfoUnderConstruction
                 && !includesBlockInfoUnderConstruction?
                     0: blocks[last].getNumBytes();
    for(int i = 0; i < last; i++) {
      bytes += blocks[i].getNumBytes();
    }
    return bytes;
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
    if(blkArr == null) 
      return 0;
    
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
      size += preferredBlockSize - blkArr[blkArr.length-1].getNumBytes();
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
  BlockInfo getPenultimateBlock() {
    if (blocks == null || blocks.length <= 1) {
      return null;
    }
    return blocks[blocks.length - 2];
  }

  /**
   * Get the last block of the file.
   * Make sure it has the right type.
   */
  <T extends BlockInfo> T getLastBlock() throws IOException {
    if (blocks == null || blocks.length == 0)
      return null;
    T returnBlock = null;
    try {
      @SuppressWarnings("unchecked")  // ClassCastException is caught below
      T tBlock = (T)blocks[blocks.length - 1];
      returnBlock = tBlock;
    } catch(ClassCastException cce) {
      throw new IOException("Unexpected last block type: " 
          + blocks[blocks.length - 1].getClass().getSimpleName());
    }
    return returnBlock;
  }

  int numBlocks() {
    return blocks == null ? 0 : blocks.length;
  }
}
