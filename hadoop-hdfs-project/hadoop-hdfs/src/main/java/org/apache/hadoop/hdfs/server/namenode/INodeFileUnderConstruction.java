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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.MutableBlockCollection;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeFileUnderConstructionWithLink;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeFileWithLink;

import com.google.common.base.Preconditions;

/**
 * I-node for file being written.
 */
@InterfaceAudience.Private
public class INodeFileUnderConstruction extends INodeFile implements MutableBlockCollection {
  /** Cast INode to INodeFileUnderConstruction. */
  public static INodeFileUnderConstruction valueOf(INode inode, String path
      ) throws FileNotFoundException {
    final INodeFile file = INodeFile.valueOf(inode, path);
    if (!file.isUnderConstruction()) {
      throw new FileNotFoundException("File is not under construction: " + path);
    }
    return (INodeFileUnderConstruction)file;
  }

  /** Convert the given file to an {@link INodeFileUnderConstruction}. */
  public static INodeFileUnderConstruction toINodeFileUnderConstruction(
      INodeFile file,
      String clientName,
      String clientMachine,
      DatanodeDescriptor clientNode) {
    Preconditions.checkArgument(!(file instanceof INodeFileUnderConstruction),
        "file is already an INodeFileUnderConstruction");
    final INodeFileUnderConstruction uc = new INodeFileUnderConstruction(
        file.getId(),
        file.getLocalNameBytes(),
        file.getFileReplication(),
        file.getModificationTime(),
        file.getPreferredBlockSize(),
        file.getBlocks(),
        file.getPermissionStatus(),
        clientName,
        clientMachine,
        clientNode);
    return file instanceof INodeFileWithLink?
        new INodeFileUnderConstructionWithLink(uc): uc;
  }

  private  String clientName;         // lease holder
  private final String clientMachine;
  private final DatanodeDescriptor clientNode; // if client is a cluster node too.
  
  INodeFileUnderConstruction(long id,
                             PermissionStatus permissions,
                             short replication,
                             long preferredBlockSize,
                             long modTime,
                             String clientName,
                             String clientMachine,
                             DatanodeDescriptor clientNode) {
    this(id, null, replication, modTime, preferredBlockSize, BlockInfo.EMPTY_ARRAY,
        permissions.applyUMask(UMASK), clientName, clientMachine, clientNode);
  }

  INodeFileUnderConstruction(long id,
                             byte[] name,
                             short blockReplication,
                             long modificationTime,
                             long preferredBlockSize,
                             BlockInfo[] blocks,
                             PermissionStatus perm,
                             String clientName,
                             String clientMachine,
                             DatanodeDescriptor clientNode) {
    super(id, name, perm, modificationTime, modificationTime,
        blocks, blockReplication, preferredBlockSize);
    this.clientName = clientName;
    this.clientMachine = clientMachine;
    this.clientNode = clientNode;
  }
 
  protected INodeFileUnderConstruction(INodeFileUnderConstruction that) {
    super(that);
    this.clientName = that.clientName;
    this.clientMachine = that.clientMachine;
    this.clientNode = that.clientNode;
  }

  String getClientName() {
    return clientName;
  }

  void setClientName(String clientName) {
    this.clientName = clientName;
  }

  String getClientMachine() {
    return clientMachine;
  }

  DatanodeDescriptor getClientNode() {
    return clientNode;
  }

  /**
   * Is this inode being constructed?
   */
  @Override
  public boolean isUnderConstruction() {
    return true;
  }

  /**
   * Converts an INodeFileUnderConstruction to an INodeFile.
   * The original modification time is used as the access time.
   * The new modification is the specified mtime.
   */
  protected INodeFile toINodeFile(long mtime) {
    assertAllBlocksComplete();
    return new INodeFile(getId(), getLocalNameBytes(), getPermissionStatus(),
        mtime, getModificationTime(),
        getBlocks(), getFileReplication(), getPreferredBlockSize());
  }
  
  /** Assert all blocks are complete. */
  protected void assertAllBlocksComplete() {
    final BlockInfo[] blocks = getBlocks();
    for (int i = 0; i < blocks.length; i++) {
      Preconditions.checkState(blocks[i].isComplete(), "Failed to finalize"
          + " %s %s since blocks[%s] is non-complete, where blocks=%s.",
          getClass().getSimpleName(), this, i, Arrays.asList(getBlocks()));
    }
  }

  /**
   * Remove a block from the block list. This block should be
   * the last one on the list.
   */
  void removeLastBlock(Block oldblock) throws IOException {
    final BlockInfo[] blocks = getBlocks();
    if (blocks == null) {
      throw new IOException("Trying to delete non-existant block " + oldblock);
    }
    int size_1 = blocks.length - 1;
    if (!blocks[size_1].equals(oldblock)) {
      throw new IOException("Trying to delete non-last block " + oldblock);
    }

    //copy to a new list
    BlockInfo[] newlist = new BlockInfo[size_1];
    System.arraycopy(blocks, 0, newlist, 0, size_1);
    setBlocks(newlist);
  }

  /**
   * Convert the last block of the file to an under-construction block.
   * Set its locations.
   */
  @Override
  public BlockInfoUnderConstruction setLastBlock(BlockInfo lastBlock,
      DatanodeDescriptor[] targets) throws IOException {
    if (numBlocks() == 0) {
      throw new IOException("Failed to set last block: File is empty.");
    }
    BlockInfoUnderConstruction ucBlock =
      lastBlock.convertToBlockUnderConstruction(
          BlockUCState.UNDER_CONSTRUCTION, targets);
    ucBlock.setBlockCollection(this);
    setBlock(numBlocks()-1, ucBlock);
    return ucBlock;
  }

  /**
   * Update the length for the last block
   * 
   * @param lastBlockLength
   *          The length of the last block reported from client
   * @throws IOException
   */
  void updateLengthOfLastBlock(long lastBlockLength) throws IOException {
    BlockInfo lastBlock = this.getLastBlock();
    assert (lastBlock != null) : "The last block for path "
        + this.getFullPathName() + " is null when updating its length";
    assert (lastBlock instanceof BlockInfoUnderConstruction) : "The last block for path "
        + this.getFullPathName()
        + " is not a BlockInfoUnderConstruction when updating its length";
    lastBlock.setNumBytes(lastBlockLength);
  }
  
}
