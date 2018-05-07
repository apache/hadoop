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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Verify that TestCommitBlockSynchronization is idempotent.
 */
public class TestCommitBlockSynchronization {
  private static final long blockId = 100;
  private static final long length = 200;
  private static final long genStamp = 300;

  private FSNamesystem makeNameSystemSpy(Block block, INodeFile file)
      throws IOException {
    Configuration conf = new Configuration();
    FSImage image = new FSImage(conf);
    final DatanodeStorageInfo[] targets = {};

    FSNamesystem namesystem = new FSNamesystem(conf, image);
    namesystem.setImageLoaded(true);

    // set file's parent as root and put the file to inodeMap, so
    // FSNamesystem's isFileDeleted() method will return false on this file
    if (file.getParent() == null) {
      INodeDirectory mparent = mock(INodeDirectory.class);
      INodeDirectory parent = new INodeDirectory(mparent.getId(), new byte[0],
          mparent.getPermissionStatus(), mparent.getAccessTime());
      parent.setLocalName(new byte[0]);
      parent.addChild(file);
      file.setParent(parent);
    }
    namesystem.dir.getINodeMap().put(file);

    FSNamesystem namesystemSpy = spy(namesystem);
    BlockInfoContiguousUnderConstruction blockInfo = new BlockInfoContiguousUnderConstruction(
        block, (short) 1, HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION, targets);
    blockInfo.setBlockCollection(file);
    blockInfo.setGenerationStamp(genStamp);
    blockInfo.initializeBlockRecovery(genStamp, true);
    doReturn(true).when(file).removeLastBlock(any(Block.class));
    doReturn(true).when(file).isUnderConstruction();
    doReturn(new BlockInfoContiguous[1]).when(file).getBlocks();

    doReturn(blockInfo).when(namesystemSpy).getStoredBlock(any(Block.class));
    doReturn(blockInfo).when(file).getLastBlock();
    doNothing().when(namesystemSpy).closeFileCommitBlocks(
        any(String.class), any(INodeFile.class), any(BlockInfoContiguous.class));
    doReturn(mock(FSEditLog.class)).when(namesystemSpy).getEditLog();

    return namesystemSpy;
  }

  private INodeFile mockFileUnderConstruction() {
    INodeFile file = mock(INodeFile.class);
    return file;
  }

  @Test
  public void testCommitBlockSynchronization() throws IOException {
    INodeFile file = mockFileUnderConstruction();
    Block block = new Block(blockId, length, genStamp);
    FSNamesystem namesystemSpy = makeNameSystemSpy(block, file);
    DatanodeID[] newTargets = new DatanodeID[0];

    ExtendedBlock lastBlock = new ExtendedBlock();
    namesystemSpy.commitBlockSynchronization(
        lastBlock, genStamp, length, false,
        false, newTargets, null);

    // Repeat the call to make sure it does not throw
    namesystemSpy.commitBlockSynchronization(
        lastBlock, genStamp, length, false, false, newTargets, null);

    // Simulate 'completing' the block.
    BlockInfoContiguous completedBlockInfo = new BlockInfoContiguous(block, (short) 1);
    completedBlockInfo.setBlockCollection(file);
    completedBlockInfo.setGenerationStamp(genStamp);
    doReturn(completedBlockInfo).when(namesystemSpy)
        .getStoredBlock(any(Block.class));
    doReturn(completedBlockInfo).when(file).getLastBlock();

    // Repeat the call to make sure it does not throw
    namesystemSpy.commitBlockSynchronization(
        lastBlock, genStamp, length, false, false, newTargets, null);
  }

  @Test
  public void testCommitBlockSynchronization2() throws IOException {
    INodeFile file = mockFileUnderConstruction();
    Block block = new Block(blockId, length, genStamp);
    FSNamesystem namesystemSpy = makeNameSystemSpy(block, file);
    DatanodeID[] newTargets = new DatanodeID[0];

    ExtendedBlock lastBlock = new ExtendedBlock();
    namesystemSpy.commitBlockSynchronization(
        lastBlock, genStamp, length, false,
        false, newTargets, null);

    // Make sure the call fails if the generation stamp does not match
    // the block recovery ID.
    try {
      namesystemSpy.commitBlockSynchronization(
          lastBlock, genStamp - 1, length, false, false, newTargets, null);
      fail("Failed to get expected IOException on generation stamp/" +
           "recovery ID mismatch");
    } catch (IOException ioe) {
      // Expected exception.
    }
  }

  @Test
  public void testCommitBlockSynchronizationWithDelete() throws IOException {
    INodeFile file = mockFileUnderConstruction();
    Block block = new Block(blockId, length, genStamp);
    FSNamesystem namesystemSpy = makeNameSystemSpy(block, file);
    DatanodeID[] newTargets = new DatanodeID[0];

    ExtendedBlock lastBlock = new ExtendedBlock();
      namesystemSpy.commitBlockSynchronization(
          lastBlock, genStamp, length, false,
          true, newTargets, null);

    // Simulate removing the last block from the file.
    doReturn(false).when(file).removeLastBlock(any(Block.class));

    // Repeat the call to make sure it does not throw
    namesystemSpy.commitBlockSynchronization(
        lastBlock, genStamp, length, false, true, newTargets, null);
  }

  @Test
  public void testCommitBlockSynchronizationWithClose() throws IOException {
    INodeFile file = mockFileUnderConstruction();
    Block block = new Block(blockId, length, genStamp);
    FSNamesystem namesystemSpy = makeNameSystemSpy(block, file);
    DatanodeID[] newTargets = new DatanodeID[0];

    ExtendedBlock lastBlock = new ExtendedBlock();
      namesystemSpy.commitBlockSynchronization(
          lastBlock, genStamp, length, true,
          false, newTargets, null);

    // Repeat the call to make sure it returns true
    namesystemSpy.commitBlockSynchronization(
        lastBlock, genStamp, length, true, false, newTargets, null);

    BlockInfoContiguous completedBlockInfo = new BlockInfoContiguous(block, (short) 1);
    completedBlockInfo.setBlockCollection(file);
    completedBlockInfo.setGenerationStamp(genStamp);
    doReturn(completedBlockInfo).when(namesystemSpy)
        .getStoredBlock(any(Block.class));
    doReturn(completedBlockInfo).when(file).getLastBlock();

    namesystemSpy.commitBlockSynchronization(
        lastBlock, genStamp, length, true, false, newTargets, null);
  }

  @Test
  public void testCommitBlockSynchronizationWithCloseAndNonExistantTarget()
      throws IOException {
    INodeFile file = mockFileUnderConstruction();
    Block block = new Block(blockId, length, genStamp);
    FSNamesystem namesystemSpy = makeNameSystemSpy(block, file);
    DatanodeID[] newTargets = new DatanodeID[]{
        new DatanodeID("0.0.0.0", "nonexistantHost", "1", 0, 0, 0, 0)};

    ExtendedBlock lastBlock = new ExtendedBlock();
    namesystemSpy.commitBlockSynchronization(
        lastBlock, genStamp, length, true,
        false, newTargets, null);

    // Repeat the call to make sure it returns true
    namesystemSpy.commitBlockSynchronization(
        lastBlock, genStamp, length, true, false, newTargets, null);
  }
}
