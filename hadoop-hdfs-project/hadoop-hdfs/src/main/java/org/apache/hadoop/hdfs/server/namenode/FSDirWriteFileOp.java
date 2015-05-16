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
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class FSDirWriteFileOp {
  private FSDirWriteFileOp() {}
  static boolean unprotectedRemoveBlock(
      FSDirectory fsd, String path, INodesInPath iip, INodeFile fileNode,
      Block block) throws IOException {
    // modify file-> block and blocksMap
    // fileNode should be under construction
    BlockInfoContiguousUnderConstruction uc = fileNode.removeLastBlock(block);
    if (uc == null) {
      return false;
    }
    fsd.getBlockManager().removeBlockFromMap(block);

    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.removeBlock: "
          +path+" with "+block
          +" block is removed from the file system");
    }

    // update space consumed
    fsd.updateCount(iip, 0, -fileNode.getPreferredBlockSize(),
                    fileNode.getPreferredBlockReplication(), true);
    return true;
  }

  /**
   * Persist the block list for the inode.
   */
  static void persistBlocks(
      FSDirectory fsd, String path, INodeFile file, boolean logRetryCache) {
    assert fsd.getFSNamesystem().hasWriteLock();
    Preconditions.checkArgument(file.isUnderConstruction());
    fsd.getEditLog().logUpdateBlocks(path, file, logRetryCache);
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("persistBlocks: " + path
              + " with " + file.getBlocks().length + " blocks is persisted to" +
              " the file system");
    }
  }

  static void abandonBlock(
      FSDirectory fsd, FSPermissionChecker pc, ExtendedBlock b, long fileId,
      String src, String holder) throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    src = fsd.resolvePath(pc, src, pathComponents);

    final INode inode;
    final INodesInPath iip;
    if (fileId == HdfsConstants.GRANDFATHER_INODE_ID) {
      // Older clients may not have given us an inode ID to work with.
      // In this case, we have to try to resolve the path and hope it
      // hasn't changed or been deleted since the file was opened for write.
      iip = fsd.getINodesInPath(src, true);
      inode = iip.getLastINode();
    } else {
      inode = fsd.getInode(fileId);
      iip = INodesInPath.fromINode(inode);
      if (inode != null) {
        src = iip.getPath();
      }
    }
    FSNamesystem fsn = fsd.getFSNamesystem();
    final INodeFile file = fsn.checkLease(src, holder, inode, fileId);
    Preconditions.checkState(file.isUnderConstruction());

    Block localBlock = ExtendedBlock.getLocalBlock(b);
    fsd.writeLock();
    try {
      // Remove the block from the pending creates list
      if (!unprotectedRemoveBlock(fsd, src, iip, file, localBlock)) {
        return;
      }
    } finally {
      fsd.writeUnlock();
    }
    persistBlocks(fsd, src, file, false);
  }

  static void checkBlock(FSNamesystem fsn, ExtendedBlock block)
      throws IOException {
    String bpId = fsn.getBlockPoolId();
    if (block != null && !bpId.equals(block.getBlockPoolId())) {
      throw new IOException("Unexpected BlockPoolId " + block.getBlockPoolId()
          + " - expected " + bpId);
    }
  }

  /**
   * Part I of getAdditionalBlock().
   * Analyze the state of the file under read lock to determine if the client
   * can add a new block, detect potential retries, lease mismatches,
   * and minimal replication of the penultimate block.
   *
   * Generate target DataNode locations for the new block,
   * but do not create the new block yet.
   */
  static ValidateAddBlockResult validateAddBlock(
      FSNamesystem fsn, FSPermissionChecker pc,
      String src, long fileId, String clientName,
      ExtendedBlock previous, LocatedBlock[] onRetryBlock) throws IOException {
    final long blockSize;
    final int replication;
    final byte storagePolicyID;
    String clientMachine;

    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    src = fsn.dir.resolvePath(pc, src, pathComponents);
    FileState fileState = analyzeFileState(fsn, src, fileId, clientName,
                                           previous, onRetryBlock);
    final INodeFile pendingFile = fileState.inode;
    // Check if the penultimate block is minimally replicated
    if (!fsn.checkFileProgress(src, pendingFile, false)) {
      throw new NotReplicatedYetException("Not replicated yet: " + src);
    }

    if (onRetryBlock[0] != null && onRetryBlock[0].getLocations().length > 0) {
      // This is a retry. No need to generate new locations.
      // Use the last block if it has locations.
      return null;
    }
    if (pendingFile.getBlocks().length >= fsn.maxBlocksPerFile) {
      throw new IOException("File has reached the limit on maximum number of"
          + " blocks (" + DFSConfigKeys.DFS_NAMENODE_MAX_BLOCKS_PER_FILE_KEY
          + "): " + pendingFile.getBlocks().length + " >= "
          + fsn.maxBlocksPerFile);
    }
    blockSize = pendingFile.getPreferredBlockSize();
    clientMachine = pendingFile.getFileUnderConstructionFeature()
        .getClientMachine();
    replication = pendingFile.getFileReplication();
    storagePolicyID = pendingFile.getStoragePolicyID();
    return new ValidateAddBlockResult(blockSize, replication, storagePolicyID,
                                    clientMachine);
  }

  static LocatedBlock makeLocatedBlock(FSNamesystem fsn, Block blk,
      DatanodeStorageInfo[] locs, long offset) throws IOException {
    LocatedBlock lBlk = BlockManager.newLocatedBlock(fsn.getExtendedBlock(blk),
                                                     locs, offset, false);
    fsn.getBlockManager().setBlockToken(lBlk,
                                        BlockTokenIdentifier.AccessMode.WRITE);
    return lBlk;
  }

  /**
   * Part II of getAdditionalBlock().
   * Should repeat the same analysis of the file state as in Part 1,
   * but under the write lock.
   * If the conditions still hold, then allocate a new block with
   * the new targets, add it to the INode and to the BlocksMap.
   */
  static LocatedBlock storeAllocatedBlock(FSNamesystem fsn, String src,
      long fileId, String clientName, ExtendedBlock previous,
      DatanodeStorageInfo[] targets) throws IOException {
    long offset;
    // Run the full analysis again, since things could have changed
    // while chooseTarget() was executing.
    LocatedBlock[] onRetryBlock = new LocatedBlock[1];
    FileState fileState = analyzeFileState(fsn, src, fileId, clientName,
                                           previous, onRetryBlock);
    final INodeFile pendingFile = fileState.inode;
    src = fileState.path;

    if (onRetryBlock[0] != null) {
      if (onRetryBlock[0].getLocations().length > 0) {
        // This is a retry. Just return the last block if having locations.
        return onRetryBlock[0];
      } else {
        // add new chosen targets to already allocated block and return
        BlockInfoContiguous lastBlockInFile = pendingFile.getLastBlock();
        ((BlockInfoContiguousUnderConstruction) lastBlockInFile)
            .setExpectedLocations(targets);
        offset = pendingFile.computeFileSize();
        return makeLocatedBlock(fsn, lastBlockInFile, targets, offset);
      }
    }

    // commit the last block and complete it if it has minimum replicas
    fsn.commitOrCompleteLastBlock(pendingFile, fileState.iip,
                                  ExtendedBlock.getLocalBlock(previous));

    // allocate new block, record block locations in INode.
    Block newBlock = fsn.createNewBlock();
    INodesInPath inodesInPath = INodesInPath.fromINode(pendingFile);
    saveAllocatedBlock(fsn, src, inodesInPath, newBlock, targets);

    persistNewBlock(fsn, src, pendingFile);
    offset = pendingFile.computeFileSize();

    // Return located block
    return makeLocatedBlock(fsn, newBlock, targets, offset);
  }

  static DatanodeStorageInfo[] chooseTargetForNewBlock(
      BlockManager bm, String src, DatanodeInfo[] excludedNodes, String[]
      favoredNodes, ValidateAddBlockResult r) throws IOException {
    Node clientNode = bm.getDatanodeManager()
        .getDatanodeByHost(r.clientMachine);
    if (clientNode == null) {
      clientNode = getClientNode(bm, r.clientMachine);
    }

    Set<Node> excludedNodesSet = null;
    if (excludedNodes != null) {
      excludedNodesSet = new HashSet<>(excludedNodes.length);
      Collections.addAll(excludedNodesSet, excludedNodes);
    }
    List<String> favoredNodesList = (favoredNodes == null) ? null
        : Arrays.asList(favoredNodes);

    // choose targets for the new block to be allocated.
    return bm.chooseTarget4NewBlock(src, r.replication, clientNode,
                                    excludedNodesSet, r.blockSize,
                                    favoredNodesList, r.storagePolicyID);
  }

  /**
   * Resolve clientmachine address to get a network location path
   */
  static Node getClientNode(BlockManager bm, String clientMachine) {
    List<String> hosts = new ArrayList<>(1);
    hosts.add(clientMachine);
    List<String> rName = bm.getDatanodeManager()
        .resolveNetworkLocation(hosts);
    Node clientNode = null;
    if (rName != null) {
      // Able to resolve clientMachine mapping.
      // Create a temp node to findout the rack local nodes
      clientNode = new NodeBase(rName.get(0) + NodeBase.PATH_SEPARATOR_STR
          + clientMachine);
    }
    return clientNode;
  }

  /**
   * Add a block to the file. Returns a reference to the added block.
   */
  private static BlockInfoContiguous addBlock(
      FSDirectory fsd, String path, INodesInPath inodesInPath, Block block,
      DatanodeStorageInfo[] targets) throws IOException {
    fsd.writeLock();
    try {
      final INodeFile fileINode = inodesInPath.getLastINode().asFile();
      Preconditions.checkState(fileINode.isUnderConstruction());

      // check quota limits and updated space consumed
      fsd.updateCount(inodesInPath, 0, fileINode.getPreferredBlockSize(),
          fileINode.getPreferredBlockReplication(), true);

      // associate new last block for the file
      BlockInfoContiguousUnderConstruction blockInfo =
        new BlockInfoContiguousUnderConstruction(
            block,
            fileINode.getFileReplication(),
            HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION,
            targets);
      fsd.getBlockManager().addBlockCollection(blockInfo, fileINode);
      fileINode.addBlock(blockInfo);

      if(NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* FSDirectory.addBlock: "
            + path + " with " + block
            + " block is added to the in-memory "
            + "file system");
      }
      return blockInfo;
    } finally {
      fsd.writeUnlock();
    }
  }

  private static FileState analyzeFileState(
      FSNamesystem fsn, String src, long fileId, String clientName,
      ExtendedBlock previous, LocatedBlock[] onRetryBlock)
          throws IOException  {
    assert fsn.hasReadLock();

    checkBlock(fsn, previous);
    onRetryBlock[0] = null;
    fsn.checkNameNodeSafeMode("Cannot add block to " + src);

    // have we exceeded the configured limit of fs objects.
    fsn.checkFsObjectLimit();

    Block previousBlock = ExtendedBlock.getLocalBlock(previous);
    final INode inode;
    final INodesInPath iip;
    if (fileId == HdfsConstants.GRANDFATHER_INODE_ID) {
      // Older clients may not have given us an inode ID to work with.
      // In this case, we have to try to resolve the path and hope it
      // hasn't changed or been deleted since the file was opened for write.
      iip = fsn.dir.getINodesInPath4Write(src);
      inode = iip.getLastINode();
    } else {
      // Newer clients pass the inode ID, so we can just get the inode
      // directly.
      inode = fsn.dir.getInode(fileId);
      iip = INodesInPath.fromINode(inode);
      if (inode != null) {
        src = iip.getPath();
      }
    }
    final INodeFile file = fsn.checkLease(src, clientName,
                                                 inode, fileId);
    BlockInfoContiguous lastBlockInFile = file.getLastBlock();
    if (!Block.matchingIdAndGenStamp(previousBlock, lastBlockInFile)) {
      // The block that the client claims is the current last block
      // doesn't match up with what we think is the last block. There are
      // four possibilities:
      // 1) This is the first block allocation of an append() pipeline
      //    which started appending exactly at or exceeding the block boundary.
      //    In this case, the client isn't passed the previous block,
      //    so it makes the allocateBlock() call with previous=null.
      //    We can distinguish this since the last block of the file
      //    will be exactly a full block.
      // 2) This is a retry from a client that missed the response of a
      //    prior getAdditionalBlock() call, perhaps because of a network
      //    timeout, or because of an HA failover. In that case, we know
      //    by the fact that the client is re-issuing the RPC that it
      //    never began to write to the old block. Hence it is safe to
      //    to return the existing block.
      // 3) This is an entirely bogus request/bug -- we should error out
      //    rather than potentially appending a new block with an empty
      //    one in the middle, etc
      // 4) This is a retry from a client that timed out while
      //    the prior getAdditionalBlock() is still being processed,
      //    currently working on chooseTarget().
      //    There are no means to distinguish between the first and
      //    the second attempts in Part I, because the first one hasn't
      //    changed the namesystem state yet.
      //    We run this analysis again in Part II where case 4 is impossible.

      BlockInfoContiguous penultimateBlock = file.getPenultimateBlock();
      if (previous == null &&
          lastBlockInFile != null &&
          lastBlockInFile.getNumBytes() >= file.getPreferredBlockSize() &&
          lastBlockInFile.isComplete()) {
        // Case 1
        if (NameNode.stateChangeLog.isDebugEnabled()) {
           NameNode.stateChangeLog.debug(
               "BLOCK* NameSystem.allocateBlock: handling block allocation" +
               " writing to a file with a complete previous block: src=" +
               src + " lastBlock=" + lastBlockInFile);
        }
      } else if (Block.matchingIdAndGenStamp(penultimateBlock, previousBlock)) {
        if (lastBlockInFile.getNumBytes() != 0) {
          throw new IOException(
              "Request looked like a retry to allocate block " +
              lastBlockInFile + " but it already contains " +
              lastBlockInFile.getNumBytes() + " bytes");
        }

        // Case 2
        // Return the last block.
        NameNode.stateChangeLog.info("BLOCK* allocateBlock: caught retry for " +
            "allocation of a new block in " + src + ". Returning previously" +
            " allocated block " + lastBlockInFile);
        long offset = file.computeFileSize();
        BlockInfoContiguousUnderConstruction lastBlockUC =
            (BlockInfoContiguousUnderConstruction) lastBlockInFile;
        onRetryBlock[0] = makeLocatedBlock(fsn, lastBlockInFile,
            lastBlockUC.getExpectedStorageLocations(), offset);
        return new FileState(file, src, iip);
      } else {
        // Case 3
        throw new IOException("Cannot allocate block in " + src + ": " +
            "passed 'previous' block " + previous + " does not match actual " +
            "last block in file " + lastBlockInFile);
      }
    }
    return new FileState(file, src, iip);
  }

  static boolean completeFile(FSNamesystem fsn, FSPermissionChecker pc,
      final String srcArg, String holder, ExtendedBlock last, long fileId)
      throws IOException {
    String src = srcArg;
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.completeFile: " +
                                        src + " for " + holder);
    }
    checkBlock(fsn, last);
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    src = fsn.dir.resolvePath(pc, src, pathComponents);
    boolean success = completeFileInternal(fsn, src, holder,
                                           ExtendedBlock.getLocalBlock(last),
                                           fileId);
    if (success) {
      NameNode.stateChangeLog.info("DIR* completeFile: " + srcArg
                                       + " is closed by " + holder);
    }
    return success;
  }

  private static boolean completeFileInternal(
      FSNamesystem fsn, String src, String holder, Block last, long fileId)
      throws IOException {
    assert fsn.hasWriteLock();
    final INodeFile pendingFile;
    final INodesInPath iip;
    INode inode = null;
    try {
      if (fileId == HdfsConstants.GRANDFATHER_INODE_ID) {
        // Older clients may not have given us an inode ID to work with.
        // In this case, we have to try to resolve the path and hope it
        // hasn't changed or been deleted since the file was opened for write.
        iip = fsn.dir.getINodesInPath(src, true);
        inode = iip.getLastINode();
      } else {
        inode = fsn.dir.getInode(fileId);
        iip = INodesInPath.fromINode(inode);
        if (inode != null) {
          src = iip.getPath();
        }
      }
      pendingFile = fsn.checkLease(src, holder, inode, fileId);
    } catch (LeaseExpiredException lee) {
      if (inode != null && inode.isFile() &&
          !inode.asFile().isUnderConstruction()) {
        // This could be a retry RPC - i.e the client tried to close
        // the file, but missed the RPC response. Thus, it is trying
        // again to close the file. If the file still exists and
        // the client's view of the last block matches the actual
        // last block, then we'll treat it as a successful close.
        // See HDFS-3031.
        final Block realLastBlock = inode.asFile().getLastBlock();
        if (Block.matchingIdAndGenStamp(last, realLastBlock)) {
          NameNode.stateChangeLog.info("DIR* completeFile: " +
              "request from " + holder + " to complete inode " + fileId +
              "(" + src + ") which is already closed. But, it appears to be " +
              "an RPC retry. Returning success");
          return true;
        }
      }
      throw lee;
    }
    // Check the state of the penultimate block. It should be completed
    // before attempting to complete the last one.
    if (!fsn.checkFileProgress(src, pendingFile, false)) {
      return false;
    }

    // commit the last block and complete it if it has minimum replicas
    fsn.commitOrCompleteLastBlock(pendingFile, iip, last);

    if (!fsn.checkFileProgress(src, pendingFile, true)) {
      return false;
    }

    fsn.finalizeINodeFileUnderConstruction(src, pendingFile,
        Snapshot.CURRENT_STATE_ID);
    return true;
  }

  /**
   * Persist the new block (the last block of the given file).
   */
  private static void persistNewBlock(
      FSNamesystem fsn, String path, INodeFile file) {
    Preconditions.checkArgument(file.isUnderConstruction());
    fsn.getEditLog().logAddBlock(path, file);
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("persistNewBlock: "
              + path + " with new block " + file.getLastBlock().toString()
              + ", current total block count is " + file.getBlocks().length);
    }
  }

  /**
   * Save allocated block at the given pending filename
   *
   * @param fsn FSNamesystem
   * @param src path to the file
   * @param inodesInPath representing each of the components of src.
   *                     The last INode is the INode for {@code src} file.
   * @param newBlock newly allocated block to be save
   * @param targets target datanodes where replicas of the new block is placed
   * @throws QuotaExceededException If addition of block exceeds space quota
   */
  private static void saveAllocatedBlock(
      FSNamesystem fsn, String src, INodesInPath inodesInPath, Block newBlock,
      DatanodeStorageInfo[] targets)
      throws IOException {
    assert fsn.hasWriteLock();
    BlockInfoContiguous b = addBlock(fsn.dir, src, inodesInPath, newBlock,
                                     targets);
    NameNode.stateChangeLog.info("BLOCK* allocate " + b + " for " + src);
    DatanodeStorageInfo.incrementBlocksScheduled(targets);
  }

  private static class FileState {
    final INodeFile inode;
    final String path;
    final INodesInPath iip;

    FileState(INodeFile inode, String fullPath, INodesInPath iip) {
      this.inode = inode;
      this.path = fullPath;
      this.iip = iip;
    }
  }

  static class ValidateAddBlockResult {
    final long blockSize;
    final int replication;
    final byte storagePolicyID;
    final String clientMachine;

    ValidateAddBlockResult(
        long blockSize, int replication, byte storagePolicyID,
        String clientMachine) {
      this.blockSize = blockSize;
      this.replication = replication;
      this.storagePolicyID = storagePolicyID;
      this.clientMachine = clientMachine;
    }
  }
}
