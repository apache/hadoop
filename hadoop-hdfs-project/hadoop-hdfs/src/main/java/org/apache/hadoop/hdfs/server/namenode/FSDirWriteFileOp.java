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
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.FSLimitException;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;

import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockUnderConstructionFeature;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.ChunkedArrayList;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.CURRENT_STATE_ID;
import static org.apache.hadoop.util.Time.now;

class FSDirWriteFileOp {
  private FSDirWriteFileOp() {}
  static boolean unprotectedRemoveBlock(
      FSDirectory fsd, String path, INodesInPath iip, INodeFile fileNode,
      Block block) throws IOException {
    // modify file-> block and blocksMap
    // fileNode should be under construction
    BlockInfo uc = fileNode.removeLastBlock(block);
    if (uc == null) {
      return false;
    }
    if (uc.getUnderConstructionFeature() != null) {
      DatanodeStorageInfo.decrementBlocksScheduled(uc
          .getUnderConstructionFeature().getExpectedStorageLocations());
    }
    fsd.getBlockManager().removeBlockFromMap(uc);

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
    final INodesInPath iip = fsd.resolvePath(pc, src, fileId);
    src = iip.getPath();
    FSNamesystem fsn = fsd.getFSNamesystem();
    final INodeFile file = fsn.checkLease(iip, holder, fileId);
    Preconditions.checkState(file.isUnderConstruction());
    if (file.getBlockType() == BlockType.STRIPED) {
      return; // do not abandon block for striped file
    }

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
    final short numTargets;
    final byte storagePolicyID;
    String clientMachine;
    final BlockType blockType;

    INodesInPath iip = fsn.dir.resolvePath(pc, src, fileId);
    FileState fileState = analyzeFileState(fsn, iip, fileId, clientName,
                                           previous, onRetryBlock);
    if (onRetryBlock[0] != null && onRetryBlock[0].getLocations().length > 0) {
      // This is a retry. No need to generate new locations.
      // Use the last block if it has locations.
      return null;
    }

    final INodeFile pendingFile = fileState.inode;
    if (!fsn.checkFileProgress(src, pendingFile, false)) {
      throw new NotReplicatedYetException("Not replicated yet: " + src);
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
    blockType = pendingFile.getBlockType();
    ErasureCodingPolicy ecPolicy = null;
    if (blockType == BlockType.STRIPED) {
      ecPolicy =
          FSDirErasureCodingOp.unprotectedGetErasureCodingPolicy(fsn, iip);
      numTargets = (short) (ecPolicy.getSchema().getNumDataUnits()
          + ecPolicy.getSchema().getNumParityUnits());
    } else {
      numTargets = pendingFile.getFileReplication();
    }
    storagePolicyID = pendingFile.getStoragePolicyID();
    return new ValidateAddBlockResult(blockSize, numTargets, storagePolicyID,
                                      clientMachine, blockType, ecPolicy);
  }

  static LocatedBlock makeLocatedBlock(FSNamesystem fsn, BlockInfo blk,
      DatanodeStorageInfo[] locs, long offset) throws IOException {
    LocatedBlock lBlk = BlockManager.newLocatedBlock(
        fsn.getExtendedBlock(new Block(blk)), blk, locs, offset);
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
    INodesInPath iip = fsn.dir.resolvePath(null, src, fileId);
    FileState fileState = analyzeFileState(fsn, iip, fileId, clientName,
                                           previous, onRetryBlock);
    final INodeFile pendingFile = fileState.inode;
    src = fileState.path;

    if (onRetryBlock[0] != null) {
      if (onRetryBlock[0].getLocations().length > 0) {
        // This is a retry. Just return the last block if having locations.
        return onRetryBlock[0];
      } else {
        // add new chosen targets to already allocated block and return
        BlockInfo lastBlockInFile = pendingFile.getLastBlock();
        lastBlockInFile.getUnderConstructionFeature().setExpectedLocations(
            lastBlockInFile, targets, pendingFile.getBlockType());
        offset = pendingFile.computeFileSize();
        return makeLocatedBlock(fsn, lastBlockInFile, targets, offset);
      }
    }

    // commit the last block and complete it if it has minimum replicas
    fsn.commitOrCompleteLastBlock(pendingFile, fileState.iip,
                                  ExtendedBlock.getLocalBlock(previous));

    // allocate new block, record block locations in INode.
    final BlockType blockType = pendingFile.getBlockType();
    // allocate new block, record block locations in INode.
    Block newBlock = fsn.createNewBlock(blockType);
    INodesInPath inodesInPath = INodesInPath.fromINode(pendingFile);
    saveAllocatedBlock(fsn, src, inodesInPath, newBlock, targets, blockType);

    persistNewBlock(fsn, src, pendingFile);
    offset = pendingFile.computeFileSize();

    // Return located block
    return makeLocatedBlock(fsn, fsn.getStoredBlock(newBlock), targets, offset);
  }

  static DatanodeStorageInfo[] chooseTargetForNewBlock(
      BlockManager bm, String src, DatanodeInfo[] excludedNodes,
      String[] favoredNodes, EnumSet<AddBlockFlag> flags,
      ValidateAddBlockResult r) throws IOException {
    Node clientNode = null;

    boolean ignoreClientLocality = (flags != null
            && flags.contains(AddBlockFlag.IGNORE_CLIENT_LOCALITY));

    // If client locality is ignored, clientNode remains 'null' to indicate
    if (!ignoreClientLocality) {
      clientNode = bm.getDatanodeManager().getDatanodeByHost(r.clientMachine);
      if (clientNode == null) {
        clientNode = getClientNode(bm, r.clientMachine);
      }
    }

    Set<Node> excludedNodesSet =
        (excludedNodes == null) ? new HashSet<>()
            : new HashSet<>(Arrays.asList(excludedNodes));

    List<String> favoredNodesList =
        (favoredNodes == null) ? Collections.emptyList()
            : Arrays.asList(favoredNodes);

    // choose targets for the new block to be allocated.
    return bm.chooseTarget4NewBlock(src, r.numTargets, clientNode,
                                    excludedNodesSet, r.blockSize,
                                    favoredNodesList, r.storagePolicyID,
                                    r.blockType, r.ecPolicy, flags);
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

  static INodesInPath resolvePathForStartFile(FSDirectory dir,
      FSPermissionChecker pc, String src, EnumSet<CreateFlag> flag,
      boolean createParent) throws IOException {
    INodesInPath iip = dir.resolvePath(pc, src, DirOp.CREATE);
    if (dir.isPermissionEnabled()) {
      dir.checkAncestorAccess(pc, iip, FsAction.WRITE);
    }
    INode inode = iip.getLastINode();
    if (inode != null) {
      // Verify that the destination does not exist as a directory already.
      if (inode.isDirectory()) {
        throw new FileAlreadyExistsException(iip.getPath() +
            " already exists as a directory");
      }
      // Verifies it's indeed a file and perms allow overwrite
      INodeFile.valueOf(inode, src);
      if (dir.isPermissionEnabled() && flag.contains(CreateFlag.OVERWRITE)) {
        dir.checkPathAccess(pc, iip, FsAction.WRITE);
      }
    } else {
      if (!createParent) {
        dir.verifyParentDir(iip);
      }
      if (!flag.contains(CreateFlag.CREATE)) {
        throw new FileNotFoundException("Can't overwrite non-existent " + src);
      }
    }
    return iip;
  }


  /**
   * Create a new file or overwrite an existing file<br>
   *
   * Once the file is create the client then allocates a new block with the next
   * call using {@link ClientProtocol#addBlock}.
   * <p>
   * For description of parameters and exceptions thrown see
   * {@link ClientProtocol#create}
   */
  static HdfsFileStatus startFile(
      FSNamesystem fsn, INodesInPath iip,
      PermissionStatus permissions, String holder, String clientMachine,
      EnumSet<CreateFlag> flag, boolean createParent,
      short replication, long blockSize,
      FileEncryptionInfo feInfo, INode.BlocksMapUpdateInfo toRemoveBlocks,
      boolean shouldReplicate, String ecPolicyName, String storagePolicy,
      boolean logRetryEntry)
      throws IOException {
    assert fsn.hasWriteLock();
    boolean overwrite = flag.contains(CreateFlag.OVERWRITE);
    boolean isLazyPersist = flag.contains(CreateFlag.LAZY_PERSIST);

    final String src = iip.getPath();
    FSDirectory fsd = fsn.getFSDirectory();

    if (iip.getLastINode() != null) {
      if (overwrite) {
        List<INode> toRemoveINodes = new ChunkedArrayList<>();
        List<Long> toRemoveUCFiles = new ChunkedArrayList<>();
        long ret = FSDirDeleteOp.delete(fsd, iip, toRemoveBlocks,
                                        toRemoveINodes, toRemoveUCFiles, now());
        if (ret >= 0) {
          iip = INodesInPath.replace(iip, iip.length() - 1, null);
          FSDirDeleteOp.incrDeletedFileCount(ret);
          fsn.removeLeasesAndINodes(toRemoveUCFiles, toRemoveINodes, true);
        }
      } else {
        // If lease soft limit time is expired, recover the lease
        fsn.recoverLeaseInternal(FSNamesystem.RecoverLeaseOp.CREATE_FILE, iip,
                                 src, holder, clientMachine, false);
        throw new FileAlreadyExistsException(src + " for client " +
            clientMachine + " already exists");
      }
    }
    fsn.checkFsObjectLimit();
    INodeFile newNode = null;
    INodesInPath parent =
        FSDirMkdirOp.createAncestorDirectories(fsd, iip, permissions);
    if (parent != null) {
      iip = addFile(fsd, parent, iip.getLastLocalName(), permissions,
          replication, blockSize, holder, clientMachine, shouldReplicate,
          ecPolicyName, storagePolicy);
      newNode = iip != null ? iip.getLastINode().asFile() : null;
    }
    if (newNode == null) {
      throw new IOException("Unable to add " + src +  " to namespace");
    }
    fsn.leaseManager.addLease(
        newNode.getFileUnderConstructionFeature().getClientName(),
        newNode.getId());
    if (feInfo != null) {
      FSDirEncryptionZoneOp.setFileEncryptionInfo(fsd, iip, feInfo,
          XAttrSetFlag.CREATE);
    }
    setNewINodeStoragePolicy(fsd.getBlockManager(), iip, isLazyPersist);
    fsd.getEditLog().logOpenFile(src, newNode, overwrite, logRetryEntry);
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.startFile: added " +
          src + " inode " + newNode.getId() + " " + holder);
    }
    return FSDirStatAndListingOp.getFileInfo(fsd, iip, false, false);
  }

  static INodeFile addFileForEditLog(
      FSDirectory fsd, long id, INodesInPath existing, byte[] localName,
      PermissionStatus permissions, List<AclEntry> aclEntries,
      List<XAttr> xAttrs, short replication, long modificationTime, long atime,
      long preferredBlockSize, boolean underConstruction, String clientName,
      String clientMachine, byte storagePolicyId, byte ecPolicyID) {
    final INodeFile newNode;
    Preconditions.checkNotNull(existing);
    assert fsd.hasWriteLock();
    try {
      // check if the file has an EC policy
      boolean isStriped =
          ecPolicyID != ErasureCodeConstants.REPLICATION_POLICY_ID;
      ErasureCodingPolicy ecPolicy = null;
      if (isStriped) {
        ecPolicy = fsd.getFSNamesystem().getErasureCodingPolicyManager()
          .getByID(ecPolicyID);
        if (ecPolicy == null) {
          throw new IOException(String.format(
              "Cannot find erasure coding policy for new file %s/%s, " +
                  "ecPolicyID=%d",
              existing.getPath(), Arrays.toString(localName), ecPolicyID));
        }
      }
      final BlockType blockType = isStriped ?
          BlockType.STRIPED : BlockType.CONTIGUOUS;
      final Short replicationFactor = (!isStriped ? replication : null);
      if (underConstruction) {
        newNode = newINodeFile(id, permissions, modificationTime,
            modificationTime, replicationFactor, ecPolicyID, preferredBlockSize,
            storagePolicyId, blockType);
        newNode.toUnderConstruction(clientName, clientMachine);
      } else {
        newNode = newINodeFile(id, permissions, modificationTime, atime,
            replicationFactor, ecPolicyID, preferredBlockSize,
            storagePolicyId, blockType);
      }
      newNode.setLocalName(localName);
      INodesInPath iip = fsd.addINode(existing, newNode,
          permissions.getPermission());
      if (iip != null) {
        if (aclEntries != null) {
          AclStorage.updateINodeAcl(newNode, aclEntries, CURRENT_STATE_ID);
        }
        if (xAttrs != null) {
          XAttrStorage.updateINodeXAttrs(newNode, xAttrs, CURRENT_STATE_ID);
        }
        return newNode;
      }
    } catch (IOException e) {
      NameNode.stateChangeLog.warn(
          "DIR* FSDirectory.unprotectedAddFile: exception when add " + existing
              .getPath() + " to the file system", e);
      if (e instanceof FSLimitException.MaxDirectoryItemsExceededException) {
        NameNode.stateChangeLog.warn("Please increase "
            + "dfs.namenode.fs-limits.max-directory-items and make it "
            + "consistent across all NameNodes.");
      }
    }
    return null;
  }

  /**
   * Add a block to the file. Returns a reference to the added block.
   */
  private static BlockInfo addBlock(FSDirectory fsd, String path,
      INodesInPath inodesInPath, Block block, DatanodeStorageInfo[] targets,
      BlockType blockType) throws IOException {
    fsd.writeLock();
    try {
      final INodeFile fileINode = inodesInPath.getLastINode().asFile();
      Preconditions.checkState(fileINode.isUnderConstruction());

      // associate new last block for the file
      final BlockInfo blockInfo;
      if (blockType == BlockType.STRIPED) {
        ErasureCodingPolicy ecPolicy =
            FSDirErasureCodingOp.unprotectedGetErasureCodingPolicy(
                fsd.getFSNamesystem(), inodesInPath);
        short numDataUnits = (short) ecPolicy.getNumDataUnits();
        short numParityUnits = (short) ecPolicy.getNumParityUnits();
        short numLocations = (short) (numDataUnits + numParityUnits);

        // check quota limits and updated space consumed
        fsd.updateCount(inodesInPath, 0, fileINode.getPreferredBlockSize(),
            numLocations, true);
        blockInfo = new BlockInfoStriped(block, ecPolicy);
        blockInfo.convertToBlockUnderConstruction(
            HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION, targets);
      } else {
        // check quota limits and updated space consumed
        fsd.updateCount(inodesInPath, 0, fileINode.getPreferredBlockSize(),
            fileINode.getFileReplication(), true);

        short numLocations = fileINode.getFileReplication();
        blockInfo = new BlockInfoContiguous(block, numLocations);
        blockInfo.convertToBlockUnderConstruction(
            HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION, targets);
      }
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

  /**
   * Add the given filename to the fs.
   * @return the new INodesInPath instance that contains the new INode
   */
  private static INodesInPath addFile(
      FSDirectory fsd, INodesInPath existing, byte[] localName,
      PermissionStatus permissions, short replication, long preferredBlockSize,
      String clientName, String clientMachine, boolean shouldReplicate,
      String ecPolicyName, String storagePolicy) throws IOException {

    Preconditions.checkNotNull(existing);
    long modTime = now();
    INodesInPath newiip;
    fsd.writeLock();
    try {
      boolean isStriped = false;
      ErasureCodingPolicy ecPolicy = null;
      byte storagepolicyid = 0;
      if (storagePolicy != null && !storagePolicy.isEmpty()) {
        BlockStoragePolicy policy =
            fsd.getBlockManager().getStoragePolicy(storagePolicy);
        if (policy == null) {
          throw new HadoopIllegalArgumentException(
              "Cannot find a block policy with the name " + storagePolicy);
        }
        storagepolicyid = policy.getId();
      }
      if (!shouldReplicate) {
        ecPolicy = FSDirErasureCodingOp.getErasureCodingPolicy(
            fsd.getFSNamesystem(), ecPolicyName, existing);
        if (ecPolicy != null && (!ecPolicy.isReplicationPolicy())) {
          isStriped = true;
        }
      }
      final BlockType blockType = isStriped ?
          BlockType.STRIPED : BlockType.CONTIGUOUS;
      final Short replicationFactor = (!isStriped ? replication : null);
      final Byte ecPolicyID = (isStriped ? ecPolicy.getId() : null);
      INodeFile newNode = newINodeFile(fsd.allocateNewInodeId(), permissions,
          modTime, modTime, replicationFactor, ecPolicyID, preferredBlockSize,
          storagepolicyid, blockType);
      newNode.setLocalName(localName);
      newNode.toUnderConstruction(clientName, clientMachine);
      newiip = fsd.addINode(existing, newNode, permissions.getPermission());
    } finally {
      fsd.writeUnlock();
    }
    if (newiip == null) {
      NameNode.stateChangeLog.info("DIR* addFile: failed to add " +
          existing.getPath() + "/" + DFSUtil.bytes2String(localName));
      return null;
    }

    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* addFile: " +
          DFSUtil.bytes2String(localName) + " is added");
    }
    return newiip;
  }

  private static FileState analyzeFileState(
      FSNamesystem fsn, INodesInPath iip, long fileId, String clientName,
      ExtendedBlock previous, LocatedBlock[] onRetryBlock)
      throws IOException {
    assert fsn.hasReadLock();
    String src = iip.getPath();
    checkBlock(fsn, previous);
    onRetryBlock[0] = null;
    fsn.checkNameNodeSafeMode("Cannot add block to " + src);

    // have we exceeded the configured limit of fs objects.
    fsn.checkFsObjectLimit();

    Block previousBlock = ExtendedBlock.getLocalBlock(previous);
    final INodeFile file = fsn.checkLease(iip, clientName, fileId);
    BlockInfo lastBlockInFile = file.getLastBlock();
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

      BlockInfo penultimateBlock = file.getPenultimateBlock();
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
        BlockUnderConstructionFeature uc =
            lastBlockInFile.getUnderConstructionFeature();
        onRetryBlock[0] = makeLocatedBlock(fsn, lastBlockInFile,
            uc.getExpectedStorageLocations(), offset);
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
    INodesInPath iip = fsn.dir.resolvePath(pc, src, fileId);
    return completeFileInternal(fsn, iip, holder,
        ExtendedBlock.getLocalBlock(last), fileId);
  }

  private static boolean completeFileInternal(
      FSNamesystem fsn, INodesInPath iip,
      String holder, Block last, long fileId)
      throws IOException {
    assert fsn.hasWriteLock();
    final String src = iip.getPath();
    final INodeFile pendingFile;
    INode inode = null;
    try {
      inode = iip.getLastINode();
      pendingFile = fsn.checkLease(iip, holder, fileId);
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

    fsn.addCommittedBlocksToPending(pendingFile);

    fsn.finalizeINodeFileUnderConstruction(src, pendingFile,
        Snapshot.CURRENT_STATE_ID, true);
    return true;
  }

  private static INodeFile newINodeFile(
      long id, PermissionStatus permissions, long mtime, long atime,
      Short replication, Byte ecPolicyID, long preferredBlockSize,
      byte storagePolicyId, BlockType blockType) {
    return new INodeFile(id, null, permissions, mtime, atime,
        BlockInfo.EMPTY_ARRAY, replication, ecPolicyID, preferredBlockSize,
        storagePolicyId, blockType);
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
  private static void saveAllocatedBlock(FSNamesystem fsn, String src,
      INodesInPath inodesInPath, Block newBlock, DatanodeStorageInfo[] targets,
      BlockType blockType) throws IOException {
    assert fsn.hasWriteLock();
    BlockInfo b = addBlock(fsn.dir, src, inodesInPath, newBlock, targets,
        blockType);
    logAllocatedBlock(src, b);
    DatanodeStorageInfo.incrementBlocksScheduled(targets);
  }

  private static void logAllocatedBlock(String src, BlockInfo b) {
    if (!NameNode.stateChangeLog.isInfoEnabled()) {
      return;
    }
    StringBuilder sb = new StringBuilder(150);
    sb.append("BLOCK* allocate ");
    b.appendStringTo(sb);
    sb.append(", ");
    BlockUnderConstructionFeature uc = b.getUnderConstructionFeature();
    if (uc != null) {
      uc.appendUCPartsConcise(sb);
    }
    sb.append(" for " + src);
    NameNode.stateChangeLog.info(sb.toString());
  }

  private static void setNewINodeStoragePolicy(BlockManager bm,
      INodesInPath iip, boolean isLazyPersist) throws IOException {
    INodeFile inode = iip.getLastINode().asFile();
    if (isLazyPersist) {
      BlockStoragePolicy lpPolicy =
          bm.getStoragePolicy("LAZY_PERSIST");

      // Set LAZY_PERSIST storage policy if the flag was passed to
      // CreateFile.
      if (lpPolicy == null) {
        throw new HadoopIllegalArgumentException(
            "The LAZY_PERSIST storage policy has been disabled " +
            "by the administrator.");
      }
      inode.setStoragePolicyID(lpPolicy.getId(),
                                 iip.getLatestSnapshotId());
    } else {
      BlockStoragePolicy effectivePolicy =
          bm.getStoragePolicy(inode.getStoragePolicyID());

      if (effectivePolicy != null &&
          effectivePolicy.isCopyOnCreateFile()) {
        // Copy effective policy from ancestor directory to current file.
        inode.setStoragePolicyID(effectivePolicy.getId(),
                                 iip.getLatestSnapshotId());
      }
    }
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
    private final long blockSize;
    private final int numTargets;
    private final byte storagePolicyID;
    private final String clientMachine;
    private final BlockType blockType;
    private final ErasureCodingPolicy ecPolicy;

    ValidateAddBlockResult(
        long blockSize, int numTargets, byte storagePolicyID,
        String clientMachine, BlockType blockType,
        ErasureCodingPolicy ecPolicy) {
      this.blockSize = blockSize;
      this.numTargets = numTargets;
      this.storagePolicyID = storagePolicyID;
      this.clientMachine = clientMachine;
      this.blockType = blockType;
      this.ecPolicy = ecPolicy;

      if (blockType == BlockType.STRIPED) {
        Preconditions.checkArgument(ecPolicy != null,
            "ecPolicy is not specified for striped block");
      }
    }
  }
}
