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
import com.google.protobuf.ByteString;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.PermissionStatus;
import com.google.protobuf.ByteString;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.ChunkedArrayList;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
                    uc.getReplication(), true);
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

    FSDirectory fsd = fsn.getFSDirectory();
    try (ROTransaction tx = fsd.newROTransaction().begin()) {
      FileState fileState = analyzeFileState(tx, fsn, src, fileId, clientName,
                                             previous, onRetryBlock);
      final FlatINode pendingFile = fileState.inode;
      FlatINodeFileFeature f = pendingFile.feature(FlatINodeFileFeature.class);
      // Check if the penultimate block is minimally replicated
      if (!fsn.checkFileProgress(src, f, false)) {
        throw new NotReplicatedYetException("Not replicated yet: " + src);
      }

      if (onRetryBlock[0] != null && onRetryBlock[0].getLocations().length > 0) {
        // This is a retry. No need to generate new locations.
        // Use the last block if it has locations.
        return null;
      }
      if (f.numBlocks() >= fsn.maxBlocksPerFile) {
        throw new IOException("File has reached the limit on maximum number of"
            + " blocks (" + DFSConfigKeys.DFS_NAMENODE_MAX_BLOCKS_PER_FILE_KEY
            + "): " + f.numBlocks() + " >= " + fsn.maxBlocksPerFile);
      }
      blockSize = f.blockSize();
      clientMachine = f.clientMachine();
      replication = f.replication();
      storagePolicyID = f.storagePolicyId();
      return new ValidateAddBlockResult(blockSize, replication, storagePolicyID,
                                      clientMachine);
    }
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
    FSDirectory fsd = fsn.getFSDirectory();
    BlockManager bm = fsn.getBlockManager();
    try (RWTransaction tx = fsd.newRWTransaction().begin()) {
      FileState fileState = analyzeFileState(tx, fsn, src, fileId, clientName,
                                             previous, onRetryBlock);
      final FlatINode inode = fileState.inode;
      FlatINodeFileFeature file = inode.feature(FlatINodeFileFeature.class);
      src = fileState.path;

      if (onRetryBlock[0] != null) {
        if (onRetryBlock[0].getLocations().length > 0) {
          // This is a retry. Just return the last block if having locations.
          return onRetryBlock[0];
        } else {
          // add new chosen targets to already allocated block and return
          Block lastBlock = file.lastBlock();
          BlockInfoContiguous lastBlockInFile = bm.getStoredBlock(lastBlock);
          ((BlockInfoContiguousUnderConstruction) lastBlockInFile)
              .setExpectedLocations(targets);
          offset = file.fileSize();
          return makeLocatedBlock(fsn, lastBlockInFile, targets, offset);
        }
      }

      // commit the last block and complete it if it has minimum replicas
      FlatINodeFileFeature.Builder newFile = fsn.commitOrCompleteLastBlock(
          file, ExtendedBlock.getLocalBlock(previous));

      // allocate new block, record block locations in INode.
      Block newBlock = fsn.createNewBlock();
      saveAllocatedBlock(fsn, src, inode, newBlock, targets);
      FlatINode newInode = persistNewBlock(tx, src, inode, newFile, newBlock);
      offset = newInode.<FlatINodeFileFeature>feature(
          FlatINodeFileFeature.class).fileSize();

      // TODO: Update quota
      // check quota limits and updated space consumed
//      fsd.updateCount(inodesInPath, 0, fileINode.getPreferredBlockSize(),
//                      fileINode.getFileReplication(), true);

      tx.commit();
      // Return located block
      return makeLocatedBlock(fsn, newBlock, targets, offset);
    }
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
   * Create a new file or overwrite an existing file<br>
   *
   * Once the file is create the client then allocates a new block with the next
   * call using {@link ClientProtocol#addBlock}.
   * <p>
   * For description of parameters and exceptions thrown see
   * {@link ClientProtocol#create}
   */
  static HdfsFileStatus startFile(
      FSNamesystem fsn, FSPermissionChecker pc, String src,
      PermissionStatus permissions, String holder, String clientMachine,
      EnumSet<CreateFlag> flag, boolean createParent,
      short replication, long blockSize,
      EncryptionKeyInfo ezInfo, INode.BlocksMapUpdateInfo toRemoveBlocks,
      boolean logRetryEntry)
      throws IOException {
    assert fsn.hasWriteLock();

    boolean create = flag.contains(CreateFlag.CREATE);
    boolean overwrite = flag.contains(CreateFlag.OVERWRITE);
    boolean isLazyPersist = flag.contains(CreateFlag.LAZY_PERSIST);

    CipherSuite suite = null;
    CryptoProtocolVersion version = null;
    KeyProviderCryptoExtension.EncryptedKeyVersion edek = null;

    if (ezInfo != null) {
      edek = ezInfo.edek;
      suite = ezInfo.suite;
      version = ezInfo.protocolVersion;
    }

    boolean isRawPath = FSDirectory.isReservedRawName(src);
    FSDirectory fsd = fsn.getFSDirectory();
    final StringMap ugid = fsd.ugid();

    try (RWTransaction tx = fsd.newRWTransaction().begin()) {
      Resolver.Result paths = Resolver.resolve(tx, src);
      if (paths.invalidPath()) {
        throw new InvalidPathException(src);
      }

      final FlatINodesInPath iip = paths.inodesInPath();
      // Verify that the destination does not exist as a directory already.
      if (paths.ok()) {
        FlatINode inode = paths.inodesInPath().getLastINode();
        if (inode.isDirectory()) {
          throw new FileAlreadyExistsException(src +
              " already exists as a directory");
        }

        if (fsd.isPermissionEnabled()) {
          if (overwrite) {
            fsd.checkPathAccess(pc, iip, FsAction.WRITE);
          }
        }
      }

      if (fsd.isPermissionEnabled()) {
      /*
       * To overwrite existing file, need to check 'w' permission
       * of parent (equals to ancestor in this case)
       */
        fsd.checkAncestorAccess(pc, paths, FsAction.WRITE);
      }

      if (!createParent && FlatNSUtil.hasNextLevelInPath(paths.src, paths
          .offset)) {
        throw new FileNotFoundException(paths.src.substring(0, paths.offset));
      }

      if (paths.notFound() && !create) {
        throw new FileNotFoundException("Can't overwrite non-existent " +
            src + " for client " + clientMachine);
      }

      // TODO: Handle encryption
      FileEncryptionInfo feInfo = null;

//      final EncryptionZone zone = fsd.getEZForPath(iip);
//      if (zone != null) {
//        // The path is now within an EZ, but we're missing encryption parameters
//        if (suite == null || edek == null) {
//          throw new RetryStartFileException();
//        }
//        // Path is within an EZ and we have provided encryption parameters.
//        // Make sure that the generated EDEK matches the settings of the EZ.
//        final String ezKeyName = zone.getKeyName();
//        if (!ezKeyName.equals(edek.getEncryptionKeyName())) {
//          throw new RetryStartFileException();
//        }
//        feInfo = new FileEncryptionInfo(suite, version,
//                                        edek.getEncryptedKeyVersion().getMaterial(),
//                                        edek.getEncryptedKeyIv(),
//                                        ezKeyName, edek.getEncryptionKeyVersionName());
//      }

      if (paths.ok()) {
        if (overwrite) {
          // TODO
          List<Long> toRemoveUCFiles = new ChunkedArrayList<>();
          long ret = FSDirDeleteOp.delete(tx, paths, toRemoveBlocks,
                                          toRemoveUCFiles, now());
          if (ret >= 0) {
            FSDirDeleteOp.incrDeletedFileCount(ret);
            fsn.removeLeases(toRemoveUCFiles);
          }
        } else {
          // TODO
          // If lease soft limit time is expired, recover the lease
//          fsn.recoverLeaseInternal(FSNamesystem.RecoverLeaseOp.CREATE_FILE, iip,
//                                   src, holder, clientMachine, false);
          throw new FileAlreadyExistsException(src + " for client " +
                                                   clientMachine + " already exists");
        }
      }
      fsn.checkFsObjectLimit();
      paths = Resolver.resolve(tx, src);
      Map.Entry<FlatINodesInPath, String> parent = FSDirMkdirOp
          .createAncestorDirectories(tx, fsd, paths, permissions);
      long newId = tx.allocateNewInodeId();
      FlatINodeFileFeature.Builder fileFeatureBuilder = new FlatINodeFileFeature
          .Builder()
          .replication(replication)
          .blockSize(blockSize)
          .inConstruction(true)
          .clientName(holder)
          .clientMachine(clientMachine);

      setNewINodeStoragePolicy(fsn.getBlockManager(), fileFeatureBuilder,
                               isLazyPersist);

      int userId = tx.getStringId(permissions.getUserName());
      FlatINode parentINode = parent.getKey().getLastINode();
      int groupId = permissions.getGroupName() == null
          ? parentINode.groupId()
          : tx.getStringId(permissions.getGroupName());

      FlatINodeFileFeature fileFeature = FlatINodeFileFeature.wrap(
          fileFeatureBuilder.build());
      ByteString b = new FlatINode.Builder()
          .id(newId)
          .type(FlatINode.Type.FILE)
          .parentId(parentINode.id())
          .mtime(now())
          .userId(userId)
          .groupId(groupId)
          .permission(permissions.getPermission().toShort())
          .addFeature(fileFeature)
          .build();

      FlatINode newNode = FlatINode.wrap(b);
      // TODO: check .reserved path, quotas and ACL
      byte[] localName = parent.getValue().getBytes(Charsets.UTF_8);
      tx.putINode(newId, b);
      tx.putChild(parentINode.id(), ByteBuffer.wrap(localName), newId);

      ByteString newParent = new FlatINode.Builder().mergeFrom(parentINode)
          .mtime(now()).build();
      tx.putINode(parentINode.id(), newParent);

      fsn.leaseManager.addLease(holder, newId);
//      if (feInfo != null) {
//        fsd.setFileEncryptionInfo(src, feInfo);
//        newNode = fsd.getInode(newNode.getId()).asFile();
//      }
      tx.logOpenFile(fsd.ugid(), src, newNode, overwrite, logRetryEntry);
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* NameSystem.startFile: added " +
            src + " inode " + newId + " " + holder);
      }
      tx.commit();
      return FSDirStatAndListingOp.createFileStatus(
          tx, fsd, newNode, localName, fileFeature.storagePolicyId());
    }
  }

  static EncryptionKeyInfo getEncryptionKeyInfo(FSNamesystem fsn,
      FSPermissionChecker pc, String src,
      CryptoProtocolVersion[] supportedVersions)
      throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    FSDirectory fsd = fsn.getFSDirectory();
    src = fsd.resolvePath(pc, src, pathComponents);
    INodesInPath iip = fsd.getINodesInPath4Write(src);
    // Nothing to do if the path is not within an EZ
    final EncryptionZone zone = fsd.getEZForPath(iip);
    if (zone == null) {
      return null;
    }
    CryptoProtocolVersion protocolVersion = fsn.chooseProtocolVersion(
        zone, supportedVersions);
    CipherSuite suite = zone.getSuite();
    String ezKeyName = zone.getKeyName();

    Preconditions.checkNotNull(protocolVersion);
    Preconditions.checkNotNull(suite);
    Preconditions.checkArgument(!suite.equals(CipherSuite.UNKNOWN),
                                "Chose an UNKNOWN CipherSuite!");
    Preconditions.checkNotNull(ezKeyName);
    return new EncryptionKeyInfo(protocolVersion, suite, ezKeyName);
  }

  static INodeFile addFileForEditLog(
      FSDirectory fsd, long id, INodesInPath existing, byte[] localName,
      PermissionStatus permissions, List<AclEntry> aclEntries,
      List<XAttr> xAttrs, short replication, long modificationTime, long atime,
      long preferredBlockSize, boolean underConstruction, String clientName,
      String clientMachine, byte storagePolicyId) {
    final INodeFile newNode;
    assert fsd.hasWriteLock();
    if (underConstruction) {
      newNode = newINodeFile(id, permissions, modificationTime,
                                              modificationTime, replication,
                                              preferredBlockSize,
                                              storagePolicyId);
      newNode.toUnderConstruction(clientName, clientMachine);
    } else {
      newNode = newINodeFile(id, permissions, modificationTime,
                                              atime, replication,
                                              preferredBlockSize,
                                              storagePolicyId);
    }

    newNode.setLocalName(localName);
    try {
      INodesInPath iip = fsd.addINode(existing, newNode);
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
      if(NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug(
            "DIR* FSDirectory.unprotectedAddFile: exception when add "
                + existing.getPath() + " to the file system", e);
      }
    }
    return null;
  }

  /**
   * Add a block to the file. Returns a reference to the added block.
   */
  private static BlockInfoContiguous addBlock(
      BlockManager bm, String path, FlatINode inode, Block block,
      DatanodeStorageInfo[] targets) throws IOException {
    FlatINodeFileFeature f = inode.feature(FlatINodeFileFeature.class);
    Preconditions.checkState(f.inConstruction());
    // associate new last block for the file
    BlockInfoContiguousUnderConstruction blockInfo =
        new BlockInfoContiguousUnderConstruction(
            block, f.replication(),
            HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION,
            targets);

    bm.addBlockCollection(blockInfo, inode.id());
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.addBlock: " + path
          + " with " + block + " block is added to the in-memory file system");
    }
    return blockInfo;
  }

  private static FileState analyzeFileState(
      Transaction tx, FSNamesystem fsn, String src, long fileId, String clientName,
      ExtendedBlock previous, LocatedBlock[] onRetryBlock)
          throws IOException  {
    assert fsn.hasReadLock();
    BlockManager bm = fsn.getBlockManager();
    checkBlock(fsn, previous);
    onRetryBlock[0] = null;

    Block previousBlock = ExtendedBlock.getLocalBlock(previous);
    final Resolver.Result paths;
    if (true || fileId == HdfsConstants.GRANDFATHER_INODE_ID) {
      // Older clients may not have given us an inode ID to work with.
      // In this case, we have to try to resolve the path and hope it
      // hasn't changed or been deleted since the file was opened for write.
      paths = Resolver.resolve(tx, src);
    } else {
      // Newer clients pass the inode ID, so we can just get the inode
      // directly.
      paths = Resolver.resolveById(tx, fileId);
    }
    if (paths.invalidPath()) {
      throw new InvalidPathException(src);
    } else if (paths.notFound()) {
      throw new FileNotFoundException(src);
    }
    FlatINode inode = paths.inodesInPath().getLastINode();
    fsn.checkLease(src, clientName, inode);
    FlatINodeFileFeature pendingFile = inode.feature(FlatINodeFileFeature.class);
    BlockInfoContiguous lastBlockInFile = pendingFile.lastBlock() == null ?
        null : bm.getStoredBlock(pendingFile.lastBlock());
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

      BlockInfoContiguous penultimateBlock = bm.getStoredBlock(
          pendingFile.penultimateBlock());
      if (previous == null &&
          lastBlockInFile != null &&
          lastBlockInFile.getNumBytes() >= pendingFile.blockSize() &&
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
        NameNode.stateChangeLog.info("BLOCK* allocateBlock: " +
            "caught retry for allocation of a new block in " +
            src + ". Returning previously allocated block " + lastBlockInFile);
        long offset = pendingFile.fileSize();
        BlockInfoContiguousUnderConstruction lastBlockUC =
            (BlockInfoContiguousUnderConstruction) lastBlockInFile;
        onRetryBlock[0] = makeLocatedBlock(fsn, lastBlockInFile,
            lastBlockUC.getExpectedStorageLocations(), offset);
        return new FileState(inode, src);
      } else {
        // Case 3
        throw new IOException("Cannot allocate block in " + src + ": " +
            "passed 'previous' block " + previous + " does not match actual " +
                                  "last block in file " + lastBlockInFile);
      }
    }
    return new FileState(inode, src);
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

  private static INodeFile newINodeFile(
      long id, PermissionStatus permissions, long mtime, long atime,
      short replication, long preferredBlockSize, byte storagePolicyId) {
    return new INodeFile(id, null, permissions, mtime, atime,
        BlockInfoContiguous.EMPTY_ARRAY, replication, preferredBlockSize,
        storagePolicyId);
  }

  /**
   * Persist the new block (the last block of the given file).
   */
  private static FlatINode persistNewBlock(
      RWTransaction tx, String path, FlatINode inode,
      FlatINodeFileFeature.Builder newFile, Block newBlock) {
    Preconditions.checkArgument(newFile.inConstruction());
    newFile.addBlock(newBlock);
    FlatINodeFileFeature newFeature = FlatINodeFileFeature.wrap(newFile.build());

    FlatINode.Builder builder = new FlatINode.Builder()
        .mergeFrom(inode).replaceFeature(newFeature);
    ByteString newFileBytes = builder.build();
    FlatINode newInode = FlatINode.wrap(newFileBytes);
    tx.putINode(inode.id(), newFileBytes);
    tx.logAddBlock(path, newFeature);
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("persistNewBlock: " + path
          + " with new block " + newBlock + ", current total block count is "
          + newFeature.numBlocks());
    }
    return newInode;
  }

  /**
   * Save allocated block at the given pending filename
   *
   * @param fsn FSNamesystem
   * @param src path to the file
   * @param inode the file
   * @param newBlock newly allocated block to be save
   * @param targets target datanodes where replicas of the new block is placed
   * @throws QuotaExceededException If addition of block exceeds space quota
   */
  private static void saveAllocatedBlock(
      FSNamesystem fsn, String src, FlatINode inode, Block newBlock,
      DatanodeStorageInfo[] targets)
      throws IOException {
    assert fsn.hasWriteLock();
    BlockManager bm = fsn.getBlockManager();
    BlockInfoContiguous b = addBlock(bm, src, inode, newBlock, targets);
    NameNode.stateChangeLog.info("BLOCK* allocate " + b + " for " + src);
    DatanodeStorageInfo.incrementBlocksScheduled(targets);
  }

  private static void setNewINodeStoragePolicy(
      BlockManager bm, FlatINodeFileFeature.Builder file, boolean isLazyPersist)
      throws IOException {

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
      file.storagePolicyId(lpPolicy.getId());
    } else {
      // TODO: handle effective storage policy id
//      BlockStoragePolicy effectivePolicy =
//          bm.getStoragePolicy(parent.getLastINode().storagePolicyId());
//
//      if (effectivePolicy != null &&
//          effectivePolicy.isCopyOnCreateFile()) {
//        // Copy effective policy from ancestor directory to current file.
//        file.storagePolicyId(effectivePolicy.getId());
//      }
    }
  }

  private static class FileState {
    final FlatINode inode;
    final String path;

    FileState(FlatINode inode, String fullPath) {
      this.inode = inode;
      this.path = fullPath;
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

  static class EncryptionKeyInfo {
    final CryptoProtocolVersion protocolVersion;
    final CipherSuite suite;
    final String ezKeyName;
    KeyProviderCryptoExtension.EncryptedKeyVersion edek;

    EncryptionKeyInfo(
        CryptoProtocolVersion protocolVersion, CipherSuite suite,
        String ezKeyName) {
      this.protocolVersion = protocolVersion;
      this.suite = suite;
      this.ezKeyName = ezKeyName;
    }
  }
}
