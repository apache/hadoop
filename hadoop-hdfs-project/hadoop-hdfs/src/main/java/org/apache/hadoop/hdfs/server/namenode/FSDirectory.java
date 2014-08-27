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

import static org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.CRYPTO_XATTR_ENCRYPTION_ZONE;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.CRYPTO_XATTR_FILE_ENCRYPTION_INFO;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.SECURITY_XATTR_UNREADABLE_BY_SUPERUSER;
import static org.apache.hadoop.util.Time.now;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.FSLimitException.MaxDirectoryItemsExceededException;
import org.apache.hadoop.hdfs.protocol.FSLimitException.PathComponentTooLongException;
import org.apache.hadoop.hdfs.protocol.FsPermissionExtension;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithCount;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectorySnapshottableFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.Root;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.hdfs.util.ChunkedArrayList;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.security.AccessControlException;

/**
 * Both FSDirectory and FSNamesystem manage the state of the namespace.
 * FSDirectory is a pure in-memory data structure, all of whose operations
 * happen entirely in memory. In contrast, FSNamesystem persists the operations
 * to the disk.
 * @see org.apache.hadoop.hdfs.server.namenode.FSNamesystem
 **/
@InterfaceAudience.Private
public class FSDirectory implements Closeable {
  private static INodeDirectory createRoot(FSNamesystem namesystem) {
    final INodeDirectory r = new INodeDirectory(
        INodeId.ROOT_INODE_ID,
        INodeDirectory.ROOT_NAME,
        namesystem.createFsOwnerPermissions(new FsPermission((short) 0755)),
        0L);
    r.addDirectoryWithQuotaFeature(
        DirectoryWithQuotaFeature.DEFAULT_NAMESPACE_QUOTA,
        DirectoryWithQuotaFeature.DEFAULT_DISKSPACE_QUOTA);
    r.addSnapshottableFeature();
    r.setSnapshotQuota(0);
    return r;
  }

  @VisibleForTesting
  static boolean CHECK_RESERVED_FILE_NAMES = true;
  public final static String DOT_RESERVED_STRING = ".reserved";
  public final static String DOT_RESERVED_PATH_PREFIX = Path.SEPARATOR
      + DOT_RESERVED_STRING;
  public final static byte[] DOT_RESERVED = 
      DFSUtil.string2Bytes(DOT_RESERVED_STRING);
  private final static String RAW_STRING = "raw";
  private final static byte[] RAW = DFSUtil.string2Bytes(RAW_STRING);
  public final static String DOT_INODES_STRING = ".inodes";
  public final static byte[] DOT_INODES = 
      DFSUtil.string2Bytes(DOT_INODES_STRING);
  private final XAttr KEYID_XATTR =
      XAttrHelper.buildXAttr(CRYPTO_XATTR_ENCRYPTION_ZONE, null);
  private final XAttr UNREADABLE_BY_SUPERUSER_XATTR =
      XAttrHelper.buildXAttr(SECURITY_XATTR_UNREADABLE_BY_SUPERUSER, null);

  INodeDirectory rootDir;
  private final FSNamesystem namesystem;
  private volatile boolean skipQuotaCheck = false; //skip while consuming edits
  private final int maxComponentLength;
  private final int maxDirItems;
  private final int lsLimit;  // max list limit
  private final int contentCountLimit; // max content summary counts per run
  private final INodeMap inodeMap; // Synchronized by dirLock
  private long yieldCount = 0; // keep track of lock yield count.
  private final int inodeXAttrsLimit; //inode xattrs max limit

  // lock to protect the directory and BlockMap
  private final ReentrantReadWriteLock dirLock;

  // utility methods to acquire and release read lock and write lock
  void readLock() {
    this.dirLock.readLock().lock();
  }

  void readUnlock() {
    this.dirLock.readLock().unlock();
  }

  void writeLock() {
    this.dirLock.writeLock().lock();
  }

  void writeUnlock() {
    this.dirLock.writeLock().unlock();
  }

  boolean hasWriteLock() {
    return this.dirLock.isWriteLockedByCurrentThread();
  }

  boolean hasReadLock() {
    return this.dirLock.getReadHoldCount() > 0 || hasWriteLock();
  }

  public int getReadHoldCount() {
    return this.dirLock.getReadHoldCount();
  }

  public int getWriteHoldCount() {
    return this.dirLock.getWriteHoldCount();
  }

  @VisibleForTesting
  public final EncryptionZoneManager ezManager;

  /**
   * Caches frequently used file names used in {@link INode} to reuse 
   * byte[] objects and reduce heap usage.
   */
  private final NameCache<ByteArray> nameCache;

  FSDirectory(FSNamesystem ns, Configuration conf) {
    this.dirLock = new ReentrantReadWriteLock(true); // fair
    rootDir = createRoot(ns);
    inodeMap = INodeMap.newInstance(rootDir);
    int configuredLimit = conf.getInt(
        DFSConfigKeys.DFS_LIST_LIMIT, DFSConfigKeys.DFS_LIST_LIMIT_DEFAULT);
    this.lsLimit = configuredLimit>0 ?
        configuredLimit : DFSConfigKeys.DFS_LIST_LIMIT_DEFAULT;
    this.contentCountLimit = conf.getInt(
        DFSConfigKeys.DFS_CONTENT_SUMMARY_LIMIT_KEY,
        DFSConfigKeys.DFS_CONTENT_SUMMARY_LIMIT_DEFAULT);
    
    // filesystem limits
    this.maxComponentLength = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_DEFAULT);
    this.maxDirItems = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_DEFAULT);
    this.inodeXAttrsLimit = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_DEFAULT);

    Preconditions.checkArgument(this.inodeXAttrsLimit >= 0,
        "Cannot set a negative limit on the number of xattrs per inode (%s).",
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY);
    // We need a maximum maximum because by default, PB limits message sizes
    // to 64MB. This means we can only store approximately 6.7 million entries
    // per directory, but let's use 6.4 million for some safety.
    final int MAX_DIR_ITEMS = 64 * 100 * 1000;
    Preconditions.checkArgument(
        maxDirItems > 0 && maxDirItems <= MAX_DIR_ITEMS, "Cannot set "
            + DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY
            + " to a value less than 0 or greater than " + MAX_DIR_ITEMS);

    int threshold = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_NAME_CACHE_THRESHOLD_KEY,
        DFSConfigKeys.DFS_NAMENODE_NAME_CACHE_THRESHOLD_DEFAULT);
    NameNode.LOG.info("Caching file names occuring more than " + threshold
        + " times");
    nameCache = new NameCache<ByteArray>(threshold);
    namesystem = ns;

    ezManager = new EncryptionZoneManager(this, conf);
  }
    
  private FSNamesystem getFSNamesystem() {
    return namesystem;
  }

  private BlockManager getBlockManager() {
    return getFSNamesystem().getBlockManager();
  }

  /** @return the root directory inode. */
  public INodeDirectory getRoot() {
    return rootDir;
  }

  /**
   * Shutdown the filestore
   */
  @Override
  public void close() throws IOException {}

  void markNameCacheInitialized() {
    writeLock();
    try {
      nameCache.initialized();
    } finally {
      writeUnlock();
    }
  }

  /** Enable quota verification */
  void enableQuotaChecks() {
    skipQuotaCheck = false;
  }

  /** Disable quota verification */
  void disableQuotaChecks() {
    skipQuotaCheck = true;
  }

  private static INodeFile newINodeFile(long id, PermissionStatus permissions,
      long mtime, long atime, short replication, long preferredBlockSize) {
    return newINodeFile(id, permissions, mtime, atime, replication, preferredBlockSize,
        (byte)0);
  }

  private static INodeFile newINodeFile(long id, PermissionStatus permissions,
      long mtime, long atime, short replication, long preferredBlockSize,
      byte storagePolicyId) {
    return new INodeFile(id, null, permissions, mtime, atime,
        BlockInfo.EMPTY_ARRAY, replication, preferredBlockSize,
        storagePolicyId);
  }

  /**
   * Add the given filename to the fs.
   * @throws FileAlreadyExistsException
   * @throws QuotaExceededException
   * @throws UnresolvedLinkException
   * @throws SnapshotAccessControlException 
   */
  INodeFile addFile(String path, PermissionStatus permissions,
                    short replication, long preferredBlockSize,
                    String clientName, String clientMachine)
    throws FileAlreadyExistsException, QuotaExceededException,
      UnresolvedLinkException, SnapshotAccessControlException, AclException {

    long modTime = now();
    INodeFile newNode = newINodeFile(namesystem.allocateNewInodeId(),
        permissions, modTime, modTime, replication, preferredBlockSize);
    newNode.toUnderConstruction(clientName, clientMachine);

    boolean added = false;
    writeLock();
    try {
      added = addINode(path, newNode);
    } finally {
      writeUnlock();
    }
    if (!added) {
      NameNode.stateChangeLog.info("DIR* addFile: failed to add " + path);
      return null;
    }

    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* addFile: " + path + " is added");
    }
    return newNode;
  }

  INodeFile unprotectedAddFile( long id,
                            String path, 
                            PermissionStatus permissions,
                            List<AclEntry> aclEntries,
                            List<XAttr> xAttrs,
                            short replication,
                            long modificationTime,
                            long atime,
                            long preferredBlockSize,
                            boolean underConstruction,
                            String clientName,
                            String clientMachine,
                            byte storagePolicyId) {
    final INodeFile newNode;
    assert hasWriteLock();
    if (underConstruction) {
      newNode = newINodeFile(id, permissions, modificationTime,
          modificationTime, replication, preferredBlockSize, storagePolicyId);
      newNode.toUnderConstruction(clientName, clientMachine);

    } else {
      newNode = newINodeFile(id, permissions, modificationTime, atime,
          replication, preferredBlockSize, storagePolicyId);
    }

    try {
      if (addINode(path, newNode)) {
        if (aclEntries != null) {
          AclStorage.updateINodeAcl(newNode, aclEntries,
            Snapshot.CURRENT_STATE_ID);
        }
        if (xAttrs != null) {
          XAttrStorage.updateINodeXAttrs(newNode, xAttrs,
              Snapshot.CURRENT_STATE_ID);
        }
        return newNode;
      }
    } catch (IOException e) {
      if(NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug(
            "DIR* FSDirectory.unprotectedAddFile: exception when add " + path
                + " to the file system", e);
      }
    }
    return null;
  }

  /**
   * Add a block to the file. Returns a reference to the added block.
   */
  BlockInfo addBlock(String path, INodesInPath inodesInPath, Block block,
      DatanodeStorageInfo[] targets) throws IOException {
    writeLock();
    try {
      final INodeFile fileINode = inodesInPath.getLastINode().asFile();
      Preconditions.checkState(fileINode.isUnderConstruction());

      // check quota limits and updated space consumed
      updateCount(inodesInPath, 0, fileINode.getBlockDiskspace(), true);

      // associate new last block for the file
      BlockInfoUnderConstruction blockInfo =
        new BlockInfoUnderConstruction(
            block,
            fileINode.getFileReplication(),
            BlockUCState.UNDER_CONSTRUCTION,
            targets);
      getBlockManager().addBlockCollection(blockInfo, fileINode);
      fileINode.addBlock(blockInfo);

      if(NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* FSDirectory.addBlock: "
            + path + " with " + block
            + " block is added to the in-memory "
            + "file system");
      }
      return blockInfo;
    } finally {
      writeUnlock();
    }
  }

  /**
   * Remove a block from the file.
   * @return Whether the block exists in the corresponding file
   */
  boolean removeBlock(String path, INodeFile fileNode, Block block)
      throws IOException {
    Preconditions.checkArgument(fileNode.isUnderConstruction());
    writeLock();
    try {
      return unprotectedRemoveBlock(path, fileNode, block);
    } finally {
      writeUnlock();
    }
  }
  
  boolean unprotectedRemoveBlock(String path,
      INodeFile fileNode, Block block) throws IOException {
    // modify file-> block and blocksMap
    // fileNode should be under construction
    boolean removed = fileNode.removeLastBlock(block);
    if (!removed) {
      return false;
    }
    getBlockManager().removeBlockFromMap(block);

    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.removeBlock: "
          +path+" with "+block
          +" block is removed from the file system");
    }

    // update space consumed
    final INodesInPath iip = getINodesInPath4Write(path, true);
    updateCount(iip, 0, -fileNode.getBlockDiskspace(), true);
    return true;
  }

  /**
   * @throws SnapshotAccessControlException 
   * @see #unprotectedRenameTo(String, String, long)
   * @deprecated Use {@link #renameTo(String, String, long,
   *                                  BlocksMapUpdateInfo, Rename...)}
   */
  @Deprecated
  boolean renameTo(String src, String dst, long mtime)
      throws QuotaExceededException, UnresolvedLinkException, 
      FileAlreadyExistsException, SnapshotAccessControlException, IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.renameTo: "
          +src+" to "+dst);
    }
    writeLock();
    try {
      if (!unprotectedRenameTo(src, dst, mtime))
        return false;
    } finally {
      writeUnlock();
    }
    return true;
  }

  /**
   * @see #unprotectedRenameTo(String, String, long, Options.Rename...)
   */
  void renameTo(String src, String dst, long mtime,
      BlocksMapUpdateInfo collectedBlocks, Options.Rename... options)
      throws FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, QuotaExceededException,
      UnresolvedLinkException, IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.renameTo: " + src
          + " to " + dst);
    }
    writeLock();
    try {
      if (unprotectedRenameTo(src, dst, mtime, collectedBlocks, options)) {
        namesystem.incrDeletedFileCount(1);
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * Change a path name
   * 
   * @param src source path
   * @param dst destination path
   * @return true if rename succeeds; false otherwise
   * @throws QuotaExceededException if the operation violates any quota limit
   * @throws FileAlreadyExistsException if the src is a symlink that points to dst
   * @throws SnapshotAccessControlException if path is in RO snapshot
   * @deprecated See {@link #renameTo(String, String, long, BlocksMapUpdateInfo, Rename...)}
   */
  @Deprecated
  boolean unprotectedRenameTo(String src, String dst, long timestamp)
    throws QuotaExceededException, UnresolvedLinkException, 
    FileAlreadyExistsException, SnapshotAccessControlException, IOException {
    assert hasWriteLock();
    INodesInPath srcIIP = getINodesInPath4Write(src, false);
    final INode srcInode = srcIIP.getLastINode();
    try {
      validateRenameSource(src, srcIIP);
    } catch (SnapshotException e) {
      throw e;
    } catch (IOException ignored) {
      return false;
    }

    if (isDir(dst)) {
      dst += Path.SEPARATOR + new Path(src).getName();
    }

    // validate the destination
    if (dst.equals(src)) {
      return true;
    }

    try {
      validateRenameDestination(src, dst, srcInode);
    } catch (IOException ignored) {
      return false;
    }

    INodesInPath dstIIP = getINodesInPath4Write(dst, false);
    if (dstIIP.getLastINode() != null) {
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                                   +"failed to rename "+src+" to "+dst+ 
                                   " because destination exists");
      return false;
    }
    INode dstParent = dstIIP.getINode(-2);
    if (dstParent == null) {
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          +"failed to rename "+src+" to "+dst+ 
          " because destination's parent does not exist");
      return false;
    }
    
    ezManager.checkMoveValidity(srcIIP, dstIIP, src);
    // Ensure dst has quota to accommodate rename
    verifyFsLimitsForRename(srcIIP, dstIIP);
    verifyQuotaForRename(srcIIP.getINodes(), dstIIP.getINodes());

    RenameOperation tx = new RenameOperation(src, dst, srcIIP, dstIIP);

    boolean added = false;

    try {
      // remove src
      final long removedSrc = removeLastINode(srcIIP);
      if (removedSrc == -1) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
            + "failed to rename " + src + " to " + dst
            + " because the source can not be removed");
        return false;
      }

      added = tx.addSourceToDestination();
      if (added) {
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedRenameTo: " 
              + src + " is renamed to " + dst);
        }

        tx.updateMtimeAndLease(timestamp);
        tx.updateQuotasInSourceTree();
        
        return true;
      }
    } finally {
      if (!added) {
        tx.restoreSource();
      }
    }
    NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
        +"failed to rename "+src+" to "+dst);
    return false;
  }

  /**
   * Rename src to dst.
   * <br>
   * Note: This is to be used by {@link FSEditLog} only.
   * <br>
   * 
   * @param src source path
   * @param dst destination path
   * @param timestamp modification time
   * @param options Rename options
   */
  boolean unprotectedRenameTo(String src, String dst, long timestamp,
      Options.Rename... options) throws FileAlreadyExistsException, 
      FileNotFoundException, ParentNotDirectoryException, 
      QuotaExceededException, UnresolvedLinkException, IOException {
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    boolean ret = unprotectedRenameTo(src, dst, timestamp, 
        collectedBlocks, options);
    if (!collectedBlocks.getToDeleteList().isEmpty()) {
      getFSNamesystem().removeBlocksAndUpdateSafemodeTotal(collectedBlocks);
    }
    return ret;
  }
  
  /**
   * Rename src to dst.
   * See {@link DistributedFileSystem#rename(Path, Path, Options.Rename...)}
   * for details related to rename semantics and exceptions.
   * 
   * @param src source path
   * @param dst destination path
   * @param timestamp modification time
   * @param collectedBlocks blocks to be removed
   * @param options Rename options
   */
  boolean unprotectedRenameTo(String src, String dst, long timestamp,
      BlocksMapUpdateInfo collectedBlocks, Options.Rename... options) 
      throws FileAlreadyExistsException, FileNotFoundException, 
      ParentNotDirectoryException, QuotaExceededException, 
      UnresolvedLinkException, IOException {
    assert hasWriteLock();
    boolean overwrite = options != null && Arrays.asList(options).contains
            (Rename.OVERWRITE);

    final String error;
    final INodesInPath srcIIP = getINodesInPath4Write(src, false);
    final INode srcInode = srcIIP.getLastINode();
    validateRenameSource(src, srcIIP);

    // validate the destination
    if (dst.equals(src)) {
      throw new FileAlreadyExistsException(
          "The source "+src+" and destination "+dst+" are the same");
    }
    validateRenameDestination(src, dst, srcInode);

    INodesInPath dstIIP = getINodesInPath4Write(dst, false);
    if (dstIIP.getINodes().length == 1) {
      error = "rename destination cannot be the root";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new IOException(error);
    }

    ezManager.checkMoveValidity(srcIIP, dstIIP, src);
    final INode dstInode = dstIIP.getLastINode();
    List<INodeDirectory> snapshottableDirs = new ArrayList<INodeDirectory>();
    if (dstInode != null) { // Destination exists
      validateRenameOverwrite(src, dst, overwrite, srcInode, dstInode);
      checkSnapshot(dstInode, snapshottableDirs);
    }

    INode dstParent = dstIIP.getINode(-2);
    if (dstParent == null) {
      error = "rename destination parent " + dst + " not found.";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new FileNotFoundException(error);
    }
    if (!dstParent.isDirectory()) {
      error = "rename destination parent " + dst + " is a file.";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new ParentNotDirectoryException(error);
    }

    // Ensure dst has quota to accommodate rename
    verifyFsLimitsForRename(srcIIP, dstIIP);
    verifyQuotaForRename(srcIIP.getINodes(), dstIIP.getINodes());

    RenameOperation tx = new RenameOperation(src, dst, srcIIP, dstIIP);

    boolean undoRemoveSrc = true;
    final long removedSrc = removeLastINode(srcIIP);
    if (removedSrc == -1) {
      error = "Failed to rename " + src + " to " + dst
          + " because the source can not be removed";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new IOException(error);
    }
    
    boolean undoRemoveDst = false;
    INode removedDst = null;
    long removedNum = 0;
    try {
      if (dstInode != null) { // dst exists remove it
        if ((removedNum = removeLastINode(dstIIP)) != -1) {
          removedDst = dstIIP.getLastINode();
          undoRemoveDst = true;
        }
      }

      // add src as dst to complete rename
      if (tx.addSourceToDestination()) {
        undoRemoveSrc = false;
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug(
              "DIR* FSDirectory.unprotectedRenameTo: " + src
              + " is renamed to " + dst);
        }

        tx.updateMtimeAndLease(timestamp);

        // Collect the blocks and remove the lease for previous dst
        boolean filesDeleted = false;
        if (removedDst != null) {
          undoRemoveDst = false;
          if (removedNum > 0) {
            List<INode> removedINodes = new ChunkedArrayList<INode>();
            if (!removedDst.isInLatestSnapshot(dstIIP.getLatestSnapshotId())) {
              removedDst.destroyAndCollectBlocks(collectedBlocks, removedINodes);
              filesDeleted = true;
            } else {
              filesDeleted = removedDst.cleanSubtree(Snapshot.CURRENT_STATE_ID,
                  dstIIP.getLatestSnapshotId(), collectedBlocks, removedINodes,
                  true).get(Quota.NAMESPACE) >= 0;
            }
            getFSNamesystem().removePathAndBlocks(src, null, 
                removedINodes, false);
          }
        }

        if (snapshottableDirs.size() > 0) {
          // There are snapshottable directories (without snapshots) to be
          // deleted. Need to update the SnapshotManager.
          namesystem.removeSnapshottableDirs(snapshottableDirs);
        }

        tx.updateQuotasInSourceTree();
        return filesDeleted;
      }
    } finally {
      if (undoRemoveSrc) {
        tx.restoreSource();
      }

      if (undoRemoveDst) {
        // Rename failed - restore dst
        if (dstParent.isDirectory() && dstParent.asDirectory().isWithSnapshot()) {
          dstParent.asDirectory().undoRename4DstParent(removedDst,
              dstIIP.getLatestSnapshotId());
        } else {
          addLastINodeNoQuotaCheck(dstIIP, removedDst);
        }
        if (removedDst.isReference()) {
          final INodeReference removedDstRef = removedDst.asReference();
          final INodeReference.WithCount wc = 
              (WithCount) removedDstRef.getReferredINode().asReference();
          wc.addReference(removedDstRef);
        }
      }
    }
    NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
        + "failed to rename " + src + " to " + dst);
    throw new IOException("rename from " + src + " to " + dst + " failed.");
  }

  private static void validateRenameOverwrite(String src, String dst,
                                              boolean overwrite,
                                              INode srcInode, INode dstInode)
          throws IOException {
    String error;// It's OK to rename a file to a symlink and vice versa
    if (dstInode.isDirectory() != srcInode.isDirectory()) {
      error = "Source " + src + " and destination " + dst
          + " must both be directories";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new IOException(error);
    }
    if (!overwrite) { // If destination exists, overwrite flag must be true
      error = "rename destination " + dst + " already exists";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new FileAlreadyExistsException(error);
    }
    if (dstInode.isDirectory()) {
      final ReadOnlyList<INode> children = dstInode.asDirectory()
          .getChildrenList(Snapshot.CURRENT_STATE_ID);
      if (!children.isEmpty()) {
        error = "rename destination directory is not empty: " + dst;
        NameNode.stateChangeLog.warn(
            "DIR* FSDirectory.unprotectedRenameTo: " + error);
        throw new IOException(error);
      }
    }
  }

  private static void validateRenameDestination(String src, String dst, INode srcInode)
          throws IOException {
    String error;
    if (srcInode.isSymlink() &&
        dst.equals(srcInode.asSymlink().getSymlinkString())) {
      throw new FileAlreadyExistsException(
          "Cannot rename symlink "+src+" to its target "+dst);
    }
    // dst cannot be a directory or a file under src
    if (dst.startsWith(src) &&
        dst.charAt(src.length()) == Path.SEPARATOR_CHAR) {
      error = "Rename destination " + dst
          + " is a directory or file under source " + src;
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new IOException(error);
    }
  }

  private static void validateRenameSource(String src, INodesInPath srcIIP)
          throws IOException {
    String error;
    final INode srcInode = srcIIP.getLastINode();
    // validate source
    if (srcInode == null) {
      error = "rename source " + src + " is not found.";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new FileNotFoundException(error);
    }
    if (srcIIP.getINodes().length == 1) {
      error = "rename source cannot be the root";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new IOException(error);
    }
    // srcInode and its subtree cannot contain snapshottable directories with
    // snapshots
    checkSnapshot(srcInode, null);
  }

  private class RenameOperation {
    private final INodesInPath srcIIP;
    private final INodesInPath dstIIP;
    private final String src;
    private final String dst;

    private INode srcChild;
    private final INodeReference.WithCount withCount;
    private final int srcRefDstSnapshot;
    private final INodeDirectory srcParent;
    private final byte[] srcChildName;
    private final boolean isSrcInSnapshot;
    private final boolean srcChildIsReference;
    private final Quota.Counts oldSrcCounts;

    private RenameOperation(String src, String dst, INodesInPath srcIIP, INodesInPath dstIIP)
            throws QuotaExceededException {
      this.srcIIP = srcIIP;
      this.dstIIP = dstIIP;
      this.src = src;
      this.dst = dst;
      srcChild = srcIIP.getLastINode();
      srcChildName = srcChild.getLocalNameBytes();
      isSrcInSnapshot = srcChild.isInLatestSnapshot(
              srcIIP.getLatestSnapshotId());
      srcChildIsReference = srcChild.isReference();
      srcParent = srcIIP.getINode(-2).asDirectory();

      // Record the snapshot on srcChild. After the rename, before any new
      // snapshot is taken on the dst tree, changes will be recorded in the latest
      // snapshot of the src tree.
      if (isSrcInSnapshot) {
        srcChild.recordModification(srcIIP.getLatestSnapshotId());
      }

      // check srcChild for reference
      srcRefDstSnapshot = srcChildIsReference ? srcChild.asReference()
              .getDstSnapshotId() : Snapshot.CURRENT_STATE_ID;
      oldSrcCounts = Quota.Counts.newInstance();
      if (isSrcInSnapshot) {
        final INodeReference.WithName withName = srcIIP.getINode(-2).asDirectory()
                .replaceChild4ReferenceWithName(srcChild, srcIIP.getLatestSnapshotId());
        withCount = (INodeReference.WithCount) withName.getReferredINode();
        srcChild = withName;
        srcIIP.setLastINode(srcChild);
        // get the counts before rename
        withCount.getReferredINode().computeQuotaUsage(oldSrcCounts, true);
      } else if (srcChildIsReference) {
        // srcChild is reference but srcChild is not in latest snapshot
        withCount = (WithCount) srcChild.asReference().getReferredINode();
      } else {
        withCount = null;
      }
    }

    boolean addSourceToDestination() {
      final INode dstParent = dstIIP.getINode(-2);
      srcChild = srcIIP.getLastINode();
      final byte[] dstChildName = dstIIP.getLastLocalName();
      final INode toDst;
      if (withCount == null) {
        srcChild.setLocalName(dstChildName);
        toDst = srcChild;
      } else {
        withCount.getReferredINode().setLocalName(dstChildName);
        int dstSnapshotId = dstIIP.getLatestSnapshotId();
        toDst = new INodeReference.DstReference(
                dstParent.asDirectory(), withCount, dstSnapshotId);
      }
      return addLastINodeNoQuotaCheck(dstIIP, toDst);
    }

    void updateMtimeAndLease(long timestamp) throws QuotaExceededException {
      srcParent.updateModificationTime(timestamp, srcIIP.getLatestSnapshotId());
      final INode dstParent = dstIIP.getINode(-2);
      dstParent.updateModificationTime(timestamp, dstIIP.getLatestSnapshotId());
      // update moved lease with new filename
      getFSNamesystem().unprotectedChangeLease(src, dst);
    }

    void restoreSource() throws QuotaExceededException {
      // Rename failed - restore src
      final INode oldSrcChild = srcChild;
      // put it back
      if (withCount == null) {
        srcChild.setLocalName(srcChildName);
      } else if (!srcChildIsReference) { // src must be in snapshot
        // the withCount node will no longer be used thus no need to update
        // its reference number here
        srcChild = withCount.getReferredINode();
        srcChild.setLocalName(srcChildName);
      } else {
        withCount.removeReference(oldSrcChild.asReference());
        srcChild = new INodeReference.DstReference(
                srcParent, withCount, srcRefDstSnapshot);
        withCount.getReferredINode().setLocalName(srcChildName);
      }

      if (isSrcInSnapshot) {
        srcParent.undoRename4ScrParent(oldSrcChild.asReference(), srcChild);
      } else {
        // srcParent is not an INodeDirectoryWithSnapshot, we only need to add
        // the srcChild back
        addLastINodeNoQuotaCheck(srcIIP, srcChild);
      }
    }

    void updateQuotasInSourceTree() throws QuotaExceededException {
      // update the quota usage in src tree
      if (isSrcInSnapshot) {
        // get the counts after rename
        Quota.Counts newSrcCounts = srcChild.computeQuotaUsage(
                Quota.Counts.newInstance(), false);
        newSrcCounts.subtract(oldSrcCounts);
        srcParent.addSpaceConsumed(newSrcCounts.get(Quota.NAMESPACE),
                newSrcCounts.get(Quota.DISKSPACE), false);
      }
    }
  }

  /**
   * Set file replication
   * 
   * @param src file name
   * @param replication new replication
   * @param blockRepls block replications - output parameter
   * @return array of file blocks
   * @throws QuotaExceededException
   * @throws SnapshotAccessControlException 
   */
  Block[] setReplication(String src, short replication, short[] blockRepls)
      throws QuotaExceededException, UnresolvedLinkException,
      SnapshotAccessControlException {
    writeLock();
    try {
      return unprotectedSetReplication(src, replication, blockRepls);
    } finally {
      writeUnlock();
    }
  }

  Block[] unprotectedSetReplication(String src, short replication,
      short[] blockRepls) throws QuotaExceededException,
      UnresolvedLinkException, SnapshotAccessControlException {
    assert hasWriteLock();

    final INodesInPath iip = getINodesInPath4Write(src, true);
    final INode inode = iip.getLastINode();
    if (inode == null || !inode.isFile()) {
      return null;
    }
    INodeFile file = inode.asFile();
    final short oldBR = file.getBlockReplication();

    // before setFileReplication, check for increasing block replication.
    // if replication > oldBR, then newBR == replication.
    // if replication < oldBR, we don't know newBR yet. 
    if (replication > oldBR) {
      long dsDelta = (replication - oldBR)*(file.diskspaceConsumed()/oldBR);
      updateCount(iip, 0, dsDelta, true);
    }

    file.setFileReplication(replication, iip.getLatestSnapshotId());
    
    final short newBR = file.getBlockReplication(); 
    // check newBR < oldBR case. 
    if (newBR < oldBR) {
      long dsDelta = (newBR - oldBR)*(file.diskspaceConsumed()/newBR);
      updateCount(iip, 0, dsDelta, true);
    }

    if (blockRepls != null) {
      blockRepls[0] = oldBR;
      blockRepls[1] = newBR;
    }
    return file.getBlocks();
  }

  /** Set block storage policy for a directory */
  void setStoragePolicy(String src, byte policyId)
      throws IOException {
    writeLock();
    try {
      unprotectedSetStoragePolicy(src, policyId);
    } finally {
      writeUnlock();
    }
  }

  void unprotectedSetStoragePolicy(String src, byte policyId)
      throws IOException {
    assert hasWriteLock();
    final INodesInPath iip = getINodesInPath4Write(src, true);
    final INode inode = iip.getLastINode();
    if (inode == null) {
      throw new FileNotFoundException("File/Directory does not exist: " + src);
    }
    final int snapshotId = iip.getLatestSnapshotId();
    if (inode.isFile()) {
      BlockStoragePolicy newPolicy = getBlockManager().getStoragePolicy(policyId);
      if (newPolicy.isCopyOnCreateFile()) {
        throw new HadoopIllegalArgumentException(
            "Policy " + newPolicy + " cannot be set after file creation.");
      }

      BlockStoragePolicy currentPolicy =
          getBlockManager().getStoragePolicy(inode.getLocalStoragePolicyID());

      if (currentPolicy != null && currentPolicy.isCopyOnCreateFile()) {
        throw new HadoopIllegalArgumentException(
            "Existing policy " + currentPolicy.getName() +
                " cannot be changed after file creation.");
      }
      inode.asFile().setStoragePolicyID(policyId, snapshotId);
    } else if (inode.isDirectory()) {
      setDirStoragePolicy(inode.asDirectory(), policyId, snapshotId);  
    } else {
      throw new FileNotFoundException(src + " is not a file or directory");
    }
  }

  private void setDirStoragePolicy(INodeDirectory inode, byte policyId,
      int latestSnapshotId) throws IOException {
    List<XAttr> existingXAttrs = XAttrStorage.readINodeXAttrs(inode);
    XAttr xAttr = BlockStoragePolicySuite.buildXAttr(policyId);
    List<XAttr> newXAttrs = setINodeXAttrs(existingXAttrs, Arrays.asList(xAttr),
        EnumSet.of(XAttrSetFlag.CREATE, XAttrSetFlag.REPLACE));
    XAttrStorage.updateINodeXAttrs(inode, newXAttrs, latestSnapshotId);
  }

  /**
   * @param path the file path
   * @return the block size of the file. 
   */
  long getPreferredBlockSize(String path) throws UnresolvedLinkException,
      FileNotFoundException, IOException {
    readLock();
    try {
      return INodeFile.valueOf(getNode(path, false), path
          ).getPreferredBlockSize();
    } finally {
      readUnlock();
    }
  }

  void setPermission(String src, FsPermission permission)
      throws FileNotFoundException, UnresolvedLinkException,
      QuotaExceededException, SnapshotAccessControlException {
    writeLock();
    try {
      unprotectedSetPermission(src, permission);
    } finally {
      writeUnlock();
    }
  }
  
  void unprotectedSetPermission(String src, FsPermission permissions)
      throws FileNotFoundException, UnresolvedLinkException,
      QuotaExceededException, SnapshotAccessControlException {
    assert hasWriteLock();
    final INodesInPath inodesInPath = getINodesInPath4Write(src, true);
    final INode inode = inodesInPath.getLastINode();
    if (inode == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }
    int snapshotId = inodesInPath.getLatestSnapshotId();
    inode.setPermission(permissions, snapshotId);
  }

  void setOwner(String src, String username, String groupname)
      throws FileNotFoundException, UnresolvedLinkException,
      QuotaExceededException, SnapshotAccessControlException {
    writeLock();
    try {
      unprotectedSetOwner(src, username, groupname);
    } finally {
      writeUnlock();
    }
  }

  void unprotectedSetOwner(String src, String username, String groupname)
      throws FileNotFoundException, UnresolvedLinkException,
      QuotaExceededException, SnapshotAccessControlException {
    assert hasWriteLock();
    final INodesInPath inodesInPath = getINodesInPath4Write(src, true);
    INode inode = inodesInPath.getLastINode();
    if (inode == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }
    if (username != null) {
      inode = inode.setUser(username, inodesInPath.getLatestSnapshotId());
    }
    if (groupname != null) {
      inode.setGroup(groupname, inodesInPath.getLatestSnapshotId());
    }
  }

  /**
   * Concat all the blocks from srcs to trg and delete the srcs files
   */
  void concat(String target, String[] srcs, long timestamp)
      throws UnresolvedLinkException, QuotaExceededException,
      SnapshotAccessControlException, SnapshotException {
    writeLock();
    try {
      // actual move
      unprotectedConcat(target, srcs, timestamp);
    } finally {
      writeUnlock();
    }
  }

  /**
   * Concat all the blocks from srcs to trg and delete the srcs files
   * @param target target file to move the blocks to
   * @param srcs list of file to move the blocks from
   */
  void unprotectedConcat(String target, String [] srcs, long timestamp) 
      throws UnresolvedLinkException, QuotaExceededException,
      SnapshotAccessControlException, SnapshotException {
    assert hasWriteLock();
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSNamesystem.concat to "+target);
    }
    // do the move
    
    final INodesInPath trgIIP = getINodesInPath4Write(target, true);
    final INode[] trgINodes = trgIIP.getINodes();
    final INodeFile trgInode = trgIIP.getLastINode().asFile();
    INodeDirectory trgParent = trgINodes[trgINodes.length-2].asDirectory();
    final int trgLatestSnapshot = trgIIP.getLatestSnapshotId();
    
    final INodeFile [] allSrcInodes = new INodeFile[srcs.length];
    for(int i = 0; i < srcs.length; i++) {
      final INodesInPath iip = getINodesInPath4Write(srcs[i]);
      final int latest = iip.getLatestSnapshotId();
      final INode inode = iip.getLastINode();

      // check if the file in the latest snapshot
      if (inode.isInLatestSnapshot(latest)) {
        throw new SnapshotException("Concat: the source file " + srcs[i]
            + " is in snapshot " + latest);
      }

      // check if the file has other references.
      if (inode.isReference() && ((INodeReference.WithCount)
          inode.asReference().getReferredINode()).getReferenceCount() > 1) {
        throw new SnapshotException("Concat: the source file " + srcs[i]
            + " is referred by some other reference in some snapshot.");
      }

      allSrcInodes[i] = inode.asFile();
    }
    trgInode.concatBlocks(allSrcInodes);
    
    // since we are in the same dir - we can use same parent to remove files
    int count = 0;
    for(INodeFile nodeToRemove: allSrcInodes) {
      if(nodeToRemove == null) continue;
      
      nodeToRemove.setBlocks(null);
      trgParent.removeChild(nodeToRemove, trgLatestSnapshot);
      inodeMap.remove(nodeToRemove);
      count++;
    }
    
    trgInode.setModificationTime(timestamp, trgLatestSnapshot);
    trgParent.updateModificationTime(timestamp, trgLatestSnapshot);
    // update quota on the parent directory ('count' files removed, 0 space)
    unprotectedUpdateCount(trgIIP, trgINodes.length-1, -count, 0);
  }

  /**
   * Delete the target directory and collect the blocks under it
   * 
   * @param src Path of a directory to delete
   * @param collectedBlocks Blocks under the deleted directory
   * @param removedINodes INodes that should be removed from {@link #inodeMap}
   * @return the number of files that have been removed
   */
  long delete(String src, BlocksMapUpdateInfo collectedBlocks,
              List<INode> removedINodes, long mtime) throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.delete: " + src);
    }
    final long filesRemoved;
    writeLock();
    try {
      final INodesInPath inodesInPath = getINodesInPath4Write(
          normalizePath(src), false);
      if (!deleteAllowed(inodesInPath, src) ) {
        filesRemoved = -1;
      } else {
        List<INodeDirectory> snapshottableDirs = new ArrayList<INodeDirectory>();
        checkSnapshot(inodesInPath.getLastINode(), snapshottableDirs);
        filesRemoved = unprotectedDelete(inodesInPath, collectedBlocks,
            removedINodes, mtime);
        namesystem.removeSnapshottableDirs(snapshottableDirs);
      }
    } finally {
      writeUnlock();
    }
    return filesRemoved;
  }
  
  private static boolean deleteAllowed(final INodesInPath iip,
      final String src) {
    final INode[] inodes = iip.getINodes(); 
    if (inodes == null || inodes.length == 0
        || inodes[inodes.length - 1] == null) {
      if(NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedDelete: "
            + "failed to remove " + src + " because it does not exist");
      }
      return false;
    } else if (inodes.length == 1) { // src is the root
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedDelete: "
          + "failed to remove " + src
          + " because the root is not allowed to be deleted");
      return false;
    }
    return true;
  }
  
  /**
   * @return true if the path is a non-empty directory; otherwise, return false.
   */
  boolean isNonEmptyDirectory(String path) throws UnresolvedLinkException {
    readLock();
    try {
      final INodesInPath inodesInPath = getLastINodeInPath(path, false);
      final INode inode = inodesInPath.getINode(0);
      if (inode == null || !inode.isDirectory()) {
        //not found or not a directory
        return false;
      }
      final int s = inodesInPath.getPathSnapshotId();
      return !inode.asDirectory().getChildrenList(s).isEmpty();
    } finally {
      readUnlock();
    }
  }

  /**
   * Delete a path from the name space
   * Update the count at each ancestor directory with quota
   * <br>
   * Note: This is to be used by {@link FSEditLog} only.
   * <br>
   * @param src a string representation of a path to an inode
   * @param mtime the time the inode is removed
   * @throws SnapshotAccessControlException if path is in RO snapshot
   */
  void unprotectedDelete(String src, long mtime) throws UnresolvedLinkException,
      QuotaExceededException, SnapshotAccessControlException, IOException {
    assert hasWriteLock();
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    List<INode> removedINodes = new ChunkedArrayList<INode>();

    final INodesInPath inodesInPath = getINodesInPath4Write(
        normalizePath(src), false);
    long filesRemoved = -1;
    if (deleteAllowed(inodesInPath, src)) {
      List<INodeDirectory> snapshottableDirs = new ArrayList<INodeDirectory>();
      checkSnapshot(inodesInPath.getLastINode(), snapshottableDirs);
      filesRemoved = unprotectedDelete(inodesInPath, collectedBlocks,
          removedINodes, mtime);
      namesystem.removeSnapshottableDirs(snapshottableDirs); 
    }

    if (filesRemoved >= 0) {
      getFSNamesystem().removePathAndBlocks(src, collectedBlocks, 
          removedINodes, false);
    }
  }
  
  /**
   * Delete a path from the name space
   * Update the count at each ancestor directory with quota
   * @param iip the inodes resolved from the path
   * @param collectedBlocks blocks collected from the deleted path
   * @param removedINodes inodes that should be removed from {@link #inodeMap}
   * @param mtime the time the inode is removed
   * @return the number of inodes deleted; 0 if no inodes are deleted.
   */ 
  long unprotectedDelete(INodesInPath iip, BlocksMapUpdateInfo collectedBlocks,
      List<INode> removedINodes, long mtime) throws QuotaExceededException {
    assert hasWriteLock();

    // check if target node exists
    INode targetNode = iip.getLastINode();
    if (targetNode == null) {
      return -1;
    }

    // record modification
    final int latestSnapshot = iip.getLatestSnapshotId();
    targetNode.recordModification(latestSnapshot);

    // Remove the node from the namespace
    long removed = removeLastINode(iip);
    if (removed == -1) {
      return -1;
    }

    // set the parent's modification time
    final INodeDirectory parent = targetNode.getParent();
    parent.updateModificationTime(mtime, latestSnapshot);
    if (removed == 0) {
      return 0;
    }
    
    // collect block
    if (!targetNode.isInLatestSnapshot(latestSnapshot)) {
      targetNode.destroyAndCollectBlocks(collectedBlocks, removedINodes);
    } else {
      Quota.Counts counts = targetNode.cleanSubtree(Snapshot.CURRENT_STATE_ID,
          latestSnapshot, collectedBlocks, removedINodes, true);
      parent.addSpaceConsumed(-counts.get(Quota.NAMESPACE),
          -counts.get(Quota.DISKSPACE), true);
      removed = counts.get(Quota.NAMESPACE);
    }
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedDelete: "
          + targetNode.getFullPathName() + " is removed");
    }
    return removed;
  }
  
  /**
   * Check if the given INode (or one of its descendants) is snapshottable and
   * already has snapshots.
   * 
   * @param target The given INode
   * @param snapshottableDirs The list of directories that are snapshottable 
   *                          but do not have snapshots yet
   */
  private static void checkSnapshot(INode target,
      List<INodeDirectory> snapshottableDirs) throws SnapshotException {
    if (target.isDirectory()) {
      INodeDirectory targetDir = target.asDirectory();
      DirectorySnapshottableFeature sf = targetDir
          .getDirectorySnapshottableFeature();
      if (sf != null) {
        if (sf.getNumSnapshots() > 0) {
          String fullPath = targetDir.getFullPathName();
          throw new SnapshotException("The directory " + fullPath
              + " cannot be deleted since " + fullPath
              + " is snapshottable and already has snapshots");
        } else {
          if (snapshottableDirs != null) {
            snapshottableDirs.add(targetDir);
          }
        }
      } 
      for (INode child : targetDir.getChildrenList(Snapshot.CURRENT_STATE_ID)) {
        checkSnapshot(child, snapshottableDirs);
      }
    }
  }

  private byte getStoragePolicyID(byte inodePolicy, byte parentPolicy) {
    return inodePolicy != BlockStoragePolicySuite.ID_UNSPECIFIED ? inodePolicy :
        parentPolicy;
  }

  /**
   * Get a partial listing of the indicated directory
   *
   * We will stop when any of the following conditions is met:
   * 1) this.lsLimit files have been added
   * 2) needLocation is true AND enough files have been added such
   * that at least this.lsLimit block locations are in the response
   *
   * @param src the directory name
   * @param startAfter the name to start listing after
   * @param needLocation if block locations are returned
   * @return a partial listing starting after startAfter
   */
  DirectoryListing getListing(String src, byte[] startAfter,
      boolean needLocation, boolean isSuperUser)
      throws UnresolvedLinkException, IOException {
    String srcs = normalizePath(src);
    final boolean isRawPath = isReservedRawName(src);

    readLock();
    try {
      if (srcs.endsWith(HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR)) {
        return getSnapshotsListing(srcs, startAfter);
      }
      final INodesInPath inodesInPath = getINodesInPath(srcs, true);
      final INode[] inodes = inodesInPath.getINodes();
      final int snapshot = inodesInPath.getPathSnapshotId();
      final INode targetNode = inodes[inodes.length - 1];
      if (targetNode == null)
        return null;
      byte parentStoragePolicy = isSuperUser ?
          targetNode.getStoragePolicyID() : BlockStoragePolicySuite.ID_UNSPECIFIED;
      
      if (!targetNode.isDirectory()) {
        return new DirectoryListing(
            new HdfsFileStatus[]{createFileStatus(HdfsFileStatus.EMPTY_NAME,
                targetNode, needLocation, parentStoragePolicy, snapshot,
                isRawPath, inodesInPath)}, 0);
      }

      final INodeDirectory dirInode = targetNode.asDirectory();
      final ReadOnlyList<INode> contents = dirInode.getChildrenList(snapshot);
      int startChild = INodeDirectory.nextChild(contents, startAfter);
      int totalNumChildren = contents.size();
      int numOfListing = Math.min(totalNumChildren-startChild, this.lsLimit);
      int locationBudget = this.lsLimit;
      int listingCnt = 0;
      HdfsFileStatus listing[] = new HdfsFileStatus[numOfListing];
      for (int i=0; i<numOfListing && locationBudget>0; i++) {
        INode cur = contents.get(startChild+i);
        byte curPolicy = isSuperUser && !cur.isSymlink()?
            cur.getLocalStoragePolicyID():
            BlockStoragePolicySuite.ID_UNSPECIFIED;
        listing[i] = createFileStatus(cur.getLocalNameBytes(), cur, needLocation,
            getStoragePolicyID(curPolicy, parentStoragePolicy), snapshot,
            isRawPath, inodesInPath);
        listingCnt++;
        if (needLocation) {
            // Once we  hit lsLimit locations, stop.
            // This helps to prevent excessively large response payloads.
            // Approximate #locations with locatedBlockCount() * repl_factor
            LocatedBlocks blks = 
                ((HdfsLocatedFileStatus)listing[i]).getBlockLocations();
            locationBudget -= (blks == null) ? 0 :
               blks.locatedBlockCount() * listing[i].getReplication();
        }
      }
      // truncate return array if necessary
      if (listingCnt < numOfListing) {
          listing = Arrays.copyOf(listing, listingCnt);
      }
      return new DirectoryListing(
          listing, totalNumChildren-startChild-listingCnt);
    } finally {
      readUnlock();
    }
  }
  
  /**
   * Get a listing of all the snapshots of a snapshottable directory
   */
  private DirectoryListing getSnapshotsListing(String src, byte[] startAfter)
      throws UnresolvedLinkException, IOException {
    Preconditions.checkState(hasReadLock());
    Preconditions.checkArgument(
        src.endsWith(HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR),
        "%s does not end with %s", src, HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR);

    final String dirPath = normalizePath(src.substring(0,
        src.length() - HdfsConstants.DOT_SNAPSHOT_DIR.length()));
    
    final INode node = this.getINode(dirPath);
    final INodeDirectory dirNode = INodeDirectory.valueOf(node, dirPath);
    final DirectorySnapshottableFeature sf = dirNode.getDirectorySnapshottableFeature();
    if (sf == null) {
      throw new SnapshotException(
          "Directory is not a snapshottable directory: " + dirPath);
    }
    final ReadOnlyList<Snapshot> snapshots = sf.getSnapshotList();
    int skipSize = ReadOnlyList.Util.binarySearch(snapshots, startAfter);
    skipSize = skipSize < 0 ? -skipSize - 1 : skipSize + 1;
    int numOfListing = Math.min(snapshots.size() - skipSize, this.lsLimit);
    final HdfsFileStatus listing[] = new HdfsFileStatus[numOfListing];
    for (int i = 0; i < numOfListing; i++) {
      Root sRoot = snapshots.get(i + skipSize).getRoot();
      listing[i] = createFileStatus(sRoot.getLocalNameBytes(), sRoot,
          BlockStoragePolicySuite.ID_UNSPECIFIED, Snapshot.CURRENT_STATE_ID,
          false, null);
    }
    return new DirectoryListing(
        listing, snapshots.size() - skipSize - numOfListing);
  }

  /** Get the file info for a specific file.
   * @param src The string representation of the path to the file
   * @param resolveLink whether to throw UnresolvedLinkException
   * @param isRawPath true if a /.reserved/raw pathname was passed by the user
   * @param includeStoragePolicy whether to include storage policy
   * @return object containing information regarding the file
   *         or null if file not found
   */
  HdfsFileStatus getFileInfo(String src, boolean resolveLink,
      boolean isRawPath, boolean includeStoragePolicy)
    throws IOException {
    String srcs = normalizePath(src);
    readLock();
    try {
      if (srcs.endsWith(HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR)) {
        return getFileInfo4DotSnapshot(srcs);
      }
      final INodesInPath inodesInPath = getINodesInPath(srcs, resolveLink);
      final INode[] inodes = inodesInPath.getINodes();
      final INode i = inodes[inodes.length - 1];
      byte policyId = includeStoragePolicy && i != null && !i.isSymlink() ?
          i.getStoragePolicyID() : BlockStoragePolicySuite.ID_UNSPECIFIED;
      return i == null ? null : createFileStatus(HdfsFileStatus.EMPTY_NAME, i,
          policyId, inodesInPath.getPathSnapshotId(), isRawPath,
          inodesInPath);
    } finally {
      readUnlock();
    }
  }
  
  /**
   * Currently we only support "ls /xxx/.snapshot" which will return all the
   * snapshots of a directory. The FSCommand Ls will first call getFileInfo to
   * make sure the file/directory exists (before the real getListing call).
   * Since we do not have a real INode for ".snapshot", we return an empty
   * non-null HdfsFileStatus here.
   */
  private HdfsFileStatus getFileInfo4DotSnapshot(String src)
      throws UnresolvedLinkException {
    if (getINode4DotSnapshot(src) != null) {
      return new HdfsFileStatus(0, true, 0, 0, 0, 0, null, null, null, null,
          HdfsFileStatus.EMPTY_NAME, -1L, 0, null,
          BlockStoragePolicySuite.ID_UNSPECIFIED);
    }
    return null;
  }

  private INode getINode4DotSnapshot(String src) throws UnresolvedLinkException {
    Preconditions.checkArgument(
        src.endsWith(HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR),
        "%s does not end with %s", src, HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR);
    
    final String dirPath = normalizePath(src.substring(0,
        src.length() - HdfsConstants.DOT_SNAPSHOT_DIR.length()));
    
    final INode node = this.getINode(dirPath);
    if (node != null && node.isDirectory()
        && node.asDirectory().isSnapshottable()) {
      return node;
    }
    return null;
  }

  INodesInPath getExistingPathINodes(byte[][] components)
      throws UnresolvedLinkException {
    return INodesInPath.resolve(rootDir, components);
  }

  /**
   * Get {@link INode} associated with the file / directory.
   */
  public INode getINode(String src) throws UnresolvedLinkException {
    return getLastINodeInPath(src).getINode(0);
  }

  /**
   * Get {@link INode} associated with the file / directory.
   */
  public INodesInPath getLastINodeInPath(String src)
       throws UnresolvedLinkException {
    readLock();
    try {
      return getLastINodeInPath(src, true);
    } finally {
      readUnlock();
    }
  }

  /**
   * Get {@link INode} associated with the file / directory.
   */
  public INodesInPath getINodesInPath4Write(String src
      ) throws UnresolvedLinkException, SnapshotAccessControlException {
    readLock();
    try {
      return getINodesInPath4Write(src, true);
    } finally {
      readUnlock();
    }
  }

  /**
   * Get {@link INode} associated with the file / directory.
   * @throws SnapshotAccessControlException if path is in RO snapshot
   */
  public INode getINode4Write(String src) throws UnresolvedLinkException,
      SnapshotAccessControlException {
    readLock();
    try {
      return getINode4Write(src, true);
    } finally {
      readUnlock();
    }
  }

  /** 
   * Check whether the filepath could be created
   * @throws SnapshotAccessControlException if path is in RO snapshot
   */
  boolean isValidToCreate(String src) throws UnresolvedLinkException,
      SnapshotAccessControlException {
    String srcs = normalizePath(src);
    readLock();
    try {
      return srcs.startsWith("/") && !srcs.endsWith("/")
              && getINode4Write(srcs, false) == null;
    } finally {
      readUnlock();
    }
  }

  /**
   * Check whether the path specifies a directory
   */
  boolean isDir(String src) throws UnresolvedLinkException {
    src = normalizePath(src);
    readLock();
    try {
      INode node = getNode(src, false);
      return node != null && node.isDirectory();
    } finally {
      readUnlock();
    }
  }
  
  /**
   * Check whether the path specifies a directory
   * @throws SnapshotAccessControlException if path is in RO snapshot
   */
  boolean isDirMutable(String src) throws UnresolvedLinkException,
      SnapshotAccessControlException {
    src = normalizePath(src);
    readLock();
    try {
      INode node = getINode4Write(src, false);
      return node != null && node.isDirectory();
    } finally {
      readUnlock();
    }
  }

  /** Updates namespace and diskspace consumed for all
   * directories until the parent directory of file represented by path.
   * 
   * @param path path for the file.
   * @param nsDelta the delta change of namespace
   * @param dsDelta the delta change of diskspace
   * @throws QuotaExceededException if the new count violates any quota limit
   * @throws FileNotFoundException if path does not exist.
   */
  void updateSpaceConsumed(String path, long nsDelta, long dsDelta)
      throws QuotaExceededException, FileNotFoundException,
          UnresolvedLinkException, SnapshotAccessControlException {
    writeLock();
    try {
      final INodesInPath iip = getINodesInPath4Write(path, false);
      if (iip.getLastINode() == null) {
        throw new FileNotFoundException("Path not found: " + path);
      }
      updateCount(iip, nsDelta, dsDelta, true);
    } finally {
      writeUnlock();
    }
  }
  
  private void updateCount(INodesInPath iip, long nsDelta, long dsDelta,
      boolean checkQuota) throws QuotaExceededException {
    updateCount(iip, iip.getINodes().length - 1, nsDelta, dsDelta, checkQuota);
  }

  /** update count of each inode with quota
   * 
   * @param iip inodes in a path
   * @param numOfINodes the number of inodes to update starting from index 0
   * @param nsDelta the delta change of namespace
   * @param dsDelta the delta change of diskspace
   * @param checkQuota if true then check if quota is exceeded
   * @throws QuotaExceededException if the new count violates any quota limit
   */
  private void updateCount(INodesInPath iip, int numOfINodes, 
                           long nsDelta, long dsDelta, boolean checkQuota)
                           throws QuotaExceededException {
    assert hasWriteLock();
    if (!namesystem.isImageLoaded()) {
      //still initializing. do not check or update quotas.
      return;
    }
    final INode[] inodes = iip.getINodes();
    if (numOfINodes > inodes.length) {
      numOfINodes = inodes.length;
    }
    if (checkQuota && !skipQuotaCheck) {
      verifyQuota(inodes, numOfINodes, nsDelta, dsDelta, null);
    }
    unprotectedUpdateCount(iip, numOfINodes, nsDelta, dsDelta);
  }
  
  /** 
   * update quota of each inode and check to see if quota is exceeded. 
   * See {@link #updateCount(INodesInPath, long, long, boolean)}
   */ 
  private void updateCountNoQuotaCheck(INodesInPath inodesInPath,
      int numOfINodes, long nsDelta, long dsDelta) {
    assert hasWriteLock();
    try {
      updateCount(inodesInPath, numOfINodes, nsDelta, dsDelta, false);
    } catch (QuotaExceededException e) {
      NameNode.LOG.error("BUG: unexpected exception ", e);
    }
  }
  
  /**
   * updates quota without verification
   * callers responsibility is to make sure quota is not exceeded
   */
  private static void unprotectedUpdateCount(INodesInPath inodesInPath,
      int numOfINodes, long nsDelta, long dsDelta) {
    final INode[] inodes = inodesInPath.getINodes();
    for(int i=0; i < numOfINodes; i++) {
      if (inodes[i].isQuotaSet()) { // a directory with quota
        inodes[i].asDirectory().getDirectoryWithQuotaFeature()
            .addSpaceConsumed2Cache(nsDelta, dsDelta);
      }
    }
  }
  
  /** Return the name of the path represented by inodes at [0, pos] */
  static String getFullPathName(INode[] inodes, int pos) {
    StringBuilder fullPathName = new StringBuilder();
    if (inodes[0].isRoot()) {
      if (pos == 0) return Path.SEPARATOR;
    } else {
      fullPathName.append(inodes[0].getLocalName());
    }
    
    for (int i=1; i<=pos; i++) {
      fullPathName.append(Path.SEPARATOR_CHAR).append(inodes[i].getLocalName());
    }
    return fullPathName.toString();
  }

  /**
   * @return the relative path of an inode from one of its ancestors,
   *         represented by an array of inodes.
   */
  private static INode[] getRelativePathINodes(INode inode, INode ancestor) {
    // calculate the depth of this inode from the ancestor
    int depth = 0;
    for (INode i = inode; i != null && !i.equals(ancestor); i = i.getParent()) {
      depth++;
    }
    INode[] inodes = new INode[depth];

    // fill up the inodes in the path from this inode to root
    for (int i = 0; i < depth; i++) {
      if (inode == null) {
        NameNode.stateChangeLog.warn("Could not get full path."
            + " Corresponding file might have deleted already.");
        return null;
      }
      inodes[depth-i-1] = inode;
      inode = inode.getParent();
    }
    return inodes;
  }
  
  private static INode[] getFullPathINodes(INode inode) {
    return getRelativePathINodes(inode, null);
  }
  
  /** Return the full path name of the specified inode */
  static String getFullPathName(INode inode) {
    INode[] inodes = getFullPathINodes(inode);
    // inodes can be null only when its called without holding lock
    return inodes == null ? "" : getFullPathName(inodes, inodes.length - 1);
  }

  INode unprotectedMkdir(long inodeId, String src, PermissionStatus permissions,
                          List<AclEntry> aclEntries, long timestamp)
      throws QuotaExceededException, UnresolvedLinkException, AclException {
    assert hasWriteLock();
    byte[][] components = INode.getPathComponents(src);
    INodesInPath iip = getExistingPathINodes(components);
    INode[] inodes = iip.getINodes();
    final int pos = inodes.length - 1;
    unprotectedMkdir(inodeId, iip, pos, components[pos], permissions, aclEntries,
        timestamp);
    return inodes[pos];
  }

  /** create a directory at index pos.
   * The parent path to the directory is at [0, pos-1].
   * All ancestors exist. Newly created one stored at index pos.
   */
  void unprotectedMkdir(long inodeId, INodesInPath inodesInPath,
      int pos, byte[] name, PermissionStatus permission,
      List<AclEntry> aclEntries, long timestamp)
      throws QuotaExceededException, AclException {
    assert hasWriteLock();
    final INodeDirectory dir = new INodeDirectory(inodeId, name, permission,
        timestamp);
    if (addChild(inodesInPath, pos, dir, true)) {
      if (aclEntries != null) {
        AclStorage.updateINodeAcl(dir, aclEntries, Snapshot.CURRENT_STATE_ID);
      }
      inodesInPath.setINode(pos, dir);
    }
  }
  
  /**
   * Add the given child to the namespace.
   * @param src The full path name of the child node.
   * @throws QuotaExceededException is thrown if it violates quota limit
   */
  private boolean addINode(String src, INode child
      ) throws QuotaExceededException, UnresolvedLinkException {
    byte[][] components = INode.getPathComponents(src);
    child.setLocalName(components[components.length-1]);
    cacheName(child);
    writeLock();
    try {
      return addLastINode(getExistingPathINodes(components), child, true);
    } finally {
      writeUnlock();
    }
  }

  /**
   * Verify quota for adding or moving a new INode with required 
   * namespace and diskspace to a given position.
   *  
   * @param inodes INodes corresponding to a path
   * @param pos position where a new INode will be added
   * @param nsDelta needed namespace
   * @param dsDelta needed diskspace
   * @param commonAncestor Last node in inodes array that is a common ancestor
   *          for a INode that is being moved from one location to the other.
   *          Pass null if a node is not being moved.
   * @throws QuotaExceededException if quota limit is exceeded.
   */
  private static void verifyQuota(INode[] inodes, int pos, long nsDelta,
      long dsDelta, INode commonAncestor) throws QuotaExceededException {
    if (nsDelta <= 0 && dsDelta <= 0) {
      // if quota is being freed or not being consumed
      return;
    }

    // check existing components in the path
    for(int i = (pos > inodes.length? inodes.length: pos) - 1; i >= 0; i--) {
      if (commonAncestor == inodes[i]) {
        // Stop checking for quota when common ancestor is reached
        return;
      }
      final DirectoryWithQuotaFeature q
          = inodes[i].asDirectory().getDirectoryWithQuotaFeature();
      if (q != null) { // a directory with quota
        try {
          q.verifyQuota(nsDelta, dsDelta);
        } catch (QuotaExceededException e) {
          e.setPathName(getFullPathName(inodes, i));
          throw e;
        }
      }
    }
  }
  
  /**
   * Verify quota for rename operation where srcInodes[srcInodes.length-1] moves
   * dstInodes[dstInodes.length-1]
   * 
   * @param src directory from where node is being moved.
   * @param dst directory to where node is moved to.
   * @throws QuotaExceededException if quota limit is exceeded.
   */
  private void verifyQuotaForRename(INode[] src, INode[] dst)
      throws QuotaExceededException {
    if (!namesystem.isImageLoaded() || skipQuotaCheck) {
      // Do not check quota if edits log is still being processed
      return;
    }
    int i = 0;
    while(src[i] == dst[i]) { i++; }
    // src[i - 1] is the last common ancestor.

    final Quota.Counts delta = src[src.length - 1].computeQuotaUsage();
    
    // Reduce the required quota by dst that is being removed
    final int dstIndex = dst.length - 1;
    if (dst[dstIndex] != null) {
      delta.subtract(dst[dstIndex].computeQuotaUsage());
    }
    verifyQuota(dst, dstIndex, delta.get(Quota.NAMESPACE),
        delta.get(Quota.DISKSPACE), src[i - 1]);
  }

  /**
   * Checks file system limits (max component length and max directory items)
   * during a rename operation.
   *
   * @param srcIIP INodesInPath containing every inode in the rename source
   * @param dstIIP INodesInPath containing every inode in the rename destination
   * @throws PathComponentTooLongException child's name is too long.
   * @throws MaxDirectoryItemsExceededException too many children.
   */
  private void verifyFsLimitsForRename(INodesInPath srcIIP, INodesInPath dstIIP)
      throws PathComponentTooLongException, MaxDirectoryItemsExceededException {
    byte[] dstChildName = dstIIP.getLastLocalName();
    INode[] dstInodes = dstIIP.getINodes();
    int pos = dstInodes.length - 1;
    verifyMaxComponentLength(dstChildName, dstInodes, pos);
    // Do not enforce max directory items if renaming within same directory.
    if (srcIIP.getINode(-2) != dstIIP.getINode(-2)) {
      verifyMaxDirItems(dstInodes, pos);
    }
  }

  /** Verify if the snapshot name is legal. */
  void verifySnapshotName(String snapshotName, String path)
      throws PathComponentTooLongException {
    if (snapshotName.contains(Path.SEPARATOR)) {
      throw new HadoopIllegalArgumentException(
          "Snapshot name cannot contain \"" + Path.SEPARATOR + "\"");
    }
    final byte[] bytes = DFSUtil.string2Bytes(snapshotName);
    verifyINodeName(bytes);
    verifyMaxComponentLength(bytes, path, 0);
  }
  
  /** Verify if the inode name is legal. */
  void verifyINodeName(byte[] childName) throws HadoopIllegalArgumentException {
    if (Arrays.equals(HdfsConstants.DOT_SNAPSHOT_DIR_BYTES, childName)) {
      String s = "\"" + HdfsConstants.DOT_SNAPSHOT_DIR + "\" is a reserved name.";
      if (!namesystem.isImageLoaded()) {
        s += "  Please rename it before upgrade.";
      }
      throw new HadoopIllegalArgumentException(s);
    }
  }

  /**
   * Verify child's name for fs limit.
   *
   * @param childName byte[] containing new child name
   * @param parentPath Object either INode[] or String containing parent path
   * @param pos int position of new child in path
   * @throws PathComponentTooLongException child's name is too long.
   */
  private void verifyMaxComponentLength(byte[] childName, Object parentPath,
      int pos) throws PathComponentTooLongException {
    if (maxComponentLength == 0) {
      return;
    }

    final int length = childName.length;
    if (length > maxComponentLength) {
      final String p = parentPath instanceof INode[]?
          getFullPathName((INode[])parentPath, pos - 1): (String)parentPath;
      final PathComponentTooLongException e = new PathComponentTooLongException(
          maxComponentLength, length, p, DFSUtil.bytes2String(childName));
      if (namesystem.isImageLoaded()) {
        throw e;
      } else {
        // Do not throw if edits log is still being processed
        NameNode.LOG.error("ERROR in FSDirectory.verifyINodeName", e);
      }
    }
  }

  /**
   * Verify children size for fs limit.
   *
   * @param pathComponents INode[] containing full path of inodes to new child
   * @param pos int position of new child in pathComponents
   * @throws MaxDirectoryItemsExceededException too many children.
   */
  private void verifyMaxDirItems(INode[] pathComponents, int pos)
      throws MaxDirectoryItemsExceededException {

    final INodeDirectory parent = pathComponents[pos-1].asDirectory();
    final int count = parent.getChildrenList(Snapshot.CURRENT_STATE_ID).size();
    if (count >= maxDirItems) {
      final MaxDirectoryItemsExceededException e
          = new MaxDirectoryItemsExceededException(maxDirItems, count);
      if (namesystem.isImageLoaded()) {
        e.setPathName(getFullPathName(pathComponents, pos - 1));
        throw e;
      } else {
        // Do not throw if edits log is still being processed
        NameNode.LOG.error("FSDirectory.verifyMaxDirItems: "
            + e.getLocalizedMessage());
      }
    }
  }
  
  /**
   * The same as {@link #addChild(INodesInPath, int, INode, boolean)}
   * with pos = length - 1.
   */
  private boolean addLastINode(INodesInPath inodesInPath,
      INode inode, boolean checkQuota) throws QuotaExceededException {
    final int pos = inodesInPath.getINodes().length - 1;
    return addChild(inodesInPath, pos, inode, checkQuota);
  }

  /** Add a node child to the inodes at index pos. 
   * Its ancestors are stored at [0, pos-1].
   * @return false if the child with this name already exists; 
   *         otherwise return true;
   * @throws QuotaExceededException is thrown if it violates quota limit
   */
  private boolean addChild(INodesInPath iip, int pos,
      INode child, boolean checkQuota) throws QuotaExceededException {
    final INode[] inodes = iip.getINodes();
    // Disallow creation of /.reserved. This may be created when loading
    // editlog/fsimage during upgrade since /.reserved was a valid name in older
    // release. This may also be called when a user tries to create a file
    // or directory /.reserved.
    if (pos == 1 && inodes[0] == rootDir && isReservedName(child)) {
      throw new HadoopIllegalArgumentException(
          "File name \"" + child.getLocalName() + "\" is reserved and cannot "
              + "be created. If this is during upgrade change the name of the "
              + "existing file or directory to another name before upgrading "
              + "to the new release.");
    }
    // The filesystem limits are not really quotas, so this check may appear
    // odd. It's because a rename operation deletes the src, tries to add
    // to the dest, if that fails, re-adds the src from whence it came.
    // The rename code disables the quota when it's restoring to the
    // original location becase a quota violation would cause the the item
    // to go "poof".  The fs limits must be bypassed for the same reason.
    if (checkQuota) {
      verifyMaxComponentLength(child.getLocalNameBytes(), inodes, pos);
      verifyMaxDirItems(inodes, pos);
    }
    // always verify inode name
    verifyINodeName(child.getLocalNameBytes());
    
    final Quota.Counts counts = child.computeQuotaUsage();
    updateCount(iip, pos,
        counts.get(Quota.NAMESPACE), counts.get(Quota.DISKSPACE), checkQuota);
    boolean isRename = (child.getParent() != null);
    final INodeDirectory parent = inodes[pos-1].asDirectory();
    boolean added;
    try {
      added = parent.addChild(child, true, iip.getLatestSnapshotId());
    } catch (QuotaExceededException e) {
      updateCountNoQuotaCheck(iip, pos,
          -counts.get(Quota.NAMESPACE), -counts.get(Quota.DISKSPACE));
      throw e;
    }
    if (!added) {
      updateCountNoQuotaCheck(iip, pos,
          -counts.get(Quota.NAMESPACE), -counts.get(Quota.DISKSPACE));
    } else {
      iip.setINode(pos - 1, child.getParent());
      if (!isRename) {
        AclStorage.copyINodeDefaultAcl(child);
      }
      addToInodeMap(child);
    }
    return added;
  }
  
  private boolean addLastINodeNoQuotaCheck(INodesInPath inodesInPath, INode i) {
    try {
      return addLastINode(inodesInPath, i, false);
    } catch (QuotaExceededException e) {
      NameNode.LOG.warn("FSDirectory.addChildNoQuotaCheck - unexpected", e);
    }
    return false;
  }
  
  /**
   * Remove the last inode in the path from the namespace.
   * Count of each ancestor with quota is also updated.
   * @return -1 for failing to remove;
   *          0 for removing a reference whose referred inode has other 
   *            reference nodes;
   *         >0 otherwise. 
   */
  private long removeLastINode(final INodesInPath iip)
      throws QuotaExceededException {
    final int latestSnapshot = iip.getLatestSnapshotId();
    final INode last = iip.getLastINode();
    final INodeDirectory parent = iip.getINode(-2).asDirectory();
    if (!parent.removeChild(last, latestSnapshot)) {
      return -1;
    }
    
    if (!last.isInLatestSnapshot(latestSnapshot)) {
      final Quota.Counts counts = last.computeQuotaUsage();
      updateCountNoQuotaCheck(iip, iip.getINodes().length - 1,
          -counts.get(Quota.NAMESPACE), -counts.get(Quota.DISKSPACE));

      if (INodeReference.tryRemoveReference(last) > 0) {
        return 0;
      } else {
        return counts.get(Quota.NAMESPACE);
      }
    }
    return 1;
  }

  static String normalizePath(String src) {
    if (src.length() > 1 && src.endsWith("/")) {
      src = src.substring(0, src.length() - 1);
    }
    return src;
  }

  ContentSummary getContentSummary(String src) 
    throws FileNotFoundException, UnresolvedLinkException {
    String srcs = normalizePath(src);
    readLock();
    try {
      INode targetNode = getNode(srcs, false);
      if (targetNode == null) {
        throw new FileNotFoundException("File does not exist: " + srcs);
      }
      else {
        // Make it relinquish locks everytime contentCountLimit entries are
        // processed. 0 means disabled. I.e. blocking for the entire duration.
        ContentSummaryComputationContext cscc =

            new ContentSummaryComputationContext(this, getFSNamesystem(),
            contentCountLimit);
        ContentSummary cs = targetNode.computeAndConvertContentSummary(cscc);
        yieldCount += cscc.getYieldCount();
        return cs;
      }
    } finally {
      readUnlock();
    }
  }

  @VisibleForTesting
  public long getYieldCount() {
    return yieldCount;
  }

  public INodeMap getINodeMap() {
    return inodeMap;
  }
  
  /**
   * This method is always called with writeLock of FSDirectory held.
   */
  public final void addToInodeMap(INode inode) {
    if (inode instanceof INodeWithAdditionalFields) {
      inodeMap.put(inode);
      if (!inode.isSymlink()) {
        final XAttrFeature xaf = inode.getXAttrFeature();
        if (xaf != null) {
          final List<XAttr> xattrs = xaf.getXAttrs();
          for (XAttr xattr : xattrs) {
            final String xaName = XAttrHelper.getPrefixName(xattr);
            if (CRYPTO_XATTR_ENCRYPTION_ZONE.equals(xaName)) {
              try {
                final HdfsProtos.ZoneEncryptionInfoProto ezProto =
                    HdfsProtos.ZoneEncryptionInfoProto.parseFrom(
                        xattr.getValue());
                ezManager.unprotectedAddEncryptionZone(inode.getId(),
                    PBHelper.convert(ezProto.getSuite()),
                    PBHelper.convert(ezProto.getCryptoProtocolVersion()),
                    ezProto.getKeyName());
              } catch (InvalidProtocolBufferException e) {
                NameNode.LOG.warn("Error parsing protocol buffer of " +
                    "EZ XAttr " + xattr.getName());
              }
            }
          }
        }
      }
    }
  }
  
  /**
   * This method is always called with writeLock of FSDirectory held.
   */
  public final void removeFromInodeMap(List<? extends INode> inodes) {
    if (inodes != null) {
      for (INode inode : inodes) {
        if (inode != null && inode instanceof INodeWithAdditionalFields) {
          inodeMap.remove(inode);
          ezManager.removeEncryptionZone(inode.getId());
        }
      }
    }
  }
  
  /**
   * Get the inode from inodeMap based on its inode id.
   * @param id The given id
   * @return The inode associated with the given id
   */
  public INode getInode(long id) {
    readLock();
    try {
      return inodeMap.get(id);
    } finally {
      readUnlock();
    }
  }
  
  @VisibleForTesting
  int getInodeMapSize() {
    return inodeMap.size();
  }
  
  /**
   * See {@link ClientProtocol#setQuota(String, long, long)} for the contract.
   * Sets quota for for a directory.
   * @return INodeDirectory if any of the quotas have changed. null otherwise.
   * @throws FileNotFoundException if the path does not exist.
   * @throws PathIsNotDirectoryException if the path is not a directory.
   * @throws QuotaExceededException if the directory tree size is 
   *                                greater than the given quota
   * @throws UnresolvedLinkException if a symlink is encountered in src.
   * @throws SnapshotAccessControlException if path is in RO snapshot
   */
  INodeDirectory unprotectedSetQuota(String src, long nsQuota, long dsQuota)
      throws FileNotFoundException, PathIsNotDirectoryException,
      QuotaExceededException, UnresolvedLinkException,
      SnapshotAccessControlException {
    assert hasWriteLock();
    // sanity check
    if ((nsQuota < 0 && nsQuota != HdfsConstants.QUOTA_DONT_SET && 
         nsQuota != HdfsConstants.QUOTA_RESET) || 
        (dsQuota < 0 && dsQuota != HdfsConstants.QUOTA_DONT_SET && 
          dsQuota != HdfsConstants.QUOTA_RESET)) {
      throw new IllegalArgumentException("Illegal value for nsQuota or " +
                                         "dsQuota : " + nsQuota + " and " +
                                         dsQuota);
    }
    
    String srcs = normalizePath(src);
    final INodesInPath iip = getINodesInPath4Write(srcs, true);
    INodeDirectory dirNode = INodeDirectory.valueOf(iip.getLastINode(), srcs);
    if (dirNode.isRoot() && nsQuota == HdfsConstants.QUOTA_RESET) {
      throw new IllegalArgumentException("Cannot clear namespace quota on root.");
    } else { // a directory inode
      final Quota.Counts oldQuota = dirNode.getQuotaCounts();
      final long oldNsQuota = oldQuota.get(Quota.NAMESPACE);
      final long oldDsQuota = oldQuota.get(Quota.DISKSPACE);
      if (nsQuota == HdfsConstants.QUOTA_DONT_SET) {
        nsQuota = oldNsQuota;
      }
      if (dsQuota == HdfsConstants.QUOTA_DONT_SET) {
        dsQuota = oldDsQuota;
      }        
      if (oldNsQuota == nsQuota && oldDsQuota == dsQuota) {
        return null;
      }

      final int latest = iip.getLatestSnapshotId();
      dirNode.recordModification(latest);
      dirNode.setQuota(nsQuota, dsQuota);
      return dirNode;
    }
  }
  
  /**
   * See {@link ClientProtocol#setQuota(String, long, long)} for the contract.
   * @return INodeDirectory if any of the quotas have changed. null otherwise.
   * @throws SnapshotAccessControlException if path is in RO snapshot
   * @see #unprotectedSetQuota(String, long, long)
   */
  INodeDirectory setQuota(String src, long nsQuota, long dsQuota)
      throws FileNotFoundException, PathIsNotDirectoryException,
      QuotaExceededException, UnresolvedLinkException,
      SnapshotAccessControlException {
    writeLock();
    try {
      return unprotectedSetQuota(src, nsQuota, dsQuota);
    } finally {
      writeUnlock();
    }
  }
  
  long totalInodes() {
    readLock();
    try {
      return rootDir.getDirectoryWithQuotaFeature().getSpaceConsumed()
          .get(Quota.NAMESPACE);
    } finally {
      readUnlock();
    }
  }

  /**
   * Sets the access time on the file/directory. Logs it in the transaction log.
   */
  boolean setTimes(INode inode, long mtime, long atime, boolean force,
                   int latestSnapshotId) throws QuotaExceededException {
    writeLock();
    try {
      return unprotectedSetTimes(inode, mtime, atime, force, latestSnapshotId);
    } finally {
      writeUnlock();
    }
  }

  boolean unprotectedSetTimes(String src, long mtime, long atime, boolean force) 
      throws UnresolvedLinkException, QuotaExceededException {
    assert hasWriteLock();
    final INodesInPath i = getLastINodeInPath(src); 
    return unprotectedSetTimes(i.getLastINode(), mtime, atime, force,
        i.getLatestSnapshotId());
  }

  private boolean unprotectedSetTimes(INode inode, long mtime,
      long atime, boolean force, int latest) throws QuotaExceededException {
    assert hasWriteLock();
    boolean status = false;
    if (mtime != -1) {
      inode = inode.setModificationTime(mtime, latest);
      status = true;
    }
    if (atime != -1) {
      long inodeTime = inode.getAccessTime();

      // if the last access time update was within the last precision interval, then
      // no need to store access time
      if (atime <= inodeTime + getFSNamesystem().getAccessTimePrecision() && !force) {
        status =  false;
      } else {
        inode.setAccessTime(atime, latest);
        status = true;
      }
    } 
    return status;
  }

  /**
   * Reset the entire namespace tree.
   */
  void reset() {
    writeLock();
    try {
      rootDir = createRoot(getFSNamesystem());
      inodeMap.clear();
      addToInodeMap(rootDir);
      nameCache.reset();
    } finally {
      writeUnlock();
    }
  }

  /**
   * create an hdfs file status from an inode
   * 
   * @param path the local name
   * @param node inode
   * @param needLocation if block locations need to be included or not
   * @param isRawPath true if this is being called on behalf of a path in
   *                  /.reserved/raw
   * @param iip
   * @return a file status
   * @throws IOException if any error occurs
   */
  private HdfsFileStatus createFileStatus(byte[] path, INode node,
      boolean needLocation, byte storagePolicy, int snapshot,
      boolean isRawPath, INodesInPath iip)
      throws IOException {
    if (needLocation) {
      return createLocatedFileStatus(path, node, storagePolicy, snapshot,
          isRawPath, iip);
    } else {
      return createFileStatus(path, node, storagePolicy, snapshot,
          isRawPath, iip);
    }
  }

  /**
   * Create FileStatus by file INode 
   */
  HdfsFileStatus createFileStatus(byte[] path, INode node, byte storagePolicy,
      int snapshot, boolean isRawPath, INodesInPath iip) throws IOException {
     long size = 0;     // length is zero for directories
     short replication = 0;
     long blocksize = 0;
     final boolean isEncrypted;

     final FileEncryptionInfo feInfo = isRawPath ? null :
         getFileEncryptionInfo(node, snapshot, iip);

     boolean isLazyPersist = false;
     if (node.isFile()) {
       final INodeFile fileNode = node.asFile();
       size = fileNode.computeFileSize(snapshot);
       replication = fileNode.getFileReplication(snapshot);
       blocksize = fileNode.getPreferredBlockSize();
       isEncrypted = (feInfo != null) ||
           (isRawPath && isInAnEZ(INodesInPath.fromINode(node)));
     } else {
       isEncrypted = isInAnEZ(INodesInPath.fromINode(node));
     }

     int childrenNum = node.isDirectory() ? 
         node.asDirectory().getChildrenNum(snapshot) : 0;

     return new HdfsFileStatus(
        size, 
        node.isDirectory(), 
        replication, 
        blocksize,
        node.getModificationTime(snapshot),
        node.getAccessTime(snapshot),
        getPermissionForFileStatus(node, snapshot, isEncrypted),
        node.getUserName(snapshot),
        node.getGroupName(snapshot),
        node.isSymlink() ? node.asSymlink().getSymlink() : null,
        path,
        node.getId(),
        childrenNum,
        feInfo,
        storagePolicy);
  }

  /**
   * Create FileStatus with location info by file INode
   */
  private HdfsLocatedFileStatus createLocatedFileStatus(byte[] path, INode node,
      byte storagePolicy, int snapshot, boolean isRawPath,
      INodesInPath iip) throws IOException {
    assert hasReadLock();
    long size = 0; // length is zero for directories
    short replication = 0;
    long blocksize = 0;
    LocatedBlocks loc = null;
    final boolean isEncrypted;
    final FileEncryptionInfo feInfo = isRawPath ? null :
        getFileEncryptionInfo(node, snapshot, iip);
    if (node.isFile()) {
      final INodeFile fileNode = node.asFile();
      size = fileNode.computeFileSize(snapshot);
      replication = fileNode.getFileReplication(snapshot);
      blocksize = fileNode.getPreferredBlockSize();

      final boolean inSnapshot = snapshot != Snapshot.CURRENT_STATE_ID; 
      final boolean isUc = !inSnapshot && fileNode.isUnderConstruction();
      final long fileSize = !inSnapshot && isUc ? 
          fileNode.computeFileSizeNotIncludingLastUcBlock() : size;

      loc = getFSNamesystem().getBlockManager().createLocatedBlocks(
          fileNode.getBlocks(), fileSize, isUc, 0L, size, false,
          inSnapshot, feInfo);
      if (loc == null) {
        loc = new LocatedBlocks();
      }
      isEncrypted = (feInfo != null) ||
          (isRawPath && isInAnEZ(INodesInPath.fromINode(node)));
    } else {
      isEncrypted = isInAnEZ(INodesInPath.fromINode(node));
    }
    int childrenNum = node.isDirectory() ? 
        node.asDirectory().getChildrenNum(snapshot) : 0;

    HdfsLocatedFileStatus status =
        new HdfsLocatedFileStatus(size, node.isDirectory(), replication,
          blocksize, node.getModificationTime(snapshot),
          node.getAccessTime(snapshot),
          getPermissionForFileStatus(node, snapshot, isEncrypted),
          node.getUserName(snapshot), node.getGroupName(snapshot),
          node.isSymlink() ? node.asSymlink().getSymlink() : null, path,
          node.getId(), loc, childrenNum, feInfo, storagePolicy);
    // Set caching information for the located blocks.
    if (loc != null) {
      CacheManager cacheManager = namesystem.getCacheManager();
      for (LocatedBlock lb: loc.getLocatedBlocks()) {
        cacheManager.setCachedLocations(lb);
      }
    }
    return status;
  }

  /**
   * Returns an inode's FsPermission for use in an outbound FileStatus.  If the
   * inode has an ACL or is for an encrypted file/dir, then this method will
   * return an FsPermissionExtension.
   *
   * @param node INode to check
   * @param snapshot int snapshot ID
   * @param isEncrypted boolean true if the file/dir is encrypted
   * @return FsPermission from inode, with ACL bit on if the inode has an ACL
   * and encrypted bit on if it represents an encrypted file/dir.
   */
  private static FsPermission getPermissionForFileStatus(INode node,
      int snapshot, boolean isEncrypted) {
    FsPermission perm = node.getFsPermission(snapshot);
    boolean hasAcl = node.getAclFeature(snapshot) != null;
    if (hasAcl || isEncrypted) {
      perm = new FsPermissionExtension(perm, hasAcl, isEncrypted);
    }
    return perm;
  }

  /**
   * Add the specified path into the namespace.
   */
  INodeSymlink addSymlink(long id, String path, String target,
                          long mtime, long atime, PermissionStatus perm)
          throws UnresolvedLinkException, QuotaExceededException {
    writeLock();
    try {
      return unprotectedAddSymlink(id, path, target, mtime, atime, perm);
    } finally {
      writeUnlock();
    }
  }

  INodeSymlink unprotectedAddSymlink(long id, String path, String target,
      long mtime, long atime, PermissionStatus perm)
      throws UnresolvedLinkException, QuotaExceededException {
    assert hasWriteLock();
    final INodeSymlink symlink = new INodeSymlink(id, null, perm, mtime, atime,
        target);
    return addINode(path, symlink) ? symlink : null;
  }

  List<AclEntry> modifyAclEntries(String src, List<AclEntry> aclSpec) throws IOException {
    writeLock();
    try {
      return unprotectedModifyAclEntries(src, aclSpec);
    } finally {
      writeUnlock();
    }
  }

  private List<AclEntry> unprotectedModifyAclEntries(String src,
      List<AclEntry> aclSpec) throws IOException {
    assert hasWriteLock();
    INodesInPath iip = getINodesInPath4Write(normalizePath(src), true);
    INode inode = resolveLastINode(src, iip);
    int snapshotId = iip.getLatestSnapshotId();
    List<AclEntry> existingAcl = AclStorage.readINodeLogicalAcl(inode);
    List<AclEntry> newAcl = AclTransformation.mergeAclEntries(existingAcl,
      aclSpec);
    AclStorage.updateINodeAcl(inode, newAcl, snapshotId);
    return newAcl;
  }

  List<AclEntry> removeAclEntries(String src, List<AclEntry> aclSpec) throws IOException {
    writeLock();
    try {
      return unprotectedRemoveAclEntries(src, aclSpec);
    } finally {
      writeUnlock();
    }
  }

  private List<AclEntry> unprotectedRemoveAclEntries(String src,
      List<AclEntry> aclSpec) throws IOException {
    assert hasWriteLock();
    INodesInPath iip = getINodesInPath4Write(normalizePath(src), true);
    INode inode = resolveLastINode(src, iip);
    int snapshotId = iip.getLatestSnapshotId();
    List<AclEntry> existingAcl = AclStorage.readINodeLogicalAcl(inode);
    List<AclEntry> newAcl = AclTransformation.filterAclEntriesByAclSpec(
      existingAcl, aclSpec);
    AclStorage.updateINodeAcl(inode, newAcl, snapshotId);
    return newAcl;
  }

  List<AclEntry> removeDefaultAcl(String src) throws IOException {
    writeLock();
    try {
      return unprotectedRemoveDefaultAcl(src);
    } finally {
      writeUnlock();
    }
  }

  private List<AclEntry> unprotectedRemoveDefaultAcl(String src)
      throws IOException {
    assert hasWriteLock();
    INodesInPath iip = getINodesInPath4Write(normalizePath(src), true);
    INode inode = resolveLastINode(src, iip);
    int snapshotId = iip.getLatestSnapshotId();
    List<AclEntry> existingAcl = AclStorage.readINodeLogicalAcl(inode);
    List<AclEntry> newAcl = AclTransformation.filterDefaultAclEntries(
      existingAcl);
    AclStorage.updateINodeAcl(inode, newAcl, snapshotId);
    return newAcl;
  }

  void removeAcl(String src) throws IOException {
    writeLock();
    try {
      unprotectedRemoveAcl(src);
    } finally {
      writeUnlock();
    }
  }

  private void unprotectedRemoveAcl(String src) throws IOException {
    assert hasWriteLock();
    INodesInPath iip = getINodesInPath4Write(normalizePath(src), true);
    INode inode = resolveLastINode(src, iip);
    int snapshotId = iip.getLatestSnapshotId();
    AclStorage.removeINodeAcl(inode, snapshotId);
  }

  List<AclEntry> setAcl(String src, List<AclEntry> aclSpec) throws IOException {
    writeLock();
    try {
      return unprotectedSetAcl(src, aclSpec);
    } finally {
      writeUnlock();
    }
  }

  List<AclEntry> unprotectedSetAcl(String src, List<AclEntry> aclSpec)
      throws IOException {
    // ACL removal is logged to edits as OP_SET_ACL with an empty list.
    if (aclSpec.isEmpty()) {
      unprotectedRemoveAcl(src);
      return AclFeature.EMPTY_ENTRY_LIST;
    }

    assert hasWriteLock();
    INodesInPath iip = getINodesInPath4Write(normalizePath(src), true);
    INode inode = resolveLastINode(src, iip);
    int snapshotId = iip.getLatestSnapshotId();
    List<AclEntry> existingAcl = AclStorage.readINodeLogicalAcl(inode);
    List<AclEntry> newAcl = AclTransformation.replaceAclEntries(existingAcl,
      aclSpec);
    AclStorage.updateINodeAcl(inode, newAcl, snapshotId);
    return newAcl;
  }

  AclStatus getAclStatus(String src) throws IOException {
    String srcs = normalizePath(src);
    readLock();
    try {
      // There is no real inode for the path ending in ".snapshot", so return a
      // non-null, unpopulated AclStatus.  This is similar to getFileInfo.
      if (srcs.endsWith(HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR) &&
          getINode4DotSnapshot(srcs) != null) {
        return new AclStatus.Builder().owner("").group("").build();
      }
      INodesInPath iip = getLastINodeInPath(srcs, true);
      INode inode = resolveLastINode(src, iip);
      int snapshotId = iip.getPathSnapshotId();
      List<AclEntry> acl = AclStorage.readINodeAcl(inode, snapshotId);
      return new AclStatus.Builder()
          .owner(inode.getUserName()).group(inode.getGroupName())
          .stickyBit(inode.getFsPermission(snapshotId).getStickyBit())
          .addEntries(acl).build();
    } finally {
      readUnlock();
    }
  }

  /**
   * Removes a list of XAttrs from an inode at a path.
   *
   * @param src path of inode
   * @param toRemove XAttrs to be removed
   * @return List of XAttrs that were removed
   * @throws IOException if the inode does not exist, if quota is exceeded
   */
  List<XAttr> removeXAttrs(final String src, final List<XAttr> toRemove)
      throws IOException {
    writeLock();
    try {
      return unprotectedRemoveXAttrs(src, toRemove);
    } finally {
      writeUnlock();
    }
  }

  List<XAttr> unprotectedRemoveXAttrs(final String src,
      final List<XAttr> toRemove) throws IOException {
    assert hasWriteLock();
    INodesInPath iip = getINodesInPath4Write(normalizePath(src), true);
    INode inode = resolveLastINode(src, iip);
    int snapshotId = iip.getLatestSnapshotId();
    List<XAttr> existingXAttrs = XAttrStorage.readINodeXAttrs(inode);
    List<XAttr> removedXAttrs = Lists.newArrayListWithCapacity(toRemove.size());
    List<XAttr> newXAttrs = filterINodeXAttrs(existingXAttrs, toRemove,
        removedXAttrs);
    if (existingXAttrs.size() != newXAttrs.size()) {
      XAttrStorage.updateINodeXAttrs(inode, newXAttrs, snapshotId);
      return removedXAttrs;
    }
    return null;
  }

  /**
   * Filter XAttrs from a list of existing XAttrs. Removes matched XAttrs from
   * toFilter and puts them into filtered. Upon completion,
   * toFilter contains the filter XAttrs that were not found, while
   * fitleredXAttrs contains the XAttrs that were found.
   *
   * @param existingXAttrs Existing XAttrs to be filtered
   * @param toFilter XAttrs to filter from the existing XAttrs
   * @param filtered Return parameter, XAttrs that were filtered
   * @return List of XAttrs that does not contain filtered XAttrs
   */
  @VisibleForTesting
  List<XAttr> filterINodeXAttrs(final List<XAttr> existingXAttrs,
      final List<XAttr> toFilter, final List<XAttr> filtered)
    throws AccessControlException {
    if (existingXAttrs == null || existingXAttrs.isEmpty() ||
        toFilter == null || toFilter.isEmpty()) {
      return existingXAttrs;
    }

    // Populate a new list with XAttrs that pass the filter
    List<XAttr> newXAttrs =
        Lists.newArrayListWithCapacity(existingXAttrs.size());
    for (XAttr a : existingXAttrs) {
      boolean add = true;
      for (ListIterator<XAttr> it = toFilter.listIterator(); it.hasNext()
          ;) {
        XAttr filter = it.next();
        Preconditions.checkArgument(!KEYID_XATTR.equalsIgnoreValue(filter),
            "The encryption zone xattr should never be deleted.");
        if (UNREADABLE_BY_SUPERUSER_XATTR.equalsIgnoreValue(filter)) {
          throw new AccessControlException("The xattr '" +
              SECURITY_XATTR_UNREADABLE_BY_SUPERUSER + "' can not be deleted.");
        }
        if (a.equalsIgnoreValue(filter)) {
          add = false;
          it.remove();
          filtered.add(filter);
          break;
        }
      }
      if (add) {
        newXAttrs.add(a);
      }
    }

    return newXAttrs;
  }

  boolean isInAnEZ(INodesInPath iip)
      throws UnresolvedLinkException, SnapshotAccessControlException {
    readLock();
    try {
      return ezManager.isInAnEZ(iip);
    } finally {
      readUnlock();
    }
  }

  String getKeyName(INodesInPath iip) {
    readLock();
    try {
      return ezManager.getKeyName(iip);
    } finally {
      readUnlock();
    }
  }

  XAttr createEncryptionZone(String src, CipherSuite suite,
      CryptoProtocolVersion version, String keyName)
    throws IOException {
    writeLock();
    try {
      return ezManager.createEncryptionZone(src, suite, version, keyName);
    } finally {
      writeUnlock();
    }
  }

  EncryptionZone getEZForPath(INodesInPath iip) {
    readLock();
    try {
      return ezManager.getEZINodeForPath(iip);
    } finally {
      readUnlock();
    }
  }

  BatchedListEntries<EncryptionZone> listEncryptionZones(long prevId)
      throws IOException {
    readLock();
    try {
      return ezManager.listEncryptionZones(prevId);
    } finally {
      readUnlock();
    }
  }

  /**
   * Set the FileEncryptionInfo for an INode.
   */
  void setFileEncryptionInfo(String src, FileEncryptionInfo info)
      throws IOException {
    // Make the PB for the xattr
    final HdfsProtos.PerFileEncryptionInfoProto proto =
        PBHelper.convertPerFileEncInfo(info);
    final byte[] protoBytes = proto.toByteArray();
    final XAttr fileEncryptionAttr =
        XAttrHelper.buildXAttr(CRYPTO_XATTR_FILE_ENCRYPTION_INFO, protoBytes);
    final List<XAttr> xAttrs = Lists.newArrayListWithCapacity(1);
    xAttrs.add(fileEncryptionAttr);

    writeLock();
    try {
      unprotectedSetXAttrs(src, xAttrs, EnumSet.of(XAttrSetFlag.CREATE));
    } finally {
      writeUnlock();
    }
  }

  /**
   * This function combines the per-file encryption info (obtained
   * from the inode's XAttrs), and the encryption info from its zone, and
   * returns a consolidated FileEncryptionInfo instance. Null is returned
   * for non-encrypted files.
   *
   * @param inode inode of the file
   * @param snapshotId ID of the snapshot that
   *                   we want to get encryption info from
   * @param iip inodes in the path containing the file, passed in to
   *            avoid obtaining the list of inodes again; if iip is
   *            null then the list of inodes will be obtained again
   * @return consolidated file encryption info; null for non-encrypted files
   */
  FileEncryptionInfo getFileEncryptionInfo(INode inode, int snapshotId,
      INodesInPath iip) throws IOException {
    if (!inode.isFile()) {
      return null;
    }
    readLock();
    try {
      if (iip == null) {
        iip = getINodesInPath(inode.getFullPathName(), true);
      }
      EncryptionZone encryptionZone = getEZForPath(iip);
      if (encryptionZone == null) {
        // not an encrypted file
        return null;
      } else if(encryptionZone.getPath() == null
          || encryptionZone.getPath().isEmpty()) {
        if (NameNode.LOG.isDebugEnabled()) {
          NameNode.LOG.debug("Encryption zone " +
              encryptionZone.getPath() + " does not have a valid path.");
        }
      }

      final CryptoProtocolVersion version = encryptionZone.getVersion();
      final CipherSuite suite = encryptionZone.getSuite();
      final String keyName = encryptionZone.getKeyName();

      XAttr fileXAttr = unprotectedGetXAttrByName(inode, snapshotId,
          CRYPTO_XATTR_FILE_ENCRYPTION_INFO);

      if (fileXAttr == null) {
        NameNode.LOG.warn("Could not find encryption XAttr for file " +
            inode.getFullPathName() + " in encryption zone " +
            encryptionZone.getPath());
        return null;
      }

      try {
        HdfsProtos.PerFileEncryptionInfoProto fileProto =
            HdfsProtos.PerFileEncryptionInfoProto.parseFrom(
                fileXAttr.getValue());
        return PBHelper.convert(fileProto, suite, version, keyName);
      } catch (InvalidProtocolBufferException e) {
        throw new IOException("Could not parse file encryption info for " +
            "inode " + inode, e);
      }
    } finally {
      readUnlock();
    }
  }

  void setXAttrs(final String src, final List<XAttr> xAttrs,
      final EnumSet<XAttrSetFlag> flag) throws IOException {
    writeLock();
    try {
      unprotectedSetXAttrs(src, xAttrs, flag);
    } finally {
      writeUnlock();
    }
  }
  
  INode unprotectedSetXAttrs(final String src, final List<XAttr> xAttrs,
      final EnumSet<XAttrSetFlag> flag)
      throws QuotaExceededException, IOException {
    assert hasWriteLock();
    INodesInPath iip = getINodesInPath4Write(normalizePath(src), true);
    INode inode = resolveLastINode(src, iip);
    int snapshotId = iip.getLatestSnapshotId();
    List<XAttr> existingXAttrs = XAttrStorage.readINodeXAttrs(inode);
    List<XAttr> newXAttrs = setINodeXAttrs(existingXAttrs, xAttrs, flag);
    final boolean isFile = inode.isFile();

    for (XAttr xattr : newXAttrs) {
      final String xaName = XAttrHelper.getPrefixName(xattr);

      /*
       * If we're adding the encryption zone xattr, then add src to the list
       * of encryption zones.
       */
      if (CRYPTO_XATTR_ENCRYPTION_ZONE.equals(xaName)) {
        final HdfsProtos.ZoneEncryptionInfoProto ezProto =
            HdfsProtos.ZoneEncryptionInfoProto.parseFrom(xattr.getValue());
        ezManager.addEncryptionZone(inode.getId(),
            PBHelper.convert(ezProto.getSuite()),
            PBHelper.convert(ezProto.getCryptoProtocolVersion()),
            ezProto.getKeyName());
      }

      if (!isFile && SECURITY_XATTR_UNREADABLE_BY_SUPERUSER.equals(xaName)) {
        throw new IOException("Can only set '" +
            SECURITY_XATTR_UNREADABLE_BY_SUPERUSER + "' on a file.");
      }
    }

    XAttrStorage.updateINodeXAttrs(inode, newXAttrs, snapshotId);
    return inode;
  }

  List<XAttr> setINodeXAttrs(final List<XAttr> existingXAttrs,
      final List<XAttr> toSet, final EnumSet<XAttrSetFlag> flag)
      throws IOException {
    // Check for duplicate XAttrs in toSet
    // We need to use a custom comparator, so using a HashSet is not suitable
    for (int i = 0; i < toSet.size(); i++) {
      for (int j = i + 1; j < toSet.size(); j++) {
        if (toSet.get(i).equalsIgnoreValue(toSet.get(j))) {
          throw new IOException("Cannot specify the same XAttr to be set " +
              "more than once");
        }
      }
    }

    // Count the current number of user-visible XAttrs for limit checking
    int userVisibleXAttrsNum = 0; // Number of user visible xAttrs

    // The XAttr list is copied to an exactly-sized array when it's stored,
    // so there's no need to size it precisely here.
    int newSize = (existingXAttrs != null) ? existingXAttrs.size() : 0;
    newSize += toSet.size();
    List<XAttr> xAttrs = Lists.newArrayListWithCapacity(newSize);

    // Check if the XAttr already exists to validate with the provided flag
    for (XAttr xAttr: toSet) {
      boolean exist = false;
      if (existingXAttrs != null) {
        for (XAttr a : existingXAttrs) {
          if (a.equalsIgnoreValue(xAttr)) {
            exist = true;
            break;
          }
        }
      }
      XAttrSetFlag.validate(xAttr.getName(), exist, flag);
      // add the new XAttr since it passed validation
      xAttrs.add(xAttr);
      if (isUserVisible(xAttr)) {
        userVisibleXAttrsNum++;
      }
    }

    // Add the existing xattrs back in, if they weren't already set
    if (existingXAttrs != null) {
      for (XAttr existing : existingXAttrs) {
        boolean alreadySet = false;
        for (XAttr set : toSet) {
          if (set.equalsIgnoreValue(existing)) {
            alreadySet = true;
            break;
          }
        }
        if (!alreadySet) {
          xAttrs.add(existing);
          if (isUserVisible(existing)) {
            userVisibleXAttrsNum++;
          }
        }
      }
    }

    if (userVisibleXAttrsNum > inodeXAttrsLimit) {
      throw new IOException("Cannot add additional XAttr to inode, "
          + "would exceed limit of " + inodeXAttrsLimit);
    }

    return xAttrs;
  }
  
  private boolean isUserVisible(XAttr xAttr) {
    if (xAttr.getNameSpace() == XAttr.NameSpace.USER || 
        xAttr.getNameSpace() == XAttr.NameSpace.TRUSTED) {
      return true;
    }
    return false;
  }
  
  List<XAttr> getXAttrs(String src) throws IOException {
    String srcs = normalizePath(src);
    readLock();
    try {
      INodesInPath iip = getLastINodeInPath(srcs, true);
      INode inode = resolveLastINode(src, iip);
      int snapshotId = iip.getPathSnapshotId();
      return unprotectedGetXAttrs(inode, snapshotId);
    } finally {
      readUnlock();
    }
  }

  List<XAttr> getXAttrs(INode inode, int snapshotId) throws IOException {
    readLock();
    try {
      return unprotectedGetXAttrs(inode, snapshotId);
    } finally {
      readUnlock();
    }
  }

  private List<XAttr> unprotectedGetXAttrs(INode inode, int snapshotId)
      throws IOException {
    return XAttrStorage.readINodeXAttrs(inode, snapshotId);
  }

  private XAttr unprotectedGetXAttrByName(INode inode, int snapshotId,
      String xAttrName)
      throws IOException {
    List<XAttr> xAttrs = XAttrStorage.readINodeXAttrs(inode, snapshotId);
    if (xAttrs == null) {
      return null;
    }
    for (XAttr x : xAttrs) {
      if (XAttrHelper.getPrefixName(x)
          .equals(xAttrName)) {
        return x;
      }
    }
    return null;
  }

  private static INode resolveLastINode(String src, INodesInPath iip)
      throws FileNotFoundException {
    INode inode = iip.getLastINode();
    if (inode == null)
      throw new FileNotFoundException("cannot find " + src);
    return inode;
  }

  /**
   * Caches frequently used file names to reuse file name objects and
   * reduce heap size.
   */
  void cacheName(INode inode) {
    // Name is cached only for files
    if (!inode.isFile()) {
      return;
    }
    ByteArray name = new ByteArray(inode.getLocalNameBytes());
    name = nameCache.put(name);
    if (name != null) {
      inode.setLocalName(name.getBytes());
    }
  }
  
  void shutdown() {
    nameCache.reset();
    inodeMap.clear();
  }
  
  /**
   * Given an INode get all the path complents leading to it from the root.
   * If an Inode corresponding to C is given in /A/B/C, the returned
   * patch components will be {root, A, B, C}.
   * Note that this method cannot handle scenarios where the inode is in a
   * snapshot.
   */
  public static byte[][] getPathComponents(INode inode) {
    List<byte[]> components = new ArrayList<byte[]>();
    components.add(0, inode.getLocalNameBytes());
    while(inode.getParent() != null) {
      components.add(0, inode.getParent().getLocalNameBytes());
      inode = inode.getParent();
    }
    return components.toArray(new byte[components.size()][]);
  }

  /**
   * @return path components for reserved path, else null.
   */
  static byte[][] getPathComponentsForReservedPath(String src) {
    return !isReservedName(src) ? null : INode.getPathComponents(src);
  }

  /** Check if a given inode name is reserved */
  public static boolean isReservedName(INode inode) {
    return CHECK_RESERVED_FILE_NAMES
            && Arrays.equals(inode.getLocalNameBytes(), DOT_RESERVED);
  }

  /** Check if a given path is reserved */
  public static boolean isReservedName(String src) {
    return src.startsWith(DOT_RESERVED_PATH_PREFIX);
  }

  static boolean isReservedRawName(String src) {
    return src.startsWith(DOT_RESERVED_PATH_PREFIX +
        Path.SEPARATOR + RAW_STRING);
  }

  /**
   * Resolve a /.reserved/... path to a non-reserved path.
   * <p/>
   * There are two special hierarchies under /.reserved/:
   * <p/>
   * /.reserved/.inodes/<inodeid> performs a path lookup by inodeid,
   * <p/>
   * /.reserved/raw/... returns the encrypted (raw) bytes of a file in an
   * encryption zone. For instance, if /ezone is an encryption zone, then
   * /ezone/a refers to the decrypted file and /.reserved/raw/ezone/a refers to
   * the encrypted (raw) bytes of /ezone/a.
   * <p/>
   * Pathnames in the /.reserved/raw directory that resolve to files not in an
   * encryption zone are equivalent to the corresponding non-raw path. Hence,
   * if /a/b/c refers to a file that is not in an encryption zone, then
   * /.reserved/raw/a/b/c is equivalent (they both refer to the same
   * unencrypted file).
   * 
   * @param src path that is being processed
   * @param pathComponents path components corresponding to the path
   * @param fsd FSDirectory
   * @return if the path indicates an inode, return path after replacing up to
   *         <inodeid> with the corresponding path of the inode, else the path
   *         in {@code src} as is. If the path refers to a path in the "raw"
   *         directory, return the non-raw pathname.
   * @throws FileNotFoundException if inodeid is invalid
   */
  static String resolvePath(String src, byte[][] pathComponents,
      FSDirectory fsd) throws FileNotFoundException {
    final int nComponents = (pathComponents == null) ?
        0 : pathComponents.length;
    if (nComponents <= 2) {
      return src;
    }
    if (!Arrays.equals(DOT_RESERVED, pathComponents[1])) {
      /* This is not a /.reserved/ path so do nothing. */
      return src;
    }

    if (Arrays.equals(DOT_INODES, pathComponents[2])) {
      /* It's a /.reserved/.inodes path. */
      if (nComponents > 3) {
        return resolveDotInodesPath(src, pathComponents, fsd);
      } else {
        return src;
      }
    } else if (Arrays.equals(RAW, pathComponents[2])) {
      /* It's /.reserved/raw so strip off the /.reserved/raw prefix. */
      if (nComponents == 3) {
        return Path.SEPARATOR;
      } else {
        return constructRemainingPath("", pathComponents, 3);
      }
    } else {
      /* It's some sort of /.reserved/<unknown> path. Ignore it. */
      return src;
    }
  }

  private static String resolveDotInodesPath(String src,
      byte[][] pathComponents, FSDirectory fsd)
      throws FileNotFoundException {
    final String inodeId = DFSUtil.bytes2String(pathComponents[3]);
    final long id;
    try {
      id = Long.parseLong(inodeId);
    } catch (NumberFormatException e) {
      throw new FileNotFoundException("Invalid inode path: " + src);
    }
    if (id == INodeId.ROOT_INODE_ID && pathComponents.length == 4) {
      return Path.SEPARATOR;
    }
    INode inode = fsd.getInode(id);
    if (inode == null) {
      throw new FileNotFoundException(
          "File for given inode path does not exist: " + src);
    }
    
    // Handle single ".." for NFS lookup support.
    if ((pathComponents.length > 4)
        && DFSUtil.bytes2String(pathComponents[4]).equals("..")) {
      INode parent = inode.getParent();
      if (parent == null || parent.getId() == INodeId.ROOT_INODE_ID) {
        // inode is root, or its parent is root.
        return Path.SEPARATOR;
      } else {
        return parent.getFullPathName();
      }
    }

    String path = "";
    if (id != INodeId.ROOT_INODE_ID) {
      path = inode.getFullPathName();
    }
    return constructRemainingPath(path, pathComponents, 4);
  }

  private static String constructRemainingPath(String pathPrefix,
      byte[][] pathComponents, int startAt) {

    StringBuilder path = new StringBuilder(pathPrefix);
    for (int i = startAt; i < pathComponents.length; i++) {
      path.append(Path.SEPARATOR).append(
          DFSUtil.bytes2String(pathComponents[i]));
    }
    if (NameNode.LOG.isDebugEnabled()) {
      NameNode.LOG.debug("Resolved path is " + path);
    }
    return path.toString();
  }

  /** @return the {@link INodesInPath} containing only the last inode. */
  private INodesInPath getLastINodeInPath(String path, boolean resolveLink
  ) throws UnresolvedLinkException {
    return INodesInPath.resolve(rootDir, INode.getPathComponents(path), 1,
            resolveLink);
  }

  /** @return the {@link INodesInPath} containing all inodes in the path. */
  INodesInPath getINodesInPath(String path, boolean resolveLink
  ) throws UnresolvedLinkException {
    final byte[][] components = INode.getPathComponents(path);
    return INodesInPath.resolve(rootDir, components, components.length,
            resolveLink);
  }

  /** @return the last inode in the path. */
  INode getNode(String path, boolean resolveLink)
          throws UnresolvedLinkException {
    return getLastINodeInPath(path, resolveLink).getINode(0);
  }

  /**
   * @return the INode of the last component in src, or null if the last
   * component does not exist.
   * @throws UnresolvedLinkException if symlink can't be resolved
   * @throws SnapshotAccessControlException if path is in RO snapshot
   */
  private INode getINode4Write(String src, boolean resolveLink)
          throws UnresolvedLinkException, SnapshotAccessControlException {
    return getINodesInPath4Write(src, resolveLink).getLastINode();
  }

  /**
   * @return the INodesInPath of the components in src
   * @throws UnresolvedLinkException if symlink can't be resolved
   * @throws SnapshotAccessControlException if path is in RO snapshot
   */
  INodesInPath getINodesInPath4Write(String src, boolean resolveLink)
          throws UnresolvedLinkException, SnapshotAccessControlException {
    final byte[][] components = INode.getPathComponents(src);
    INodesInPath inodesInPath = INodesInPath.resolve(rootDir, components,
            components.length, resolveLink);
    if (inodesInPath.isSnapshot()) {
      throw new SnapshotAccessControlException(
              "Modification on a read-only snapshot is disallowed");
    }
    return inodesInPath;
  }
}
