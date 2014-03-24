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

import static org.apache.hadoop.util.Time.now;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.FSLimitException.MaxDirectoryItemsExceededException;
import org.apache.hadoop.hdfs.protocol.FSLimitException.PathComponentTooLongException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithCount;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectorySnapshottable;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.Root;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.hdfs.util.ChunkedArrayList;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/*************************************************
 * FSDirectory stores the filesystem directory state.
 * It handles writing/loading values to disk, and logging
 * changes as we go.
 *
 * It keeps the filename->blockset mapping always-current
 * and logged to disk.
 * 
 *************************************************/
public class FSDirectory implements Closeable {
  private static INodeDirectorySnapshottable createRoot(FSNamesystem namesystem) {
    final INodeDirectory r = new INodeDirectory(
        INodeId.ROOT_INODE_ID,
        INodeDirectory.ROOT_NAME,
        namesystem.createFsOwnerPermissions(new FsPermission((short) 0755)),
        0L);
    r.addDirectoryWithQuotaFeature(
        DirectoryWithQuotaFeature.DEFAULT_NAMESPACE_QUOTA,
        DirectoryWithQuotaFeature.DEFAULT_DISKSPACE_QUOTA);
    final INodeDirectorySnapshottable s = new INodeDirectorySnapshottable(r);
    s.setSnapshotQuota(0);
    return s;
  }

  @VisibleForTesting
  static boolean CHECK_RESERVED_FILE_NAMES = true;
  public final static String DOT_RESERVED_STRING = ".reserved";
  public final static String DOT_RESERVED_PATH_PREFIX = Path.SEPARATOR
      + DOT_RESERVED_STRING;
  public final static byte[] DOT_RESERVED = 
      DFSUtil.string2Bytes(DOT_RESERVED_STRING);
  public final static String DOT_INODES_STRING = ".inodes";
  public final static byte[] DOT_INODES = 
      DFSUtil.string2Bytes(DOT_INODES_STRING);
  INodeDirectory rootDir;
  FSImage fsImage;  
  private final FSNamesystem namesystem;
  private volatile boolean ready = false;
  private final int maxComponentLength;
  private final int maxDirItems;
  private final int lsLimit;  // max list limit
  private final int contentCountLimit; // max content summary counts per run
  private final INodeMap inodeMap; // Synchronized by dirLock
  private long yieldCount = 0; // keep track of lock yield count.

  // lock to protect the directory and BlockMap
  private final ReentrantReadWriteLock dirLock;
  private final Condition cond;

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
    return this.dirLock.getReadHoldCount() > 0;
  }

  public int getReadHoldCount() {
    return this.dirLock.getReadHoldCount();
  }

  public int getWriteHoldCount() {
    return this.dirLock.getWriteHoldCount();
  }

  /**
   * Caches frequently used file names used in {@link INode} to reuse 
   * byte[] objects and reduce heap usage.
   */
  private final NameCache<ByteArray> nameCache;

  FSDirectory(FSImage fsImage, FSNamesystem ns, Configuration conf) {
    this.dirLock = new ReentrantReadWriteLock(true); // fair
    this.cond = dirLock.writeLock().newCondition();
    rootDir = createRoot(ns);
    inodeMap = INodeMap.newInstance(rootDir);
    this.fsImage = fsImage;
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
   * Notify that loading of this FSDirectory is complete, and
   * it is ready for use 
   */
  void imageLoadComplete() {
    Preconditions.checkState(!ready, "FSDirectory already loaded");
    setReady();
  }

  void setReady() {
    if(ready) return;
    writeLock();
    try {
      setReady(true);
      this.nameCache.initialized();
      cond.signalAll();
    } finally {
      writeUnlock();
    }
  }
  
  //This is for testing purposes only
  @VisibleForTesting
  boolean isReady() {
    return ready;
  }

  // exposed for unit tests
  protected void setReady(boolean flag) {
    ready = flag;
  }

  private void incrDeletedFileCount(long count) {
    if (getFSNamesystem() != null)
      NameNode.getNameNodeMetrics().incrFilesDeleted(count);
  }
    
  /**
   * Shutdown the filestore
   */
  @Override
  public void close() throws IOException {
    fsImage.close();
  }

  /**
   * Block until the object is ready to be used.
   */
  void waitForReady() {
    if (!ready) {
      writeLock();
      try {
        while (!ready) {
          try {
            cond.await(5000, TimeUnit.MILLISECONDS);
          } catch (InterruptedException ie) {
          }
        }
      } finally {
        writeUnlock();
      }
    }
  }

  /**
   * Add the given filename to the fs.
   * @throws FileAlreadyExistsException
   * @throws QuotaExceededException
   * @throws UnresolvedLinkException
   * @throws SnapshotAccessControlException 
   */
  INodeFile addFile(String path, PermissionStatus permissions,
      short replication, long preferredBlockSize, String clientName,
      String clientMachine, DatanodeDescriptor clientNode)
    throws FileAlreadyExistsException, QuotaExceededException,
      UnresolvedLinkException, SnapshotAccessControlException, AclException {
    waitForReady();

    // Always do an implicit mkdirs for parent directory tree.
    long modTime = now();
    
    Path parent = new Path(path).getParent();
    if (parent == null) {
      // Trying to add "/" as a file - this path has no
      // parent -- avoids an NPE below.
      return null;
    }
    
    if (!mkdirs(parent.toString(), permissions, true, modTime)) {
      return null;
    }
    INodeFile newNode = new INodeFile(namesystem.allocateNewInodeId(), null,
        permissions, modTime, modTime, BlockInfo.EMPTY_ARRAY, replication,
        preferredBlockSize);
    newNode.toUnderConstruction(clientName, clientMachine, clientNode);

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
                            short replication,
                            long modificationTime,
                            long atime,
                            long preferredBlockSize,
                            boolean underConstruction,
                            String clientName,
                            String clientMachine) {
    final INodeFile newNode;
    assert hasWriteLock();
    if (underConstruction) {
      newNode = new INodeFile(id, null, permissions, modificationTime,
          modificationTime, BlockInfo.EMPTY_ARRAY, replication,
          preferredBlockSize);
      newNode.toUnderConstruction(clientName, clientMachine, null);

    } else {
      newNode = new INodeFile(id, null, permissions, modificationTime, atime,
          BlockInfo.EMPTY_ARRAY, replication, preferredBlockSize);
    }

    try {
      if (addINode(path, newNode)) {
        if (aclEntries != null) {
          AclStorage.updateINodeAcl(newNode, aclEntries,
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
    waitForReady();

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
   * Persist the block list for the inode.
   */
  void persistBlocks(String path, INodeFile file, boolean logRetryCache) {
    Preconditions.checkArgument(file.isUnderConstruction());
    waitForReady();

    writeLock();
    try {
      fsImage.getEditLog().logUpdateBlocks(path, file, logRetryCache);
      if(NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* FSDirectory.persistBlocks: "
            +path+" with "+ file.getBlocks().length 
            +" blocks is persisted to the file system");
      }
    } finally {
      writeUnlock();
    }
  }
  
  /**
   * Persist the new block (the last block of the given file).
   */
  void persistNewBlock(String path, INodeFile file) {
    Preconditions.checkArgument(file.isUnderConstruction());
    waitForReady();

    writeLock();
    try {
      fsImage.getEditLog().logAddBlock(path, file);
    } finally {
      writeUnlock();
    }
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.persistNewBlock: "
          + path + " with new block " + file.getLastBlock().toString()
          + ", current total block count is " + file.getBlocks().length);
    }
  }
  
  /**
   * Close file.
   */
  void closeFile(String path, INodeFile file) {
    waitForReady();
    writeLock();
    try {
      // file is closed
      fsImage.getEditLog().logCloseFile(path, file);
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* FSDirectory.closeFile: "
            +path+" with "+ file.getBlocks().length 
            +" blocks is persisted to the file system");
      }
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
    waitForReady();

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
    final INodesInPath iip = rootDir.getINodesInPath4Write(path, true);
    updateCount(iip, 0, -fileNode.getBlockDiskspace(), true);
    return true;
  }

  /**
   * @throws SnapshotAccessControlException 
   * @see #unprotectedRenameTo(String, String, long)
   * @deprecated Use {@link #renameTo(String, String, Rename...)} instead.
   */
  @Deprecated
  boolean renameTo(String src, String dst, boolean logRetryCache) 
      throws QuotaExceededException, UnresolvedLinkException, 
      FileAlreadyExistsException, SnapshotAccessControlException, IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.renameTo: "
          +src+" to "+dst);
    }
    waitForReady();
    long now = now();
    writeLock();
    try {
      if (!unprotectedRenameTo(src, dst, now))
        return false;
    } finally {
      writeUnlock();
    }
    fsImage.getEditLog().logRename(src, dst, now, logRetryCache);
    return true;
  }

  /**
   * @see #unprotectedRenameTo(String, String, long, Options.Rename...)
   */
  void renameTo(String src, String dst, boolean logRetryCache, 
      Options.Rename... options)
      throws FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, QuotaExceededException,
      UnresolvedLinkException, IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.renameTo: " + src
          + " to " + dst);
    }
    waitForReady();
    long now = now();
    writeLock();
    try {
      if (unprotectedRenameTo(src, dst, now, options)) {
        incrDeletedFileCount(1);
      }
    } finally {
      writeUnlock();
    }
    fsImage.getEditLog().logRename(src, dst, now, logRetryCache, options);
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
   * @deprecated See {@link #renameTo(String, String)}
   */
  @Deprecated
  boolean unprotectedRenameTo(String src, String dst, long timestamp)
    throws QuotaExceededException, UnresolvedLinkException, 
    FileAlreadyExistsException, SnapshotAccessControlException, IOException {
    assert hasWriteLock();
    INodesInPath srcIIP = rootDir.getINodesInPath4Write(src, false);
    final INode srcInode = srcIIP.getLastINode();
    
    // check the validation of the source
    if (srcInode == null) {
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + "failed to rename " + src + " to " + dst
          + " because source does not exist");
      return false;
    } 
    if (srcIIP.getINodes().length == 1) {
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          +"failed to rename "+src+" to "+dst+ " because source is the root");
      return false;
    }
    
    // srcInode and its subtree cannot contain snapshottable directories with
    // snapshots
    List<INodeDirectorySnapshottable> snapshottableDirs = 
        new ArrayList<INodeDirectorySnapshottable>();
    checkSnapshot(srcInode, snapshottableDirs);
    
    if (isDir(dst)) {
      dst += Path.SEPARATOR + new Path(src).getName();
    }
    
    // check the validity of the destination
    if (dst.equals(src)) {
      return true;
    }
    if (srcInode.isSymlink() && 
        dst.equals(srcInode.asSymlink().getSymlinkString())) {
      throw new FileAlreadyExistsException(
          "Cannot rename symlink "+src+" to its target "+dst);
    }
    
    // dst cannot be directory or a file under src
    if (dst.startsWith(src) && 
        dst.charAt(src.length()) == Path.SEPARATOR_CHAR) {
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + "failed to rename " + src + " to " + dst
          + " because destination starts with src");
      return false;
    }
    
    byte[][] dstComponents = INode.getPathComponents(dst);
    INodesInPath dstIIP = getExistingPathINodes(dstComponents);
    if (dstIIP.isSnapshot()) {
      throw new SnapshotAccessControlException(
          "Modification on RO snapshot is disallowed");
    }
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
    
    // Ensure dst has quota to accommodate rename
    verifyFsLimitsForRename(srcIIP, dstIIP);
    verifyQuotaForRename(srcIIP.getINodes(), dstIIP.getINodes());
    
    boolean added = false;
    INode srcChild = srcIIP.getLastINode();
    final byte[] srcChildName = srcChild.getLocalNameBytes();
    final boolean isSrcInSnapshot = srcChild.isInLatestSnapshot(
        srcIIP.getLatestSnapshotId());
    final boolean srcChildIsReference = srcChild.isReference();
    
    // Record the snapshot on srcChild. After the rename, before any new 
    // snapshot is taken on the dst tree, changes will be recorded in the latest
    // snapshot of the src tree.
    if (isSrcInSnapshot) {
      srcChild = srcChild.recordModification(srcIIP.getLatestSnapshotId());
      srcIIP.setLastINode(srcChild);
    }
    
    // check srcChild for reference
    final INodeReference.WithCount withCount;
    Quota.Counts oldSrcCounts = Quota.Counts.newInstance();
    int srcRefDstSnapshot = srcChildIsReference ? srcChild.asReference()
        .getDstSnapshotId() : Snapshot.CURRENT_STATE_ID;
    if (isSrcInSnapshot) {
      final INodeReference.WithName withName = 
          srcIIP.getINode(-2).asDirectory().replaceChild4ReferenceWithName(
              srcChild, srcIIP.getLatestSnapshotId()); 
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

    try {
      // remove src
      final long removedSrc = removeLastINode(srcIIP);
      if (removedSrc == -1) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
            + "failed to rename " + src + " to " + dst
            + " because the source can not be removed");
        return false;
      }
      
      if (dstParent.getParent() == null) {
        // src and dst file/dir are in the same directory, and the dstParent has
        // been replaced when we removed the src. Refresh the dstIIP and
        // dstParent.
        dstIIP = getExistingPathINodes(dstComponents);
        dstParent = dstIIP.getINode(-2);
      }
      
      // add src to the destination
      
      srcChild = srcIIP.getLastINode();
      final byte[] dstChildName = dstIIP.getLastLocalName();
      final INode toDst;
      if (withCount == null) {
        srcChild.setLocalName(dstChildName);
        toDst = srcChild;
      } else {
        withCount.getReferredINode().setLocalName(dstChildName);
        int dstSnapshotId = dstIIP.getLatestSnapshotId();
        final INodeReference.DstReference ref = new INodeReference.DstReference(
            dstParent.asDirectory(), withCount, dstSnapshotId);
        toDst = ref;
      }
      
      added = addLastINodeNoQuotaCheck(dstIIP, toDst);
      if (added) {
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedRenameTo: " 
              + src + " is renamed to " + dst);
        }
        // update modification time of dst and the parent of src
        final INode srcParent = srcIIP.getINode(-2);
        srcParent.updateModificationTime(timestamp, srcIIP.getLatestSnapshotId());
        dstParent = dstIIP.getINode(-2); // refresh dstParent
        dstParent.updateModificationTime(timestamp, dstIIP.getLatestSnapshotId());
        // update moved leases with new filename
        getFSNamesystem().unprotectedChangeLease(src, dst);     

        // update the quota usage in src tree
        if (isSrcInSnapshot) {
          // get the counts after rename
          Quota.Counts newSrcCounts = srcChild.computeQuotaUsage(
              Quota.Counts.newInstance(), false);
          newSrcCounts.subtract(oldSrcCounts);
          srcParent.addSpaceConsumed(newSrcCounts.get(Quota.NAMESPACE),
              newSrcCounts.get(Quota.DISKSPACE), false);
        }
        
        return true;
      }
    } finally {
      if (!added) {
        final INodeDirectory srcParent = srcIIP.getINode(-2).asDirectory();
        final INode oldSrcChild = srcChild;
        // put it back
        if (withCount == null) {
          srcChild.setLocalName(srcChildName);
        } else if (!srcChildIsReference) { // src must be in snapshot
          // the withCount node will no longer be used thus no need to update
          // its reference number here
          final INode originalChild = withCount.getReferredINode();
          srcChild = originalChild;
          srcChild.setLocalName(srcChildName);
        } else {
          withCount.removeReference(oldSrcChild.asReference());
          final INodeReference originalRef = new INodeReference.DstReference(
              srcParent, withCount, srcRefDstSnapshot);
          srcChild = originalRef;
          withCount.getReferredINode().setLocalName(srcChildName);
        }
        
        if (isSrcInSnapshot) {
          // srcParent must have snapshot feature since isSrcInSnapshot is true
          // and src node has been removed from srcParent 
          srcParent.undoRename4ScrParent(oldSrcChild.asReference(), srcChild);
        } else {
          // original srcChild is not in latest snapshot, we only need to add
          // the srcChild back
          addLastINodeNoQuotaCheck(srcIIP, srcChild);
        }
      }
    }
    NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
        +"failed to rename "+src+" to "+dst);
    return false;
  }

  /**
   * Rename src to dst.
   * See {@link DistributedFileSystem#rename(Path, Path, Options.Rename...)}
   * for details related to rename semantics and exceptions.
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
    assert hasWriteLock();
    boolean overwrite = false;
    if (null != options) {
      for (Rename option : options) {
        if (option == Rename.OVERWRITE) {
          overwrite = true;
        }
      }
    }
    String error = null;
    final INodesInPath srcIIP = rootDir.getINodesInPath4Write(src, false);
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
    
    // validate the destination
    if (dst.equals(src)) {
      throw new FileAlreadyExistsException(
          "The source "+src+" and destination "+dst+" are the same");
    }
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
    INodesInPath dstIIP = rootDir.getINodesInPath4Write(dst, false);
    if (dstIIP.getINodes().length == 1) {
      error = "rename destination cannot be the root";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new IOException(error);
    }

    final INode dstInode = dstIIP.getLastINode();
    List<INodeDirectorySnapshottable> snapshottableDirs = 
        new ArrayList<INodeDirectorySnapshottable>();
    if (dstInode != null) { // Destination exists
      // It's OK to rename a file to a symlink and vice versa
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

    INode srcChild = srcIIP.getLastINode();
    final byte[] srcChildName = srcChild.getLocalNameBytes();
    final boolean isSrcInSnapshot = srcChild.isInLatestSnapshot(
        srcIIP.getLatestSnapshotId());
    final boolean srcChildIsReference = srcChild.isReference();
    
    // Record the snapshot on srcChild. After the rename, before any new 
    // snapshot is taken on the dst tree, changes will be recorded in the latest
    // snapshot of the src tree.
    if (isSrcInSnapshot) {
      srcChild = srcChild.recordModification(srcIIP.getLatestSnapshotId());
      srcIIP.setLastINode(srcChild);
    }
    
    // check srcChild for reference
    final INodeReference.WithCount withCount;
    int srcRefDstSnapshot = srcChildIsReference ? srcChild.asReference()
        .getDstSnapshotId() : Snapshot.CURRENT_STATE_ID;
    Quota.Counts oldSrcCounts = Quota.Counts.newInstance();    
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
    
    boolean undoRemoveSrc = true;
    final long removedSrc = removeLastINode(srcIIP);
    if (removedSrc == -1) {
      error = "Failed to rename " + src + " to " + dst
          + " because the source can not be removed";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new IOException(error);
    }
    
    if (dstParent.getParent() == null) {
      // src and dst file/dir are in the same directory, and the dstParent has
      // been replaced when we removed the src. Refresh the dstIIP and
      // dstParent.
      dstIIP = rootDir.getINodesInPath4Write(dst, false);
    }
    
    boolean undoRemoveDst = false;
    INode removedDst = null;
    try {
      if (dstInode != null) { // dst exists remove it
        if (removeLastINode(dstIIP) != -1) {
          removedDst = dstIIP.getLastINode();
          undoRemoveDst = true;
        }
      }
      
      srcChild = srcIIP.getLastINode();

      final byte[] dstChildName = dstIIP.getLastLocalName();
      final INode toDst;
      if (withCount == null) {
        srcChild.setLocalName(dstChildName);
        toDst = srcChild;
      } else {
        withCount.getReferredINode().setLocalName(dstChildName);
        int dstSnapshotId = dstIIP.getLatestSnapshotId();
        final INodeReference.DstReference ref = new INodeReference.DstReference(
            dstIIP.getINode(-2).asDirectory(), withCount, dstSnapshotId);
        toDst = ref;
      }

      // add src as dst to complete rename
      if (addLastINodeNoQuotaCheck(dstIIP, toDst)) {
        undoRemoveSrc = false;
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug(
              "DIR* FSDirectory.unprotectedRenameTo: " + src
              + " is renamed to " + dst);
        }

        final INode srcParent = srcIIP.getINode(-2);
        srcParent.updateModificationTime(timestamp, srcIIP.getLatestSnapshotId());
        dstParent = dstIIP.getINode(-2);
        dstParent.updateModificationTime(timestamp, dstIIP.getLatestSnapshotId());
        // update moved lease with new filename
        getFSNamesystem().unprotectedChangeLease(src, dst);

        // Collect the blocks and remove the lease for previous dst
        long filesDeleted = -1;
        if (removedDst != null) {
          undoRemoveDst = false;
          BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
          List<INode> removedINodes = new ChunkedArrayList<INode>();
          filesDeleted = removedDst.cleanSubtree(Snapshot.CURRENT_STATE_ID,
              dstIIP.getLatestSnapshotId(), collectedBlocks, removedINodes, true)
              .get(Quota.NAMESPACE);
          getFSNamesystem().removePathAndBlocks(src, collectedBlocks,
              removedINodes);
        }

        if (snapshottableDirs.size() > 0) {
          // There are snapshottable directories (without snapshots) to be
          // deleted. Need to update the SnapshotManager.
          namesystem.removeSnapshottableDirs(snapshottableDirs);
        }
        
        // update the quota usage in src tree
        if (isSrcInSnapshot) {
          // get the counts after rename
          Quota.Counts newSrcCounts = srcChild.computeQuotaUsage(
              Quota.Counts.newInstance(), false);
          newSrcCounts.subtract(oldSrcCounts);
          srcParent.addSpaceConsumed(newSrcCounts.get(Quota.NAMESPACE),
              newSrcCounts.get(Quota.DISKSPACE), false);
        }
        
        return filesDeleted >= 0;
      }
    } finally {
      if (undoRemoveSrc) {
        // Rename failed - restore src
        final INodeDirectory srcParent = srcIIP.getINode(-2).asDirectory();
        final INode oldSrcChild = srcChild;
        // put it back
        if (withCount == null) {
          srcChild.setLocalName(srcChildName);
        } else if (!srcChildIsReference) { // src must be in snapshot
          // the withCount node will no longer be used thus no need to update
          // its reference number here
          final INode originalChild = withCount.getReferredINode();
          srcChild = originalChild;
          srcChild.setLocalName(srcChildName);
        } else {
          withCount.removeReference(oldSrcChild.asReference());
          final INodeReference originalRef = new INodeReference.DstReference(
              srcParent, withCount, srcRefDstSnapshot);
          srcChild = originalRef;
          withCount.getReferredINode().setLocalName(srcChildName);
        }
        
        if (srcParent.isWithSnapshot()) {
          srcParent.undoRename4ScrParent(oldSrcChild.asReference(), srcChild);
        } else {
          // srcParent is not an INodeDirectoryWithSnapshot, we only need to add
          // the srcChild back
          addLastINodeNoQuotaCheck(srcIIP, srcChild);
        }
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
    waitForReady();
    writeLock();
    try {
      final Block[] fileBlocks = unprotectedSetReplication(
          src, replication, blockRepls);
      if (fileBlocks != null)  // log replication change
        fsImage.getEditLog().logSetReplication(src, replication);
      return fileBlocks;
    } finally {
      writeUnlock();
    }
  }

  Block[] unprotectedSetReplication(String src, short replication,
      short[] blockRepls) throws QuotaExceededException,
      UnresolvedLinkException, SnapshotAccessControlException {
    assert hasWriteLock();

    final INodesInPath iip = rootDir.getINodesInPath4Write(src, true);
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

    file = file.setFileReplication(replication, iip.getLatestSnapshotId(),
        inodeMap);
    
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

  /**
   * @param path the file path
   * @return the block size of the file. 
   */
  long getPreferredBlockSize(String path) throws UnresolvedLinkException,
      FileNotFoundException, IOException {
    readLock();
    try {
      return INodeFile.valueOf(rootDir.getNode(path, false), path
          ).getPreferredBlockSize();
    } finally {
      readUnlock();
    }
  }

  boolean exists(String src) throws UnresolvedLinkException {
    src = normalizePath(src);
    readLock();
    try {
      INode inode = rootDir.getNode(src, false);
      if (inode == null) {
         return false;
      }
      return !inode.isFile() || inode.asFile().getBlocks() != null;
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
    fsImage.getEditLog().logSetPermissions(src, permission);
  }
  
  void unprotectedSetPermission(String src, FsPermission permissions)
      throws FileNotFoundException, UnresolvedLinkException,
      QuotaExceededException, SnapshotAccessControlException {
    assert hasWriteLock();
    final INodesInPath inodesInPath = rootDir.getINodesInPath4Write(src, true);
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
    fsImage.getEditLog().logSetOwner(src, username, groupname);
  }

  void unprotectedSetOwner(String src, String username, String groupname)
      throws FileNotFoundException, UnresolvedLinkException,
      QuotaExceededException, SnapshotAccessControlException {
    assert hasWriteLock();
    final INodesInPath inodesInPath = rootDir.getINodesInPath4Write(src, true);
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
  void concat(String target, String [] srcs, boolean supportRetryCache) 
      throws UnresolvedLinkException, QuotaExceededException,
      SnapshotAccessControlException, SnapshotException {
    writeLock();
    try {
      // actual move
      waitForReady();
      long timestamp = now();
      unprotectedConcat(target, srcs, timestamp);
      // do the commit
      fsImage.getEditLog().logConcat(target, srcs, timestamp, 
          supportRetryCache);
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
    
    final INodesInPath trgIIP = rootDir.getINodesInPath4Write(target, true);
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
    
    // update inodeMap
    removeFromInodeMap(Arrays.asList(allSrcInodes));
    
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
   * @param logRetryCache Whether to record RPC IDs in editlog to support retry
   *                      cache rebuilding.
   * @return true on successful deletion; else false
   */
  boolean delete(String src, BlocksMapUpdateInfo collectedBlocks,
      List<INode> removedINodes, boolean logRetryCache) throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.delete: " + src);
    }
    waitForReady();
    long now = now();
    final long filesRemoved;
    writeLock();
    try {
      final INodesInPath inodesInPath = rootDir.getINodesInPath4Write(
          normalizePath(src), false);
      if (!deleteAllowed(inodesInPath, src) ) {
        filesRemoved = -1;
      } else {
        List<INodeDirectorySnapshottable> snapshottableDirs = 
            new ArrayList<INodeDirectorySnapshottable>();
        checkSnapshot(inodesInPath.getLastINode(), snapshottableDirs);
        filesRemoved = unprotectedDelete(inodesInPath, collectedBlocks,
            removedINodes, now);
        namesystem.removeSnapshottableDirs(snapshottableDirs);
      }
    } finally {
      writeUnlock();
    }
    if (filesRemoved < 0) {
      return false;
    }
    fsImage.getEditLog().logDelete(src, now, logRetryCache);
    incrDeletedFileCount(filesRemoved);
    // Blocks/INodes will be handled later by the caller of this method
    getFSNamesystem().removePathAndBlocks(src, null, null);
    return true;
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
      final INodesInPath inodesInPath = rootDir.getLastINodeInPath(path, false);
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

    final INodesInPath inodesInPath = rootDir.getINodesInPath4Write(
        normalizePath(src), false);
    long filesRemoved = -1;
    if (deleteAllowed(inodesInPath, src)) {
      List<INodeDirectorySnapshottable> snapshottableDirs = 
          new ArrayList<INodeDirectorySnapshottable>();
      checkSnapshot(inodesInPath.getLastINode(), snapshottableDirs);
      filesRemoved = unprotectedDelete(inodesInPath, collectedBlocks,
          removedINodes, mtime);
      namesystem.removeSnapshottableDirs(snapshottableDirs); 
    }

    if (filesRemoved >= 0) {
      getFSNamesystem().removePathAndBlocks(src, collectedBlocks, 
          removedINodes);
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
    targetNode = targetNode.recordModification(latestSnapshot);
    iip.setLastINode(targetNode);

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
      List<INodeDirectorySnapshottable> snapshottableDirs) throws IOException {
    if (target.isDirectory()) {
      INodeDirectory targetDir = target.asDirectory();
      if (targetDir.isSnapshottable()) {
        INodeDirectorySnapshottable ssTargetDir = 
            (INodeDirectorySnapshottable) targetDir;
        if (ssTargetDir.getNumSnapshots() > 0) {
          throw new IOException("The directory " + ssTargetDir.getFullPathName()
              + " cannot be deleted since " + ssTargetDir.getFullPathName()
              + " is snapshottable and already has snapshots");
        } else {
          if (snapshottableDirs != null) {
            snapshottableDirs.add(ssTargetDir);
          }
        }
      } 
      for (INode child : targetDir.getChildrenList(Snapshot.CURRENT_STATE_ID)) {
        checkSnapshot(child, snapshottableDirs);
      }
    }
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
      boolean needLocation) throws UnresolvedLinkException, IOException {
    String srcs = normalizePath(src);

    readLock();
    try {
      if (srcs.endsWith(HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR)) {
        return getSnapshotsListing(srcs, startAfter);
      }
      final INodesInPath inodesInPath = rootDir.getLastINodeInPath(srcs, true);
      final int snapshot = inodesInPath.getPathSnapshotId();
      final INode targetNode = inodesInPath.getINode(0);
      if (targetNode == null)
        return null;
      
      if (!targetNode.isDirectory()) {
        return new DirectoryListing(
            new HdfsFileStatus[]{createFileStatus(HdfsFileStatus.EMPTY_NAME,
                targetNode, needLocation, snapshot)}, 0);
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
        listing[i] = createFileStatus(cur.getLocalNameBytes(), cur,
            needLocation, snapshot);
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
    final INodeDirectorySnapshottable dirNode = INodeDirectorySnapshottable
        .valueOf(node, dirPath);
    final ReadOnlyList<Snapshot> snapshots = dirNode.getSnapshotList();
    int skipSize = ReadOnlyList.Util.binarySearch(snapshots, startAfter);
    skipSize = skipSize < 0 ? -skipSize - 1 : skipSize + 1;
    int numOfListing = Math.min(snapshots.size() - skipSize, this.lsLimit);
    final HdfsFileStatus listing[] = new HdfsFileStatus[numOfListing];
    for (int i = 0; i < numOfListing; i++) {
      Root sRoot = snapshots.get(i + skipSize).getRoot();
      listing[i] = createFileStatus(sRoot.getLocalNameBytes(), sRoot,
          Snapshot.CURRENT_STATE_ID);
    }
    return new DirectoryListing(
        listing, snapshots.size() - skipSize - numOfListing);
  }

  /** Get the file info for a specific file.
   * @param src The string representation of the path to the file
   * @param resolveLink whether to throw UnresolvedLinkException 
   * @return object containing information regarding the file
   *         or null if file not found
   */
  HdfsFileStatus getFileInfo(String src, boolean resolveLink) 
      throws UnresolvedLinkException {
    String srcs = normalizePath(src);
    readLock();
    try {
      if (srcs.endsWith(HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR)) {
        return getFileInfo4DotSnapshot(srcs);
      }
      final INodesInPath inodesInPath = rootDir.getLastINodeInPath(srcs, resolveLink);
      final INode i = inodesInPath.getINode(0);
      return i == null? null: createFileStatus(HdfsFileStatus.EMPTY_NAME, i,
          inodesInPath.getPathSnapshotId());
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
          HdfsFileStatus.EMPTY_NAME, -1L, 0);
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
    if (node != null
        && node.isDirectory()
        && node.asDirectory() instanceof INodeDirectorySnapshottable) {
      return node;
    }
    return null;
  }

  /**
   * Get the blocks associated with the file.
   */
  Block[] getFileBlocks(String src) throws UnresolvedLinkException {
    waitForReady();
    readLock();
    try {
      final INode i = rootDir.getNode(src, false);
      return i != null && i.isFile()? i.asFile().getBlocks(): null;
    } finally {
      readUnlock();
    }
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
      return rootDir.getLastINodeInPath(src, true);
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
      return rootDir.getINodesInPath4Write(src, true);
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
      return rootDir.getINode4Write(src, true);
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
      if (srcs.startsWith("/") && !srcs.endsWith("/")
          && rootDir.getINode4Write(srcs, false) == null) {
        return true;
      } else {
        return false;
      }
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
      INode node = rootDir.getNode(src, false);
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
      INode node = rootDir.getINode4Write(src, false);
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
      final INodesInPath iip = rootDir.getINodesInPath4Write(path, false);
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
    if (!ready) {
      //still initializing. do not check or update quotas.
      return;
    }
    final INode[] inodes = iip.getINodes();
    if (numOfINodes > inodes.length) {
      numOfINodes = inodes.length;
    }
    if (checkQuota) {
      verifyQuota(inodes, numOfINodes, nsDelta, dsDelta, null);
    }
    unprotectedUpdateCount(iip, numOfINodes, nsDelta, dsDelta);
  }
  
  /** 
   * update quota of each inode and check to see if quota is exceeded. 
   * See {@link #updateCount(INode[], int, long, long, boolean)}
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
  
  /**
   * Create a directory 
   * If ancestor directories do not exist, automatically create them.

   * @param src string representation of the path to the directory
   * @param permissions the permission of the directory
   * @param isAutocreate if the permission of the directory should inherit
   *                          from its parent or not. u+wx is implicitly added to
   *                          the automatically created directories, and to the
   *                          given directory if inheritPermission is true
   * @param now creation time
   * @return true if the operation succeeds false otherwise
   * @throws FileNotFoundException if an ancestor or itself is a file
   * @throws QuotaExceededException if directory creation violates 
   *                                any quota limit
   * @throws UnresolvedLinkException if a symlink is encountered in src.                      
   * @throws SnapshotAccessControlException if path is in RO snapshot
   */
  boolean mkdirs(String src, PermissionStatus permissions,
      boolean inheritPermission, long now)
      throws FileAlreadyExistsException, QuotaExceededException, 
             UnresolvedLinkException, SnapshotAccessControlException,
             AclException {
    src = normalizePath(src);
    String[] names = INode.getPathNames(src);
    byte[][] components = INode.getPathComponents(names);
    final int lastInodeIndex = components.length - 1;

    writeLock();
    try {
      INodesInPath iip = getExistingPathINodes(components);
      if (iip.isSnapshot()) {
        throw new SnapshotAccessControlException(
            "Modification on RO snapshot is disallowed");
      }
      INode[] inodes = iip.getINodes();

      // find the index of the first null in inodes[]
      StringBuilder pathbuilder = new StringBuilder();
      int i = 1;
      for(; i < inodes.length && inodes[i] != null; i++) {
        pathbuilder.append(Path.SEPARATOR).append(names[i]);
        if (!inodes[i].isDirectory()) {
          throw new FileAlreadyExistsException("Parent path is not a directory: "
              + pathbuilder+ " "+inodes[i].getLocalName());
        }
      }

      // default to creating parent dirs with the given perms
      PermissionStatus parentPermissions = permissions;

      // if not inheriting and it's the last inode, there's no use in
      // computing perms that won't be used
      if (inheritPermission || (i < lastInodeIndex)) {
        // if inheriting (ie. creating a file or symlink), use the parent dir,
        // else the supplied permissions
        // NOTE: the permissions of the auto-created directories violate posix
        FsPermission parentFsPerm = inheritPermission
            ? inodes[i-1].getFsPermission() : permissions.getPermission();
        
        // ensure that the permissions allow user write+execute
        if (!parentFsPerm.getUserAction().implies(FsAction.WRITE_EXECUTE)) {
          parentFsPerm = new FsPermission(
              parentFsPerm.getUserAction().or(FsAction.WRITE_EXECUTE),
              parentFsPerm.getGroupAction(),
              parentFsPerm.getOtherAction()
          );
        }
        
        if (!parentPermissions.getPermission().equals(parentFsPerm)) {
          parentPermissions = new PermissionStatus(
              parentPermissions.getUserName(),
              parentPermissions.getGroupName(),
              parentFsPerm
          );
          // when inheriting, use same perms for entire path
          if (inheritPermission) permissions = parentPermissions;
        }
      }
      
      // create directories beginning from the first null index
      for(; i < inodes.length; i++) {
        pathbuilder.append(Path.SEPARATOR + names[i]);
        unprotectedMkdir(namesystem.allocateNewInodeId(), iip, i,
            components[i], (i < lastInodeIndex) ? parentPermissions
                : permissions, null, now);
        if (inodes[i] == null) {
          return false;
        }
        // Directory creation also count towards FilesCreated
        // to match count of FilesDeleted metric.
        if (getFSNamesystem() != null)
          NameNode.getNameNodeMetrics().incrFilesCreated();

        final String cur = pathbuilder.toString();
        fsImage.getEditLog().logMkDir(cur, inodes[i]);
        if(NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug(
              "DIR* FSDirectory.mkdirs: created directory " + cur);
        }
      }
    } finally {
      writeUnlock();
    }
    return true;
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
  private void unprotectedMkdir(long inodeId, INodesInPath inodesInPath,
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
   * @throw QuotaExceededException is thrown if it violates quota limit
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
    if (!ready) {
      // Do not check quota if edits log is still being processed
      return;
    }
    int i = 0;
    for(; src[i] == dst[i]; i++);
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
      if (!ready) {
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
      if (ready) {
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
      if (ready) {
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
   * @throw QuotaExceededException is thrown if it violates quota limit
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
    boolean added = false;
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
    INodeDirectory newParent = last.getParent();
    if (parent != newParent) {
      iip.setINode(-2, newParent);
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
  
  /**
   */
  String normalizePath(String src) {
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
      INode targetNode = rootDir.getNode(srcs, false);
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
      inodeMap.put((INodeWithAdditionalFields)inode);
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
   * @returns INodeDirectory if any of the quotas have changed. null other wise.
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
         nsQuota < HdfsConstants.QUOTA_RESET) || 
        (dsQuota < 0 && dsQuota != HdfsConstants.QUOTA_DONT_SET && 
          dsQuota < HdfsConstants.QUOTA_RESET)) {
      throw new IllegalArgumentException("Illegal value for nsQuota or " +
                                         "dsQuota : " + nsQuota + " and " +
                                         dsQuota);
    }
    
    String srcs = normalizePath(src);
    final INodesInPath iip = rootDir.getINodesInPath4Write(srcs, true);
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
      dirNode = dirNode.recordModification(latest);
      dirNode.setQuota(nsQuota, dsQuota);
      return dirNode;
    }
  }
  
  /**
   * See {@link ClientProtocol#setQuota(String, long, long)} for the contract.
   * @throws SnapshotAccessControlException if path is in RO snapshot
   * @see #unprotectedSetQuota(String, long, long)
   */
  void setQuota(String src, long nsQuota, long dsQuota) 
      throws FileNotFoundException, PathIsNotDirectoryException,
      QuotaExceededException, UnresolvedLinkException,
      SnapshotAccessControlException {
    writeLock();
    try {
      INodeDirectory dir = unprotectedSetQuota(src, nsQuota, dsQuota);
      if (dir != null) {
        final Quota.Counts q = dir.getQuotaCounts();
        fsImage.getEditLog().logSetQuota(src,
            q.get(Quota.NAMESPACE), q.get(Quota.DISKSPACE));
      }
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
  void setTimes(String src, INode inode, long mtime, long atime, boolean force,
      int latestSnapshotId) throws QuotaExceededException {
    boolean status = false;
    writeLock();
    try {
      status = unprotectedSetTimes(inode, mtime, atime, force, latestSnapshotId);
    } finally {
      writeUnlock();
    }
    if (status) {
      fsImage.getEditLog().logTimes(src, mtime, atime);
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
      setReady(false);
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
   * @return a file status
   * @throws IOException if any error occurs
   */
  private HdfsFileStatus createFileStatus(byte[] path, INode node,
      boolean needLocation, int snapshot) throws IOException {
    if (needLocation) {
      return createLocatedFileStatus(path, node, snapshot);
    } else {
      return createFileStatus(path, node, snapshot);
    }
  }
  /**
   * Create FileStatus by file INode 
   */
   HdfsFileStatus createFileStatus(byte[] path, INode node,
       int snapshot) {
     long size = 0;     // length is zero for directories
     short replication = 0;
     long blocksize = 0;
     if (node.isFile()) {
       final INodeFile fileNode = node.asFile();
       size = fileNode.computeFileSize(snapshot);
       replication = fileNode.getFileReplication(snapshot);
       blocksize = fileNode.getPreferredBlockSize();
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
        node.getFsPermission(snapshot),
        node.getUserName(snapshot),
        node.getGroupName(snapshot),
        node.isSymlink() ? node.asSymlink().getSymlink() : null,
        path,
        node.getId(),
        childrenNum);
  }

  /**
   * Create FileStatus with location info by file INode
   */
  private HdfsLocatedFileStatus createLocatedFileStatus(byte[] path,
      INode node, int snapshot) throws IOException {
    assert hasReadLock();
    long size = 0; // length is zero for directories
    short replication = 0;
    long blocksize = 0;
    LocatedBlocks loc = null;
    if (node.isFile()) {
      final INodeFile fileNode = node.asFile();
      size = fileNode.computeFileSize(snapshot);
      replication = fileNode.getFileReplication(snapshot);
      blocksize = fileNode.getPreferredBlockSize();

      final boolean inSnapshot = snapshot != Snapshot.CURRENT_STATE_ID; 
      final boolean isUc = inSnapshot ? false : fileNode.isUnderConstruction();
      final long fileSize = !inSnapshot && isUc ? 
          fileNode.computeFileSizeNotIncludingLastUcBlock() : size;
      loc = getFSNamesystem().getBlockManager().createLocatedBlocks(
          fileNode.getBlocks(), fileSize, isUc, 0L, size, false,
          inSnapshot);
      if (loc == null) {
        loc = new LocatedBlocks();
      }
    }
    int childrenNum = node.isDirectory() ? 
        node.asDirectory().getChildrenNum(snapshot) : 0;
        
    HdfsLocatedFileStatus status =
        new HdfsLocatedFileStatus(size, node.isDirectory(), replication,
          blocksize, node.getModificationTime(snapshot),
          node.getAccessTime(snapshot), node.getFsPermission(snapshot),
          node.getUserName(snapshot), node.getGroupName(snapshot),
          node.isSymlink() ? node.asSymlink().getSymlink() : null, path,
          node.getId(), loc, childrenNum);
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
   * Add the given symbolic link to the fs. Record it in the edits log.
   */
  INodeSymlink addSymlink(String path, String target,
      PermissionStatus dirPerms, boolean createParent, boolean logRetryCache)
      throws UnresolvedLinkException, FileAlreadyExistsException,
      QuotaExceededException, SnapshotAccessControlException, AclException {
    waitForReady();

    final long modTime = now();
    if (createParent) {
      final String parent = new Path(path).getParent().toString();
      if (!mkdirs(parent, dirPerms, true, modTime)) {
        return null;
      }
    }
    final String userName = dirPerms.getUserName();
    INodeSymlink newNode  = null;
    long id = namesystem.allocateNewInodeId();
    writeLock();
    try {
      newNode = unprotectedAddSymlink(id, path, target, modTime, modTime,
          new PermissionStatus(userName, null, FsPermission.getDefault()));
    } finally {
      writeUnlock();
    }
    if (newNode == null) {
      NameNode.stateChangeLog.info("DIR* addSymlink: failed to add " + path);
      return null;
    }
    fsImage.getEditLog().logSymlink(path, target, modTime, modTime, newNode,
        logRetryCache);
    
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* addSymlink: " + path + " is added");
    }
    return newNode;
  }

  /**
   * Add the specified path into the namespace. Invoked from edit log processing.
   */
  INodeSymlink unprotectedAddSymlink(long id, String path, String target,
      long mtime, long atime, PermissionStatus perm)
      throws UnresolvedLinkException, QuotaExceededException {
    assert hasWriteLock();
    final INodeSymlink symlink = new INodeSymlink(id, null, perm, mtime, atime,
        target);
    return addINode(path, symlink) ? symlink : null;
  }

  void modifyAclEntries(String src, List<AclEntry> aclSpec) throws IOException {
    writeLock();
    try {
      List<AclEntry> newAcl = unprotectedModifyAclEntries(src, aclSpec);
      fsImage.getEditLog().logSetAcl(src, newAcl);
    } finally {
      writeUnlock();
    }
  }

  private List<AclEntry> unprotectedModifyAclEntries(String src,
      List<AclEntry> aclSpec) throws IOException {
    assert hasWriteLock();
    INodesInPath iip = rootDir.getINodesInPath4Write(normalizePath(src), true);
    INode inode = resolveLastINode(src, iip);
    int snapshotId = iip.getLatestSnapshotId();
    List<AclEntry> existingAcl = AclStorage.readINodeLogicalAcl(inode);
    List<AclEntry> newAcl = AclTransformation.mergeAclEntries(existingAcl,
      aclSpec);
    AclStorage.updateINodeAcl(inode, newAcl, snapshotId);
    return newAcl;
  }

  void removeAclEntries(String src, List<AclEntry> aclSpec) throws IOException {
    writeLock();
    try {
      List<AclEntry> newAcl = unprotectedRemoveAclEntries(src, aclSpec);
      fsImage.getEditLog().logSetAcl(src, newAcl);
    } finally {
      writeUnlock();
    }
  }

  private List<AclEntry> unprotectedRemoveAclEntries(String src,
      List<AclEntry> aclSpec) throws IOException {
    assert hasWriteLock();
    INodesInPath iip = rootDir.getINodesInPath4Write(normalizePath(src), true);
    INode inode = resolveLastINode(src, iip);
    int snapshotId = iip.getLatestSnapshotId();
    List<AclEntry> existingAcl = AclStorage.readINodeLogicalAcl(inode);
    List<AclEntry> newAcl = AclTransformation.filterAclEntriesByAclSpec(
      existingAcl, aclSpec);
    AclStorage.updateINodeAcl(inode, newAcl, snapshotId);
    return newAcl;
  }

  void removeDefaultAcl(String src) throws IOException {
    writeLock();
    try {
      List<AclEntry> newAcl = unprotectedRemoveDefaultAcl(src);
      fsImage.getEditLog().logSetAcl(src, newAcl);
    } finally {
      writeUnlock();
    }
  }

  private List<AclEntry> unprotectedRemoveDefaultAcl(String src)
      throws IOException {
    assert hasWriteLock();
    INodesInPath iip = rootDir.getINodesInPath4Write(normalizePath(src), true);
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
      fsImage.getEditLog().logSetAcl(src, AclFeature.EMPTY_ENTRY_LIST);
    } finally {
      writeUnlock();
    }
  }

  private void unprotectedRemoveAcl(String src) throws IOException {
    assert hasWriteLock();
    INodesInPath iip = rootDir.getINodesInPath4Write(normalizePath(src), true);
    INode inode = resolveLastINode(src, iip);
    int snapshotId = iip.getLatestSnapshotId();
    AclStorage.removeINodeAcl(inode, snapshotId);
  }

  void setAcl(String src, List<AclEntry> aclSpec) throws IOException {
    writeLock();
    try {
      List<AclEntry> newAcl = unprotectedSetAcl(src, aclSpec);
      fsImage.getEditLog().logSetAcl(src, newAcl);
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
    INodesInPath iip = rootDir.getINodesInPath4Write(normalizePath(src), true);
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
      INodesInPath iip = rootDir.getLastINodeInPath(srcs, true);
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
   * patch components will be {root, A, B, C}
   */
  static byte[][] getPathComponents(INode inode) {
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
  
  /**
   * Resolve the path of /.reserved/.inodes/<inodeid>/... to a regular path
   * 
   * @param src path that is being processed
   * @param pathComponents path components corresponding to the path
   * @param fsd FSDirectory
   * @return if the path indicates an inode, return path after replacing upto
   *         <inodeid> with the corresponding path of the inode, else the path
   *         in {@code src} as is.
   * @throws FileNotFoundException if inodeid is invalid
   */
  static String resolvePath(String src, byte[][] pathComponents, FSDirectory fsd)
      throws FileNotFoundException {
    if (pathComponents == null || pathComponents.length <= 3) {
      return src;
    }
    // Not /.reserved/.inodes
    if (!Arrays.equals(DOT_RESERVED, pathComponents[1])
        || !Arrays.equals(DOT_INODES, pathComponents[2])) { // Not .inodes path
      return src;
    }
    final String inodeId = DFSUtil.bytes2String(pathComponents[3]);
    long id = 0;
    try {
      id = Long.valueOf(inodeId);
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

    StringBuilder path = id == INodeId.ROOT_INODE_ID ? new StringBuilder()
        : new StringBuilder(inode.getFullPathName());
    for (int i = 4; i < pathComponents.length; i++) {
      path.append(Path.SEPARATOR).append(DFSUtil.bytes2String(pathComponents[i]));
    }
    if (NameNode.LOG.isDebugEnabled()) {
      NameNode.LOG.debug("Resolved path is " + path);
    }
    return path.toString();
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
}
