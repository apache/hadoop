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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.FSLimitException;
import org.apache.hadoop.hdfs.protocol.FSLimitException.MaxDirectoryItemsExceededException;
import org.apache.hadoop.hdfs.protocol.FSLimitException.PathComponentTooLongException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory.INodesInPath;
import org.apache.hadoop.hdfs.util.ByteArray;

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
  private static INodeDirectoryWithQuota createRoot(FSNamesystem namesystem) {
    return new INodeDirectoryWithQuota(namesystem.allocateNewInodeId(),
        INodeDirectory.ROOT_NAME,
        namesystem.createFsOwnerPermissions(new FsPermission((short) 0755)));
  }

  INodeDirectoryWithQuota rootDir;
  FSImage fsImage;  
  private final FSNamesystem namesystem;
  private volatile boolean ready = false;
  private final int maxComponentLength;
  private final int maxDirItems;
  private final int lsLimit;  // max list limit

  // lock to protect the directory and BlockMap
  private ReentrantReadWriteLock dirLock;
  private Condition cond;

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

  /**
   * Caches frequently used file names used in {@link INode} to reuse 
   * byte[] objects and reduce heap usage.
   */
  private final NameCache<ByteArray> nameCache;

  FSDirectory(FSImage fsImage, FSNamesystem ns, Configuration conf) {
    this.dirLock = new ReentrantReadWriteLock(true); // fair
    this.cond = dirLock.writeLock().newCondition();
    rootDir = createRoot(ns);
    this.fsImage = fsImage;
    int configuredLimit = conf.getInt(
        DFSConfigKeys.DFS_LIST_LIMIT, DFSConfigKeys.DFS_LIST_LIMIT_DEFAULT);
    this.lsLimit = configuredLimit>0 ?
        configuredLimit : DFSConfigKeys.DFS_LIST_LIMIT_DEFAULT;
    
    // filesystem limits
    this.maxComponentLength = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_DEFAULT);
    this.maxDirItems = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_DEFAULT);

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

  private void incrDeletedFileCount(int count) {
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
   */
  INodeFileUnderConstruction addFile(String path, 
                PermissionStatus permissions,
                short replication,
                long preferredBlockSize,
                String clientName,
                String clientMachine,
                DatanodeDescriptor clientNode,
                long generationStamp) 
    throws FileAlreadyExistsException, QuotaExceededException,
      UnresolvedLinkException {
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
    long id = namesystem.allocateNewInodeId();
    INodeFileUnderConstruction newNode = new INodeFileUnderConstruction(
                                 id,
                                 permissions,replication,
                                 preferredBlockSize, modTime, clientName, 
                                 clientMachine, clientNode);
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

  INode unprotectedAddFile( long id,
                            String path, 
                            PermissionStatus permissions,
                            short replication,
                            long modificationTime,
                            long atime,
                            long preferredBlockSize,
                            boolean underConstruction,
                            String clientName,
                            String clientMachine) {
    final INode newNode;
    assert hasWriteLock();
    if (underConstruction) {
      newNode = new INodeFileUnderConstruction(id, permissions, replication,
          preferredBlockSize, modificationTime, clientName, clientMachine, null);
    } else {
      newNode = new INodeFile(id, permissions, BlockInfo.EMPTY_ARRAY,
          replication, modificationTime, atime, preferredBlockSize);
    }

    try {
      if (addINode(path, newNode)) {
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
      DatanodeDescriptor targets[]) throws IOException {
    waitForReady();

    writeLock();
    try {
      final INode[] inodes = inodesInPath.getINodes();
      final INodeFileUnderConstruction fileINode = 
          INodeFileUnderConstruction.valueOf(inodes[inodes.length-1], path);

      // check quota limits and updated space consumed
      updateCount(inodesInPath, inodes.length-1, 0,
          fileINode.getPreferredBlockSize()*fileINode.getBlockReplication(), true);

      // associate new last block for the file
      BlockInfoUnderConstruction blockInfo =
        new BlockInfoUnderConstruction(
            block,
            fileINode.getBlockReplication(),
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
  void persistBlocks(String path, INodeFileUnderConstruction file) {
    waitForReady();

    writeLock();
    try {
      fsImage.getEditLog().logUpdateBlocks(path, file);
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
   * Close file.
   */
  void closeFile(String path, INodeFile file) {
    waitForReady();
    long now = now();
    writeLock();
    try {
      // file is closed
      file.setModificationTimeForce(now);
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
   */
  void removeBlock(String path, INodeFileUnderConstruction fileNode,
                      Block block) throws IOException {
    waitForReady();

    writeLock();
    try {
      unprotectedRemoveBlock(path, fileNode, block);
    } finally {
      writeUnlock();
    }
  }
  
  void unprotectedRemoveBlock(String path, INodeFileUnderConstruction fileNode, 
      Block block) throws IOException {
    // modify file-> block and blocksMap
    fileNode.removeLastBlock(block);
    getBlockManager().removeBlockFromMap(block);

    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.removeBlock: "
          +path+" with "+block
          +" block is removed from the file system");
    }

    // update space consumed
    final INodesInPath inodesInPath = rootDir.getExistingPathINodes(path, true);
    final INode[] inodes = inodesInPath.getINodes();
    updateCount(inodesInPath, inodes.length-1, 0,
        -fileNode.getPreferredBlockSize()*fileNode.getBlockReplication(), true);
  }

  /**
   * @see #unprotectedRenameTo(String, String, long)
   * @deprecated Use {@link #renameTo(String, String, Rename...)} instead.
   */
  @Deprecated
  boolean renameTo(String src, String dst) 
      throws QuotaExceededException, UnresolvedLinkException, 
      FileAlreadyExistsException {
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
    fsImage.getEditLog().logRename(src, dst, now);
    return true;
  }

  /**
   * @see #unprotectedRenameTo(String, String, long, Options.Rename...)
   */
  void renameTo(String src, String dst, Options.Rename... options)
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
    fsImage.getEditLog().logRename(src, dst, now, options);
  }

  /**
   * Change a path name
   * 
   * @param src source path
   * @param dst destination path
   * @return true if rename succeeds; false otherwise
   * @throws QuotaExceededException if the operation violates any quota limit
   * @throws FileAlreadyExistsException if the src is a symlink that points to dst
   * @deprecated See {@link #renameTo(String, String)}
   */
  @Deprecated
  boolean unprotectedRenameTo(String src, String dst, long timestamp)
    throws QuotaExceededException, UnresolvedLinkException, 
    FileAlreadyExistsException {
    assert hasWriteLock();
    INodesInPath srcInodesInPath = rootDir.getExistingPathINodes(src, false);
    INode[] srcInodes = srcInodesInPath.getINodes();
    INode srcInode = srcInodes[srcInodes.length-1];
    
    // check the validation of the source
    if (srcInode == null) {
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + "failed to rename " + src + " to " + dst
          + " because source does not exist");
      return false;
    } 
    if (srcInodes.length == 1) {
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          +"failed to rename "+src+" to "+dst+ " because source is the root");
      return false;
    }
    if (isDir(dst)) {
      dst += Path.SEPARATOR + new Path(src).getName();
    }
    
    // check the validity of the destination
    if (dst.equals(src)) {
      return true;
    }
    if (srcInode.isSymlink() && 
        dst.equals(((INodeSymlink)srcInode).getSymlinkString())) {
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
    INodesInPath dstInodesInPath = rootDir.getExistingPathINodes(dstComponents,
        dstComponents.length, false);
    INode[] dstInodes = dstInodesInPath.getINodes();
    if (dstInodes[dstInodes.length-1] != null) {
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                                   +"failed to rename "+src+" to "+dst+ 
                                   " because destination exists");
      return false;
    }
    if (dstInodes[dstInodes.length-2] == null) {
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          +"failed to rename "+src+" to "+dst+ 
          " because destination's parent does not exist");
      return false;
    }
    
    // Ensure dst has quota to accommodate rename
    verifyQuotaForRename(srcInodes, dstInodes);
    
    boolean added = false;
    INode srcChild = null;
    String srcChildName = null;
    try {
      // remove src
      srcChild = removeLastINode(srcInodesInPath);
      if (srcChild == null) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
            + "failed to rename " + src + " to " + dst
            + " because the source can not be removed");
        return false;
      }
      srcChildName = srcChild.getLocalName();
      srcChild.setLocalName(dstComponents[dstInodes.length-1]);
      
      // add src to the destination
      added = addLastINodeNoQuotaCheck(dstInodesInPath, srcChild);
      if (added) {
        srcChild = null;
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedRenameTo: " 
              + src + " is renamed to " + dst);
        }
        // update modification time of dst and the parent of src
        srcInodes[srcInodes.length-2].setModificationTime(timestamp);
        dstInodes[dstInodes.length-2].setModificationTime(timestamp);
        // update moved leases with new filename
        getFSNamesystem().unprotectedChangeLease(src, dst);        
        return true;
      }
    } finally {
      if (!added && srcChild != null) {
        // put it back
        srcChild.setLocalName(srcChildName);
        addLastINodeNoQuotaCheck(srcInodesInPath, srcChild);
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
    final INodesInPath srcInodesInPath = rootDir.getExistingPathINodes(src, false);
    final INode[] srcInodes = srcInodesInPath.getINodes();
    final INode srcInode = srcInodes[srcInodes.length - 1];
    // validate source
    if (srcInode == null) {
      error = "rename source " + src + " is not found.";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new FileNotFoundException(error);
    }
    if (srcInodes.length == 1) {
      error = "rename source cannot be the root";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new IOException(error);
    }

    // validate the destination
    if (dst.equals(src)) {
      throw new FileAlreadyExistsException(
          "The source "+src+" and destination "+dst+" are the same");
    }
    if (srcInode.isSymlink() && 
        dst.equals(((INodeSymlink)srcInode).getSymlinkString())) {
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
    final byte[][] dstComponents = INode.getPathComponents(dst);
    INodesInPath dstInodesInPath = rootDir.getExistingPathINodes(dstComponents,
        dstComponents.length, false);
    final INode[] dstInodes = dstInodesInPath.getINodes();
    INode dstInode = dstInodes[dstInodes.length - 1];
    if (dstInodes.length == 1) {
      error = "rename destination cannot be the root";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new IOException(error);
    }
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
        final List<INode> children = ((INodeDirectory) dstInode
            ).getChildrenList();
        if (!children.isEmpty()) {
          error = "rename destination directory is not empty: " + dst;
          NameNode.stateChangeLog.warn(
              "DIR* FSDirectory.unprotectedRenameTo: " + error);
          throw new IOException(error);
        }
      }
    }
    if (dstInodes[dstInodes.length - 2] == null) {
      error = "rename destination parent " + dst + " not found.";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new FileNotFoundException(error);
    }
    if (!dstInodes[dstInodes.length - 2].isDirectory()) {
      error = "rename destination parent " + dst + " is a file.";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new ParentNotDirectoryException(error);
    }

    // Ensure dst has quota to accommodate rename
    verifyQuotaForRename(srcInodes, dstInodes);
    INode removedSrc = removeLastINode(srcInodesInPath);
    if (removedSrc == null) {
      error = "Failed to rename " + src + " to " + dst
          + " because the source can not be removed";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new IOException(error);
    }
    final String srcChildName = removedSrc.getLocalName();
    String dstChildName = null;
    INode removedDst = null;
    try {
      if (dstInode != null) { // dst exists remove it
        removedDst = removeLastINode(dstInodesInPath);
        dstChildName = removedDst.getLocalName();
      }

      removedSrc.setLocalName(dstComponents[dstInodes.length - 1]);
      // add src as dst to complete rename
      if (addLastINodeNoQuotaCheck(dstInodesInPath, removedSrc)) {
        removedSrc = null;
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug(
              "DIR* FSDirectory.unprotectedRenameTo: " + src
              + " is renamed to " + dst);
        }
        srcInodes[srcInodes.length - 2].setModificationTime(timestamp);
        dstInodes[dstInodes.length - 2].setModificationTime(timestamp);
        // update moved lease with new filename
        getFSNamesystem().unprotectedChangeLease(src, dst);

        // Collect the blocks and remove the lease for previous dst
        int filesDeleted = 0;
        if (removedDst != null) {
          INode rmdst = removedDst;
          removedDst = null;
          BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
          filesDeleted = rmdst.collectSubtreeBlocksAndClear(collectedBlocks);
          getFSNamesystem().removePathAndBlocks(src, collectedBlocks);
        }
        return filesDeleted >0;
      }
    } finally {
      if (removedSrc != null) {
        // Rename failed - restore src
        removedSrc.setLocalName(srcChildName);
        addLastINodeNoQuotaCheck(srcInodesInPath, removedSrc);
      }
      if (removedDst != null) {
        // Rename failed - restore dst
        removedDst.setLocalName(dstChildName);
        addLastINodeNoQuotaCheck(dstInodesInPath, removedDst);
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
   * @param oldReplication old replication - output parameter
   * @return array of file blocks
   * @throws QuotaExceededException
   */
  Block[] setReplication(String src, short replication, short[] oldReplication)
      throws QuotaExceededException, UnresolvedLinkException {
    waitForReady();
    Block[] fileBlocks = null;
    writeLock();
    try {
      fileBlocks = unprotectedSetReplication(src, replication, oldReplication);
      if (fileBlocks != null)  // log replication change
        fsImage.getEditLog().logSetReplication(src, replication);
      return fileBlocks;
    } finally {
      writeUnlock();
    }
  }

  Block[] unprotectedSetReplication(String src, 
                                    short replication,
                                    short[] oldReplication
                                    ) throws QuotaExceededException, 
                                    UnresolvedLinkException {
    assert hasWriteLock();

    final INodesInPath inodesInPath = rootDir.getExistingPathINodes(src, true);
    final INode[] inodes = inodesInPath.getINodes();
    INode inode = inodes[inodes.length - 1];
    if (inode == null || !inode.isFile()) {
      return null;
    }
    INodeFile fileNode = (INodeFile)inode;
    final short oldRepl = fileNode.getBlockReplication();

    // check disk quota
    long dsDelta = (replication - oldRepl) * (fileNode.diskspaceConsumed()/oldRepl);
    updateCount(inodesInPath, inodes.length-1, 0, dsDelta, true);

    fileNode.setReplication(replication);

    if (oldReplication != null) {
      oldReplication[0] = oldRepl;
    }
    return fileNode.getBlocks();
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
      return !inode.isFile() || ((INodeFile)inode).getBlocks() != null;
    } finally {
      readUnlock();
    }
  }

  void setPermission(String src, FsPermission permission)
      throws FileNotFoundException, UnresolvedLinkException {
    writeLock();
    try {
      unprotectedSetPermission(src, permission);
    } finally {
      writeUnlock();
    }
    fsImage.getEditLog().logSetPermissions(src, permission);
  }

  void unprotectedSetPermission(String src, FsPermission permissions) 
      throws FileNotFoundException, UnresolvedLinkException {
    assert hasWriteLock();
    INode inode = rootDir.getNode(src, true);
    if (inode == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }
    inode.setPermission(permissions);
  }

  void setOwner(String src, String username, String groupname)
      throws FileNotFoundException, UnresolvedLinkException {
    writeLock();
    try {
      unprotectedSetOwner(src, username, groupname);
    } finally {
      writeUnlock();
    }
    fsImage.getEditLog().logSetOwner(src, username, groupname);
  }

  void unprotectedSetOwner(String src, String username, String groupname) 
      throws FileNotFoundException, UnresolvedLinkException {
    assert hasWriteLock();
    INode inode = rootDir.getNode(src, true);
    if (inode == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }
    if (username != null) {
      inode.setUser(username);
    }
    if (groupname != null) {
      inode.setGroup(groupname);
    }
  }

  /**
   * Concat all the blocks from srcs to trg and delete the srcs files
   */
  public void concat(String target, String [] srcs) 
      throws UnresolvedLinkException {
    writeLock();
    try {
      // actual move
      waitForReady();
      long timestamp = now();
      unprotectedConcat(target, srcs, timestamp);
      // do the commit
      fsImage.getEditLog().logConcat(target, srcs, timestamp);
    } finally {
      writeUnlock();
    }
  }
  

  
  /**
   * Concat all the blocks from srcs to trg and delete the srcs files
   * @param target target file to move the blocks to
   * @param srcs list of file to move the blocks from
   * Must be public because also called from EditLogs
   * NOTE: - it does not update quota (not needed for concat)
   */
  public void unprotectedConcat(String target, String [] srcs, long timestamp) 
      throws UnresolvedLinkException {
    assert hasWriteLock();
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSNamesystem.concat to "+target);
    }
    // do the move
    
    final INodesInPath trgINodesInPath = rootDir.getExistingPathINodes(target, true);
    final INode[] trgINodes = trgINodesInPath.getINodes();
    INodeFile trgInode = (INodeFile) trgINodes[trgINodes.length-1];
    INodeDirectory trgParent = (INodeDirectory)trgINodes[trgINodes.length-2];
    
    INodeFile [] allSrcInodes = new INodeFile[srcs.length];
    int i = 0;
    int totalBlocks = 0;
    for(String src : srcs) {
      INodeFile srcInode = (INodeFile)getINode(src);
      allSrcInodes[i++] = srcInode;
      totalBlocks += srcInode.numBlocks();  
    }
    trgInode.appendBlocks(allSrcInodes, totalBlocks); // copy the blocks
    
    // since we are in the same dir - we can use same parent to remove files
    int count = 0;
    for(INodeFile nodeToRemove: allSrcInodes) {
      if(nodeToRemove == null) continue;
      
      nodeToRemove.setBlocks(null);
      trgParent.removeChild(nodeToRemove);
      count++;
    }
    
    trgInode.setModificationTimeForce(timestamp);
    trgParent.setModificationTime(timestamp);
    // update quota on the parent directory ('count' files removed, 0 space)
    unprotectedUpdateCount(trgINodesInPath, trgINodes.length-1, -count, 0);
  }

  /**
   * Delete the target directory and collect the blocks under it
   * 
   * @param src Path of a directory to delete
   * @param collectedBlocks Blocks under the deleted directory
   * @return true on successful deletion; else false
   */
  boolean delete(String src, BlocksMapUpdateInfo collectedBlocks) 
    throws UnresolvedLinkException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.delete: " + src);
    }
    waitForReady();
    long now = now();
    int filesRemoved;
    writeLock();
    try {
      filesRemoved = unprotectedDelete(src, collectedBlocks, now);
    } finally {
      writeUnlock();
    }
    if (filesRemoved <= 0) {
      return false;
    }
    incrDeletedFileCount(filesRemoved);
    // Blocks will be deleted later by the caller of this method
    getFSNamesystem().removePathAndBlocks(src, null);
    fsImage.getEditLog().logDelete(src, now);
    return true;
  }
  
  /**
   * @return true if the path is a non-empty directory; otherwise, return false.
   */
  boolean isNonEmptyDirectory(String path) throws UnresolvedLinkException {
    readLock();
    try {
      final INode inode = rootDir.getNode(path, false);
      if (inode == null || !inode.isDirectory()) {
        //not found or not a directory
        return false;
      }
      return ((INodeDirectory)inode).getChildrenList().size() != 0;
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
   */ 
  void unprotectedDelete(String src, long mtime) 
    throws UnresolvedLinkException {
    assert hasWriteLock();
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    int filesRemoved = unprotectedDelete(src, collectedBlocks, mtime);
    if (filesRemoved > 0) {
      getFSNamesystem().removePathAndBlocks(src, collectedBlocks);
    }
  }
  
  /**
   * Delete a path from the name space
   * Update the count at each ancestor directory with quota
   * @param src a string representation of a path to an inode
   * @param collectedBlocks blocks collected from the deleted path
   * @param mtime the time the inode is removed
   * @return the number of inodes deleted; 0 if no inodes are deleted.
   */ 
  int unprotectedDelete(String src, BlocksMapUpdateInfo collectedBlocks, 
      long mtime) throws UnresolvedLinkException {
    assert hasWriteLock();
    src = normalizePath(src);

    final INodesInPath inodesInPath = rootDir.getExistingPathINodes(src, false);
    final INode[] inodes = inodesInPath.getINodes();
    INode targetNode = inodes[inodes.length-1];

    if (targetNode == null) { // non-existent src
      if(NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedDelete: "
            +"failed to remove "+src+" because it does not exist");
      }
      return 0;
    }
    if (inodes.length == 1) { // src is the root
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedDelete: " +
          "failed to remove " + src +
          " because the root is not allowed to be deleted");
      return 0;
    }
    // Remove the node from the namespace
    targetNode = removeLastINode(inodesInPath);
    if (targetNode == null) {
      return 0;
    }
    // set the parent's modification time
    inodes[inodes.length - 2].setModificationTime(mtime);
    int filesRemoved = targetNode.collectSubtreeBlocksAndClear(collectedBlocks);
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedDelete: "
          +src+" is removed");
    }
    return filesRemoved;
  }

  /**
   * Replaces the specified inode with the specified one.
   */
  public void replaceNode(String path, INodeFile oldnode, INodeFile newnode)
      throws IOException, UnresolvedLinkException {    
    writeLock();
    try {
      unprotectedReplaceNode(path, oldnode, newnode);
    } finally {
      writeUnlock();
    }
  }
  
  void unprotectedReplaceNode(String path, INodeFile oldnode, INodeFile newnode)
      throws IOException, UnresolvedLinkException {
    assert hasWriteLock();
    INodeDirectory parent = oldnode.parent;
    // Remove the node from the namespace 
    if (!oldnode.removeNode()) {
      NameNode.stateChangeLog.warn("DIR* FSDirectory.replaceNode: " +
                                   "failed to remove " + path);
      throw new IOException("FSDirectory.replaceNode: " +
                            "failed to remove " + path);
    } 
    
    // Parent should be non-null, otherwise oldnode.removeNode() will return
    // false
    newnode.setLocalName(oldnode.getLocalNameBytes());
    parent.addChild(newnode, true);
    
    /* Currently oldnode and newnode are assumed to contain the same
     * blocks. Otherwise, blocks need to be removed from the blocksMap.
     */
    int index = 0;
    for (BlockInfo b : newnode.getBlocks()) {
      BlockInfo info = getBlockManager().addBlockCollection(b, newnode);
      newnode.setBlock(index, info); // inode refers to the block in BlocksMap
      index++;
    }
  }

  /**
   * Get a partial listing of the indicated directory
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
      INode targetNode = rootDir.getNode(srcs, true);
      if (targetNode == null)
        return null;
      
      if (!targetNode.isDirectory()) {
        return new DirectoryListing(
            new HdfsFileStatus[]{createFileStatus(HdfsFileStatus.EMPTY_NAME,
                targetNode, needLocation)}, 0);
      }
      INodeDirectory dirInode = (INodeDirectory)targetNode;
      List<INode> contents = dirInode.getChildrenList();
      int startChild = dirInode.nextChild(startAfter);
      int totalNumChildren = contents.size();
      int numOfListing = Math.min(totalNumChildren-startChild, this.lsLimit);
      HdfsFileStatus listing[] = new HdfsFileStatus[numOfListing];
      for (int i=0; i<numOfListing; i++) {
        INode cur = contents.get(startChild+i);
        listing[i] = createFileStatus(cur.getLocalNameBytes(), cur, needLocation);
      }
      return new DirectoryListing(
          listing, totalNumChildren-startChild-numOfListing);
    } finally {
      readUnlock();
    }
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
      INode targetNode = rootDir.getNode(srcs, resolveLink);
      if (targetNode == null) {
        return null;
      }
      else {
        return createFileStatus(HdfsFileStatus.EMPTY_NAME, targetNode);
      }
    } finally {
      readUnlock();
    }
  }

  /**
   * Get the blocks associated with the file.
   */
  Block[] getFileBlocks(String src) throws UnresolvedLinkException {
    waitForReady();
    readLock();
    try {
      final INode i = rootDir.getNode(src, false);
      return i != null && i.isFile()? ((INodeFile)i).getBlocks(): null;
    } finally {
      readUnlock();
    }
  }

  /**
   * Get {@link INode} associated with the file / directory.
   */
  INode getINode(String src) throws UnresolvedLinkException {
    readLock();
    try {
      return rootDir.getNode(src, true);
    } finally {
      readUnlock();
    }
  }
  
  
  /** 
   * Check whether the filepath could be created
   */
  boolean isValidToCreate(String src) throws UnresolvedLinkException {
    String srcs = normalizePath(src);
    readLock();
    try {
      if (srcs.startsWith("/") && 
          !srcs.endsWith("/") && 
          rootDir.getNode(srcs, false) == null) {
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
      throws QuotaExceededException, FileNotFoundException, UnresolvedLinkException {
    writeLock();
    try {
      final INodesInPath inodesInPath = rootDir.getExistingPathINodes(path, false);
      final INode[] inodes = inodesInPath.getINodes();
      int len = inodes.length;
      if (inodes[len - 1] == null) {
        throw new FileNotFoundException("Path not found: " + path);
      }
      updateCount(inodesInPath, len-1, nsDelta, dsDelta, true);
    } finally {
      writeUnlock();
    }
  }
  
  /** update count of each inode with quota
   * 
   * @param inodes an array of inodes on a path
   * @param numOfINodes the number of inodes to update starting from index 0
   * @param nsDelta the delta change of namespace
   * @param dsDelta the delta change of diskspace
   * @param checkQuota if true then check if quota is exceeded
   * @throws QuotaExceededException if the new count violates any quota limit
   */
  private void updateCount(INodesInPath inodesInPath, int numOfINodes, 
                           long nsDelta, long dsDelta, boolean checkQuota)
                           throws QuotaExceededException {
    assert hasWriteLock();
    if (!ready) {
      //still initializing. do not check or update quotas.
      return;
    }
    final INode[] inodes = inodesInPath.getINodes();
    if (numOfINodes > inodes.length) {
      numOfINodes = inodes.length;
    }
    if (checkQuota) {
      verifyQuota(inodes, numOfINodes, nsDelta, dsDelta, null);
    }
    unprotectedUpdateCount(inodesInPath, numOfINodes, nsDelta, dsDelta);
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
      NameNode.LOG.warn("FSDirectory.updateCountNoQuotaCheck - unexpected ", e);
    }
  }
  
  /**
   * updates quota without verification
   * callers responsibility is to make sure quota is not exceeded
   * @param inodes
   * @param numOfINodes
   * @param nsDelta
   * @param dsDelta
   */
  private void unprotectedUpdateCount(INodesInPath inodesInPath,
      int numOfINodes, long nsDelta, long dsDelta) {
    assert hasWriteLock();
    final INode[] inodes = inodesInPath.getINodes();
    for(int i=0; i < numOfINodes; i++) {
      if (inodes[i].isQuotaSet()) { // a directory with quota
        INodeDirectoryWithQuota node =(INodeDirectoryWithQuota)inodes[i]; 
        node.addSpaceConsumed(nsDelta, dsDelta);
      }
    }
  }
  
  /** Return the name of the path represented by inodes at [0, pos] */
  private static String getFullPathName(INode[] inodes, int pos) {
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

  /** Return the full path name of the specified inode */
  static String getFullPathName(INode inode) {
    // calculate the depth of this inode from root
    int depth = 0;
    for (INode i = inode; i != null; i = i.parent) {
      depth++;
    }
    INode[] inodes = new INode[depth];

    // fill up the inodes in the path from this inode to root
    for (int i = 0; i < depth; i++) {
      inodes[depth-i-1] = inode;
      inode = inode.parent;
    }
    return getFullPathName(inodes, depth-1);
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
   */
  boolean mkdirs(String src, PermissionStatus permissions,
      boolean inheritPermission, long now)
      throws FileAlreadyExistsException, QuotaExceededException, 
             UnresolvedLinkException {
    src = normalizePath(src);
    String[] names = INode.getPathNames(src);
    byte[][] components = INode.getPathComponents(names);
    final int lastInodeIndex = components.length - 1;

    writeLock();
    try {
      INodesInPath inodesInPath = rootDir.getExistingPathINodes(components,
          components.length, false);
      INode[] inodes = inodesInPath.getINodes();

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
        unprotectedMkdir(namesystem.allocateNewInodeId(), inodesInPath, i,
            components[i], (i < lastInodeIndex) ? parentPermissions
                : permissions, now);
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
                          long timestamp) throws QuotaExceededException,
                          UnresolvedLinkException {
    assert hasWriteLock();
    byte[][] components = INode.getPathComponents(src);
    INodesInPath inodesInPath = rootDir.getExistingPathINodes(components,
        components.length, false);
    INode[] inodes = inodesInPath.getINodes();
    final int pos = inodes.length - 1;
    unprotectedMkdir(inodeId, inodesInPath, pos, components[pos], permissions,
        timestamp);
    return inodes[pos];
  }

  /** create a directory at index pos.
   * The parent path to the directory is at [0, pos-1].
   * All ancestors exist. Newly created one stored at index pos.
   */
  private void unprotectedMkdir(long inodeId, INodesInPath inodesInPath,
      int pos, byte[] name, PermissionStatus permission, long timestamp)
      throws QuotaExceededException {
    assert hasWriteLock();
    final INodeDirectory dir = new INodeDirectory(inodeId, name, permission,
        timestamp);
    if (addChild(inodesInPath, pos, dir, true)) {
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
    byte[] path = components[components.length-1];
    child.setLocalName(path);
    cacheName(child);
    writeLock();
    try {
      INodesInPath inodesInPath = rootDir.getExistingPathINodes(components,
          components.length, false);
      return addLastINode(inodesInPath, child, true);
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
  private void verifyQuota(INode[] inodes, int pos, long nsDelta, long dsDelta,
      INode commonAncestor) throws QuotaExceededException {
    if (!ready) {
      // Do not check quota if edits log is still being processed
      return;
    }
    if (nsDelta <= 0 && dsDelta <= 0) {
      // if quota is being freed or not being consumed
      return;
    }
    if (pos>inodes.length) {
      pos = inodes.length;
    }
    int i = pos - 1;
    try {
      // check existing components in the path  
      for(; i >= 0; i--) {
        if (commonAncestor == inodes[i]) {
          // Moving an existing node. Stop checking for quota when common
          // ancestor is reached
          return;
        }
        if (inodes[i].isQuotaSet()) { // a directory with quota
          INodeDirectoryWithQuota node =(INodeDirectoryWithQuota)inodes[i]; 
          node.verifyQuota(nsDelta, dsDelta);
        }
      }
    } catch (QuotaExceededException e) {
      e.setPathName(getFullPathName(inodes, i));
      throw e;
    }
  }
  
  /**
   * Verify quota for rename operation where srcInodes[srcInodes.length-1] moves
   * dstInodes[dstInodes.length-1]
   * 
   * @param srcInodes directory from where node is being moved.
   * @param dstInodes directory to where node is moved to.
   * @throws QuotaExceededException if quota limit is exceeded.
   */
  private void verifyQuotaForRename(INode[] srcInodes, INode[]dstInodes)
      throws QuotaExceededException {
    if (!ready) {
      // Do not check quota if edits log is still being processed
      return;
    }
    INode srcInode = srcInodes[srcInodes.length - 1];
    INode commonAncestor = null;
    for(int i =0;srcInodes[i] == dstInodes[i]; i++) {
      commonAncestor = srcInodes[i];
    }
    INode.DirCounts srcCounts = new INode.DirCounts();
    srcInode.spaceConsumedInTree(srcCounts);
    long nsDelta = srcCounts.getNsCount();
    long dsDelta = srcCounts.getDsCount();
    
    // Reduce the required quota by dst that is being removed
    INode dstInode = dstInodes[dstInodes.length - 1];
    if (dstInode != null) {
      INode.DirCounts dstCounts = new INode.DirCounts();
      dstInode.spaceConsumedInTree(dstCounts);
      nsDelta -= dstCounts.getNsCount();
      dsDelta -= dstCounts.getDsCount();
    }
    verifyQuota(dstInodes, dstInodes.length - 1, nsDelta, dsDelta,
        commonAncestor);
  }
  
  /**
   * Verify that filesystem limit constraints are not violated
   * @throws PathComponentTooLongException child's name is too long
   * @throws MaxDirectoryItemsExceededException items per directory is exceeded
   */
  protected <T extends INode> void verifyFsLimits(INode[] pathComponents,
      int pos, T child) throws FSLimitException {
    boolean includeChildName = false;
    try {
      if (maxComponentLength != 0) {
        int length = child.getLocalName().length();
        if (length > maxComponentLength) {
          includeChildName = true;
          throw new PathComponentTooLongException(maxComponentLength, length);
        }
      }
      if (maxDirItems != 0) {
        INodeDirectory parent = (INodeDirectory)pathComponents[pos-1];
        int count = parent.getChildrenList().size();
        if (count >= maxDirItems) {
          throw new MaxDirectoryItemsExceededException(maxDirItems, count);
        }
      }
    } catch (FSLimitException e) {
      String badPath = getFullPathName(pathComponents, pos-1);
      if (includeChildName) {
        badPath += Path.SEPARATOR + child.getLocalName();
      }
      e.setPathName(badPath);
      // Do not throw if edits log is still being processed
      if (ready) throw(e);
      // log pre-existing paths that exceed limits
      NameNode.LOG.error("FSDirectory.verifyFsLimits - " + e.getLocalizedMessage());
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
  private boolean addChild(INodesInPath inodesInPath, int pos,
      INode child, boolean checkQuota) throws QuotaExceededException {
    final INode[] inodes = inodesInPath.getINodes();
    // The filesystem limits are not really quotas, so this check may appear
    // odd. It's because a rename operation deletes the src, tries to add
    // to the dest, if that fails, re-adds the src from whence it came.
    // The rename code disables the quota when it's restoring to the
    // original location becase a quota violation would cause the the item
    // to go "poof".  The fs limits must be bypassed for the same reason.
    if (checkQuota) {
      verifyFsLimits(inodes, pos, child);
    }
    
    INode.DirCounts counts = new INode.DirCounts();
    child.spaceConsumedInTree(counts);
    updateCount(inodesInPath, pos, counts.getNsCount(), counts.getDsCount(), checkQuota);
    if (inodes[pos-1] == null) {
      throw new NullPointerException("Panic: parent does not exist");
    }
    final boolean added = ((INodeDirectory)inodes[pos-1]).addChild(child, true);
    if (!added) {
      updateCount(inodesInPath, pos, -counts.getNsCount(), -counts.getDsCount(), true);
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
   * @return the removed node; null if the removal fails.
   */
  private INode removeLastINode(final INodesInPath inodesInPath) {
    final INode[] inodes = inodesInPath.getINodes();
    final int pos = inodes.length - 1;
    INode removedNode = ((INodeDirectory)inodes[pos-1]).removeChild(inodes[pos]);
    if (removedNode != null) {
      INode.DirCounts counts = new INode.DirCounts();
      removedNode.spaceConsumedInTree(counts);
      updateCountNoQuotaCheck(inodesInPath, pos,
                  -counts.getNsCount(), -counts.getDsCount());
    }
    return removedNode;
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
        return targetNode.computeContentSummary();
      }
    } finally {
      readUnlock();
    }
  }

  /** Update the count of each directory with quota in the namespace
   * A directory's count is defined as the total number inodes in the tree
   * rooted at the directory.
   * 
   * This is an update of existing state of the filesystem and does not
   * throw QuotaExceededException.
   */
  void updateCountForINodeWithQuota() {
    updateCountForINodeWithQuota(rootDir, new INode.DirCounts(), 
                                 new ArrayList<INode>(50));
  }
  
  /** 
   * Update the count of the directory if it has a quota and return the count
   * 
   * This does not throw a QuotaExceededException. This is just an update
   * of of existing state and throwing QuotaExceededException does not help
   * with fixing the state, if there is a problem.
   * 
   * @param dir the root of the tree that represents the directory
   * @param counters counters for name space and disk space
   * @param nodesInPath INodes for the each of components in the path.
   */
  private static void updateCountForINodeWithQuota(INodeDirectory dir, 
                                               INode.DirCounts counts,
                                               ArrayList<INode> nodesInPath) {
    long parentNamespace = counts.nsCount;
    long parentDiskspace = counts.dsCount;
    
    counts.nsCount = 1L;//for self. should not call node.spaceConsumedInTree()
    counts.dsCount = 0L;
    
    /* We don't need nodesInPath if we could use 'parent' field in 
     * INode. using 'parent' is not currently recommended. */
    nodesInPath.add(dir);

    for (INode child : dir.getChildrenList()) {
      if (child.isDirectory()) {
        updateCountForINodeWithQuota((INodeDirectory)child, 
                                     counts, nodesInPath);
      } else if (child.isSymlink()) {
        counts.nsCount += 1;
      } else { // reduce recursive calls
        counts.nsCount += 1;
        counts.dsCount += ((INodeFile)child).diskspaceConsumed();
      }
    }
      
    if (dir.isQuotaSet()) {
      ((INodeDirectoryWithQuota)dir).setSpaceConsumed(counts.nsCount,
                                                      counts.dsCount);

      // check if quota is violated for some reason.
      if ((dir.getNsQuota() >= 0 && counts.nsCount > dir.getNsQuota()) ||
          (dir.getDsQuota() >= 0 && counts.dsCount > dir.getDsQuota())) {

        // can only happen because of a software bug. the bug should be fixed.
        StringBuilder path = new StringBuilder(512);
        for (INode n : nodesInPath) {
          path.append('/');
          path.append(n.getLocalName());
        }
        
        NameNode.LOG.warn("Quota violation in image for " + path + 
                          " (Namespace quota : " + dir.getNsQuota() +
                          " consumed : " + counts.nsCount + ")" +
                          " (Diskspace quota : " + dir.getDsQuota() +
                          " consumed : " + counts.dsCount + ").");
      }            
    }
      
    // pop 
    nodesInPath.remove(nodesInPath.size()-1);
    
    counts.nsCount += parentNamespace;
    counts.dsCount += parentDiskspace;
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
   */
  INodeDirectory unprotectedSetQuota(String src, long nsQuota, long dsQuota)
      throws FileNotFoundException, PathIsNotDirectoryException,
      QuotaExceededException, UnresolvedLinkException {
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

    final INodesInPath inodesInPath = rootDir.getExistingPathINodes(src, true);
    final INode[] inodes = inodesInPath.getINodes();
    INodeDirectory dirNode = INodeDirectory.valueOf(inodes[inodes.length-1], srcs);
    if (dirNode.isRoot() && nsQuota == HdfsConstants.QUOTA_RESET) {
      throw new IllegalArgumentException("Cannot clear namespace quota on root.");
    } else { // a directory inode
      long oldNsQuota = dirNode.getNsQuota();
      long oldDsQuota = dirNode.getDsQuota();
      if (nsQuota == HdfsConstants.QUOTA_DONT_SET) {
        nsQuota = oldNsQuota;
      }
      if (dsQuota == HdfsConstants.QUOTA_DONT_SET) {
        dsQuota = oldDsQuota;
      }        

      if (dirNode instanceof INodeDirectoryWithQuota) { 
        // a directory with quota; so set the quota to the new value
        ((INodeDirectoryWithQuota)dirNode).setQuota(nsQuota, dsQuota);
        if (!dirNode.isQuotaSet()) {
          // will not come here for root because root's nsQuota is always set
          INodeDirectory newNode = new INodeDirectory(dirNode);
          INodeDirectory parent = (INodeDirectory)inodes[inodes.length-2];
          dirNode = newNode;
          parent.replaceChild(newNode);
        }
      } else {
        // a non-quota directory; so replace it with a directory with quota
        INodeDirectoryWithQuota newNode = 
          new INodeDirectoryWithQuota(nsQuota, dsQuota, dirNode);
        // non-root directory node; parent != null
        INodeDirectory parent = (INodeDirectory)inodes[inodes.length-2];
        dirNode = newNode;
        parent.replaceChild(newNode);
      }
      return (oldNsQuota != nsQuota || oldDsQuota != dsQuota) ? dirNode : null;
    }
  }
  
  /**
   * See {@link ClientProtocol#setQuota(String, long, long)} for the contract.
   * @see #unprotectedSetQuota(String, long, long)
   */
  void setQuota(String src, long nsQuota, long dsQuota) 
      throws FileNotFoundException, PathIsNotDirectoryException,
      QuotaExceededException, UnresolvedLinkException {
    writeLock();
    try {
      INodeDirectory dir = unprotectedSetQuota(src, nsQuota, dsQuota);
      if (dir != null) {
        fsImage.getEditLog().logSetQuota(src, dir.getNsQuota(), 
                                         dir.getDsQuota());
      }
    } finally {
      writeUnlock();
    }
  }
  
  long totalInodes() {
    readLock();
    try {
      return rootDir.numItemsInTree();
    } finally {
      readUnlock();
    }
  }

  /**
   * Sets the access time on the file/directory. Logs it in the transaction log.
   */
  void setTimes(String src, INode inode, long mtime, long atime, boolean force) {
    boolean status = false;
    writeLock();
    try {
      status = unprotectedSetTimes(src, inode, mtime, atime, force);
    } finally {
      writeUnlock();
    }
    if (status) {
      fsImage.getEditLog().logTimes(src, mtime, atime);
    }
  }

  boolean unprotectedSetTimes(String src, long mtime, long atime, boolean force) 
      throws UnresolvedLinkException {
    assert hasWriteLock();
    INode inode = getINode(src);
    return unprotectedSetTimes(src, inode, mtime, atime, force);
  }

  private boolean unprotectedSetTimes(String src, INode inode, long mtime,
                                      long atime, boolean force) {
    assert hasWriteLock();
    boolean status = false;
    if (mtime != -1) {
      inode.setModificationTimeForce(mtime);
      status = true;
    }
    if (atime != -1) {
      long inodeTime = inode.getAccessTime();

      // if the last access time update was within the last precision interval, then
      // no need to store access time
      if (atime <= inodeTime + getFSNamesystem().getAccessTimePrecision() && !force) {
        status =  false;
      } else {
        inode.setAccessTime(atime);
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
      boolean needLocation) throws IOException {
    if (needLocation) {
      return createLocatedFileStatus(path, node);
    } else {
      return createFileStatus(path, node);
    }
  }
  /**
   * Create FileStatus by file INode 
   */
   private HdfsFileStatus createFileStatus(byte[] path, INode node) {
     long size = 0;     // length is zero for directories
     short replication = 0;
     long blocksize = 0;
     if (node instanceof INodeFile) {
       INodeFile fileNode = (INodeFile)node;
       size = fileNode.computeFileSize(true);
       replication = fileNode.getBlockReplication();
       blocksize = fileNode.getPreferredBlockSize();
     }
     return new HdfsFileStatus(
        size, 
        node.isDirectory(), 
        replication, 
        blocksize,
        node.getModificationTime(),
        node.getAccessTime(),
        node.getFsPermission(),
        node.getUserName(),
        node.getGroupName(),
        node.isSymlink() ? ((INodeSymlink)node).getSymlink() : null,
        path,
        node.getId());
  }

   /**
    * Create FileStatus with location info by file INode 
    */
    private HdfsLocatedFileStatus createLocatedFileStatus(
        byte[] path, INode node) throws IOException {
      assert hasReadLock();
      long size = 0;     // length is zero for directories
      short replication = 0;
      long blocksize = 0;
      LocatedBlocks loc = null;
      if (node instanceof INodeFile) {
        INodeFile fileNode = (INodeFile)node;
        size = fileNode.computeFileSize(true);
        replication = fileNode.getBlockReplication();
        blocksize = fileNode.getPreferredBlockSize();
        loc = getFSNamesystem().getBlockManager().createLocatedBlocks(
            fileNode.getBlocks(), fileNode.computeFileSize(false),
            fileNode.isUnderConstruction(), 0L, size, false);
        if (loc==null) {
          loc = new LocatedBlocks();
        }
      }
      return new HdfsLocatedFileStatus(
          size, 
          node.isDirectory(), 
          replication, 
          blocksize,
          node.getModificationTime(),
          node.getAccessTime(),
          node.getFsPermission(),
          node.getUserName(),
          node.getGroupName(),
          node.isSymlink() ? ((INodeSymlink)node).getSymlink() : null,
          path,
          node.getId(),
          loc);
      }

    
  /**
   * Add the given symbolic link to the fs. Record it in the edits log.
   */
  INodeSymlink addSymlink(String path, String target,
      PermissionStatus dirPerms, boolean createParent)
      throws UnresolvedLinkException, FileAlreadyExistsException,
      QuotaExceededException {
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
    fsImage.getEditLog().logSymlink(path, target, modTime, modTime, newNode);
    
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
    final INodeSymlink symlink = new INodeSymlink(id, target, mtime, atime,
        perm);
    return addINode(path, symlink) ? symlink : null;
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
}
