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

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.UserGroupInformation;

/*************************************************
 * FSDirectory stores the filesystem directory state.
 * It handles writing/loading values to disk, and logging
 * changes as we go.
 *
 * It keeps the filename->blockset mapping always-current
 * and logged to disk.
 * 
 *************************************************/
class FSDirectory implements Closeable {

  INodeDirectoryWithQuota rootDir;
  FSImage fsImage;  
  private volatile boolean ready = false;
  private static final long UNKNOWN_DISK_SPACE = -1;
  private final int lsLimit;  // max list limit
  
  /** Access an existing dfs name directory. */
  FSDirectory(FSNamesystem ns, Configuration conf) {
    this(new FSImage(), ns, conf);
    if(conf.getBoolean(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_KEY, 
                       DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_DEFAULT)) {
      NameNode.LOG.info("set FSImage.restoreFailedStorage");
      fsImage.setRestoreFailedStorage(true);
    }
    fsImage.setCheckpointDirectories(FSImage.getCheckpointDirs(conf, null),
                                FSImage.getCheckpointEditsDirs(conf, null));
  }

  FSDirectory(FSImage fsImage, FSNamesystem ns, Configuration conf) {
    fsImage.setFSNamesystem(ns);
    rootDir = new INodeDirectoryWithQuota(INodeDirectory.ROOT_NAME,
        ns.createFsOwnerPermissions(new FsPermission((short)0755)),
        Integer.MAX_VALUE, UNKNOWN_DISK_SPACE);
    this.fsImage = fsImage;
    int configuredLimit = conf.getInt(
        DFSConfigKeys.DFS_LIST_LIMIT, DFSConfigKeys.DFS_LIST_LIMIT_DEFAULT);
    this.lsLimit = configuredLimit>0 ?
        configuredLimit : DFSConfigKeys.DFS_LIST_LIMIT_DEFAULT;
  }
    
  private FSNamesystem getFSNamesystem() {
    return fsImage.getFSNamesystem();
  }

  private BlockManager getBlockManager() {
    return getFSNamesystem().blockManager;
  }

  void loadFSImage(Collection<URI> dataDirs,
                   Collection<URI> editsDirs,
                   StartupOption startOpt) 
      throws IOException {
    // format before starting up if requested
    if (startOpt == StartupOption.FORMAT) {
      fsImage.setStorageDirectories(dataDirs, editsDirs);
      fsImage.format();
      startOpt = StartupOption.REGULAR;
    }
    try {
      if (fsImage.recoverTransitionRead(dataDirs, editsDirs, startOpt)) {
        fsImage.saveNamespace(true);
      }
      FSEditLog editLog = fsImage.getEditLog();
      assert editLog != null : "editLog must be initialized";
      fsImage.setCheckpointDirectories(null, null);
    } catch(IOException e) {
      fsImage.close();
      throw e;
    }
    synchronized (this) {
      this.ready = true;
      this.notifyAll();
    }
  }

  private void incrDeletedFileCount(int count) {
    if (getFSNamesystem() != null)
      NameNode.getNameNodeMetrics().numFilesDeleted.inc(count);
  }
    
  /**
   * Shutdown the filestore
   */
  public void close() throws IOException {
    fsImage.close();
  }

  /**
   * Block until the object is ready to be used.
   */
  void waitForReady() {
    if (!ready) {
      synchronized (this) {
        while (!ready) {
          try {
            this.wait(5000);
          } catch (InterruptedException ie) {
          }
        }
      }
    }
  }

  /**
   * Add the given filename to the fs.
   */
  INodeFileUnderConstruction addFile(String path, 
                PermissionStatus permissions,
                short replication,
                long preferredBlockSize,
                String clientName,
                String clientMachine,
                DatanodeDescriptor clientNode,
                long generationStamp) 
                throws IOException, UnresolvedLinkException {
    waitForReady();

    // Always do an implicit mkdirs for parent directory tree.
    long modTime = FSNamesystem.now();
    if (!mkdirs(new Path(path).getParent().toString(), permissions, true,
        modTime)) {
      return null;
    }
    INodeFileUnderConstruction newNode = new INodeFileUnderConstruction(
                                 permissions,replication,
                                 preferredBlockSize, modTime, clientName, 
                                 clientMachine, clientNode);
    synchronized (rootDir) {
      newNode = addNode(path, newNode, UNKNOWN_DISK_SPACE, false);
    }
    if (newNode == null) {
      NameNode.stateChangeLog.info("DIR* FSDirectory.addFile: "
                                   +"failed to add "+path
                                   +" to the file system");
      return null;
    }
    // add create file record to log, record new generation stamp
    fsImage.getEditLog().logOpenFile(path, newNode);

    NameNode.stateChangeLog.debug("DIR* FSDirectory.addFile: "
                                  +path+" is added to the file system");
    return newNode;
  }

  /**
   */
  INode unprotectedAddFile( String path, 
                            PermissionStatus permissions,
                            BlockInfo[] blocks, 
                            short replication,
                            long modificationTime,
                            long atime,
                            long preferredBlockSize) 
      throws UnresolvedLinkException {
    INode newNode;
    long diskspace = UNKNOWN_DISK_SPACE;
    if (blocks == null)
      newNode = new INodeDirectory(permissions, modificationTime);
    else {
      newNode = new INodeFile(permissions, blocks.length, replication,
                              modificationTime, atime, preferredBlockSize);
      diskspace = ((INodeFile)newNode).diskspaceConsumed(blocks);
    }
    synchronized (rootDir) {
      try {
        newNode = addNode(path, newNode, diskspace, false);
        if(newNode != null && blocks != null) {
          int nrBlocks = blocks.length;
          // Add file->block mapping
          INodeFile newF = (INodeFile)newNode;
          for (int i = 0; i < nrBlocks; i++) {
            newF.setBlock(i, getBlockManager().addINode(blocks[i], newF));
          }
        }
      } catch (IOException e) {
        return null;
      }
      return newNode;
    }
  }

  INodeDirectory addToParent( String src,
                              INodeDirectory parentINode,
                              PermissionStatus permissions,
                              Block[] blocks, 
                              String symlink,
                              short replication,
                              long modificationTime,
                              long atime,
                              long nsQuota,
                              long dsQuota,
                              long preferredBlockSize) 
                              throws UnresolvedLinkException {
    // NOTE: This does not update space counts for parents
    // create new inode
    INode newNode;
    if (blocks == null) {
      if (nsQuota >= 0 || dsQuota >= 0) {
        newNode = new INodeDirectoryWithQuota(
            permissions, modificationTime, nsQuota, dsQuota);
      } else {
        newNode = new INodeDirectory(permissions, modificationTime);
      }
    } else  {
      if (symlink.length() != 0) {
        newNode = new INodeSymlink(symlink, modificationTime, atime, permissions);
        ((INodeSymlink)newNode).setLinkValue(symlink);
      } else {
        newNode = new INodeFile(permissions, blocks.length, replication,
                                modificationTime, atime, preferredBlockSize);
      }
    }
    // add new node to the parent
    INodeDirectory newParent = null;
    synchronized (rootDir) {
      try {
        newParent = rootDir.addToParent(src, newNode, parentINode, false);
      } catch (FileNotFoundException e) {
        return null;
      }
      if(newParent == null)
        return null;
      if(blocks != null) {
        int nrBlocks = blocks.length;
        // Add file->block mapping
        assert !newNode.isLink();
        INodeFile newF = (INodeFile)newNode;
        for (int i = 0; i < nrBlocks; i++) {
          BlockInfo blockInfo = new BlockInfo(blocks[i], newF.getReplication());
          newF.setBlock(i, getBlockManager().addINode(blockInfo, newF));
        }
      }
    }
    return newParent;
  }

  /**
   * Add a block to the file. Returns a reference to the added block.
   */
  BlockInfo addBlock(String path,
                     INode[] inodes,
                     Block block,
                     DatanodeDescriptor targets[]
  ) throws QuotaExceededException, IOException  {
    waitForReady();

    synchronized (rootDir) {
      assert inodes[inodes.length-1].isUnderConstruction() :
        "INode should correspond to a file under construction";
      INodeFileUnderConstruction fileINode = 
        (INodeFileUnderConstruction)inodes[inodes.length-1];

      // check quota limits and updated space consumed
      updateCount(inodes, inodes.length-1, 0,
          fileINode.getPreferredBlockSize()*fileINode.getReplication(), true);

      // associate new last block for the file
      BlockInfoUnderConstruction blockInfo =
        new BlockInfoUnderConstruction(
            block,
            fileINode.getReplication(),
            BlockUCState.UNDER_CONSTRUCTION,
            targets);
      getBlockManager().addINode(blockInfo, fileINode);
      fileINode.addBlock(blockInfo);

      NameNode.stateChangeLog.debug("DIR* FSDirectory.addFile: "
                                    + path + " with " + block
                                    + " block is added to the in-memory "
                                    + "file system");
      return blockInfo;
    }
  }

  /**
   * Persist the block list for the inode.
   */
  void persistBlocks(String path, INodeFileUnderConstruction file) {
    waitForReady();

    synchronized (rootDir) {
      fsImage.getEditLog().logOpenFile(path, file);
      NameNode.stateChangeLog.debug("DIR* FSDirectory.persistBlocks: "
                                    +path+" with "+ file.getBlocks().length 
                                    +" blocks is persisted to the file system");
    }
  }

  /**
   * Close file.
   */
  void closeFile(String path, INodeFile file) {
    waitForReady();
    long now = FSNamesystem.now();
    synchronized (rootDir) {
      // file is closed
      file.setModificationTimeForce(now);
      fsImage.getEditLog().logCloseFile(path, file);
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* FSDirectory.closeFile: "
                                    +path+" with "+ file.getBlocks().length 
                                    +" blocks is persisted to the file system");
      }
    }
  }

  /**
   * Remove a block to the file.
   */
  boolean removeBlock(String path, INodeFileUnderConstruction fileNode, 
                      Block block) throws IOException {
    waitForReady();

    synchronized (rootDir) {
      // modify file-> block and blocksMap
      fileNode.removeLastBlock(block);
      getBlockManager().removeBlockFromMap(block);
      // If block is removed from blocksMap remove it from corruptReplicasMap
      getBlockManager().removeFromCorruptReplicasMap(block);

      // write modified block locations to log
      fsImage.getEditLog().logOpenFile(path, fileNode);
      NameNode.stateChangeLog.debug("DIR* FSDirectory.addFile: "
                                    +path+" with "+block
                                    +" block is added to the file system");
    }
    return true;
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
    long now = FSNamesystem.now();
    if (!unprotectedRenameTo(src, dst, now))
      return false;
    fsImage.getEditLog().logRename(src, dst, now);
    return true;
  }

  /**
   * @see #unprotectedRenameTo(String, String, long, Options.Rename...)
   */
  void renameTo(String src, String dst, Options.Rename... options)
      throws IOException, UnresolvedLinkException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.renameTo: " + src
          + " to " + dst);
    }
    waitForReady();
    long now = FSNamesystem.now();
    if (unprotectedRenameTo(src, dst, now, options)) {
      incrDeletedFileCount(1);
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
    synchronized (rootDir) {
      INode[] srcInodes = rootDir.getExistingPathINodes(src, false);
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
      if (srcInode.isLink() && 
          dst.equals(((INodeSymlink)srcInode).getLinkValue())) {
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
      INode[] dstInodes = new INode[dstComponents.length];
      rootDir.getExistingPathINodes(dstComponents, dstInodes, false);
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
      verifyQuotaForRename(srcInodes,dstInodes);
      
      INode dstChild = null;
      INode srcChild = null;
      String srcChildName = null;
      try {
        // remove src
        srcChild = removeChild(srcInodes, srcInodes.length-1);
        if (srcChild == null) {
          NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
              + "failed to rename " + src + " to " + dst
              + " because the source can not be removed");
          return false;
        }
        srcChildName = srcChild.getLocalName();
        srcChild.setLocalName(dstComponents[dstInodes.length-1]);
        
        // add src to the destination
        dstChild = addChildNoQuotaCheck(dstInodes, dstInodes.length - 1,
            srcChild, UNKNOWN_DISK_SPACE, false);
        if (dstChild != null) {
          srcChild = null;
          if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedRenameTo: " 
                + src + " is renamed to " + dst);
          }
          // update modification time of dst and the parent of src
          srcInodes[srcInodes.length-2].setModificationTime(timestamp);
          dstInodes[dstInodes.length-2].setModificationTime(timestamp);
          return true;
        }
      } finally {
        if (dstChild == null && srcChild != null) {
          // put it back
          srcChild.setLocalName(srcChildName);
          addChildNoQuotaCheck(srcInodes, srcInodes.length - 1, srcChild, 
              UNKNOWN_DISK_SPACE, false);
        }
      }
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          +"failed to rename "+src+" to "+dst);
      return false;
    }
  }

  /**
   * Rename src to dst.
   * See {@link DistributedFileSystem#rename(Path, Path, Options.Rename...)}
   * for details related to rename semantics.
   * 
   * @param src source path
   * @param dst destination path
   * @param timestamp modification time
   * @param options Rename options
   * @throws IOException if the operation violates any quota limit
   * @throws FileAlreadyExistsException if src equals dst or the src is a 
   *         symlink that points to dst.
   * @return true if rename overwrites {@code dst}
   */
  boolean unprotectedRenameTo(String src, String dst, long timestamp,
      Options.Rename... options) throws IOException,
      UnresolvedLinkException {
    boolean overwrite = false;
    if (null != options) {
      for (Rename option : options) {
        if (option == Rename.OVERWRITE) {
          overwrite = true;
        }
      }
    }
    String error = null;
    synchronized (rootDir) {
      final INode[] srcInodes = rootDir.getExistingPathINodes(src, false);
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
      if (srcInode.isLink() && 
          dst.equals(((INodeSymlink)srcInode).getLinkValue())) {
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
      final INode[] dstInodes = new INode[dstComponents.length];
      rootDir.getExistingPathINodes(dstComponents, dstInodes, false);
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
        List<INode> children = dstInode.isDirectory() ? 
            ((INodeDirectory) dstInode).getChildrenRaw() : null;
        if (children != null && children.size() != 0) {
          error = "rename cannot overwrite non empty destination directory "
              + dst;
          NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
              + error);
          throw new IOException(error);
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
      INode removedSrc = removeChild(srcInodes, srcInodes.length - 1);
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
          removedDst = removeChild(dstInodes, dstInodes.length - 1);
          dstChildName = removedDst.getLocalName();
        }

        INode dstChild = null;
        removedSrc.setLocalName(dstComponents[dstInodes.length - 1]);
        // add src as dst to complete rename
        dstChild = addChildNoQuotaCheck(dstInodes, dstInodes.length - 1,
            removedSrc, UNKNOWN_DISK_SPACE, false);

        int filesDeleted = 0;
        if (dstChild != null) {
          removedSrc = null;
          if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog
                .debug("DIR* FSDirectory.unprotectedRenameTo: " + src
                    + " is renamed to " + dst);
          }
          srcInodes[srcInodes.length - 2].setModificationTime(timestamp);
          dstInodes[dstInodes.length - 2].setModificationTime(timestamp);

          // Collect the blocks and remove the lease for previous dst
          if (removedDst != null) {
            INode rmdst = removedDst;
            removedDst = null;
            List<Block> collectedBlocks = new ArrayList<Block>();
            filesDeleted = rmdst.collectSubtreeBlocksAndClear(collectedBlocks);
            getFSNamesystem().removePathAndBlocks(src, collectedBlocks);
          }
          return filesDeleted >0;
        }
      } finally {
        if (removedSrc != null) {
          // Rename failed - restore src
          removedSrc.setLocalName(srcChildName);
          addChildNoQuotaCheck(srcInodes, srcInodes.length - 1, removedSrc, 
              UNKNOWN_DISK_SPACE, false);
        }
        if (removedDst != null) {
          // Rename failed - restore dst
          removedDst.setLocalName(dstChildName);
          addChildNoQuotaCheck(dstInodes, dstInodes.length - 1, removedDst, 
              UNKNOWN_DISK_SPACE, false);
        }
      }
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + "failed to rename " + src + " to " + dst);
      throw new IOException("rename from " + src + " to " + dst + " failed.");
    }
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
  Block[] setReplication(String src, 
                         short replication,
                         int[] oldReplication
                         ) throws QuotaExceededException, 
                         UnresolvedLinkException {
    waitForReady();
    Block[] fileBlocks = unprotectedSetReplication(src, replication, oldReplication);
    if (fileBlocks != null)  // log replication change
      fsImage.getEditLog().logSetReplication(src, replication);
    return fileBlocks;
  }

  Block[] unprotectedSetReplication(String src, 
                                    short replication,
                                    int[] oldReplication
                                    ) throws QuotaExceededException, 
                                    UnresolvedLinkException {
    if (oldReplication == null) {
      oldReplication = new int[1];
    }
    oldReplication[0] = -1;
    Block[] fileBlocks = null;
    synchronized(rootDir) {
      INode[] inodes = rootDir.getExistingPathINodes(src, true);
      INode inode = inodes[inodes.length - 1];
      if (inode == null) {
        return null;
      }
      assert !inode.isLink();
      if (inode.isDirectory()) {
        return null;
      }
      INodeFile fileNode = (INodeFile)inode;
      oldReplication[0] = fileNode.getReplication();

      // check disk quota
      long dsDelta = (replication - oldReplication[0]) *
           (fileNode.diskspaceConsumed()/oldReplication[0]);
      updateCount(inodes, inodes.length-1, 0, dsDelta, true);

      fileNode.setReplication(replication);
      fileBlocks = fileNode.getBlocks();
    }
    return fileBlocks;
  }

  /**
   * Get the blocksize of a file
   * @param filename the filename
   * @return the number of bytes 
   * @throws IOException if it is a directory or does not exist.
   */
  long getPreferredBlockSize(String filename) 
      throws IOException, UnresolvedLinkException {
    synchronized (rootDir) {
      INode inode = rootDir.getNode(filename, false);
      if (inode == null) {
        throw new FileNotFoundException("File does not exist: " + filename);
      }
      if (inode.isDirectory() || inode.isLink()) {
        throw new IOException("Getting block size of non-file: "+ filename); 
      }
      return ((INodeFile)inode).getPreferredBlockSize();
    }
  }

  boolean exists(String src) throws UnresolvedLinkException {
    src = normalizePath(src);
    synchronized(rootDir) {
      INode inode = rootDir.getNode(src, false);
      if (inode == null) {
         return false;
      }
      return inode.isDirectory() || inode.isLink() 
        ? true 
        : ((INodeFile)inode).getBlocks() != null;
    }
  }

  void setPermission(String src, FsPermission permission
      ) throws FileNotFoundException, UnresolvedLinkException {
    unprotectedSetPermission(src, permission);
    fsImage.getEditLog().logSetPermissions(src, permission);
  }

  void unprotectedSetPermission(String src, FsPermission permissions) 
    throws FileNotFoundException, UnresolvedLinkException {
    synchronized(rootDir) {
        INode inode = rootDir.getNode(src, true);
        if (inode == null) {
            throw new FileNotFoundException("File does not exist: " + src);
        }
        inode.setPermission(permissions);
    }
  }

  void setOwner(String src, String username, String groupname
      ) throws FileNotFoundException, UnresolvedLinkException {
    unprotectedSetOwner(src, username, groupname);
    fsImage.getEditLog().logSetOwner(src, username, groupname);
  }

  void unprotectedSetOwner(String src, String username, String groupname) 
    throws FileNotFoundException, UnresolvedLinkException {
    synchronized(rootDir) {
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
  }

  /**
   * 
   * @param target
   * @param srcs
   * @throws IOException
   */
  public void concatInternal(String target, String [] srcs) 
      throws IOException, UnresolvedLinkException {
    synchronized(rootDir) {
      // actual move
      waitForReady();

      unprotectedConcat(target, srcs);
      // do the commit
      fsImage.getEditLog().logConcat(target, srcs, FSNamesystem.now());
    }
  }
  

  
  /**
   * Concat all the blocks from srcs to trg
   * and delete the srcs files
   * @param target target file to move the blocks to
   * @param srcs list of file to move the blocks from
   * Must be public because also called from EditLogs
   * NOTE: - it does not update quota (not needed for concat)
   */
  public void unprotectedConcat(String target, String [] srcs) 
      throws IOException, UnresolvedLinkException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSNamesystem.concat to "+target);
    }
    // do the move
    
    INode [] trgINodes =  getExistingPathINodes(target);
    INodeFile trgInode = (INodeFile) trgINodes[trgINodes.length-1];
    INodeDirectory trgParent = (INodeDirectory)trgINodes[trgINodes.length-2];
    
    INodeFile [] allSrcInodes = new INodeFile[srcs.length];
    int i = 0;
    int totalBlocks = 0;
    for(String src : srcs) {
      INodeFile srcInode = getFileINode(src);
      allSrcInodes[i++] = srcInode;
      totalBlocks += srcInode.blocks.length;  
    }
    trgInode.appendBlocks(allSrcInodes, totalBlocks); // copy the blocks
    
    // since we are in the same dir - we can use same parent to remove files
    int count = 0;
    for(INodeFile nodeToRemove: allSrcInodes) {
      if(nodeToRemove == null) continue;
      
      nodeToRemove.blocks = null;
      trgParent.removeChild(nodeToRemove);
      count++;
    }
    
    long now = FSNamesystem.now();
    trgInode.setModificationTime(now);
    trgParent.setModificationTime(now);
    // update quota on the parent directory ('count' files removed, 0 space)
    unprotectedUpdateCount(trgINodes, trgINodes.length-1, - count, 0);
  }

  /**
   * Delete the target directory and collect the blocks under it
   * 
   * @param src Path of a directory to delete
   * @param collectedBlocks Blocks under the deleted directory
   * @return true on successful deletion; else false
   */
  boolean delete(String src, List<Block>collectedBlocks) 
    throws UnresolvedLinkException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.delete: " + src);
    }
    waitForReady();
    long now = FSNamesystem.now();
    int filesRemoved = unprotectedDelete(src, collectedBlocks, now);
    if (filesRemoved <= 0) {
      return false;
    }
    incrDeletedFileCount(filesRemoved);
    // Blocks will be deleted later by the caller of this method
    getFSNamesystem().removePathAndBlocks(src, null);
    fsImage.getEditLog().logDelete(src, now);
    return true;
  }
  
  /** Return if a directory is empty or not **/
  boolean isDirEmpty(String src) throws UnresolvedLinkException {
	   boolean dirNotEmpty = true;
    if (!isDir(src)) {
      return true;
    }
    synchronized(rootDir) {
      INode targetNode = rootDir.getNode(src, false);
      assert targetNode != null : "should be taken care in isDir() above";
      if (((INodeDirectory)targetNode).getChildren().size() != 0) {
        dirNotEmpty = false;
      }
    }
    return dirNotEmpty;
  }

  boolean isEmpty() {
    try {
      return isDirEmpty("/");
    } catch (UnresolvedLinkException e) {
      NameNode.stateChangeLog.debug("/ cannot be a symlink");
      assert false : "/ cannot be a symlink";
      return true;
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
    List<Block> collectedBlocks = new ArrayList<Block>();
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
  int unprotectedDelete(String src, List<Block> collectedBlocks, 
      long mtime) throws UnresolvedLinkException {
    src = normalizePath(src);

    synchronized (rootDir) {
      INode[] inodes =  rootDir.getExistingPathINodes(src, false);
      INode targetNode = inodes[inodes.length-1];

      if (targetNode == null) { // non-existent src
        NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedDelete: "
            +"failed to remove "+src+" because it does not exist");
        return 0;
      }
      if (inodes.length == 1) { // src is the root
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedDelete: " +
            "failed to remove " + src +
            " because the root is not allowed to be deleted");
        return 0;
      }
      int pos = inodes.length - 1;
      // Remove the node from the namespace
      targetNode = removeChild(inodes, pos);
      if (targetNode == null) {
        return 0;
      }
      // set the parent's modification time
      inodes[pos-1].setModificationTime(mtime);
      int filesRemoved = targetNode.collectSubtreeBlocksAndClear(collectedBlocks);
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedDelete: "
          +src+" is removed");
      }
      return filesRemoved;
    }
  }

  /**
   * Replaces the specified inode with the specified one.
   */
  public void replaceNode(String path, INodeFile oldnode, INodeFile newnode)
      throws IOException, UnresolvedLinkException {    
    synchronized (rootDir) {
      //
      // Remove the node from the namespace 
      //
      if (!oldnode.removeNode()) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.replaceNode: " +
                                     "failed to remove " + path);
        throw new IOException("FSDirectory.replaceNode: " +
                              "failed to remove " + path);
      } 
      
      /* Currently oldnode and newnode are assumed to contain the same
       * blocks. Otherwise, blocks need to be removed from the blocksMap.
       */
      rootDir.addNode(path, newnode); 

      int index = 0;
      for (BlockInfo b : newnode.getBlocks()) {
        BlockInfo info = getBlockManager().addINode(b, newnode);
        newnode.setBlock(index, info); // inode refers to the block in BlocksMap
        index++;
      }
    }
  }

  /**
   * Get a partial listing of the indicated directory
   *
   * @param src the directory name
   * @param startAfter the name to start listing after
   * @return a partial listing starting after startAfter
   */
  DirectoryListing getListing(String src, byte[] startAfter)
  throws UnresolvedLinkException {
    String srcs = normalizePath(src);

    synchronized (rootDir) {
      INode targetNode = rootDir.getNode(srcs, true);
      if (targetNode == null)
        return null;
      
      if (!targetNode.isDirectory()) {
        return new DirectoryListing(new HdfsFileStatus[]{createFileStatus(
            HdfsFileStatus.EMPTY_NAME, targetNode)}, 0);
      }
      INodeDirectory dirInode = (INodeDirectory)targetNode;
      List<INode> contents = dirInode.getChildren();
      int startChild = dirInode.nextChild(startAfter);
      int totalNumChildren = contents.size();
      int numOfListing = Math.min(totalNumChildren-startChild, this.lsLimit);
      HdfsFileStatus listing[] = new HdfsFileStatus[numOfListing];
      for (int i=0; i<numOfListing; i++) {
        INode cur = contents.get(startChild+i);
        listing[i] = createFileStatus(cur.name, cur);
      }
      return new DirectoryListing(
          listing, totalNumChildren-startChild-numOfListing);
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
    synchronized (rootDir) {
      INode targetNode = rootDir.getNode(srcs, resolveLink);
      if (targetNode == null) {
        return null;
      }
      else {
        return createFileStatus(HdfsFileStatus.EMPTY_NAME, targetNode);
      }
    }
  }

  /**
   * Get the blocks associated with the file.
   */
  Block[] getFileBlocks(String src) throws UnresolvedLinkException {
    waitForReady();
    synchronized (rootDir) {
      INode targetNode = rootDir.getNode(src, false);
      if (targetNode == null)
        return null;
      if (targetNode.isDirectory())
        return null;
      if (targetNode.isLink()) 
        return null;
      return ((INodeFile)targetNode).getBlocks();
    }
  }

  /**
   * Get {@link INode} associated with the file.
   */
  INodeFile getFileINode(String src) throws UnresolvedLinkException {
    synchronized (rootDir) {
      INode inode = rootDir.getNode(src, true);
      if (inode == null || inode.isDirectory())
        return null;
      assert !inode.isLink();      
      return (INodeFile)inode;
    }
  }

  /**
   * Retrieve the existing INodes along the given path.
   * 
   * @param path the path to explore
   * @return INodes array containing the existing INodes in the order they
   *         appear when following the path from the root INode to the
   *         deepest INodes. The array size will be the number of expected
   *         components in the path, and non existing components will be
   *         filled with null
   *         
   * @see INodeDirectory#getExistingPathINodes(byte[][], INode[])
   */
  INode[] getExistingPathINodes(String path) 
    throws UnresolvedLinkException {
    synchronized (rootDir){
      return rootDir.getExistingPathINodes(path, true);
    }
  }
  
  /** 
   * Check whether the filepath could be created
   */
  boolean isValidToCreate(String src) throws UnresolvedLinkException {
    String srcs = normalizePath(src);
    synchronized (rootDir) {
      if (srcs.startsWith("/") && 
          !srcs.endsWith("/") && 
          rootDir.getNode(srcs, false) == null) {
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * Check whether the path specifies a directory
   */
  boolean isDir(String src) throws UnresolvedLinkException {
    synchronized (rootDir) {
      INode node = rootDir.getNode(normalizePath(src), false);
      return node != null && node.isDirectory();
    }
  }

  /** Updates namespace and diskspace consumed for all
   * directories until the parent directory of file represented by path.
   * 
   * @param path path for the file.
   * @param nsDelta the delta change of namespace
   * @param dsDelta the delta change of diskspace
   * @throws QuotaExceededException if the new count violates any quota limit
   * @throws FileNotFound if path does not exist.
   */
  void updateSpaceConsumed(String path, long nsDelta, long dsDelta)
                                         throws QuotaExceededException,
                                                FileNotFoundException,
                                                UnresolvedLinkException {
    synchronized (rootDir) {
      INode[] inodes = rootDir.getExistingPathINodes(path, false);
      int len = inodes.length;
      if (inodes[len - 1] == null) {
        throw new FileNotFoundException(path + 
                                        " does not exist under rootDir.");
      }
      updateCount(inodes, len-1, nsDelta, dsDelta, true);
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
  private void updateCount(INode[] inodes, int numOfINodes, 
                           long nsDelta, long dsDelta, boolean checkQuota)
                           throws QuotaExceededException {
    if (!ready) {
      //still initializing. do not check or update quotas.
      return;
    }
    if (numOfINodes>inodes.length) {
      numOfINodes = inodes.length;
    }
    if (checkQuota) {
      verifyQuota(inodes, numOfINodes, nsDelta, dsDelta, null);
    }
    for(int i = 0; i < numOfINodes; i++) {
      if (inodes[i].isQuotaSet()) { // a directory with quota
        INodeDirectoryWithQuota node =(INodeDirectoryWithQuota)inodes[i]; 
        node.updateNumItemsInTree(nsDelta, dsDelta);
      }
    }
  }
  
  /** 
   * update quota of each inode and check to see if quota is exceeded. 
   * See {@link #updateCount(INode[], int, long, long, boolean)}
   */ 
  private void updateCountNoQuotaCheck(INode[] inodes, int numOfINodes, 
                           long nsDelta, long dsDelta) {
    try {
      updateCount(inodes, numOfINodes, nsDelta, dsDelta, false);
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
   void unprotectedUpdateCount(INode[] inodes, int numOfINodes, 
                                      long nsDelta, long dsDelta) {
    for(int i=0; i < numOfINodes; i++) {
      if (inodes[i].isQuotaSet()) { // a directory with quota
        INodeDirectoryWithQuota node =(INodeDirectoryWithQuota)inodes[i]; 
        node.unprotectedUpdateNumItemsInTree(nsDelta, dsDelta);
      }
    }
  }
  
  /** Return the name of the path represented by inodes at [0, pos] */
  private static String getFullPathName(INode[] inodes, int pos) {
    StringBuilder fullPathName = new StringBuilder();
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
   * @param inheritPermission if the permission of the directory should inherit
   *                          from its parent or not. The automatically created
   *                          ones always inherit its permission from its parent
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
    INode[] inodes = new INode[components.length];

    synchronized(rootDir) {
      rootDir.getExistingPathINodes(components, inodes, false);

      // find the index of the first null in inodes[]
      StringBuilder pathbuilder = new StringBuilder();
      int i = 1;
      for(; i < inodes.length && inodes[i] != null; i++) {
        pathbuilder.append(Path.SEPARATOR + names[i]);
        if (!inodes[i].isDirectory()) {
          throw new FileAlreadyExistsException("Parent path is not a directory: "
              + pathbuilder+ " "+inodes[i].getLocalName());
        }
      }

      // create directories beginning from the first null index
      for(; i < inodes.length; i++) {
        pathbuilder.append(Path.SEPARATOR + names[i]);
        String cur = pathbuilder.toString();
        unprotectedMkdir(inodes, i, components[i], permissions,
            inheritPermission || i != components.length-1, now);
        if (inodes[i] == null) {
          return false;
        }
        // Directory creation also count towards FilesCreated
        // to match count of FilesDeleted metric.
        if (getFSNamesystem() != null)
          NameNode.getNameNodeMetrics().numFilesCreated.inc();
        fsImage.getEditLog().logMkDir(cur, inodes[i]);
        NameNode.stateChangeLog.debug(
            "DIR* FSDirectory.mkdirs: created directory " + cur);
      }
    }
    return true;
  }

  /**
   */
  INode unprotectedMkdir(String src, PermissionStatus permissions,
                          long timestamp) throws QuotaExceededException,
                          UnresolvedLinkException {
    byte[][] components = INode.getPathComponents(src);
    INode[] inodes = new INode[components.length];
    synchronized (rootDir) {
      rootDir.getExistingPathINodes(components, inodes, false);
      unprotectedMkdir(inodes, inodes.length-1, components[inodes.length-1],
          permissions, false, timestamp);
      return inodes[inodes.length-1];
    }
  }

  /** create a directory at index pos.
   * The parent path to the directory is at [0, pos-1].
   * All ancestors exist. Newly created one stored at index pos.
   */
  private void unprotectedMkdir(INode[] inodes, int pos,
      byte[] name, PermissionStatus permission, boolean inheritPermission,
      long timestamp) throws QuotaExceededException {
    inodes[pos] = addChild(inodes, pos, 
        new INodeDirectory(name, permission, timestamp),
        -1, inheritPermission );
  }
  
  /** Add a node child to the namespace. The full path name of the node is src.
   * childDiskspace should be -1, if unknown. 
   * QuotaExceededException is thrown if it violates quota limit */
  private <T extends INode> T addNode(String src, T child, 
        long childDiskspace, boolean inheritPermission) 
  throws QuotaExceededException, UnresolvedLinkException {
    byte[][] components = INode.getPathComponents(src);
    child.setLocalName(components[components.length-1]);
    INode[] inodes = new INode[components.length];
    synchronized (rootDir) {
      rootDir.getExistingPathINodes(components, inodes, false);
      return addChild(inodes, inodes.length-1, child, childDiskspace,
                      inheritPermission);
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
  
  /** Add a node child to the inodes at index pos. 
   * Its ancestors are stored at [0, pos-1]. 
   * QuotaExceededException is thrown if it violates quota limit */
  private <T extends INode> T addChild(INode[] pathComponents, int pos,
      T child, long childDiskspace, boolean inheritPermission,
      boolean checkQuota) throws QuotaExceededException {
    INode.DirCounts counts = new INode.DirCounts();
    child.spaceConsumedInTree(counts);
    if (childDiskspace < 0) {
      childDiskspace = counts.getDsCount();
    }
    updateCount(pathComponents, pos, counts.getNsCount(), childDiskspace,
        checkQuota);
    if (pathComponents[pos-1] == null) {
      throw new NullPointerException("Panic: parent does not exist");
    }
    T addedNode = ((INodeDirectory)pathComponents[pos-1]).addChild(
        child, inheritPermission);
    if (addedNode == null) {
      updateCount(pathComponents, pos, -counts.getNsCount(), 
          -childDiskspace, true);
    }
    return addedNode;
  }

  private <T extends INode> T addChild(INode[] pathComponents, int pos,
      T child, long childDiskspace, boolean inheritPermission)
      throws QuotaExceededException {
    return addChild(pathComponents, pos, child, childDiskspace,
        inheritPermission, true);
  }
  
  private <T extends INode> T addChildNoQuotaCheck(INode[] pathComponents,
      int pos, T child, long childDiskspace, boolean inheritPermission) {
    T inode = null;
    try {
      inode = addChild(pathComponents, pos, child, childDiskspace,
          inheritPermission, false);
    } catch (QuotaExceededException e) {
      NameNode.LOG.warn("FSDirectory.addChildNoQuotaCheck - unexpected", e); 
    }
    return inode;
  }
  
  /** Remove an inode at index pos from the namespace.
   * Its ancestors are stored at [0, pos-1].
   * Count of each ancestor with quota is also updated.
   * Return the removed node; null if the removal fails.
   */
  private INode removeChild(INode[] pathComponents, int pos) {
    INode removedNode = 
      ((INodeDirectory)pathComponents[pos-1]).removeChild(pathComponents[pos]);
    if (removedNode != null) {
      INode.DirCounts counts = new INode.DirCounts();
      removedNode.spaceConsumedInTree(counts);
      updateCountNoQuotaCheck(pathComponents, pos,
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
    synchronized (rootDir) {
      INode targetNode = rootDir.getNode(srcs, false);
      if (targetNode == null) {
        throw new FileNotFoundException("File does not exist: " + srcs);
      }
      else {
        return targetNode.computeContentSummary();
      }
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

    for (INode child : dir.getChildren()) {
      if (child.isDirectory()) {
        updateCountForINodeWithQuota((INodeDirectory)child, 
                                     counts, nodesInPath);
      } else if (child.isLink()) {
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
   * @throws FileNotFoundException if the path does not exist or is a file
   * @throws QuotaExceededException if the directory tree size is 
   *                                greater than the given quota
   * @throws UnresolvedLinkException if a symlink is encountered in src.
   */
  INodeDirectory unprotectedSetQuota(String src, long nsQuota, long dsQuota)
    throws FileNotFoundException, QuotaExceededException, 
    UnresolvedLinkException {
    // sanity check
    if ((nsQuota < 0 && nsQuota != FSConstants.QUOTA_DONT_SET && 
         nsQuota < FSConstants.QUOTA_RESET) || 
        (dsQuota < 0 && dsQuota != FSConstants.QUOTA_DONT_SET && 
          dsQuota < FSConstants.QUOTA_RESET)) {
      throw new IllegalArgumentException("Illegal value for nsQuota or " +
                                         "dsQuota : " + nsQuota + " and " +
                                         dsQuota);
    }
    
    String srcs = normalizePath(src);

    synchronized(rootDir) {
      INode[] inodes = rootDir.getExistingPathINodes(src, true);
      INode targetNode = inodes[inodes.length-1];
      if (targetNode == null) {
        throw new FileNotFoundException("Directory does not exist: " + srcs);
      } else if (!targetNode.isDirectory()) {
        throw new FileNotFoundException("Cannot set quota on a file: " + srcs);  
      } else if (targetNode.isRoot() && nsQuota == FSConstants.QUOTA_RESET) {
        throw new IllegalArgumentException("Cannot clear namespace quota on root.");
      } else { // a directory inode
        INodeDirectory dirNode = (INodeDirectory)targetNode;
        long oldNsQuota = dirNode.getNsQuota();
        long oldDsQuota = dirNode.getDsQuota();
        if (nsQuota == FSConstants.QUOTA_DONT_SET) {
          nsQuota = oldNsQuota;
        }
        if (dsQuota == FSConstants.QUOTA_DONT_SET) {
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
  }
  
  /**
   * See {@link ClientProtocol#setQuota(String, long, long)} for the 
   * contract.
   * @see #unprotectedSetQuota(String, long, long)
   */
  void setQuota(String src, long nsQuota, long dsQuota) 
    throws FileNotFoundException, QuotaExceededException,
    UnresolvedLinkException { 
    synchronized (rootDir) {    
      INodeDirectory dir = unprotectedSetQuota(src, nsQuota, dsQuota);
      if (dir != null) {
        fsImage.getEditLog().logSetQuota(src, dir.getNsQuota(), 
                                         dir.getDsQuota());
      }
    }
  }
  
  long totalInodes() {
    synchronized (rootDir) {
      return rootDir.numItemsInTree();
    }
  }

  /**
   * Sets the access time on the file. Logs it in the transaction log
   */
  void setTimes(String src, INodeFile inode, long mtime, long atime, boolean force) {
    if (unprotectedSetTimes(src, inode, mtime, atime, force)) {
      fsImage.getEditLog().logTimes(src, mtime, atime);
    }
  }

  boolean unprotectedSetTimes(String src, long mtime, long atime, boolean force) 
    throws UnresolvedLinkException {
    synchronized(rootDir) {
      INodeFile inode = getFileINode(src);
      return unprotectedSetTimes(src, inode, mtime, atime, force);
    }
  }

  private boolean unprotectedSetTimes(String src, INodeFile inode, long mtime,
                                      long atime, boolean force) {
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
    rootDir = new INodeDirectoryWithQuota(INodeDirectory.ROOT_NAME,
        getFSNamesystem().createFsOwnerPermissions(new FsPermission((short)0755)),
        Integer.MAX_VALUE, -1);
  }

  /**
   * Create FileStatus by file INode 
   */
   private static HdfsFileStatus createFileStatus(byte[] path, INode node) {
    // length is zero for directories
    return new HdfsFileStatus(
        node instanceof INodeFile ? ((INodeFile)node).computeFileSize(true) : 0, 
        node.isDirectory(), 
        (node.isDirectory() || node.isLink()) ? 0 : ((INodeFile)node).getReplication(), 
        (node.isDirectory() || node.isLink()) ? 0 : ((INodeFile)node).getPreferredBlockSize(),
        node.getModificationTime(),
        node.getAccessTime(),
        node.getFsPermission(),
        node.getUserName(),
        node.getGroupName(),
        node.isLink() ? ((INodeSymlink)node).getSymlink() : null,
        path);
  }

  /**
   * Add the given symbolic link to the fs. Record it in the edits log.
   */
  INodeSymlink addSymlink(String path, String target, 
                          PermissionStatus dirPerms,
                          boolean createParent) 
      throws IOException, UnresolvedLinkException {
    waitForReady();

    final long modTime = FSNamesystem.now();
    if (createParent) {
      final String parent = new Path(path).getParent().toString();
      if (!mkdirs(parent, dirPerms, true, modTime)) {
        return null;
      }
    }
    final String userName = UserGroupInformation.getCurrentUser().getUserName();
    INodeSymlink newNode = unprotectedSymlink(path, target, modTime, modTime,
      new PermissionStatus(userName, null, FsPermission.getDefault()));         
    if (newNode == null) {
      NameNode.stateChangeLog.info("DIR* FSDirectory.addSymlink: "
                                   +"failed to add "+path
                                   +" to the file system");
      return null;
    }
    fsImage.getEditLog().logSymlink(path, target, modTime, modTime, newNode);
    
    NameNode.stateChangeLog.debug("DIR* FSDirectory.addSymlink: "
                                  +path+" is added to the file system");
    return newNode;
  }

  /**
   * Add the specified path into the namespace. Invoked from edit log processing.
   */
  INodeSymlink unprotectedSymlink(String path, String target, long modTime, 
                                  long atime, PermissionStatus perm) 
      throws UnresolvedLinkException {
    INodeSymlink newNode = new INodeSymlink(target, modTime, atime, perm);
    try {
      synchronized (rootDir) {
        newNode = addNode(path, newNode, UNKNOWN_DISK_SPACE, false);
      }
    } catch (UnresolvedLinkException e) {
      /* All UnresolvedLinkExceptions should have been resolved by now, but we
       * should re-throw them in case that changes so they are not swallowed 
       * by catching IOException below.
       */
      throw e;
    } catch (IOException e) {
      return null;
    }
    return newNode;
  }
}
