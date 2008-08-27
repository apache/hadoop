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
package org.apache.hadoop.dfs;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.dfs.BlocksMap.BlockInfo;

/*************************************************
 * FSDirectory stores the filesystem directory state.
 * It handles writing/loading values to disk, and logging
 * changes as we go.
 *
 * It keeps the filename->blockset mapping always-current
 * and logged to disk.
 * 
 *************************************************/
class FSDirectory implements FSConstants, Closeable {

  final FSNamesystem namesystem;
  final INodeDirectoryWithQuota rootDir;
  FSImage fsImage;  
  boolean ready = false;
  // Metrics record
  private MetricsRecord directoryMetrics = null;

  /** Access an existing dfs name directory. */
  public FSDirectory(FSNamesystem ns, Configuration conf) throws IOException {
    this(new FSImage(), ns, conf);
    fsImage.setCheckpointDirectories(FSImage.getCheckpointDirs(conf, null));
  }

  public FSDirectory(FSImage fsImage, FSNamesystem ns, Configuration conf) throws IOException {
    rootDir = new INodeDirectoryWithQuota(INodeDirectory.ROOT_NAME,
        ns.createFsOwnerPermissions(new FsPermission((short)0755)),
        Integer.MAX_VALUE);
    this.fsImage = fsImage;
    namesystem = ns;
    initialize(conf);
  }
    
  private void initialize(Configuration conf) {
    MetricsContext metricsContext = MetricsUtil.getContext("dfs");
    directoryMetrics = MetricsUtil.createRecord(metricsContext, "FSDirectory");
    directoryMetrics.setTag("sessionId", conf.get("session.id"));
  }

  void loadFSImage(Collection<File> dataDirs,
                   StartupOption startOpt) throws IOException {
    // format before starting up if requested
    if (startOpt == StartupOption.FORMAT) {
      fsImage.setStorageDirectories(dataDirs);
      fsImage.format();
      startOpt = StartupOption.REGULAR;
    }
    try {
      if (fsImage.recoverTransitionRead(dataDirs, startOpt)) {
        fsImage.saveFSImage();
      }
      FSEditLog editLog = fsImage.getEditLog();
      assert editLog != null : "editLog must be initialized";
      if (!editLog.isOpen())
        editLog.open();
      fsImage.setCheckpointDirectories(null);
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
    directoryMetrics.incrMetric("files_deleted", count);
    directoryMetrics.update();
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
                throws IOException {
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
      newNode = addNode(path, newNode, false);
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
                            Block[] blocks, 
                            short replication,
                            long modificationTime,
                            long preferredBlockSize) {
    INode newNode;
    if (blocks == null)
      newNode = new INodeDirectory(permissions, modificationTime);
    else 
      newNode = new INodeFile(permissions, blocks.length, replication,
                              modificationTime, preferredBlockSize);
    synchronized (rootDir) {
      try {
        newNode = addNode(path, newNode, false);
        if(newNode != null && blocks != null) {
          int nrBlocks = blocks.length;
          // Add file->block mapping
          INodeFile newF = (INodeFile)newNode;
          for (int i = 0; i < nrBlocks; i++) {
            newF.setBlock(i, namesystem.blocksMap.addINode(blocks[i], newF));
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
                              short replication,
                              long modificationTime,
                              long quota,
                              long preferredBlockSize) {
    // create new inode
    INode newNode;
    if (blocks == null) {
      if (quota >= 0) {
        newNode = new INodeDirectoryWithQuota(
            permissions, modificationTime, quota);
      } else {
        newNode = new INodeDirectory(permissions, modificationTime);
      }
    } else 
      newNode = new INodeFile(permissions, blocks.length, replication,
                              modificationTime, preferredBlockSize);
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
        INodeFile newF = (INodeFile)newNode;
        for (int i = 0; i < nrBlocks; i++) {
          newF.setBlock(i, namesystem.blocksMap.addINode(blocks[i], newF));
        }
      }
    }
    return newParent;
  }

  /**
   * Add a block to the file. Returns a reference to the added block.
   */
  Block addBlock(String path, INode file, Block block) throws IOException {
    waitForReady();

    synchronized (rootDir) {
      INodeFile fileNode = (INodeFile) file;

      // associate the new list of blocks with this file
      namesystem.blocksMap.addINode(block, fileNode);
      BlockInfo blockInfo = namesystem.blocksMap.getStoredBlock(block);
      fileNode.addBlock(blockInfo);

      NameNode.stateChangeLog.debug("DIR* FSDirectory.addFile: "
                                    + path + " with " + block
                                    + " block is added to the in-memory "
                                    + "file system");
    }
    return block;
  }

  /**
   * Persist the block list for the inode.
   */
  void persistBlocks(String path, INodeFileUnderConstruction file) 
                     throws IOException {
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
  void closeFile(String path, INodeFile file) throws IOException {
    waitForReady();
    synchronized (rootDir) {
      // file is closed
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
      fileNode.removeBlock(block);
      namesystem.blocksMap.removeINode(block);
      // If block is removed from blocksMap remove it from corruptReplicasMap
      namesystem.corruptReplicas.removeFromCorruptReplicasMap(block);

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
   */
  boolean renameTo(String src, String dst) throws QuotaExceededException {
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

  /** Change a path name
   * 
   * @param src source path
   * @param dst destination path
   * @return true if rename succeeds; false otherwise
   * @throws QuotaExceededException if the operation violates any quota limit
   */
  boolean unprotectedRenameTo(String src, String dst, long timestamp) 
  throws QuotaExceededException {
    byte[][] srcComponents = INode.getPathComponents(src);
    INode[] srcInodes = new INode[srcComponents.length];
    synchronized (rootDir) {
      rootDir.getExistingPathINodes(srcComponents, srcInodes);

      // check the validation of the source
      if (srcInodes[srcInodes.length-1] == null) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                                     +"failed to rename "+src+" to "+dst+ " because source does not exist");
        return false;
      } else if (srcInodes.length == 1) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
            +"failed to rename "+src+" to "+dst+ " because source is the root");
        return false;
      }
      if (isDir(dst)) {
        dst += Path.SEPARATOR + new Path(src).getName();
      }
      
      // remove source
      INode srcChild = null;
      try {
        srcChild = removeChild(srcInodes, srcInodes.length-1);
      } catch (IOException e) {
        // srcChild == null; go to next if statement
      }
      if (srcChild == null) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
            +"failed to rename "+src+" to "+dst+ " because the source can not be removed");
        return false;
      }

      String srcChildName = srcChild.getLocalName();
      
      // check the validity of the destination
      INode dstChild = null;
      QuotaExceededException failureByQuota = null;

      byte[][] dstComponents = INode.getPathComponents(dst);
      INode[] dstInodes = new INode[dstComponents.length];
      rootDir.getExistingPathINodes(dstComponents, dstInodes);
      if (dstInodes[dstInodes.length-1] != null) { //check if destination exists
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                                     +"failed to rename "+src+" to "+dst+ 
                                     " because destination exists");
      } else if (dstInodes[dstInodes.length-2] == null) { // check if its parent exists
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
            +"failed to rename "+src+" to "+dst+ 
            " because destination's parent does not exists");
      }
      else {
        // add to the destination
        srcChild.setLocalName(dstComponents[dstInodes.length-1]);
        try {
          // add it to the namespace
          dstChild = addChild(dstInodes, dstInodes.length-1, srcChild, false);
        } catch (QuotaExceededException qe) {
          failureByQuota = qe;
        }
      }
      if (dstChild != null) {
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedRenameTo: "
            +src+" is renamed to "+dst);
        }

        // update modification time of dst and the parent of src
        srcInodes[srcInodes.length-2].setModificationTime(timestamp);
        dstInodes[dstInodes.length-2].setModificationTime(timestamp);
        return true;
      } else {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
            +"failed to rename "+src+" to "+dst);
        try {
          // put it back
          srcChild.setLocalName(srcChildName);
          addChild(srcInodes, srcInodes.length-1, srcChild, false);
        } catch (IOException ignored) {}
        if (failureByQuota != null) {
          throw failureByQuota;
        } else {
          return false;
        }
      }
    }
  }

  /**
   * Set file replication
   * 
   * @param src file name
   * @param replication new replication
   * @param oldReplication old replication - output parameter
   * @return array of file blocks
   * @throws IOException
   */
  Block[] setReplication(String src, 
                         short replication,
                         int[] oldReplication
                         ) throws IOException {
    waitForReady();
    Block[] fileBlocks = unprotectedSetReplication(src, replication, oldReplication);
    if (fileBlocks != null)  // log replication change
      fsImage.getEditLog().logSetReplication(src, replication);
    return fileBlocks;
  }

  Block[] unprotectedSetReplication( String src, 
                                     short replication,
                                     int[] oldReplication
                                     ) throws IOException {
    if (oldReplication == null)
      oldReplication = new int[1];
    oldReplication[0] = -1;
    Block[] fileBlocks = null;
    synchronized(rootDir) {
      INode inode = rootDir.getNode(src);
      if (inode == null)
        return null;
      if (inode.isDirectory())
        return null;
      INodeFile fileNode = (INodeFile)inode;
      oldReplication[0] = fileNode.getReplication();
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
  public long getPreferredBlockSize(String filename) throws IOException {
    synchronized (rootDir) {
      INode fileNode = rootDir.getNode(filename);
      if (fileNode == null) {
        throw new IOException("Unknown file: " + filename);
      }
      if (fileNode.isDirectory()) {
        throw new IOException("Getting block size of a directory: " + 
                              filename);
      }
      return ((INodeFile)fileNode).getPreferredBlockSize();
    }
  }

  boolean exists(String src) {
    src = normalizePath(src);
    synchronized(rootDir) {
      INode inode = rootDir.getNode(src);
      if (inode == null) {
         return false;
      }
      return inode.isDirectory()? true: ((INodeFile)inode).getBlocks() != null;
    }
  }

  void setPermission(String src, FsPermission permission
      ) throws IOException {
    unprotectedSetPermission(src, permission);
    fsImage.getEditLog().logSetPermissions(src, permission);
  }

  void unprotectedSetPermission(String src, FsPermission permissions) throws FileNotFoundException {
    synchronized(rootDir) {
        INode inode = rootDir.getNode(src);
        if(inode == null)
            throw new FileNotFoundException("File does not exist: " + src);
        inode.setPermission(permissions);
    }
  }

  void setOwner(String src, String username, String groupname
      ) throws IOException {
    unprotectedSetOwner(src, username, groupname);
    fsImage.getEditLog().logSetOwner(src, username, groupname);
  }

  void unprotectedSetOwner(String src, String username, String groupname) throws FileNotFoundException {
    synchronized(rootDir) {
      INode inode = rootDir.getNode(src);
      if(inode == null)
          throw new FileNotFoundException("File does not exist: " + src);
      if (username != null) {
        inode.setUser(username);
      }
      if (groupname != null) {
        inode.setGroup(groupname);
      }
    }
  }
    
  /**
   * Remove the file from management, return blocks
   */
  public INode delete(String src) {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.delete: "+src);
    }
    waitForReady();
    long now = FSNamesystem.now();
    INode deletedNode = unprotectedDelete(src, now);
    if (deletedNode != null) {
      fsImage.getEditLog().logDelete(src, now);
    }
    return deletedNode;
  }
  
  /** Return if a directory is empty or not **/
  public boolean isDirEmpty(String src) {
	boolean dirNotEmpty = true;
    if (!isDir(src)) {
      return true;
    }
    synchronized(rootDir) {
      INode targetNode = rootDir.getNode(src);
      assert targetNode != null : "should be taken care in isDir() above";
      if (((INodeDirectory)targetNode).getChildren().size() != 0) {
        dirNotEmpty = false;
      }
    }
    return dirNotEmpty;
  }
  
  /**
   * Delete a path from the name space
   * Update the count at each ancestor directory with quota
   * @param src a string representation of a path to an inode
   * @param modificationTime the time the inode is removed
   * @param deletedBlocks the place holder for the blocks to be removed
   * @return if the deletion succeeds
   */ 
  INode unprotectedDelete(String src, long modificationTime) {
    src = normalizePath(src);
    String[] names = INode.getPathNames(src);
    byte[][] components = INode.getPathComponents(names);
    INode[] inodes = new INode[components.length];

    synchronized (rootDir) {
      rootDir.getExistingPathINodes(components, inodes);
      INode targetNode = inodes[inodes.length-1];

      if (targetNode == null) { // non-existent src
        NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedDelete: "
            +"failed to remove "+src+" because it does not exist");
        return null;
      } else if (inodes.length == 1) { // src is the root
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedDelete: " +
            "failed to remove " + src +
            " because the root is not allowed to be deleted");
        return null;
      } else {
        try {
          // Remove the node from the namespace
          removeChild(inodes, inodes.length-1);
          // set the parent's modification time
          inodes[inodes.length-2].setModificationTime(modificationTime);
          // GC all the blocks underneath the node.
          ArrayList<Block> v = new ArrayList<Block>();
          int filesRemoved = targetNode.collectSubtreeBlocksAndClear(v);
          incrDeletedFileCount(filesRemoved);
          namesystem.removePathAndBlocks(src, v);
          if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedDelete: "
              +src+" is removed");
          }
          return targetNode;
        } catch (IOException e) {
          NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedDelete: " +
              "failed to remove " + src + " because " + e.getMessage());
          return null;
        }
      }
    }
  }

  /**
   * Replaces the specified inode with the specified one.
   */
  void replaceNode(String path, INodeFile oldnode, INodeFile newnode) 
                      throws IOException {
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
      rootDir.addNode(path, newnode); 
      int index = 0;
      for (Block b : newnode.getBlocks()) {
        BlockInfo info = namesystem.blocksMap.addINode(b, newnode);
        newnode.setBlock(index, info); // inode refers to the block in BlocksMap
        index++;
      }
    }
  }

  /**
   * Get a listing of files given path 'src'
   *
   * This function is admittedly very inefficient right now.  We'll
   * make it better later.
   */
  public DFSFileInfo[] getListing(String src) {
    String srcs = normalizePath(src);

    synchronized (rootDir) {
      INode targetNode = rootDir.getNode(srcs);
      if (targetNode == null)
        return null;
      if (!targetNode.isDirectory()) {
        return new DFSFileInfo[]{new DFSFileInfo(srcs, targetNode)};
      }
      List<INode> contents = ((INodeDirectory)targetNode).getChildren();
      DFSFileInfo listing[] = new DFSFileInfo[contents.size()];
      if(! srcs.endsWith(Path.SEPARATOR))
        srcs += Path.SEPARATOR;
      int i = 0;
      for (INode cur : contents) {
        listing[i] = new DFSFileInfo(srcs+cur.getLocalName(), cur);
        i++;
      }
      return listing;
    }
  }

  /** Get the file info for a specific file.
   * @param src The string representation of the path to the file
   * @return object containing information regarding the file
   *         or null if file not found
   */
  DFSFileInfo getFileInfo(String src) {
    String srcs = normalizePath(src);
    synchronized (rootDir) {
      INode targetNode = rootDir.getNode(srcs);
      if (targetNode == null) {
        return null;
      }
      else {
        return new DFSFileInfo(srcs, targetNode);
      }
    }
  }

  /**
   * Get the blocks associated with the file.
   */
  Block[] getFileBlocks(String src) {
    waitForReady();
    synchronized (rootDir) {
      INode targetNode = rootDir.getNode(src);
      if (targetNode == null)
        return null;
      if(targetNode.isDirectory())
        return null;
      return ((INodeFile)targetNode).getBlocks();
    }
  }

  /**
   * Get {@link INode} associated with the file.
   */
  INodeFile getFileINode(String src) {
    synchronized (rootDir) {
      INode inode = rootDir.getNode(src);
      if (inode == null || inode.isDirectory())
        return null;
      return (INodeFile)inode;
    }
  }

  /** 
   * Check whether the filepath could be created
   */
  public boolean isValidToCreate(String src) {
    String srcs = normalizePath(src);
    synchronized (rootDir) {
      if (srcs.startsWith("/") && 
          !srcs.endsWith("/") && 
          rootDir.getNode(srcs) == null) {
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * Check whether the path specifies a directory
   */
  public boolean isDir(String src) {
    synchronized (rootDir) {
      INode node = rootDir.getNode(normalizePath(src));
      return node != null && node.isDirectory();
    }
  }

  /** update count of each inode with quota
   * 
   * @param inodes an array of inodes on a path
   * @param numOfINodes the number of inodes to update starting from index 0
   * @param deltaCount the delta change of the count
   * @throws QuotaExceededException if the new count violates any quota limit
   */
  private static void updateCount(
      INode[] inodes, int numOfINodes, long deltaCount )
  throws QuotaExceededException {
    if (numOfINodes>inodes.length) {
      numOfINodes = inodes.length;
    }
    // check existing components in the path  
    List<INodeDirectoryWithQuota> inodesWithQuota = 
      new ArrayList<INodeDirectoryWithQuota>(numOfINodes);
    int i=0;
    try {
      for(; i < numOfINodes; i++) {
        if (inodes[i].getQuota() >= 0) { // a directory with quota
          INodeDirectoryWithQuota quotaINode =(INodeDirectoryWithQuota)inodes[i]; 
          quotaINode.updateNumItemsInTree(deltaCount);
          inodesWithQuota.add(quotaINode);
        }
      }
    } catch (QuotaExceededException e) {
      for (INodeDirectoryWithQuota quotaINode:inodesWithQuota) {
        try {
          quotaINode.updateNumItemsInTree(-deltaCount);
        } catch (IOException ingored) {
        }
      }
      e.setPathName(getFullPathName(inodes, i));
      throw e;
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
   */
  boolean mkdirs(String src, PermissionStatus permissions,
      boolean inheritPermission, long now)
      throws FileNotFoundException, QuotaExceededException {
    src = normalizePath(src);
    String[] names = INode.getPathNames(src);
    byte[][] components = INode.getPathComponents(names);
    INode[] inodes = new INode[components.length];

    synchronized(rootDir) {
      rootDir.getExistingPathINodes(components, inodes);

      // find the index of the first null in inodes[]
      StringBuilder pathbuilder = new StringBuilder();
      int i = 1;
      for(; i < inodes.length && inodes[i] != null; i++) {
        pathbuilder.append(Path.SEPARATOR + names[i]);
        if (!inodes[i].isDirectory()) {
          throw new FileNotFoundException("Parent path is not a directory: "
              + pathbuilder);
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
        // to match count of files_deleted metric. 
        if (namesystem != null)
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
                          long timestamp) throws QuotaExceededException {
    byte[][] components = INode.getPathComponents(src);
    INode[] inodes = new INode[components.length];
    synchronized (rootDir) {
      rootDir.getExistingPathINodes(components, inodes);
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
        inheritPermission );
  }
  
  /** Add a node child to the namespace. The full path name of the node is src. 
   * QuotaExceededException is thrown if it violates quota limit */
  private <T extends INode> T addNode(String src, T child, 
      boolean inheritPermission) 
  throws QuotaExceededException {
    byte[][] components = INode.getPathComponents(src);
    child.setLocalName(components[components.length-1]);
    INode[] inodes = new INode[components.length];
    synchronized (rootDir) {
      rootDir.getExistingPathINodes(components, inodes);
      return addChild(inodes, inodes.length-1, child, inheritPermission);
    }
  }
  
  /** Add a node child to the inodes at index pos. 
   * Its ancestors are stored at [0, pos-1]. 
   * QuotaExceededException is thrown if it violates quota limit */
  private <T extends INode> T addChild(INode[] pathComponents, int pos, T child,
      boolean inheritPermission) throws QuotaExceededException {
    long childSize = child.numItemsInTree();
    updateCount(pathComponents, pos, childSize);
    T addedNode = ((INodeDirectory)pathComponents[pos-1]).addChild(
        child, inheritPermission);
    if (addedNode == null) {
      updateCount(pathComponents, pos, -childSize);
    }
    return addedNode;
  }
  
  /** Remove an inode at index pos from the namespace.
   * Its ancestors are stored at [0, pos-1].
   * Count of each ancestor with quota is also updated.
   * Return the removed node; null if the removal fails.
   */
  private INode removeChild(INode[] pathComponents, int pos)
  throws QuotaExceededException {
    INode removedNode = 
      ((INodeDirectory)pathComponents[pos-1]).removeChild(pathComponents[pos]);
    if (removedNode != null) {
      updateCount(pathComponents, pos, 
          -removedNode.numItemsInTree());
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

  ContentSummary getContentSummary(String src) throws IOException {
    String srcs = normalizePath(src);
    synchronized (rootDir) {
      INode targetNode = rootDir.getNode(srcs);
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
   * @throws QuotaExceededException if the count update violates 
   *                                any quota limitation
   */
  void updateCountForINodeWithQuota() throws QuotaExceededException {
    updateCountForINodeWithQuota(rootDir);
  }
  
  /** Update the count of the directory if it has a quota and return the count
   * 
   * @param node the root of the tree that represents the directory
   * @return the size of the tree
   * @throws QuotaExceededException if the count is greater than its quota
   */
  private static long updateCountForINodeWithQuota(INode node) throws QuotaExceededException {
    long count = 1L;
    if (node.isDirectory()) {
      INodeDirectory dNode = (INodeDirectory)node;
      for (INode child : dNode.getChildren()) {
        count += updateCountForINodeWithQuota(child);
      }
      if (dNode.getQuota()>=0) {
        ((INodeDirectoryWithQuota)dNode).setCount(count);
      }
    }
    return count;
  }
  
  /**
   * Set the quota for a directory.
   * @param path The string representation of the path to the directory
   * @param quota The limit of the number of names in or below the directory
   * @throws FileNotFoundException if the path does not exist or is a file
   * @throws QuotaExceededException if the directory tree size is 
   *                                greater than the given quota
   */
  void unprotectedSetQuota(String src, long quota)
  throws FileNotFoundException, QuotaExceededException {
    String srcs = normalizePath(src);
    byte[][] components = INode.getPathComponents(src);
    INode[] inodes = new INode[components.length==1?1:2];
    synchronized (rootDir) {
      rootDir.getExistingPathINodes(components, inodes);
      INode targetNode = inodes[inodes.length-1];
      if (targetNode == null) {
        throw new FileNotFoundException("Directory does not exist: " + srcs);
      } else if (!targetNode.isDirectory()) {
        throw new FileNotFoundException("Cannot set quota on a file: " + srcs);  
      } else { // a directory inode
        INodeDirectory dirNode = (INodeDirectory)targetNode;
        if (dirNode instanceof INodeDirectoryWithQuota) { 
          // a directory with quota; so set the quota to the new value
          ((INodeDirectoryWithQuota)dirNode).setQuota(quota);
        } else {
          // a non-quota directory; so replace it with a directory with quota
          INodeDirectoryWithQuota newNode = 
            new INodeDirectoryWithQuota(quota, dirNode);
          // non-root directory node; parent != null
          assert inodes.length==2;
          INodeDirectory parent = (INodeDirectory)inodes[0];
          parent.replaceChild(newNode);
        }
      }
    }
  }
  
  /**
   * @see #unprotectedSetQuota(String, long)
   */
  void setQuota(String src, long quota) 
  throws FileNotFoundException, QuotaExceededException {
    unprotectedSetQuota(src, quota);
    fsImage.getEditLog().logSetQuota(src, quota);
  }
  
  /**
   * Remove the quota for a directory
   * @param src The string representation of the path to the directory
   * @throws FileNotFoundException if the path does not exist or it is a file
   */
  void unprotectedClearQuota(String src) throws IOException {
    String srcs = normalizePath(src);
    byte[][] components = INode.getPathComponents(src);
    INode[] inodes = new INode[components.length==1?1:2];
    synchronized (rootDir) {
      rootDir.getExistingPathINodes(components, inodes);
      INode targetNode = inodes[inodes.length-1];
      if (targetNode == null || !targetNode.isDirectory()) {
        throw new FileNotFoundException("Directory does not exist: " + srcs);
      } else if (targetNode instanceof INodeDirectoryWithQuota) {
        // a directory inode with quota
        // replace the directory with quota with a non-quota one
        INodeDirectoryWithQuota dirNode = (INodeDirectoryWithQuota)targetNode;
        INodeDirectory newNode = new INodeDirectory(dirNode);
        if (dirNode == rootDir) { // root
          throw new IOException("Can't clear the root's quota");
        } else { // non-root directory node; parent != null
          INodeDirectory parent = (INodeDirectory)inodes[0];
          parent.replaceChild(newNode);
        }
      }
    }
  }
  
  /**
   * @see #unprotectedClearQuota(String)
   */
  void clearQuota(String src) throws IOException {
    unprotectedClearQuota(src);
    fsImage.getEditLog().logClearQuota(src);
  }
  
  long totalInodes() {
    synchronized (rootDir) {
      return rootDir.numItemsInTree();
    }
  }
}
