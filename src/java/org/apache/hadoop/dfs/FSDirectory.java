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
class FSDirectory implements FSConstants {

  FSNamesystem namesystem = null;
  INodeDirectory rootDir = new INodeDirectory(INodeDirectory.ROOT_NAME);
  FSImage fsImage;  
  boolean ready = false;
  // Metrics record
  private MetricsRecord directoryMetrics = null;
    
  /** Access an existing dfs name directory. */
  public FSDirectory(FSNamesystem ns, Configuration conf) throws IOException {
    this.fsImage = new FSImage();
    namesystem = ns;
    initialize(conf);
  }

  public FSDirectory(FSImage fsImage, FSNamesystem ns, Configuration conf) throws IOException {
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
      fsImage.recoverTransitionRead(dataDirs, startOpt);
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
  INode addFile(String path, 
                short replication,
                long preferredBlockSize,
                String clientName,
                String clientMachine,
                DatanodeDescriptor clientNode) 
                throws IOException {
    waitForReady();

    // Always do an implicit mkdirs for parent directory tree.
    long modTime = FSNamesystem.now();
    if (!mkdirs(new Path(path).getParent().toString(), modTime)) {
      return null;
    }
    INodeFile newNode = new INodeFileUnderConstruction(replication, 
                                 preferredBlockSize, modTime, clientName, 
                                 clientMachine, clientNode);
    synchronized (rootDir) {
      try {
        newNode = rootDir.addNode(path, newNode);
      } catch (FileNotFoundException e) {
        newNode = null;
      }
    }
    if (newNode == null) {
      NameNode.stateChangeLog.info("DIR* FSDirectory.addFile: "
                                   +"failed to add "+path
                                   +" to the file system");
      return null;
    }
    // add create file record to log
    fsImage.getEditLog().logCreateFile(path, newNode);
    NameNode.stateChangeLog.debug("DIR* FSDirectory.addFile: "
                                  +path+" is added to the file system");
    return newNode;
  }

  /**
   */
  INode unprotectedAddFile( String path, 
                            Block[] blocks, 
                            short replication,
                            long modificationTime,
                            long preferredBlockSize) {
    INode newNode;
    if (blocks == null)
      newNode = new INodeDirectory(modificationTime);
    else
      newNode = new INodeFile(blocks.length, replication, modificationTime,
                              preferredBlockSize);
    synchronized (rootDir) {
      try {
        newNode = rootDir.addNode(path, newNode);
        if(newNode != null && blocks != null) {
          int nrBlocks = blocks.length;
          // Add file->block mapping
          INodeFile newF = (INodeFile)newNode;
          for (int i = 0; i < nrBlocks; i++) {
            newF.setBlock(i, namesystem.blocksMap.addINode(blocks[i], newF));
          }
        }
      } catch (FileNotFoundException e) {
        return null;
      }
      return newNode;
    }
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
  void persistBlocks(String path, INode file) throws IOException {
    waitForReady();

    synchronized (rootDir) {
      INodeFile fileNode = (INodeFile) file;

      // create two transactions. The first one deletes the empty
      // file and the second transaction recreates the same file
      // with the appropriate set of blocks.
      fsImage.getEditLog().logDelete(path, fileNode.getModificationTime());

      // re-add create file record to log
      fsImage.getEditLog().logCreateFile(path, fileNode);
      NameNode.stateChangeLog.debug("DIR* FSDirectory.addFile: "
                                    +path+" with "+ fileNode.getBlocks().length 
                                    +" blocks is persisted to the file system");
    }
  }

  /**
   * Remove a block to the file.
   */
  boolean removeBlock(String path, INode file, Block block) throws IOException {
    waitForReady();

    synchronized (rootDir) {
      INodeFile fileNode = (INodeFile) file;
      if (fileNode == null) {
        throw new IOException("Unknown file: " + path);
      }

      // modify file-> block and blocksMap
      fileNode.removeBlock(block);
      namesystem.blocksMap.removeINode(block);

      // create two transactions. The first one deletes the empty
      // file and the second transaction recreates the same file
      // with the appropriate set of blocks.
      fsImage.getEditLog().logDelete(path, fileNode.getModificationTime());

      // re-add create file record to log
      fsImage.getEditLog().logCreateFile(path, fileNode);
      NameNode.stateChangeLog.debug("DIR* FSDirectory.addFile: "
                                    +path+" with "+block
                                    +" block is added to the file system");
    }
    return true;
  }

  /**
   * Change the filename
   */
  public boolean renameTo(String src, String dst) {
    NameNode.stateChangeLog.debug("DIR* FSDirectory.renameTo: "
                                  +src+" to "+dst);
    waitForReady();
    long now = FSNamesystem.now();
    if (!unprotectedRenameTo(src, dst, now))
      return false;
    fsImage.getEditLog().logRename(src, dst, now);
    return true;
  }

  /**
   */
  boolean unprotectedRenameTo(String src, String dst, long timestamp) {
    synchronized(rootDir) {
      INode renamedNode = rootDir.getNode(src);
      if (renamedNode == null) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                                     +"failed to rename "+src+" to "+dst+ " because source does not exist");
        return false;
      }
      if (isDir(dst)) {
        dst += "/" + new File(src).getName();
      }
      if (rootDir.getNode(dst) != null) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                                     +"failed to rename "+src+" to "+dst+ " because destination exists");
        return false;
      }
      INodeDirectory oldParent = renamedNode.getParent();
      oldParent.removeChild(renamedNode);
            
      // the renamed node can be reused now
      try {
        if (rootDir.addNode(dst, renamedNode) != null) {
          NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedRenameTo: "
                                        +src+" is renamed to "+dst);

          // update modification time of old parent as well as new parent dir
          oldParent.setModificationTime(timestamp);
          renamedNode.getParent().setModificationTime(timestamp);
          return true;
        }
      } catch (FileNotFoundException e) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                                     +"failed to rename "+src+" to "+dst);
        try {
          rootDir.addNode(src, renamedNode); // put it back
        }catch(FileNotFoundException e2) {                
        }
      }
      return false;
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
    
  /**
   * Remove the file from management, return blocks
   */
  public Block[] delete(String src) {
    NameNode.stateChangeLog.debug("DIR* FSDirectory.delete: "
                                  +src);
    waitForReady();
    long now = FSNamesystem.now();
    Block[] blocks = unprotectedDelete(src, now); 
    if (blocks != null)
      fsImage.getEditLog().logDelete(src, now);
    return blocks;
  }

  /**
   */
  Block[] unprotectedDelete(String src, long modificationTime) {
    synchronized (rootDir) {
      INode targetNode = rootDir.getNode(src);
      if (targetNode == null) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedDelete: "
                                     +"failed to remove "+src+" because it does not exist");
        return null;
      } else {
        //
        // Remove the node from the namespace and GC all
        // the blocks underneath the node.
        //
        if (!targetNode.removeNode()) {
          NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedDelete: "
                                       +"failed to remove "+src+" because it does not have a parent");
          return null;
        } else {
          NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedDelete: "
                                        +src+" is removed");
          targetNode.getParent().setModificationTime(modificationTime);
          ArrayList<Block> v = new ArrayList<Block>();
          int filesRemoved = targetNode.collectSubtreeBlocks(v);
          incrDeletedFileCount(filesRemoved);
          for (Block b : v) {
            namesystem.blocksMap.removeINode(b);
          }
          return v.toArray(new Block[v.size()]);
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
      for (Block b : newnode.getBlocks()) {
        namesystem.blocksMap.addINode(b, newnode);
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

  /* Get the file info for a specific file.
   * @param src The string representation of the path to the file
   * @throws IOException if file does not exist
   * @return object containing information regarding the file
   */
  DFSFileInfo getFileInfo(String src) throws IOException {
    String srcs = normalizePath(src);
    synchronized (rootDir) {
      INode targetNode = rootDir.getNode(srcs);
      if (targetNode == null) {
        throw new IOException("File does not exist");
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
    waitForReady();
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

  /**
   * Create directory entries for every item
   */
  boolean mkdirs(String src, long now) {
    src = normalizePath(src);

    // Use this to collect all the dirs we need to construct
    List<String> v = new ArrayList<String>();

    // The dir itself
    v.add(src);

    // All its parents
    Path parent = new Path(src).getParent();
    while (parent != null) {
      v.add(parent.toString());
      parent = parent.getParent();
    }

    // Now go backwards through list of dirs, creating along
    // the way
    int numElts = v.size();
    for (int i = numElts - 1; i >= 0; i--) {
      String cur = v.get(i);
      try {
        INode inserted = unprotectedMkdir(cur, now);
        if (inserted != null) {
          NameNode.stateChangeLog.debug("DIR* FSDirectory.mkdirs: "
                                        +"created directory "+cur);
          fsImage.getEditLog().logMkDir(cur, inserted);
        } else { // otherwise cur exists, verify that it is a directory
          if (!isDir(cur)) {
            NameNode.stateChangeLog.debug("DIR* FSDirectory.mkdirs: "
                                          +"path " + cur + " is not a directory ");
            return false;
          } 
        }
      } catch (FileNotFoundException e) {
        NameNode.stateChangeLog.debug("DIR* FSDirectory.mkdirs: "
                                      +"failed to create directory "+src);
        return false;
      }
    }
    return true;
  }

  /**
   */
  INode unprotectedMkdir(String src, long timestamp) throws FileNotFoundException {
    synchronized (rootDir) {
      return rootDir.addNode(src, new INodeDirectory(timestamp));
    }
  }

  /**
   */
  String normalizePath(String src) {
    if (src.length() > 1 && src.endsWith("/")) {
      src = src.substring(0, src.length() - 1);
    }
    return src;
  }
}
