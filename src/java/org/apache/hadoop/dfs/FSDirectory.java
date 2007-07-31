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

  /******************************************************
   * We keep an in-memory representation of the file/block
   * hierarchy.
   * 
   * TODO: Factor out INode to a standalone class.
   ******************************************************/
  static class INode {

    private String name;
    private INode parent;
    private TreeMap<String, INode> children = null;
    private Block blocks[] = null;
    private short blockReplication;
    private long modificationTime;

    /**
     */
    INode(String name) {
      this.name = name;
      this.parent = null;
      this.blocks = null;
      this.blockReplication = 0;
      this.modificationTime = 0;
    }

    INode(String name, long modifictionTime) {
      this.name = name;
      this.parent = null;
      this.blocks = null;
      this.blockReplication = 0;
      this.modificationTime = modifictionTime;
    }

    /**
     */
    INode(String name, Block blocks[], short replication,
          long modificationTime) {
      this.name = name;
      this.parent = null;
      this.blocks = blocks;
      this.blockReplication = replication;
      this.modificationTime = modificationTime;
    }

    /**
     * Check whether it's a directory
     */
    synchronized public boolean isDir() {
      return (blocks == null);
    }
        
    /**
     * Get block replication for the file 
     * @return block replication
     */
    public short getReplication() {
      return this.blockReplication;
    }
        
    /**
     * Get local file name
     * @return local file name
     */
    String getLocalName() {
      return name;
    }

    /**
     * Get the full absolute path name of this file (recursively computed).
     * 
     * @return the string representation of the absolute path of this file
     */
    String getAbsoluteName() {
      return internalGetAbsolutePathName().toString();
    }

    /**
     * Recursive computation of the absolute path name of this INode using a
     * StringBuffer. This relies on the root INode name being "".
     * 
     * @return the StringBuffer containing the absolute path name.
     */
    private StringBuffer internalGetAbsolutePathName() {
      if (parent == null) {
        return new StringBuffer(name);
      } else {
        return parent.internalGetAbsolutePathName().append(
            Path.SEPARATOR_CHAR).append(name);
      }
    }

    /**
     * Get file blocks 
     * @return file blocks
     */
    Block[] getBlocks() {
      return this.blocks;
    }
        
    /**
     * Get parent directory 
     * @return parent INode
     */
    INode getParent() {
      return this.parent;
    }

    /**
     * Get last modification time of inode.
     * @return access time
     */
    long getModificationTime() {
      return this.modificationTime;
    }

    /**
     * Set last modification time of inode.
     */
    void setModificationTime(long modtime) {
      assert isDir();
      if (this.modificationTime <= modtime) {
        this.modificationTime = modtime;
      }
    }

    /**
     * Get children iterator
     * @return Iterator of children
     */
    Iterator<INode> getChildIterator() {
      return (children != null) ?  children.values().iterator() : null;
      // instead of null, we could return a static empty iterator.
    }
        
    void addChild(String name, INode node) {
      if (children == null) {
        children = new TreeMap<String, INode>();
      }
      children.put(name, node);
    }

    /**
     * This is the external interface
     */
    INode getNode(String target) {
      if (target == null || 
          !target.startsWith("/") || target.length() == 0) {
        return null;
      } else if (parent == null && "/".equals(target)) {
        return this;
      } else {
        Vector<String> components = new Vector<String>();
        int start = 0;
        int slashid = 0;
        while (start < target.length() && (slashid = target.indexOf('/', start)) >= 0) {
          components.add(target.substring(start, slashid));
          start = slashid + 1;
        }
        if (start < target.length()) {
          components.add(target.substring(start));
        }
        return getNode(components, 0);
      }
    }

    /**
     */
    INode getNode(Vector<String> components, int index) {
      if (!name.equals(components.elementAt(index))) {
        return null;
      }
      if (index == components.size()-1) {
        return this;
      }

      // Check with children
      INode child = this.getChild(components.elementAt(index+1));
      if (child == null) {
        return null;
      } else {
        return child.getNode(components, index+1);
      }
    }
        
    INode getChild(String name) {
      return (children == null) ? null : children.get(name);
    }

    /**
     * Add new INode to the file tree.
     * Find the parent and insert 
     * 
     * @param path file path
     * @param newNode INode to be added
     * @return null if the node already exists; inserted INode, otherwise
     * @throws FileNotFoundException 
     */
    INode addNode(String path, INode newNode) throws FileNotFoundException {
      File target = new File(path);
      // find parent
      Path parent = new Path(path).getParent();
      if (parent == null) { // add root
        return null;
      }
      INode parentNode = getNode(parent.toString());
      if (parentNode == null) {
        throw new FileNotFoundException(
                                        "Parent path does not exist: "+path);
      }
      if (!parentNode.isDir()) {
        throw new FileNotFoundException(
                                        "Parent path is not a directory: "+path);
      }
      // check whether the parent already has a node with that name
      String name = newNode.name = target.getName();
      if (parentNode.getChild(name) != null) {
        return null;
      }
      // insert into the parent children list
      parentNode.addChild(name, newNode);
      newNode.parent = parentNode;
      return newNode;
    }

    /**
     */
    boolean removeNode() {
      if (parent == null) {
        return false;
      } else {
        parent.children.remove(name);
        return true;
      }
    }

    /**
     * Collect all the blocks at this INode and all its children.
     * This operation is performed after a node is removed from the tree,
     * and we want to GC all the blocks at this node and below.
     */
    void collectSubtreeBlocks(FSDirectory fsDir, Vector<Block> v) {
      if (blocks != null) {
        for (int i = 0; i < blocks.length; i++) {
          v.add(blocks[i]);
        }
      }
      fsDir.incrDeletedFileCount();
      for (Iterator<INode> it = getChildIterator(); it != null &&
             it.hasNext();) {
        it.next().collectSubtreeBlocks(fsDir, v);
      }
    }

    /**
     */
    int numItemsInTree() {
      int total = 0;
      for (Iterator<INode> it = getChildIterator(); it != null && 
             it.hasNext();) {
        total += it.next().numItemsInTree();
      }
      return total + 1;
    }

    /**
     */
    long computeFileLength() {
      long total = 0;
      if (blocks != null) {
        for (int i = 0; i < blocks.length; i++) {
          total += blocks[i].getNumBytes();
        }
      }
      return total;
    }

    /**
     */
    long computeContentsLength() {
      long total = computeFileLength();
      for (Iterator<INode> it = getChildIterator(); it != null && 
             it.hasNext();) {
        total += it.next().computeContentsLength();
      }
      return total;
    }

    /**
     * Get the block size of the first block
     * @return the number of bytes
     */
    public long getBlockSize() {
      if (blocks == null || blocks.length == 0) {
        return 0;
      } else {
        return blocks[0].getNumBytes();
      }
    }
        
    /**
     */
    void listContents(Vector<INode> v) {
      if (parent != null && blocks != null) {
        v.add(this);
      }

      for (Iterator<INode> it = getChildIterator(); it != null && 
             it.hasNext();) {
        v.add(it.next());
      }
    }
  }

  FSNamesystem namesystem = null;
  INode rootDir = new INode("");
  TreeMap<StringBytesWritable, TreeSet<StringBytesWritable>> activeLocks =
    new TreeMap<StringBytesWritable, TreeSet<StringBytesWritable>>();
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

  private void incrDeletedFileCount() {
    directoryMetrics.incrMetric("files_deleted", 1);
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
  public boolean addFile(String path, Block[] blocks, short replication) {
    waitForReady();

    // Always do an implicit mkdirs for parent directory tree.
    long modTime = FSNamesystem.now();
    if (!mkdirs(new Path(path).getParent().toString(), modTime)) {
      return false;
    }
    INode newNode = new INode(new File(path).getName(), blocks, 
                              replication, modTime);
    if (!unprotectedAddFile(path, newNode, modTime)) {
      NameNode.stateChangeLog.info("DIR* FSDirectory.addFile: "
                                   +"failed to add "+path+" with "
                                   +blocks.length+" blocks to the file system");
      return false;
    }
    // add create file record to log
    fsImage.getEditLog().logCreateFile(newNode);
    NameNode.stateChangeLog.debug("DIR* FSDirectory.addFile: "
                                  +path+" with "+blocks.length+" blocks is added to the file system");
    return true;
  }
    
  /**
   */
  private boolean unprotectedAddFile(String path, INode newNode, long parentModTime) {
    synchronized (rootDir) {
      try {
        if (rootDir.addNode(path, newNode) != null) {
          int nrBlocks = (newNode.blocks == null) ? 0 : newNode.blocks.length;
          // Add file->block mapping
          for (int i = 0; i < nrBlocks; i++) {
            namesystem.blocksMap.addINode(newNode.blocks[i], newNode);
          }
          // update modification time of parent directory
          newNode.getParent().setModificationTime(parentModTime);
          return true;
        } else {
          return false;
        }
      } catch (FileNotFoundException e) {
        return false;
      }
    }
  }
    
  boolean unprotectedAddFile(String path, Block[] blocks, short replication,
                             long modificationTime) {
    return unprotectedAddFile(path,  
                              new INode(path, blocks, replication,
                                        modificationTime),
                              modificationTime);
  }

  /**
   * Change the filename
   */
  public boolean renameTo(String src, String dst) {
    NameNode.stateChangeLog.debug("DIR* FSDirectory.renameTo: "
                                  +src+" to "+dst);
    waitForReady();
    long now = namesystem.now();
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
      INode oldParent = renamedNode.getParent();
      renamedNode.removeNode();
            
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
      INode fileNode = rootDir.getNode(src);
      if (fileNode == null)
        return null;
      if (fileNode.isDir())
        return null;
      oldReplication[0] = fileNode.blockReplication;
      fileNode.blockReplication = replication;
      fileBlocks = fileNode.blocks;
    }
    return fileBlocks;
  }

  /**
   * Get the blocksize of a file
   * @param filename the filename
   * @return the number of bytes in the first block
   * @throws IOException if it is a directory or does not exist.
   */
  public long getBlockSize(String filename) throws IOException {
    synchronized (rootDir) {
      INode fileNode = rootDir.getNode(filename);
      if (fileNode == null) {
        throw new IOException("Unknown file: " + filename);
      }
      if (fileNode.isDir()) {
        throw new IOException("Getting block size of a directory: " + 
                              filename);
      }
      return fileNode.getBlockSize();
    }
  }
    
  /**
   * Remove the file from management, return blocks
   */
  public Block[] delete(String src) {
    NameNode.stateChangeLog.debug("DIR* FSDirectory.delete: "
                                  +src);
    waitForReady();
    long now = namesystem.now();
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
          Vector<Block> v = new Vector<Block>();
          targetNode.collectSubtreeBlocks(this, v);
          for (Block b : v) {
            namesystem.blocksMap.removeINode(b);
          }
          return v.toArray(new Block[v.size()]);
        }
      }
    }
  }

  /**
   */
  public int obtainLock(String src, String holder, boolean exclusive) throws IOException {
    StringBytesWritable srcSBW = new StringBytesWritable(src);
    TreeSet<StringBytesWritable> holders = activeLocks.get(srcSBW);
    if (holders == null) {
      holders = new TreeSet<StringBytesWritable>();
      activeLocks.put(srcSBW, holders);
    }
    if (exclusive && holders.size() > 0) {
      return STILL_WAITING;
    } else {
      holders.add(new StringBytesWritable(holder));
      return COMPLETE_SUCCESS;
    }
  }

  /**
   */
  public int releaseLock(String src, String holder) throws IOException {
    StringBytesWritable srcSBW = new StringBytesWritable(src);
    StringBytesWritable holderSBW = new StringBytesWritable(holder);
    TreeSet<StringBytesWritable> holders = activeLocks.get(srcSBW);
    if (holders != null && holders.contains(holderSBW)) {
      holders.remove(holderSBW);
      if (holders.size() == 0) {
        activeLocks.remove(srcSBW);
      }
      return COMPLETE_SUCCESS;
    } else {
      return OPERATION_FAILED;
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
      if (targetNode == null) {
        return null;
      } else {
        Vector<INode> contents = new Vector<INode>();
        targetNode.listContents(contents);

        DFSFileInfo listing[] = new DFSFileInfo[contents.size()];
        int i = 0;
        for (Iterator<INode> it = contents.iterator(); it.hasNext(); i++) {
          listing[i] = new DFSFileInfo(it.next());
        }
        return listing;
      }
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
        return new DFSFileInfo(targetNode);
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
      if (targetNode == null) {
        return null;
      } else {
        return targetNode.blocks;
      }
    }
  }

  /**
   * Get {@link INode} associated with the file.
   */
  INode getFileINode(String src) {
    waitForReady();
    synchronized (rootDir) {
      return rootDir.getNode(src);
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
      return node != null && node.isDir();
    }
  }

  /**
   * Create directory entries for every item
   */
  boolean mkdirs(String src, long now) {
    src = normalizePath(src);

    // Use this to collect all the dirs we need to construct
    Vector<String> v = new Vector<String>();

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
      String cur = v.elementAt(i);
      try {
        INode inserted = unprotectedMkdir(cur, now);
        if (inserted != null) {
          NameNode.stateChangeLog.debug("DIR* FSDirectory.mkdirs: "
                                        +"created directory "+cur);
          fsImage.getEditLog().logMkDir(inserted);
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
      return rootDir.addNode(src, new INode(new File(src).getName(),
                                            timestamp));
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
