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

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Arrays;
import java.util.List;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.dfs.BlocksMap.BlockInfo;

/**
 * We keep an in-memory representation of the file/block hierarchy.
 * This is a base INode class containing common fields for file and 
 * directory inodes.
 */
abstract class INode implements Comparable<byte[]> {
  protected byte[] name;
  protected INodeDirectory parent;
  protected long modificationTime;

  protected INode(String name) {
    this(0L);
    setLocalName(name);
  }

  INode(long mTime) {
    this.name = null;
    this.parent = null;
    this.modificationTime = mTime;
  }

  /**
   * Check whether it's a directory
   */
  abstract boolean isDirectory();
  abstract int collectSubtreeBlocks(List<Block> v);
  abstract long computeContentsLength();

  /**
   * Get local file name
   * @return local file name
   */
  String getLocalName() {
    return bytes2String(name);
  }

  /**
   * Set local file name
   */
  void setLocalName(String name) {
    this.name = string2Bytes(name);
  }

  /**
   * Get the full absolute path name of this file (recursively computed).
   * 
   * @return the string representation of the absolute path of this file
   * 
   * @deprecated this is used only in crc upgrade now, and should be removed 
   * in order to be able to eliminate the parent field. 
   */
  String getAbsoluteName() {
    if (this.parent == null) {
      return Path.SEPARATOR;       // root directory is "/"
    }
    if (this.parent.parent == null) {
      return Path.SEPARATOR + getLocalName();
    }
    return parent.getAbsoluteName() + Path.SEPARATOR + getLocalName();
  }

  /**
   * Get parent directory 
   * @return parent INode
   */
  INodeDirectory getParent() {
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
    assert isDirectory();
    if (this.modificationTime <= modtime) {
      this.modificationTime = modtime;
    }
  }

  /**
   * Is this inode being constructed?
   */
  boolean isUnderConstruction() {
    return false;
  }

  /**
   * Breaks file path into components.
   * @param path
   * @return array of byte arrays each of which represents 
   * a single path component.
   */
  static byte[][] getPathComponents(String path) {
    if (path == null || !path.startsWith(Path.SEPARATOR)) {
      return null;
    }
    if (Path.SEPARATOR.equals(path))  // root
      return new byte[][]{null};
    String[] strings = path.split(Path.SEPARATOR, -1);
    int size = strings.length;
    byte[][] bytes = new byte[size][];
    for (int i = 0; i < size; i++)
      bytes[i] = string2Bytes(strings[i]);
    return bytes;
  }

  /**
   */
  boolean removeNode() {
    if (parent == null) {
      return false;
    } else {
      
      parent.removeChild(this);
      return true;
    }
  }

  //
  // Comparable interface
  //
  public int compareTo(byte[] o) {
    return compareBytes(name, o);
  }

  public boolean equals(Object o) {
    if (!(o instanceof INode)) {
      return false;
    }
    return Arrays.equals(this.name, ((INode)o).name);
  }

  public int hashCode() {
    return Arrays.hashCode(this.name);
  }

  //
  // static methods
  //
  /**
   * Compare two byte arrays.
   * 
   * @return a negative integer, zero, or a positive integer 
   * as defined by {@link #compareTo(byte[])}.
   */
  static int compareBytes(byte[] a1, byte[] a2) {
    if (a1==a2)
        return 0;
    int len1 = (a1==null ? 0 : a1.length);
    int len2 = (a2==null ? 0 : a2.length);
    int n = Math.min(len1, len2);
    byte b1, b2;
    for (int i=0; i<n; i++) {
      b1 = a1[i];
      b2 = a2[i];
      if (b1 != b2)
        return b1 - b2;
    }
    return len1 - len2;
  }

  /**
   * Converts a byte array to a string using UTF8 encoding.
   */
  static String bytes2String(byte[] bytes) {
    try {
      return new String(bytes, "UTF8");
    } catch(UnsupportedEncodingException e) {
      assert false : "UTF8 encoding is not supported ";
    }
    return null;
  }

  /**
   * Converts a string to a byte array using UTF8 encoding.
   */
  static byte[] string2Bytes(String str) {
    try {
      return str.getBytes("UTF8");
    } catch(UnsupportedEncodingException e) {
      assert false : "UTF8 encoding is not supported ";
    }
    return null;
  }
}

/**
 * Directory INode class.
 */
class INodeDirectory extends INode {
  protected static final int DEFAULT_FILES_PER_DIRECTORY = 5;
  final static String ROOT_NAME = "";

  private List<INode> children;

  INodeDirectory(String name) {
    super(name);
    this.children = null;
  }

  INodeDirectory(long mTime) {
    super(mTime);
    this.children = null;
  }

  /**
   * Check whether it's a directory
   */
  boolean isDirectory() {
    return true;
  }

  void removeChild(INode node) {
    assert children != null;
    int low = Collections.binarySearch(children, node.name);
    if (low >= 0) {
      children.remove(low);
    }
  }

  INode getChild(String name) {
    return getChildINode(string2Bytes(name));
  }

  private INode getChildINode(byte[] name) {
    if (children == null) {
      return null;
    }
    int low = Collections.binarySearch(children, name);
    if (low >= 0) {
      return children.get(low);
    }
    return null;
  }

  /**
   */
  private INode getNode(byte[][] components) {
    INode[] inode  = new INode[1];
    getExistingPathINodes(components, inode);
    return inode[0];
  }

  /**
   * This is the external interface
   */
  INode getNode(String path) {
    return getNode(getPathComponents(path));
  }

  /**
   * Retrieve existing INodes from a path. If existing is big enough to store
   * all path components (existing and non-existing), then existing INodes
   * will be stored starting from the root INode into existing[0]; if
   * existing is not big enough to store all path components, then only the
   * last existing and non existing INodes will be stored so that
   * existing[existing.length-1] refers to the target INode.
   * 
   * <p>
   * Example: <br>
   * Given the path /c1/c2/c3 where only /c1/c2 exists, resulting in the
   * following path components: ["","c1","c2","c3"],
   * 
   * <p>
   * <code>getExistingPathINodes(["","c1","c2"], [?])</code> should fill the
   * array with [c2] <br>
   * <code>getExistingPathINodes(["","c1","c2","c3"], [?])</code> should fill the
   * array with [null]
   * 
   * <p>
   * <code>getExistingPathINodes(["","c1","c2"], [?,?])</code> should fill the
   * array with [c1,c2] <br>
   * <code>getExistingPathINodes(["","c1","c2","c3"], [?,?])</code> should fill
   * the array with [c2,null]
   * 
   * <p>
   * <code>getExistingPathINodes(["","c1","c2"], [?,?,?,?])</code> should fill
   * the array with [rootINode,c1,c2,null], <br>
   * <code>getExistingPathINodes(["","c1","c2","c3"], [?,?,?,?])</code> should
   * fill the array with [rootINode,c1,c2,null]
   * @param components array of path component name
   * @param existing INode array to fill with existing INodes
   * @return number of existing INodes in the path
   */
  private int getExistingPathINodes(byte[][] components, INode[] existing) {
    assert compareBytes(this.name, components[0]) == 0 :
      "Incorrect name " + getLocalName() + " expected " + components[0];

    INode curNode = this;
    int count = 0;
    int index = existing.length - components.length;
    if (index > 0)
      index = 0;
    while ((count < components.length) && (curNode != null)) {
      if (index >= 0)
        existing[index] = curNode;
      if (!curNode.isDirectory() || (count == components.length - 1))
        break; // no more child, stop here
      INodeDirectory parentDir = (INodeDirectory)curNode;
      curNode = parentDir.getChildINode(components[count + 1]);
      count += 1;
      index += 1;
    }
    return count;
  }

  /**
   * Retrieve the existing INodes along the given path. The first INode
   * always exist and is this INode.
   * 
   * @param path the path to explore
   * @return INodes array containing the existing INodes in the order they
   *         appear when following the path from the root INode to the
   *         deepest INodes. The array size will be the number of expected
   *         components in the path, and non existing components will be
   *         filled with null
   */
  INode[] getExistingPathINodes(String path) {
    byte[][] components = getPathComponents(path);
    INode[] inodes = new INode[components.length];

    this.getExistingPathINodes(components, inodes);
    
    return inodes;
  }

  /**
   * Add a child inode to the directory.
   * 
   * @param node INode to insert
   * @return  null if the child with this name already exists; 
   *          inserted INode, otherwise
   */
  private <T extends INode> T addChild(T node) {
    if (children == null) {
      children = new ArrayList<INode>(DEFAULT_FILES_PER_DIRECTORY);
    }
    int low = Collections.binarySearch(children, node.name);
    if(low >= 0)
      return null;
    node.parent = this;
    children.add(-low - 1, node);
    // update modification time of the parent directory
    setModificationTime(node.getModificationTime());
    return node;
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
  <T extends INode> T addNode(String path, T newNode) throws FileNotFoundException {
    byte[][] pathComponents = getPathComponents(path);
    assert pathComponents != null : "Incorrect path " + path;
    int pathLen = pathComponents.length;
    if (pathLen < 2)  // add root
      return null;
    // Gets the parent INode
    INode[] inode  = new INode[2];
    getExistingPathINodes(pathComponents, inode);
    INode node = inode[0];
    if (node == null) {
      throw new FileNotFoundException("Parent path does not exist: "+path);
    }
    if (!node.isDirectory()) {
      throw new FileNotFoundException("Parent path is not a directory: "+path);
    }
    INodeDirectory parentNode = (INodeDirectory)node;
    // insert into the parent children list
    newNode.name = pathComponents[pathLen-1];
    return parentNode.addChild(newNode);
  }

  /**
   */
  int numItemsInTree() {
    int total = 1;
    if (children == null) {
      return total;
    }
    for (INode child : children) {
      if(!child.isDirectory())
        total++;
      else
        total += ((INodeDirectory)child).numItemsInTree();
    }
    return total;
  }

  /**
   */
  long computeContentsLength() {
    long total = 0;
    if (children == null) {
      return total;
    }
    for (INode child : children) {
      total += child.computeContentsLength();
    }
    return total;
  }

  /**
   */
  List<INode> getChildren() {
    return children==null ? new ArrayList<INode>() : children;
  }

  /**
   * Collect all the blocks in all children of this INode.
   * Count and return the number of files in the sub tree.
   */
  int collectSubtreeBlocks(List<Block> v) {
    int total = 1;
    if (children == null) {
      return total;
    }
    for (INode child : children) {
      total += child.collectSubtreeBlocks(v);
    }
    return total;
  }
}

class INodeFile extends INode {
  private BlockInfo blocks[] = null;
  protected short blockReplication;
  protected long preferredBlockSize;

  /**
   */
  INodeFile(int nrBlocks, short replication, long modificationTime,
            long preferredBlockSize) {
    super(modificationTime);
    this.blockReplication = replication;
    this.preferredBlockSize = preferredBlockSize;
    allocateBlocks(nrBlocks);
  }

  protected INodeFile(BlockInfo[] blklist, short replication, long modificationTime,
                      long preferredBlockSize) {
    super(modificationTime);
    this.blockReplication = replication;
    this.preferredBlockSize = preferredBlockSize;
    blocks = blklist;
  }

  boolean isDirectory() {
    return false;
  }

  /**
   * Get block replication for the file 
   * @return block replication
   */
  short getReplication() {
    return this.blockReplication;
  }

  void setReplication(short replication) {
    this.blockReplication = replication;
  }

  /**
   * Get file blocks 
   * @return file blocks
   */
  BlockInfo[] getBlocks() {
    return this.blocks;
  }

  /**
   * Allocate space for blocks.
   * @param nrBlocks number of blocks
   */
  void allocateBlocks(int nrBlocks) {
    this.blocks = new BlockInfo[nrBlocks];
  }

  /**
   * add a block to the block list
   */
  void addBlock(BlockInfo newblock) {
    if (this.blocks == null) {
      this.blocks = new BlockInfo[1];
      this.blocks[0] = newblock;
    } else {
      int size = this.blocks.length;
      BlockInfo[] newlist = new BlockInfo[size + 1];
      for (int i = 0; i < size; i++) {
        newlist[i] = this.blocks[i];
      }
      newlist[size] = newblock;
      this.blocks = newlist;
    }
  }

  /**
   * remove a block from the block list. This block should be
   * the last one on the list.
   */
  void removeBlock(Block oldblock) throws IOException {
    if (this.blocks == null) {
      throw new IOException("Trying to delete non-existant block " +
                            oldblock);
    }
    int size = this.blocks.length;
    if (!this.blocks[size-1].equals(oldblock)) {
      throw new IOException("Trying to delete non-existant block " +
                            oldblock);
    }
    BlockInfo[] newlist = new BlockInfo[size - 1];
    for (int i = 0; i < size-1; i++) {
        newlist[i] = this.blocks[i];
    }
    this.blocks = newlist;
  }

  /**
   * Set file block
   */
  void setBlock(int idx, BlockInfo blk) {
    this.blocks[idx] = blk;
  }

  /**
   * Collect all the blocks in this INode.
   * Return the number of files in the sub tree.
   */
  int collectSubtreeBlocks(List<Block> v) {
    for (Block blk : blocks) {
      v.add(blk);
    }
    return 1;
  }

  long computeContentsLength() {
    long total = 0;
    for (Block blk : blocks) {
      total += blk.getNumBytes();
    }
    return total;
  }

  /**
   * Get the preferred block size of the file.
   * @return the number of bytes
   */
  long getPreferredBlockSize() {
    return preferredBlockSize;
  }

  /**
   * Return the penultimate allocated block for this file.
   */
  Block getPenultimateBlock() {
    if (blocks == null || blocks.length <= 1) {
      return null;
    }
    return blocks[blocks.length - 2];
  }
}

class INodeFileUnderConstruction extends INodeFile {
  protected StringBytesWritable clientName;         // lease holder
  protected StringBytesWritable clientMachine;
  protected DatanodeDescriptor clientNode; // if client is a cluster node too.

  INodeFileUnderConstruction(short replication,
                             long preferredBlockSize,
                             long modTime,
                             String clientName,
                             String clientMachine,
                             DatanodeDescriptor clientNode) 
                             throws IOException {
    super(0, replication, modTime, preferredBlockSize);
    this.clientName = new StringBytesWritable(clientName);
    this.clientMachine = new StringBytesWritable(clientMachine);
    this.clientNode = clientNode;
  }

  String getClientName() throws IOException {
    return clientName.getString();
  }

  String getClientMachine() throws IOException {
    return clientMachine.getString();
  }

  DatanodeDescriptor getClientNode() {
    return clientNode;
  }

  /**
   * Is this inode being constructed?
   */
  @Override
  boolean isUnderConstruction() {
    return true;
  }

  //
  // converts a INodeFileUnderConstruction into a INodeFile
  //
  INodeFile convertToInodeFile() {
    INodeFile obj = new INodeFile(getBlocks(),
                                  getReplication(),
                                  getModificationTime(),
                                  getPreferredBlockSize());
    return obj;
    
  }
}
