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

import org.apache.hadoop.fs.Path;

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
    return getINode(components, components.length-1);
  }

  /**
   * Find INode in the directory tree.
   * 
   * @param components array of path name components
   * @param end the end component of the path
   * @return found INode or null otherwise 
   */
  private INode getINode(byte[][] components, int end) {
    assert compareBytes(this.name, components[0]) == 0 :
      "Incorrect name " + getLocalName() + " expected " + components[0];
    if (end >= components.length)
      end = components.length-1;
    if (end < 0)
      return null;
    INode curNode = this;
    for(int start = 0; start < end; start++) {
      if(!curNode.isDirectory())  // file is not expected here
        return null;        // because there is more components in the path
      INodeDirectory parentDir = (INodeDirectory)curNode;
      curNode = parentDir.getChildINode(components[start+1]);
      if(curNode == null)  // not found
        return null;
    }
    return curNode;
  }

  /**
   * This is the external interface
   */
  INode getNode(String path) {
    return getNode(getPathComponents(path));
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
    INode node = getINode(pathComponents, pathLen-2);
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
  private Block blocks[] = null;
  protected short blockReplication;

  /**
   */
  INodeFile(Block blocks[], short replication, long modificationTime) {
    super(modificationTime);
    this.blocks = blocks;
    this.blockReplication = replication;
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
  Block[] getBlocks() {
    return this.blocks;
  }

  /**
   * Set file blocks 
   */
  void setBlocks(Block[] blockList) {
    this.blocks = blockList;
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
   * Get the block size of the first block
   * @return the number of bytes
   */
  long getBlockSize() {
    if (blocks == null || blocks.length == 0) {
      return 0;
    } else {
      return blocks[0].getNumBytes();
    }
  }
}
