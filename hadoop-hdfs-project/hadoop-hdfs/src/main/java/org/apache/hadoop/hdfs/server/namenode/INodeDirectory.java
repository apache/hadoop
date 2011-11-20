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

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;

/**
 * Directory INode class.
 */
class INodeDirectory extends INode {
  protected static final int DEFAULT_FILES_PER_DIRECTORY = 5;
  final static String ROOT_NAME = "";

  private List<INode> children;

  INodeDirectory(String name, PermissionStatus permissions) {
    super(name, permissions);
    this.children = null;
  }

  public INodeDirectory(PermissionStatus permissions, long mTime) {
    super(permissions, mTime, 0);
    this.children = null;
  }

  /** constructor */
  INodeDirectory(byte[] localName, PermissionStatus permissions, long mTime) {
    this(permissions, mTime);
    this.name = localName;
  }
  
  /** copy constructor
   * 
   * @param other
   */
  INodeDirectory(INodeDirectory other) {
    super(other);
    this.children = other.getChildren();
  }
  
  /**
   * Check whether it's a directory
   */
  public boolean isDirectory() {
    return true;
  }

  INode removeChild(INode node) {
    assert children != null;
    int low = Collections.binarySearch(children, node.name);
    if (low >= 0) {
      return children.remove(low);
    } else {
      return null;
    }
  }

  /** Replace a child that has the same name as newChild by newChild.
   * 
   * @param newChild Child node to be added
   */
  void replaceChild(INode newChild) {
    if ( children == null ) {
      throw new IllegalArgumentException("The directory is empty");
    }
    int low = Collections.binarySearch(children, newChild.name);
    if (low>=0) { // an old child exists so replace by the newChild
      children.set(low, newChild);
    } else {
      throw new IllegalArgumentException("No child exists to be replaced");
    }
  }
  
  INode getChild(String name) {
    return getChildINode(DFSUtil.string2Bytes(name));
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
   * Return the INode of the last component in components, or null if the last
   * component does not exist.
   */
  private INode getNode(byte[][] components, boolean resolveLink) 
    throws UnresolvedLinkException {
    INode[] inode  = new INode[1];
    getExistingPathINodes(components, inode, resolveLink);
    return inode[0];
  }

  /**
   * This is the external interface
   */
  INode getNode(String path, boolean resolveLink) 
    throws UnresolvedLinkException {
    return getNode(getPathComponents(path), resolveLink);
  }

  /**
   * Retrieve existing INodes from a path. If existing is big enough to store
   * all path components (existing and non-existing), then existing INodes
   * will be stored starting from the root INode into existing[0]; if
   * existing is not big enough to store all path components, then only the
   * last existing and non existing INodes will be stored so that
   * existing[existing.length-1] refers to the INode of the final component.
   * 
   * An UnresolvedPathException is always thrown when an intermediate path 
   * component refers to a symbolic link. If the final path component refers 
   * to a symbolic link then an UnresolvedPathException is only thrown if
   * resolveLink is true.  
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
   * 
   * @param components array of path component name
   * @param existing array to fill with existing INodes
   * @param resolveLink indicates whether UnresolvedLinkException should
   *        be thrown when the path refers to a symbolic link.
   * @return number of existing INodes in the path
   */
  int getExistingPathINodes(byte[][] components, INode[] existing, 
      boolean resolveLink) throws UnresolvedLinkException {
    assert compareBytes(this.name, components[0]) == 0 :
      "Incorrect name " + getLocalName() + " expected " + 
      DFSUtil.bytes2String(components[0]);

    INode curNode = this;
    int count = 0;
    int index = existing.length - components.length;
    if (index > 0) {
      index = 0;
    }
    while (count < components.length && curNode != null) {
      final boolean lastComp = (count == components.length - 1);      
      if (index >= 0) {
        existing[index] = curNode;
      }
      if (curNode.isLink() && (!lastComp || (lastComp && resolveLink))) {
        if(NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug("UnresolvedPathException " +
              " count: " + count +
              " componenent: " + DFSUtil.bytes2String(components[count]) +
              " full path: " + constructPath(components, 0) +
              " remaining path: " + constructPath(components, count+1) +
              " symlink: " + ((INodeSymlink)curNode).getLinkValue());
        }
        final String linkTarget = ((INodeSymlink)curNode).getLinkValue();
        throw new UnresolvedPathException(constructPath(components, 0),
                                          constructPath(components, count+1),
                                          linkTarget);
      }
      if (lastComp || !curNode.isDirectory()) {
        break;
      }
      INodeDirectory parentDir = (INodeDirectory)curNode;
      curNode = parentDir.getChildINode(components[count + 1]);
      count++;
      index++;
    }
    return count;
  }

  /**
   * Retrieve the existing INodes along the given path. The first INode
   * always exist and is this INode.
   * 
   * @param path the path to explore
   * @param resolveLink indicates whether UnresolvedLinkException should 
   *        be thrown when the path refers to a symbolic link.
   * @return INodes array containing the existing INodes in the order they
   *         appear when following the path from the root INode to the
   *         deepest INodes. The array size will be the number of expected
   *         components in the path, and non existing components will be
   *         filled with null
   *         
   * @see #getExistingPathINodes(byte[][], INode[])
   */
  INode[] getExistingPathINodes(String path, boolean resolveLink) 
    throws UnresolvedLinkException {
    byte[][] components = getPathComponents(path);
    INode[] inodes = new INode[components.length];

    this.getExistingPathINodes(components, inodes, resolveLink);
    
    return inodes;
  }

  /**
   * Given a child's name, return the index of the next child
   *
   * @param name a child's name
   * @return the index of the next child
   */
  int nextChild(byte[] name) {
    if (name.length == 0) { // empty name
      return 0;
    }
    int nextPos = Collections.binarySearch(children, name) + 1;
    if (nextPos >= 0) {
      return nextPos;
    }
    return -nextPos;
  }

  /**
   * Add a child inode to the directory.
   * 
   * @param node INode to insert
   * @param setModTime set modification time for the parent node
   *                   not needed when replaying the addition and 
   *                   the parent already has the proper mod time
   * @return  null if the child with this name already exists; 
   *          node, otherwise
   */
  <T extends INode> T addChild(final T node, boolean setModTime) {
    if (children == null) {
      children = new ArrayList<INode>(DEFAULT_FILES_PER_DIRECTORY);
    }
    int low = Collections.binarySearch(children, node.name);
    if(low >= 0)
      return null;
    node.parent = this;
    children.add(-low - 1, node);
    // update modification time of the parent directory
    if (setModTime)
      setModificationTime(node.getModificationTime());
    if (node.getGroupName() == null) {
      node.setGroup(getGroupName());
    }
    return node;
  }

  /**
   * Add new INode to the file tree.
   * Find the parent and insert 
   * 
   * @param path file path
   * @param newNode INode to be added
   * @return null if the node already exists; inserted INode, otherwise
   * @throws FileNotFoundException if parent does not exist or 
   * @throws UnresolvedLinkException if any path component is a symbolic link
   * is not a directory.
   */
  <T extends INode> T addNode(String path, T newNode
      ) throws FileNotFoundException, UnresolvedLinkException  {
    byte[][] pathComponents = getPathComponents(path);        
    if(addToParent(pathComponents, newNode,
                    true) == null)
      return null;
    return newNode;
  }

  /**
   * Add new inode to the parent if specified.
   * Optimized version of addNode() if parent is not null.
   * 
   * @return  parent INode if new inode is inserted
   *          or null if it already exists.
   * @throws  FileNotFoundException if parent does not exist or 
   *          is not a directory.
   */
  INodeDirectory addToParent( byte[] localname,
                              INode newNode,
                              INodeDirectory parent,
                              boolean propagateModTime
                              ) throws FileNotFoundException, 
                                       UnresolvedLinkException {
    // insert into the parent children list
    newNode.name = localname;
    if(parent.addChild(newNode, propagateModTime) == null)
      return null;
    return parent;
  }

  INodeDirectory getParent(byte[][] pathComponents)
  throws FileNotFoundException, UnresolvedLinkException {
    int pathLen = pathComponents.length;
    if (pathLen < 2)  // add root
      return null;
    // Gets the parent INode
    INode[] inodes  = new INode[2];
    getExistingPathINodes(pathComponents, inodes, false);
    INode inode = inodes[0];
    if (inode == null) {
      throw new FileNotFoundException("Parent path does not exist: "+
          DFSUtil.byteArray2String(pathComponents));
    }
    if (!inode.isDirectory()) {
      throw new FileNotFoundException("Parent path is not a directory: "+
          DFSUtil.byteArray2String(pathComponents));
    }
    return (INodeDirectory)inode;
  }
  
  /**
   * Add new inode 
   * Optimized version of addNode()
   * 
   * @return  parent INode if new inode is inserted
   *          or null if it already exists.
   * @throws  FileNotFoundException if parent does not exist or 
   *          is not a directory.
   */
  INodeDirectory addToParent( byte[][] pathComponents,
                              INode newNode,
                              boolean propagateModTime
                            ) throws FileNotFoundException, 
                                     UnresolvedLinkException {
              
    int pathLen = pathComponents.length;
    if (pathLen < 2)  // add root
      return null;
    newNode.name = pathComponents[pathLen-1];
    // insert into the parent children list
    INodeDirectory parent = getParent(pathComponents);
    if(parent.addChild(newNode, propagateModTime) == null)
      return null;
    return parent;
  }

  /** {@inheritDoc} */
  DirCounts spaceConsumedInTree(DirCounts counts) {
    counts.nsCount += 1;
    if (children != null) {
      for (INode child : children) {
        child.spaceConsumedInTree(counts);
      }
    }
    return counts;    
  }

  /** {@inheritDoc} */
  long[] computeContentSummary(long[] summary) {
    // Walk through the children of this node, using a new summary array
    // for the (sub)tree rooted at this node
    assert 4 == summary.length;
    long[] subtreeSummary = new long[]{0,0,0,0};
    if (children != null) {
      for (INode child : children) {
        child.computeContentSummary(subtreeSummary);
      }
    }
    if (this instanceof INodeDirectoryWithQuota) {
      // Warn if the cached and computed diskspace values differ
      INodeDirectoryWithQuota node = (INodeDirectoryWithQuota)this;
      long space = node.diskspaceConsumed();
      assert -1 == node.getDsQuota() || space == subtreeSummary[3];
      if (-1 != node.getDsQuota() && space != subtreeSummary[3]) {
        NameNode.LOG.warn("Inconsistent diskspace for directory "
            +getLocalName()+". Cached: "+space+" Computed: "+subtreeSummary[3]);
      }
    }

    // update the passed summary array with the values for this node's subtree
    for (int i = 0; i < summary.length; i++) {
      summary[i] += subtreeSummary[i];
    }

    summary[2]++;
    return summary;
  }

  /**
   */
  List<INode> getChildren() {
    return children==null ? new ArrayList<INode>() : children;
  }
  List<INode> getChildrenRaw() {
    return children;
  }

  int collectSubtreeBlocksAndClear(List<Block> v) {
    int total = 1;
    if (children == null) {
      return total;
    }
    for (INode child : children) {
      total += child.collectSubtreeBlocksAndClear(v);
    }
    parent = null;
    children = null;
    return total;
  }
}
