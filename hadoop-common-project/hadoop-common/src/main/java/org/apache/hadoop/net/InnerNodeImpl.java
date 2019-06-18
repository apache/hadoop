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
package org.apache.hadoop.net;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** InnerNode represents a switch/router of a data center or rack.
 * Different from a leaf node, it has non-null children.
 */
public class InnerNodeImpl extends NodeBase implements InnerNode {
  protected static class Factory implements InnerNode.Factory<InnerNodeImpl> {
    protected Factory() {}

    @Override
    public InnerNodeImpl newInnerNode(String path) {
      return new InnerNodeImpl(path);
    }
  }

  static final Factory FACTORY = new Factory();

  protected final List<Node> children = new ArrayList<>();
  protected final Map<String, Node> childrenMap = new HashMap<>();
  protected int numOfLeaves;

  /** Construct an InnerNode from a path-like string. */
  protected InnerNodeImpl(String path) {
    super(path);
  }

  /** Construct an InnerNode
   * from its name, its network location, its parent, and its level. */
  protected InnerNodeImpl(String name, String location,
      InnerNode parent, int level) {
    super(name, location, parent, level);
  }

  @Override
  public List<Node> getChildren() {
    return children;
  }

  /** @return the number of children this node has. */
  int getNumOfChildren() {
    return children.size();
  }

  /** Judge if this node represents a rack.
   * @return true if it has no child or its children are not InnerNodes
   */
  public boolean isRack() {
    if (children.isEmpty()) {
      return true;
    }

    Node firstChild = children.get(0);
    if (firstChild instanceof InnerNode) {
      return false;
    }

    return true;
  }

  /** Judge if this node is an ancestor of node <i>n</i>.
   *
   * @param n a node
   * @return true if this node is an ancestor of <i>n</i>
   */
  public boolean isAncestor(Node n) {
    return getPath(this).equals(NodeBase.PATH_SEPARATOR_STR) ||
      (n.getNetworkLocation()+NodeBase.PATH_SEPARATOR_STR).
      startsWith(getPath(this)+NodeBase.PATH_SEPARATOR_STR);
  }

  /** Judge if this node is the parent of node <i>n</i>.
   *
   * @param n a node
   * @return true if this node is the parent of <i>n</i>
   */
  public boolean isParent(Node n) {
    return n.getNetworkLocation().equals(getPath(this));
  }

  /* Return a child name of this node who is an ancestor of node <i>n</i> */
  public String getNextAncestorName(Node n) {
    if (!isAncestor(n)) {
      throw new IllegalArgumentException(
                                         this + "is not an ancestor of " + n);
    }
    String name = n.getNetworkLocation().substring(getPath(this).length());
    if (name.charAt(0) == PATH_SEPARATOR) {
      name = name.substring(1);
    }
    int index=name.indexOf(PATH_SEPARATOR);
    if (index != -1) {
      name = name.substring(0, index);
    }
    return name;
  }

  @Override
  public boolean add(Node n) {
    if (!isAncestor(n)) {
      throw new IllegalArgumentException(n.getName()
          + ", which is located at " + n.getNetworkLocation()
          + ", is not a descendant of " + getPath(this));
    }
    if (isParent(n)) {
      // this node is the parent of n; add n directly
      n.setParent(this);
      n.setLevel(this.level+1);
      Node prev = childrenMap.put(n.getName(), n);
      if (prev != null) {
        for(int i=0; i<children.size(); i++) {
          if (children.get(i).getName().equals(n.getName())) {
            children.set(i, n);
            return false;
          }
        }
      }
      children.add(n);
      numOfLeaves++;
      return true;
    } else {
      // find the next ancestor node
      String parentName = getNextAncestorName(n);
      InnerNode parentNode = (InnerNode)childrenMap.get(parentName);
      if (parentNode == null) {
        // create a new InnerNode
        parentNode = createParentNode(parentName);
        children.add(parentNode);
        childrenMap.put(parentNode.getName(), parentNode);
      }
      // add n to the subtree of the next ancestor node
      if (parentNode.add(n)) {
        numOfLeaves++;
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * Creates a parent node to be added to the list of children.
   * Creates a node using the InnerNode four argument constructor specifying
   * the name, location, parent, and level of this node.
   *
   * <p>To be overridden in subclasses for specific InnerNode implementations,
   * as alternative to overriding the full {@link #add(Node)} method.
   *
   * @param parentName The name of the parent node
   * @return A new inner node
   * @see InnerNodeImpl(String, String, InnerNode, int)
   */
  private InnerNodeImpl createParentNode(String parentName) {
    return new InnerNodeImpl(parentName,
        getPath(this), this, this.getLevel() + 1);
  }

  @Override
  public boolean remove(Node n) {
    if (!isAncestor(n)) {
      throw new IllegalArgumentException(n.getName()
          + ", which is located at " + n.getNetworkLocation()
          + ", is not a descendant of " + getPath(this));
    }
    if (isParent(n)) {
      // this node is the parent of n; remove n directly
      if (childrenMap.containsKey(n.getName())) {
        for (int i=0; i<children.size(); i++) {
          if (children.get(i).getName().equals(n.getName())) {
            children.remove(i);
            childrenMap.remove(n.getName());
            numOfLeaves--;
            n.setParent(null);
            return true;
          }
        }
      }
      return false;
    } else {
      // find the next ancestor node: the parent node
      String parentName = getNextAncestorName(n);
      InnerNodeImpl parentNode = (InnerNodeImpl)childrenMap.get(parentName);
      if (parentNode == null) {
        return false;
      }
      // remove n from the parent node
      boolean isRemoved = parentNode.remove(n);
      // if the parent node has no children, remove the parent node too
      if (isRemoved) {
        if (parentNode.getNumOfChildren() == 0) {
          for(int i=0; i < children.size(); i++) {
            if (children.get(i).getName().equals(parentName)) {
              children.remove(i);
              childrenMap.remove(parentName);
              break;
            }
          }
        }
        numOfLeaves--;
      }
      return isRemoved;
    }
  } // end of remove

  @Override
  public Node getLoc(String loc) {
    if (loc == null || loc.length() == 0) {
      return this;
    }

    String[] path = loc.split(PATH_SEPARATOR_STR, 2);
    Node childNode = childrenMap.get(path[0]);
    if (childNode == null || path.length == 1) {
      return childNode;
    } else if (childNode instanceof InnerNode) {
      return ((InnerNode)childNode).getLoc(path[1]);
    } else {
      return null;
    }
  }

  @Override
  public Node getLeaf(int leafIndex, Node excludedNode) {
    int count=0;
    // check if the excluded node a leaf
    boolean isLeaf = !(excludedNode instanceof InnerNode);
    // calculate the total number of excluded leaf nodes
    int numOfExcludedLeaves =
        isLeaf ? 1 : ((InnerNode)excludedNode).getNumOfLeaves();
    if (isLeafParent()) { // children are leaves
      if (isLeaf) { // excluded node is a leaf node
        if (excludedNode != null &&
            childrenMap.containsKey(excludedNode.getName())) {
          int excludedIndex = children.indexOf(excludedNode);
          if (excludedIndex != -1 && leafIndex >= 0) {
            // excluded node is one of the children so adjust the leaf index
            leafIndex = leafIndex>=excludedIndex ? leafIndex+1 : leafIndex;
          }
        }
      }
      // range check
      if (leafIndex<0 || leafIndex>=this.getNumOfChildren()) {
        return null;
      }
      return children.get(leafIndex);
    } else {
      for(int i=0; i<children.size(); i++) {
        InnerNodeImpl child = (InnerNodeImpl)children.get(i);
        if (excludedNode == null || excludedNode != child) {
          // not the excludedNode
          int numOfLeaves = child.getNumOfLeaves();
          if (excludedNode != null && child.isAncestor(excludedNode)) {
            numOfLeaves -= numOfExcludedLeaves;
          }
          if (count+numOfLeaves > leafIndex) {
            // the leaf is in the child subtree
            return child.getLeaf(leafIndex-count, excludedNode);
          } else {
            // go to the next child
            count = count+numOfLeaves;
          }
        } else { // it is the excluededNode
          // skip it and set the excludedNode to be null
          excludedNode = null;
        }
      }
      return null;
    }
  }

  private boolean isLeafParent() {
    return isRack();
  }

  @Override
  public int getNumOfLeaves() {
    return numOfLeaves;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object to) {
    return super.equals(to);
  }
}
