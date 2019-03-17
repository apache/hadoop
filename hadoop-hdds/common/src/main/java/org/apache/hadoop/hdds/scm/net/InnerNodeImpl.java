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
package org.apache.hadoop.hdds.scm.net;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.scm.net.NetConstants.PATH_SEPARATOR_STR;
import static org.apache.hadoop.hdds.scm.net.NetConstants.PATH_SEPARATOR;

/**
 * A thread safe class that implements InnerNode interface.
 */
public class InnerNodeImpl extends NodeImpl implements InnerNode {
  protected static class Factory implements InnerNode.Factory<InnerNodeImpl> {
    protected Factory() {}

    public InnerNodeImpl newInnerNode(String name, String location,
        InnerNode parent, int level, int cost) {
      return new InnerNodeImpl(name, location, parent, level, cost);
    }
  }

  static final Factory FACTORY = new Factory();
  // a map of node's network name to Node for quick search and keep
  // the insert order
  private final HashMap<String, Node> childrenMap =
      new LinkedHashMap<String, Node>();
  // number of descendant leaves under this node
  private int numOfLeaves;
  // LOGGER
  public static final Logger LOG = LoggerFactory.getLogger(InnerNodeImpl.class);

  /**
   * Construct an InnerNode from its name, network location, parent, level and
   * its cost.
   **/
  protected InnerNodeImpl(String name, String location, InnerNode parent,
      int level, int cost) {
    super(name, location, parent, level, cost);
  }

  /** @return the number of children this node has */
  private int getNumOfChildren() {
    return childrenMap.size();
  }

  /** @return its leaf nodes number */
  @Override
  public int getNumOfLeaves() {
    return numOfLeaves;
  }

  /**
   * @return number of its all nodes at level <i>level</i>. Here level is a
   * relative level. If level is 1, means node itself. If level is 2, means its
   * direct children, and so on.
   **/
  public int getNumOfNodes(int level) {
    Preconditions.checkArgument(level > 0);
    int count = 0;
    if (level == 1) {
      count += 1;
    } else if (level == 2) {
      count += getNumOfChildren();
    } else {
      for (Node node: childrenMap.values()) {
        if (node instanceof InnerNode) {
          count += ((InnerNode)node).getNumOfNodes(level -1);
        } else {
          throw new RuntimeException("Cannot support Level:" + level +
              " on this node " + this.toString());
        }
      }
    }
    return count;
  }

  /**
   * Judge if this node is the parent of a leave node <i>n</i>.
   * @return true if this node is the parent of <i>n</i>
   */
  private boolean isLeafParent() {
    if (childrenMap.isEmpty()) {
      return true;
    }
    Node child = childrenMap.values().iterator().next();
    return child instanceof InnerNode ? false : true;
  }

  /**
   * Judge if this node is the parent of node <i>node</i>.
   * @param node a node
   * @return true if this node is the parent of <i>n</i>
   */
  private boolean isParent(Node node) {
    return node.getNetworkLocation().equals(this.getNetworkFullPath());
  }

  /**
   * Add node <i>node</i> to the subtree of this node.
   * @param node node to be added
   * @return true if the node is added, false if is only updated
   */
  public boolean add(Node node) {
    if (!isAncestor(node)) {
      throw new IllegalArgumentException(node.getNetworkName()
          + ", which is located at " + node.getNetworkLocation()
          + ", is not a descendant of " + this.getNetworkFullPath());
    }
    if (isParent(node)) {
      // this node is the parent, then add it directly
      node.setParent(this);
      node.setLevel(this.getLevel() + 1);
      Node current = childrenMap.put(node.getNetworkName(), node);
      if (current != null) {
        return false;
      }
    } else {
      // find the next level ancestor node
      String ancestorName = getNextLevelAncestorName(node);
      InnerNode childNode = (InnerNode)childrenMap.get(ancestorName);
      if (childNode == null) {
        // create a new InnerNode for this ancestor node
        childNode = createChildNode(ancestorName);
        childrenMap.put(childNode.getNetworkName(), childNode);
      }
      // add node to the subtree of the next ancestor node
      if (!childNode.add(node)) {
        return false;
      }
    }
    numOfLeaves++;
    return true;
  }

  /**
   * Remove node <i>node</i> from the subtree of this node.
   * @param node node to be deleted
   */
  public void remove(Node node) {
    if (!isAncestor(node)) {
      throw new IllegalArgumentException(node.getNetworkName()
          + ", which is located at " + node.getNetworkLocation()
          + ", is not a descendant of " + this.getNetworkFullPath());
    }
    if (isParent(node)) {
      // this node is the parent, remove it directly
      if (childrenMap.containsKey(node.getNetworkName())) {
        childrenMap.remove(node.getNetworkName());
        node.setParent(null);
      } else {
        throw new RuntimeException("Should not come to here. Node:" +
            node.getNetworkFullPath() + ", Parent:" +
            this.getNetworkFullPath());
      }
    } else {
      // find the next ancestor node
      String ancestorName = getNextLevelAncestorName(node);
      InnerNodeImpl childNode = (InnerNodeImpl)childrenMap.get(ancestorName);
      Preconditions.checkNotNull(childNode, "InnerNode is deleted before leaf");
      // remove node from the parent node
      childNode.remove(node);
      // if the parent node has no children, remove the parent node too
      if (childNode.getNumOfChildren() == 0) {
        childrenMap.remove(ancestorName);
      }
    }
    numOfLeaves--;
  }

  /**
   * Given a node's string representation, return a reference to the node.
   * Node can be leaf node or inner node.
   *
   * @param loc string location of a node. If loc starts with "/", it's a
   *            absolute path, otherwise a relative path. Following examples
   *            are all accepted,
   *            1.  /dc1/rm1/rack1          -> an inner node
   *            2.  /dc1/rm1/rack1/node1    -> a leaf node
   *            3.  rack1/node1             -> a relative path to this node
   *
   * @return null if the node is not found
   */
  public Node getNode(String loc) {
    if (loc == null) {
      return null;
    }

    String fullPath = this.getNetworkFullPath();
    if (loc.equalsIgnoreCase(fullPath)) {
      return this;
    }

    // remove current node's location from loc when it's a absolute path
    if (fullPath.equals(NetConstants.PATH_SEPARATOR_STR)) {
      // current node is ROOT
      if (loc.startsWith(PATH_SEPARATOR_STR)) {
        loc = loc.substring(1);
      }
    } else if (loc.startsWith(fullPath)) {
      loc = loc.substring(fullPath.length());
      // skip the separator "/"
      loc = loc.substring(1);
    }

    String[] path = loc.split(PATH_SEPARATOR_STR, 2);
    Node child = childrenMap.get(path[0]);
    if (child == null) {
      return null;
    }
    if (path.length == 1){
      return child;
    }
    if (child instanceof InnerNode) {
      return ((InnerNode)child).getNode(path[1]);
    } else {
      return null;
    }
  }

  /**
   * get <i>leafIndex</i> leaf of this subtree.
   *
   * @param leafIndex an indexed leaf of the node
   * @return the leaf node corresponding to the given index.
   */
  public Node getLeaf(int leafIndex) {
    Preconditions.checkArgument(leafIndex >= 0);
    // children are leaves
    if (isLeafParent()) {
      // range check
      if (leafIndex >= getNumOfChildren()) {
        return null;
      }
      return getChildNode(leafIndex);
    } else {
      for(Node node : childrenMap.values()) {
        InnerNodeImpl child = (InnerNodeImpl)node;
        int leafCount = child.getNumOfLeaves();
        if (leafIndex < leafCount) {
          return child.getLeaf(leafIndex);
        } else {
          leafIndex -= leafCount;
        }
      }
      return null;
    }
  }

  /**
   * Get <i>leafIndex</i> leaf of this subtree.
   *
   * @param leafIndex node's index, start from 0, skip the nodes in
   *                 excludedScope and excludedNodes with ancestorGen
   * @param excludedScope the exclude scope
   * @param excludedNodes nodes to be excluded from. If ancestorGen is not 0,
   *                      the chosen node will not share same ancestor with
   *                      those in excluded nodes at the specified generation
   * @param ancestorGen  apply to excludeNodes, when value is 0, then no same
   *                    ancestor enforcement on excludedNodes
   * @return the leaf node corresponding to the given index.
   * Example:
   *
   *                                /  --- root
   *                              /  \
   *                             /    \
   *                            /      \
   *                           /        \
   *                         dc1         dc2
   *                        / \         / \
   *                       /   \       /   \
   *                      /     \     /     \
   *                    rack1 rack2  rack1  rack2
   *                   / \     / \  / \     / \
   *                 n1  n2  n3 n4 n5  n6  n7 n8
   *
   *   Input:
   *   leafIndex = 2
   *   excludedScope = /dc2
   *   excludedNodes = {/dc1/rack1/n1}
   *   ancestorGen = 1
   *
   *   Output:
   *   node /dc1/rack2/n5
   *
   *   Explanation:
   *   Since excludedNodes is n1 and ancestorGen is 1, it means nodes under
   *   /root/dc1/rack1 are excluded. Given leafIndex start from 0, LeafIndex 2
   *   means picking the 3th available node, which is n5.
   *
   */
  public Node getLeaf(int leafIndex, String excludedScope,
      Collection<Node> excludedNodes, int ancestorGen) {
    Preconditions.checkArgument(leafIndex >= 0 && ancestorGen >= 0);
    // come to leaf parent layer
    if (isLeafParent()) {
      return getLeafOnLeafParent(leafIndex, excludedScope, excludedNodes);
    }

    int maxLevel = NodeSchemaManager.getInstance().getMaxLevel();
    // this node's children, it's generation as the ancestor of the leaf node
    int currentGen = maxLevel - this.getLevel() - 1;
    // build an ancestor(children) to exclude node count map
    Map<Node, Integer> countMap =
        getAncestorCountMap(excludedNodes, ancestorGen, currentGen);
    // nodes covered by excluded scope
    int excludedNodeCount = getExcludedScopeNodeCount(excludedScope);

    for(Node child : childrenMap.values()) {
      int leafCount = child.getNumOfLeaves();
      // skip nodes covered by excluded scope
      if (excludedScope != null &&
          excludedScope.startsWith(child.getNetworkFullPath())) {
        leafCount -= excludedNodeCount;
      }
      // skip nodes covered by excluded nodes and ancestorGen
      Integer count = countMap.get(child);
      if (count != null) {
        leafCount -= count;
      }
      if (leafIndex < leafCount) {
        return ((InnerNode)child).getLeaf(leafIndex, excludedScope,
            excludedNodes, ancestorGen);
      } else {
        leafIndex -= leafCount;
      }
    }
    return null;
  }

  @Override
  public boolean equals(Object to) {
    if (to == null) {
      return false;
    }
    if (this == to) {
      return true;
    }
    return this.toString().equals(to.toString());
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  /**
   * Get a ancestor to its excluded node count map.
   *
   * @param nodes a collection of leaf nodes to exclude
   * @param genToExclude  the ancestor generation to exclude
   * @param genToReturn  the ancestor generation to return the count map
   * @return the map.
   * example:
   *
   *                *  --- root
   *              /    \
   *             *      *   -- genToReturn =2
   *            / \    / \
   *          *   *   *   *  -- genToExclude = 1
   *         /\  /\  /\  /\
   *       *  * * * * * * *  -- nodes
   */
  private Map<Node, Integer> getAncestorCountMap(Collection<Node> nodes,
      int genToExclude, int genToReturn) {
    Preconditions.checkState(genToExclude >= 0);
    Preconditions.checkState(genToReturn >= 0);

    if (nodes == null || nodes.size() == 0) {
      return Collections.emptyMap();
    }
    // with the recursive call, genToReturn can be smaller than genToExclude
    if (genToReturn < genToExclude) {
      genToExclude = genToReturn;
    }
    // ancestorToExclude to ancestorToReturn map
    HashMap<Node, Node> ancestorMap = new HashMap<>();
    for (Node node: nodes) {
      Node ancestorToExclude = node.getAncestor(genToExclude);
      Node ancestorToReturn = node.getAncestor(genToReturn);
      if (ancestorToExclude == null || ancestorToReturn == null) {
        LOG.warn("Ancestor not found, node: " + node.getNetworkFullPath() +
            ", generation to exclude: " + genToExclude +
            ", generation to return:" + genToReturn);
        continue;
      }
      ancestorMap.put(ancestorToExclude, ancestorToReturn);
    }
    // ancestorToReturn to exclude node count map
    HashMap<Node, Integer> countMap = new HashMap<>();
    for (Map.Entry<Node, Node> entry : ancestorMap.entrySet()) {
      countMap.compute(entry.getValue(),
          (key, n) -> (n == null ? 0 : n) + entry.getKey().getNumOfLeaves());
    }

    return countMap;
  }

  /**
   *  Get the node with leafIndex, considering skip nodes in excludedScope
   *  and in excludeNodes list.
   */
  private Node getLeafOnLeafParent(int leafIndex, String excludedScope,
      Collection<Node> excludedNodes) {
    Preconditions.checkArgument(isLeafParent() && leafIndex >= 0);
    if (leafIndex >= getNumOfChildren()) {
      return null;
    }
    for(Node node : childrenMap.values()) {
      if ((excludedNodes != null && (excludedNodes.contains(node))) ||
          (excludedScope != null &&
              (node.getNetworkFullPath().startsWith(excludedScope)))) {
        continue;
      }
      if (leafIndex == 0) {
        return node;
      }
      leafIndex--;
    }
    return null;
  }

  /**
   *  Return child's name of this node which is an ancestor of node <i>n</i>.
   */
  private String getNextLevelAncestorName(Node n) {
    int parentPathLen = this.getNetworkFullPath().length();
    String name = n.getNetworkLocation().substring(parentPathLen);
    if (name.charAt(0) == PATH_SEPARATOR) {
      name = name.substring(1);
    }
    int index = name.indexOf(PATH_SEPARATOR);
    if (index != -1) {
      name = name.substring(0, index);
    }
    return name;
  }

  /**
   * Creates a child node to be added to the list of children.
   * @param name The name of the child node
   * @return A new inner node
   * @see InnerNodeImpl(String, String, InnerNode, int)
   */
  private InnerNodeImpl createChildNode(String name) {
    int childLevel = this.getLevel() + 1;
    int cost = NodeSchemaManager.getInstance().getCost(childLevel);
    return new InnerNodeImpl(name, this.getNetworkFullPath(), this, childLevel,
        cost);
  }

  /** Get node with index <i>index</i>. */
  private Node getChildNode(int index) {
    Iterator iterator = childrenMap.values().iterator();
    Node node = null;
    while(index >= 0 && iterator.hasNext()) {
      node = (Node)iterator.next();
      index--;
    }
    return node;
  }

  /** Get how many leaf nodes are covered by the excludedScope. */
  private int getExcludedScopeNodeCount(String excludedScope) {
    if (excludedScope == null) {
      return 0;
    }
    Node excludedScopeNode = getNode(excludedScope);
    return excludedScopeNode == null ? 0 : excludedScopeNode.getNumOfLeaves();
  }
}