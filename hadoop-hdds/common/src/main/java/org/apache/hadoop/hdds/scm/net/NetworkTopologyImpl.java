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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT;
import static org.apache.hadoop.hdds.scm.net.NetConstants.SCOPE_REVERSE_STR;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ANCESTOR_GENERATION_DEFAULT;

/**
 * The class represents a cluster of computers with a tree hierarchical
 * network topology. In the network topology, leaves represent data nodes
 * (computers) and inner nodes represent datacenter/core-switches/routers that
 * manages traffic in/out of data centers or racks.
 */
public class NetworkTopologyImpl implements NetworkTopology{
  public static final Logger LOG =
      LoggerFactory.getLogger(NetworkTopology.class);

  /** The Inner node crate factory. */
  private final InnerNode.Factory factory;
  /** The root cluster tree. */
  private final InnerNode clusterTree;
  /** Depth of all leaf nodes. */
  private final int maxLevel;
  /** Schema manager. */
  private final NodeSchemaManager schemaManager;
  /** Lock to coordinate cluster tree access. */
  private ReadWriteLock netlock = new ReentrantReadWriteLock(true);

  public NetworkTopologyImpl(Configuration conf) {
    schemaManager = NodeSchemaManager.getInstance();
    schemaManager.init(conf);
    maxLevel = schemaManager.getMaxLevel();
    factory = InnerNodeImpl.FACTORY;
    clusterTree = factory.newInnerNode(ROOT, null, null,
        NetConstants.ROOT_LEVEL,
        schemaManager.getCost(NetConstants.ROOT_LEVEL));
  }

  @VisibleForTesting
  public NetworkTopologyImpl(NodeSchemaManager manager) {
    schemaManager = manager;
    maxLevel = schemaManager.getMaxLevel();
    factory = InnerNodeImpl.FACTORY;
    clusterTree = factory.newInnerNode(ROOT, null, null,
        NetConstants.ROOT_LEVEL,
        schemaManager.getCost(NetConstants.ROOT_LEVEL));
  }

  /**
   * Add a leaf node. This will be called when a new datanode is added.
   * @param node node to be added; can be null
   * @exception IllegalArgumentException if add a node to a leave or node to be
   * added is not a leaf
   */
  public void add(Node node) {
    Preconditions.checkArgument(node != null, "node cannot be null");
    if (node instanceof InnerNode) {
      throw new IllegalArgumentException(
          "Not allowed to add an inner node: "+ node.getNetworkFullPath());
    }
    int newDepth = NetUtils.locationToDepth(node.getNetworkLocation()) + 1;

    // Check depth
    if (maxLevel != newDepth) {
      throw new InvalidTopologyException("Failed to add " +
          node.getNetworkFullPath() + ": Its path depth is not " + maxLevel);
    }
    netlock.writeLock().lock();
    boolean add;
    try {
      add = clusterTree.add(node);
    }finally {
      netlock.writeLock().unlock();
    }

    if (add) {
      LOG.info("Added a new node: " + node.getNetworkFullPath());
      LOG.debug("NetworkTopology became:\n{}", this);
    }
  }

  /**
   * Remove a node from the network topology. This will be called when a
   * existing datanode is removed from the system.
   * @param node node to be removed; cannot be null
   */
  public void remove(Node node) {
    Preconditions.checkArgument(node != null, "node cannot be null");
    if (node instanceof InnerNode) {
      throw new IllegalArgumentException(
          "Not allowed to remove an inner node: "+ node.getNetworkFullPath());
    }
    netlock.writeLock().lock();
    try {
      clusterTree.remove(node);
    }finally {
      netlock.writeLock().unlock();
    }
    LOG.info("Removed a node: " + node.getNetworkFullPath());
    LOG.debug("NetworkTopology became:\n{}", this);
  }

  /**
   * Check if the tree already contains node <i>node</i>.
   * @param node a node
   * @return true if <i>node</i> is already in the tree; false otherwise
   */
  public boolean contains(Node node) {
    Preconditions.checkArgument(node != null, "node cannot be null");
    netlock.readLock().lock();
    try {
      Node parent = node.getParent();
      while (parent != null && parent != clusterTree) {
        parent = parent.getParent();
      }
      if (parent == clusterTree) {
        return true;
      }
    } finally {
      netlock.readLock().unlock();
    }
    return false;
  }

  /**
   * Compare the specified ancestor generation of each node for equality.
   * @return true if their specified generation ancestor are equal
   */
  public boolean isSameAncestor(Node node1, Node node2, int ancestorGen) {
    if (node1 == null || node2 == null || ancestorGen <= 0) {
      return false;
    }
    netlock.readLock().lock();
    try {
      return node1.getAncestor(ancestorGen) == node2.getAncestor(ancestorGen);
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * Compare the direct parent of each node for equality.
   * @return true if their parent are the same
   */
  public boolean isSameParent(Node node1, Node node2) {
    if (node1 == null || node2 == null) {
      return false;
    }
    netlock.readLock().lock();
    try {
      node1 = node1.getParent();
      node2 = node2.getParent();
      return node1 == node2;
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * Get the ancestor for node on generation <i>ancestorGen</i>.
   *
   * @param node the node to get ancestor
   * @param ancestorGen  the ancestor generation
   * @return the ancestor. If no ancestor is found, then null is returned.
   */
  public Node getAncestor(Node node, int ancestorGen) {
    if (node == null) {
      return null;
    }
    netlock.readLock().lock();
    try {
      return node.getAncestor(ancestorGen);
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * Given a string representation of a node(leaf or inner), return its
   * reference.
   * @param loc a path string representing a node, can be leaf or inner node
   * @return a reference to the node, null if the node is not in the tree
   */
  public Node getNode(String loc) {
    loc = NetUtils.normalize(loc);
    netlock.readLock().lock();
    try {
      if (!ROOT.equals(loc)) {
        return clusterTree.getNode(loc);
      } else {
        return clusterTree;
      }
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * Given a string representation of Node, return its leaf nodes count.
   * @param loc a path-like string representation of Node
   * @return the number of leaf nodes for InnerNode, 1 for leaf node, 0 if node
   * doesn't exist
   */
  public int getNumOfLeafNode(String loc) {
    netlock.readLock().lock();
    try {
      Node node = getNode(loc);
      if (node != null) {
        return node.getNumOfLeaves();
      }
    } finally {
      netlock.readLock().unlock();
    }
    return 0;
  }

  /**
   * Return the max level of this tree, start from 1 for ROOT. For example,
   * topology like "/rack/node" has the max level '3'.
   */
  public int getMaxLevel() {
    return maxLevel;
  }

  /**
   * Return the node numbers at level <i>level</i>.
   * @param level topology level, start from 1, which means ROOT
   * @return the number of nodes on the level
   */
  public int getNumOfNodes(int level) {
    Preconditions.checkArgument(level > 0 && level <= maxLevel,
        "Invalid level");
    netlock.readLock().lock();
    try {
      return clusterTree.getNumOfNodes(level);
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * Randomly choose a node in the scope.
   * @param scope range of nodes from which a node will be chosen. If scope
   *              starts with ~, choose one from the all nodes except for the
   *              ones in <i>scope</i>; otherwise, choose one from <i>scope</i>.
   * @return the chosen node
   */
  public Node chooseRandom(String scope) {
    if (scope == null) {
      scope = ROOT;
    }
    if (scope.startsWith(SCOPE_REVERSE_STR)) {
      return chooseRandom(ROOT, scope.substring(1), null, null,
          ANCESTOR_GENERATION_DEFAULT);
    } else {
      return chooseRandom(scope, null, null, null, ANCESTOR_GENERATION_DEFAULT);
    }
  }

  /**
   * Randomly choose a node in the scope, ano not in the exclude scope.
   * @param scope range of nodes from which a node will be chosen. cannot start
   *              with ~
   * @param excludedScope the chosen node cannot be in this range. cannot
   *                      starts with ~
   * @return the chosen node
   */
  public Node chooseRandom(String scope, String excludedScope) {
    return chooseRandom(scope, excludedScope, null, null,
        ANCESTOR_GENERATION_DEFAULT);
  }

  /**
   * Randomly choose a leaf node from <i>scope</i>.
   *
   * If scope starts with ~, choose one from the all nodes except for the
   * ones in <i>scope</i>; otherwise, choose nodes from <i>scope</i>.
   * If excludedNodes is given, choose a node that's not in excludedNodes.
   *
   * @param scope range of nodes from which a node will be chosen
   * @param excludedNodes nodes to be excluded
   *
   * @return the chosen node
   */
  public Node chooseRandom(String scope, Collection<Node> excludedNodes) {
    if (scope == null) {
      scope = ROOT;
    }
    if (scope.startsWith(SCOPE_REVERSE_STR)) {
      return chooseRandom(ROOT, scope.substring(1), excludedNodes, null,
          ANCESTOR_GENERATION_DEFAULT);
    } else {
      return chooseRandom(scope, null, excludedNodes, null,
          ANCESTOR_GENERATION_DEFAULT);
    }
  }

  /**
   * Randomly choose a leaf node from <i>scope</i>.
   *
   * If scope starts with ~, choose one from the all nodes except for the
   * ones in <i>scope</i>; otherwise, choose nodes from <i>scope</i>.
   * If excludedNodes is given, choose a node that's not in excludedNodes.
   *
   * @param scope range of nodes from which a node will be chosen
   * @param excludedNodes nodes to be excluded from.
   * @param ancestorGen matters when excludeNodes is not null. It means the
   * ancestor generation that's not allowed to share between chosen node and the
   * excludedNodes. For example, if ancestorGen is 1, means chosen node
   * cannot share the same parent with excludeNodes. If value is 2, cannot
   * share the same grand parent, and so on. If ancestorGen is 0, then no
   * effect.
   *
   * @return the chosen node
   */
  public Node chooseRandom(String scope, Collection<Node> excludedNodes,
      int ancestorGen) {
    if (scope == null) {
      scope = ROOT;
    }
    if (scope.startsWith(SCOPE_REVERSE_STR)) {
      return chooseRandom(ROOT, scope.substring(1), excludedNodes, null,
          ancestorGen);
    } else {
      return chooseRandom(scope, null, excludedNodes, null, ancestorGen);
    }
  }

  /**
   * Randomly choose a leaf node.
   *
   * @param scope range from which a node will be chosen, cannot start with ~
   * @param excludedNodes nodes to be excluded
   * @param excludedScope excluded node range. Cannot start with ~
   * @param ancestorGen matters when excludeNodes is not null. It means the
   * ancestor generation that's not allowed to share between chosen node and the
   * excludedNodes. For example, if ancestorGen is 1, means chosen node
   * cannot share the same parent with excludeNodes. If value is 2, cannot
   * share the same grand parent, and so on. If ancestorGen is 0, then no
   * effect.
   *
   * @return the chosen node
   */
  public Node chooseRandom(String scope, String excludedScope,
      Collection<Node> excludedNodes, int ancestorGen) {
    return chooseRandom(scope, excludedScope, excludedNodes, null, ancestorGen);
  }

  /**
   * Randomly choose one leaf node from <i>scope</i>, share the same generation
   * ancestor with <i>affinityNode</i>, and exclude nodes in
   * <i>excludeScope</i> and <i>excludeNodes</i>.
   *
   * @param scope range of nodes from which a node will be chosen, cannot start
   *              with ~
   * @param excludedScope range of nodes to be excluded, cannot start with ~
   * @param excludedNodes nodes to be excluded
   * @param affinityNode  when not null, the chosen node should share the same
   *                     ancestor with this node at generation ancestorGen.
   *                      Ignored when value is null
   * @param ancestorGen If 0, then no same generation ancestor enforcement on
   *                     both excludedNodes and affinityNode. If greater than 0,
   *                     then apply to affinityNode(if not null), or apply to
   *                     excludedNodes if affinityNode is null
   * @return the chosen node
   */
  public Node chooseRandom(String scope, String excludedScope,
      Collection<Node> excludedNodes, Node affinityNode, int ancestorGen) {
    if (scope == null) {
      scope = ROOT;
    }

    checkScope(scope);
    checkExcludedScope(excludedScope);
    checkAffinityNode(affinityNode);
    checkAncestorGen(ancestorGen);

    netlock.readLock().lock();
    try {
      return chooseNodeInternal(scope, -1, excludedScope,
          excludedNodes, affinityNode, ancestorGen);
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * Choose the leaf node at index <i>index</i> from <i>scope</i>, share the
   * same generation ancestor with <i>affinityNode</i>, and exclude nodes in
   * <i>excludeScope</i> and <i>excludeNodes</i>.
   *
   * @param leafIndex node index, exclude nodes in excludedScope and
   *                  excludedNodes
   * @param scope range of nodes from which a node will be chosen, cannot start
   *              with ~
   * @param excludedScope range of nodes to be excluded, cannot start with ~
   * @param excludedNodes nodes to be excluded
   * @param affinityNode  when not null, the chosen node should share the same
   *                     ancestor with this node at generation ancestorGen.
   *                      Ignored when value is null
   * @param ancestorGen If 0, then no same generation ancestor enforcement on
   *                     both excludedNodes and affinityNode. If greater than 0,
   *                     then apply to affinityNode(if not null), or apply to
   *                     excludedNodes if affinityNode is null
   * @return the chosen node
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
   *   leafIndex = 1
   *   excludedScope = /dc2
   *   excludedNodes = {/dc1/rack1/n1}
   *   affinityNode = /dc1/rack2/n2
   *   ancestorGen = 2
   *
   *   Output:
   *   node /dc1/rack2/n4
   *
   *   Explanation:
   *   With affinityNode n2 and ancestorGen 2, it means we can only pick node
   *   from subtree /dc1. LeafIndex 1, so we pick the 2nd available node n4.
   *
   */
  public Node getNode(int leafIndex, String scope, String excludedScope,
      Collection<Node> excludedNodes, Node affinityNode, int ancestorGen) {
    Preconditions.checkArgument(leafIndex >= 0);
    if (scope == null) {
      scope = ROOT;
    }
    checkScope(scope);
    checkExcludedScope(excludedScope);
    checkAffinityNode(affinityNode);
    checkAncestorGen(ancestorGen);

    netlock.readLock().lock();
    try {
      return chooseNodeInternal(scope, leafIndex, excludedScope,
          excludedNodes, affinityNode, ancestorGen);
    } finally {
      netlock.readLock().unlock();
    }
  }

  private Node chooseNodeInternal(String scope, int leafIndex,
      String excludedScope, Collection<Node> excludedNodes, Node affinityNode,
      int ancestorGen) {
    Preconditions.checkArgument(scope != null);

    String finalScope = scope;
    if (affinityNode != null && ancestorGen > 0) {
      Node affinityAncestor = affinityNode.getAncestor(ancestorGen);
      if (affinityAncestor == null) {
        throw new IllegalArgumentException("affinityNode " +
            affinityNode.getNetworkFullPath() + " doesn't have ancestor on" +
            " generation  " + ancestorGen);
      }
      // affinity ancestor should has overlap with scope
      if (affinityAncestor.getNetworkFullPath().startsWith(scope)){
        finalScope = affinityAncestor.getNetworkFullPath();
      } else if (!scope.startsWith(affinityAncestor.getNetworkFullPath())) {
        return null;
      }
      // reset ancestor generation since the new scope is identified now
      ancestorGen = 0;
    }

    // check overlap of excludedScope and finalScope
    if (excludedScope != null) {
      // excludeScope covers finalScope
      if (finalScope.startsWith(excludedScope)) {
        return null;
      }
      // excludeScope and finalScope share nothing
      if (!excludedScope.startsWith(finalScope)) {
        excludedScope = null;
      }
    }

    // clone excludedNodes before remove duplicate in it
    Collection<Node> mutableExNodes = null;
    if (excludedNodes != null) {
      // Remove duplicate in excludedNodes
      mutableExNodes =
          excludedNodes.stream().distinct().collect(Collectors.toList());
    }

    // remove duplicate in mutableExNodes and excludedScope, given ancestorGen
    excludedScope = NetUtils.removeDuplicate(this, mutableExNodes,
        excludedScope, ancestorGen);

    // calculate available node count
    Node scopeNode = getNode(finalScope);
    int availableNodes = getAvailableNodesCount(
        scopeNode.getNetworkFullPath(), excludedScope, mutableExNodes,
        ancestorGen);

    if (availableNodes <= 0) {
      LOG.warn("No available node in (scope=\"{}\" excludedScope=\"{}\" " +
              "excludedNodes=\"{}\"  ancestorGen=\"{}\").",
          scopeNode.getNetworkFullPath(), excludedScope, excludedNodes,
          ancestorGen);
      return null;
    }
    LOG.debug("Choosing random from \"{}\" available nodes on node \"{}\"," +
            " scope=\"{}\", excludedScope=\"{}\", excludeNodes=\"{}\".",
        availableNodes, scopeNode, scopeNode.getNetworkFullPath(),
        excludedScope, excludedNodes);

    // scope is a Leaf node
    if (!(scopeNode instanceof InnerNode)) {
      return scopeNode;
    }

    Node ret;
    if (leafIndex >= 0) {
      ret = ((InnerNode)scopeNode).getLeaf(leafIndex % availableNodes,
          excludedScope, mutableExNodes, ancestorGen);
    } else {
      final int index = ThreadLocalRandom.current().nextInt(availableNodes);
      ret = ((InnerNode)scopeNode).getLeaf(index, excludedScope, mutableExNodes,
          ancestorGen);
    }
    LOG.debug("chooseRandom return {}", ret);
    return ret;
  }

  /** Return the distance cost between two nodes
   * The distance cost from one node to its parent is it's parent's cost
   * The distance cost between two nodes is calculated by summing up their
   * distances cost to their closest common ancestor.
   * @param node1 one node
   * @param node2 another node
   * @return the distance cost between node1 and node2 which is zero if they
   * are the same or {@link Integer#MAX_VALUE} if node1 or node2 do not belong
   * to the cluster
   */
  public int getDistanceCost(Node node1, Node node2) {
    if ((node1 != null && node2 != null && node1.equals(node2)) ||
        (node1 == null && node2 == null))  {
      return 0;
    }
    int cost = 0;
    netlock.readLock().lock();
    try {
      if (node1 == null || node2 == null ||
          (node1.getAncestor(maxLevel - 1) != clusterTree) ||
          (node2.getAncestor(maxLevel - 1) != clusterTree)) {
        LOG.warn("One of the nodes is a null pointer");
        return Integer.MAX_VALUE;
      }
      int level1 = node1.getLevel();
      int level2 = node2.getLevel();
      if (level1 > maxLevel || level2 > maxLevel) {
        return Integer.MAX_VALUE;
      }
      while(level1 > level2 && node1 != null) {
        node1 = node1.getParent();
        level1--;
        cost += node1 == null? 0 : node1.getCost();
      }
      while(level2 > level1 && node2 != null) {
        node2 = node2.getParent();
        level2--;
        cost += node2 == null? 0 : node2.getCost();
      }
      while(node1 != null && node2 != null && node1 != node2) {
        node1 = node1.getParent();
        node2 = node2.getParent();
        cost += node1 == null? 0 : node1.getCost();
        cost += node2 == null? 0 : node2.getCost();
      }
      return cost;
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * Sort nodes array by network distance to <i>reader</i> to reduces network
   * traffic and improves performance.
   *
   * As an additional twist, we also randomize the nodes at each network
   * distance. This helps with load balancing when there is data skew.
   *
   * @param reader    Node where need the data
   * @param nodes     Available replicas with the requested data
   * @param activeLen Number of active nodes at the front of the array
   */
  public void sortByDistanceCost(Node reader, Node[] nodes, int activeLen) {
    /** Sort weights for the nodes array */
    int[] costs = new int[activeLen];
    for (int i = 0; i < activeLen; i++) {
      costs[i] = getDistanceCost(reader, nodes[i]);
    }
    // Add cost/node pairs to a TreeMap to sort
    TreeMap<Integer, List<Node>> tree = new TreeMap<Integer, List<Node>>();
    for (int i = 0; i < activeLen; i++) {
      int cost = costs[i];
      Node node = nodes[i];
      List<Node> list = tree.get(cost);
      if (list == null) {
        list = Lists.newArrayListWithExpectedSize(1);
        tree.put(cost, list);
      }
      list.add(node);
    }
    int idx = 0;
    for (List<Node> list: tree.values()) {
      if (list != null) {
        Collections.shuffle(list);
        for (Node n: list) {
          nodes[idx] = n;
          idx++;
        }
      }
    }
    Preconditions.checkState(idx == activeLen, "Wrong number of nodes sorted!");
  }

  /**
   * Return the number of leaves in <i>scope</i> but not in
   * <i>excludedNodes</i> and <i>excludeScope</i>.
   * @param scope the scope
   * @param excludedScope excluded scope
   * @param mutableExcludedNodes a list of excluded nodes, content might be
   *                            changed after the call
   * @param ancestorGen same generation ancestor prohibit on excludedNodes
   * @return number of available nodes
   */
  private int getAvailableNodesCount(String scope, String excludedScope,
      Collection<Node> mutableExcludedNodes, int ancestorGen) {
    Preconditions.checkArgument(scope != null);

    Node scopeNode = getNode(scope);
    if (scopeNode == null) {
      return 0;
    }
    NetUtils.removeOutscope(mutableExcludedNodes, scope);
    List<Node> excludedAncestorList =
        NetUtils.getAncestorList(this, mutableExcludedNodes, ancestorGen);
    for (Node ancestor : excludedAncestorList) {
      if (scope.startsWith(ancestor.getNetworkFullPath())){
        return 0;
      }
    }
    // number of nodes to exclude
    int excludedCount = 0;
    if (excludedScope != null) {
      Node excludedScopeNode = getNode(excludedScope);
      if (excludedScopeNode != null) {
        if (excludedScope.startsWith(scope)) {
          excludedCount += excludedScopeNode.getNumOfLeaves();
        } else if (scope.startsWith(excludedScope)) {
          return 0;
        }
      }
    }
    // excludedNodes is not null case
    if (mutableExcludedNodes != null && (!mutableExcludedNodes.isEmpty())) {
      if (ancestorGen == 0) {
        for (Node node: mutableExcludedNodes) {
          if (contains(node)) {
            excludedCount++;
          }
        }
      } else {
        for (Node ancestor : excludedAncestorList) {
          if (ancestor.getNetworkFullPath().startsWith(scope)) {
            excludedCount += ancestor.getNumOfLeaves();
          }
        }
      }
    }

    int availableCount = scopeNode.getNumOfLeaves() - excludedCount;
    Preconditions.checkState(availableCount >= 0);
    return availableCount;
  }

  @Override
  public String toString() {
    // print max level
    StringBuilder tree = new StringBuilder();
    tree.append("Level: ");
    tree.append(maxLevel);
    tree.append("\n");
    netlock.readLock().lock();
    try {
      // print the number of leaves
      int numOfLeaves = clusterTree.getNumOfLeaves();
      tree.append("Expected number of leaves:");
      tree.append(numOfLeaves);
      tree.append("\n");
      // print all nodes
      for (int i = 0; i < numOfLeaves; i++) {
        tree.append(clusterTree.getLeaf(i).getNetworkFullPath());
        tree.append("\n");
      }
    } finally {
      netlock.readLock().unlock();
    }
    return tree.toString();
  }

  private void checkScope(String scope) {
    if (scope != null && scope.startsWith(SCOPE_REVERSE_STR)) {
      throw new IllegalArgumentException("scope " + scope +
          " should not start with " + SCOPE_REVERSE_STR);
    }
  }

  private void checkExcludedScope(String excludedScope) {
    if (excludedScope != null &&
        (excludedScope.startsWith(SCOPE_REVERSE_STR))) {
      throw new IllegalArgumentException("excludedScope " + excludedScope +
          " cannot start with " + SCOPE_REVERSE_STR);
    }
  }

  private void checkAffinityNode(Node affinityNode) {
    if (affinityNode != null && (!contains(affinityNode))) {
      throw new IllegalArgumentException("Affinity node " +
          affinityNode.getNetworkFullPath() + " is not a member of topology");
    }
  }

  private void checkAncestorGen(int ancestorGen) {
    if (ancestorGen > (maxLevel - 1) || ancestorGen < 0) {
      throw new IllegalArgumentException("ancestorGen " + ancestorGen +
          " exceeds this network topology acceptable level [0, " +
          (maxLevel - 1) + "]");
    }
  }
}