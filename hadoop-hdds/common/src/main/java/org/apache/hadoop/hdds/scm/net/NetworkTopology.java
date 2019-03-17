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

/**
 * The interface defines a network topology.
 */
public interface NetworkTopology {
  /** Exception for invalid network topology detection. */
  class InvalidTopologyException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    public InvalidTopologyException(String msg) {
      super(msg);
    }
  }
  /**
   * Add a leaf node. This will be called when a new datanode is added.
   * @param node node to be added; can be null
   * @exception IllegalArgumentException if add a node to a leave or node to be
   * added is not a leaf
   */
  void add(Node node);


  /**
   * Remove a node from the network topology. This will be called when a
   * existing datanode is removed from the system.
   * @param node node to be removed; cannot be null
   */
  void remove(Node node);


  /**
   * Check if the tree already contains node <i>node</i>.
   * @param node a node
   * @return true if <i>node</i> is already in the tree; false otherwise
   */
  boolean contains(Node node);

  /**
   * Compare the direct parent of each node for equality.
   * @return true if their parent are the same
   */
  boolean isSameParent(Node node1, Node node2);

  /**
   * Compare the specified ancestor generation of each node for equality.
   * ancestorGen 1 means parent.
   * @return true if their specified generation ancestor are equal
   */
  boolean isSameAncestor(Node node1, Node node2, int ancestorGen);


  /**
   * Get the ancestor for node on generation <i>ancestorGen</i>.
   *
   * @param node the node to get ancestor
   * @param ancestorGen  the ancestor generation
   * @return the ancestor. If no ancestor is found, then null is returned.
   */
  Node getAncestor(Node node, int ancestorGen);

  /**
   * Return the max level of this topology, start from 1 for ROOT. For example,
   * topology like "/rack/node" has the max level '3'.
   */
  int getMaxLevel();

  /**
   * Given a string representation of a node, return its reference.
   * @param loc a path string representing a node, can be leaf or inner node
   * @return a reference to the node; null if the node is not in the tree
   */
  Node getNode(String loc);

  /**
   * Given a string representation of a InnerNode, return its leaf nodes count.
   * @param loc a path-like string representation of a InnerNode
   * @return the number of leaf nodes, 0 if it's not an InnerNode or the node
   * doesn't exist
   */
  int getNumOfLeafNode(String loc);

  /**
   * Return the node numbers at level <i>level</i>.
   * @param level topology level, start from 1, which means ROOT
   * @return the number of nodes on the level
   */
  int getNumOfNodes(int level);

  /**
   * Randomly choose a node in the scope.
   * @param scope range of nodes from which a node will be chosen. If scope
   *              starts with ~, choose one from the all nodes except for the
   *              ones in <i>scope</i>; otherwise, choose one from <i>scope</i>.
   * @return the chosen node
   */
  Node chooseRandom(String scope);

  /**
   * Randomly choose a node in the scope, ano not in the exclude scope.
   * @param scope range of nodes from which a node will be chosen. cannot start
   *              with ~
   * @param excludedScope the chosen node cannot be in this range. cannot
   *                      starts with ~
   * @return the chosen node
   */
  Node chooseRandom(String scope, String excludedScope);

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
  Node chooseRandom(String scope, Collection<Node> excludedNodes);

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
  Node chooseRandom(String scope, Collection<Node> excludedNodes,
      int ancestorGen);


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
  Node chooseRandom(String scope, String excludedScope,
      Collection<Node> excludedNodes, int ancestorGen);


  /**
   * Randomly choose one node from <i>scope</i>, share the same generation
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
  Node chooseRandom(String scope, String excludedScope,
      Collection<Node> excludedNodes, Node affinityNode, int ancestorGen);

  /**
   * Choose the node at index <i>index</i> from <i>scope</i>, share the same
   * generation ancestor with <i>affinityNode</i>, and exclude nodes in
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
   */
  Node getNode(int leafIndex, String scope, String excludedScope,
      Collection<Node> excludedNodes, Node affinityNode, int ancestorGen);

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
  int getDistanceCost(Node node1, Node node2);

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
  void sortByDistanceCost(Node reader, Node[] nodes, int activeLen);
}
