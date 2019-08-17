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
 * The interface defines an inner node in a network topology.
 * An inner node represents network topology entities, such as data center,
 * rack, switch or logical group.
 */
public interface InnerNode extends Node {
  /** A factory interface to get new InnerNode instance. */
  interface Factory<N extends InnerNode> {
    /** Construct an InnerNode from name, location, parent, level and cost. */
    N newInnerNode(String name, String location, InnerNode parent, int level,
        int cost);
  }

  /**
   * Add node <i>n</i> to the subtree of this node.
   * @param n node to be added
   * @return true if the node is added; false otherwise
   */
  boolean add(Node n);

  /**
   * Remove node <i>n</i> from the subtree of this node.
   * @param n node to be deleted
   */
  void remove(Node n);

  /**
   * Given a node's string representation, return a reference to the node.
   * @param loc string location of the format /dc/rack/nodegroup/node
   * @return null if the node is not found
   */
  Node getNode(String loc);

  /**
   * @return number of its all nodes at level <i>level</i>. Here level is a
   * relative level. If level is 1, means node itself. If level is 2, means its
   * direct children, and so on.
   **/
  int getNumOfNodes(int level);

  /**
   * Get <i>leafIndex</i> leaf of this subtree.
   *
   * @param leafIndex an indexed leaf of the node
   * @return the leaf node corresponding to the given index.
   */
  Node getLeaf(int leafIndex);

  /**
   * Get <i>leafIndex</i> leaf of this subtree.
   *
   * @param leafIndex ode's index, start from 0, skip the nodes in
   *                  excludedScope and excludedNodes with ancestorGen
   * @param excludedScope the excluded scope
   * @param excludedNodes nodes to be excluded. If ancestorGen is not 0,
   *                      the chosen node will not share same ancestor with
   *                      those in excluded nodes at the specified generation
   * @param ancestorGen ignored with value is 0
   * @return the leaf node corresponding to the given index
   */
  Node getLeaf(int leafIndex, String excludedScope,
      Collection<Node> excludedNodes, int ancestorGen);
}
