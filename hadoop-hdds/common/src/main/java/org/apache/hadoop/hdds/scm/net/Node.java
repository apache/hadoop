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

/**
 * The interface defines a node in a network topology.
 * A node may be a leave representing a data node or an inner
 * node representing a data center or rack.
 * Each node has a name and its location in the network is
 * decided by a string with syntax similar to a file name.
 * For example, a data node's name is hostname:port# and if it's located at
 * rack "orange" in data center "dog", the string representation of its
 * network location will be /dog/orange.
 */
public interface Node {
  /** @return the string representation of this node's network location path,
   *  exclude itself. In another words, its parent's full network location */
  String getNetworkLocation();

  /** @return this node's self name in network topology. This should be node's
   * IP or hostname.
   * */
  String getNetworkName();

  /** @return this node's full path in network topology. It's the concatenation
   *  of location and name.
   * */
  String getNetworkFullPath();

  /** @return this node's parent */
  InnerNode getParent();

  /**
   * Set this node's parent.
   * @param parent the parent
   */
  void setParent(InnerNode parent);

  /** @return this node's ancestor, generation 0 is itself, generation 1 is
   *  node's parent, and so on.*/
  Node getAncestor(int generation);

  /**
   * @return this node's level in the tree.
   * E.g. the root of a tree returns 1 and root's children return 2
   */
  int getLevel();

  /**
   * Set this node's level in the tree.
   * @param i the level
   */
  void setLevel(int i);

  /**
   * @return this node's cost when network traffic go through it.
   * E.g. the cost of going cross a switch is 1, and cost of going through a
   * datacenter can be 5.
   * Be default the cost of leaf datanode is 0, all other node is 1.
   */
  int getCost();

  /** @return the leaf nodes number under this node. */
  int getNumOfLeaves();

  /**
   * Judge if this node is an ancestor of node <i>n</i>.
   * Ancestor includes itself and parents case.
   *
   * @param n a node
   * @return true if this node is an ancestor of <i>n</i>
   */
  boolean isAncestor(Node n);
}
