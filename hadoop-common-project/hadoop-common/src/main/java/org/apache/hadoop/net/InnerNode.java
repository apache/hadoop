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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.List;


@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public interface InnerNode extends Node {
  interface Factory<N extends InnerNode> {
    /** Construct an InnerNode from a path-like string */
    N newInnerNode(String path);
  }

  /** Add node <i>n</i> to the subtree of this node
   * @param n node to be added
   * @return true if the node is added; false otherwise
   */
  boolean add(Node n);

  /** Given a node's string representation, return a reference to the node
   * @param loc string location of the form /rack/node
   * @return null if the node is not found or the childnode is there but
   * not an instance of {@link InnerNodeImpl}
   */
  Node getLoc(String loc);

  /** @return its children */
  List<Node> getChildren();

  /** @return the number of leave nodes. */
  int getNumOfLeaves();

  /** Remove node <i>n</i> from the subtree of this node
   * @param n node to be deleted
   * @return true if the node is deleted; false otherwise
   */
  boolean remove(Node n);

  /** get <i>leafIndex</i> leaf of this subtree
   * if it is not in the <i>excludedNode</i>
   *
   * @param leafIndex an indexed leaf of the node
   * @param excludedNode an excluded node (can be null)
   * @return the leaf node corresponding to the given index.
   */
  Node getLeaf(int leafIndex, Node excludedNode);
}
