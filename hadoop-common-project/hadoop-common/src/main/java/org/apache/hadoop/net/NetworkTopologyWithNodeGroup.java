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

/**
 * The class extends NetworkTopology to represents a cluster of computer with
 *  a 4-layers hierarchical network topology.
 * In this network topology, leaves represent data nodes (computers) and inner
 * nodes represent switches/routers that manage traffic in/out of data centers,
 * racks or physical host (with virtual switch).
 * 
 * @see NetworkTopology
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class NetworkTopologyWithNodeGroup extends NetworkTopology {

  public final static String DEFAULT_NODEGROUP = "/default-nodegroup";

  public NetworkTopologyWithNodeGroup() {
    clusterMap = new InnerNodeWithNodeGroup(NodeBase.ROOT);
  }

  @Override
  protected Node getNodeForNetworkLocation(Node node) {
    // if node only with default rack info, here we need to add default
    // nodegroup info
    if (NetworkTopology.DEFAULT_RACK.equals(node.getNetworkLocation())) {
      node.setNetworkLocation(node.getNetworkLocation()
          + DEFAULT_NODEGROUP);
    }
    Node nodeGroup = getNode(node.getNetworkLocation());
    if (nodeGroup == null) {
      nodeGroup = new InnerNodeWithNodeGroup(node.getNetworkLocation());
    }
    return getNode(nodeGroup.getNetworkLocation());
  }

  @Override
  public String getRack(String loc) {
    netlock.readLock().lock();
    try {
      loc = NodeBase.normalize(loc);
      Node locNode = getNode(loc);
      if (locNode instanceof InnerNodeWithNodeGroup) {
        InnerNodeWithNodeGroup node = (InnerNodeWithNodeGroup) locNode;
        if (node.isRack()) {
          return loc;
        } else if (node.isNodeGroup()) {
          return node.getNetworkLocation();
        } else {
          // may be a data center
          return null;
        }
      } else {
        // not in cluster map, don't handle it
        return loc;
      }
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * Given a string representation of a node group for a specific network
   * location
   * 
   * @param loc
   *            a path-like string representation of a network location
   * @return a node group string
   */
  public String getNodeGroup(String loc) {
    netlock.readLock().lock();
    try {
      loc = NodeBase.normalize(loc);
      Node locNode = getNode(loc);
      if (locNode instanceof InnerNodeWithNodeGroup) {
        InnerNodeWithNodeGroup node = (InnerNodeWithNodeGroup) locNode;
        if (node.isNodeGroup()) {
          return loc;
        } else if (node.isRack()) {
          // not sure the node group for a rack
          return null;
        } else {
          // may be a leaf node
          if(!(node.getNetworkLocation() == null ||
              node.getNetworkLocation().isEmpty())) {
            return getNodeGroup(node.getNetworkLocation());
          } else {
            return NodeBase.ROOT;
          }
        }
      } else {
        // not in cluster map, don't handle it
        return loc;
      }
    } finally {
      netlock.readLock().unlock();
    }
  }

  @Override
  public boolean isOnSameRack( Node node1,  Node node2) {
    if (node1 == null || node2 == null ||
        node1.getParent() == null || node2.getParent() == null) {
      return false;
    }
      
    netlock.readLock().lock();
    try {
      return isSameParents(node1.getParent(), node2.getParent());
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * Check if two nodes are on the same node group (hypervisor) The
   * assumption here is: each nodes are leaf nodes.
   * 
   * @param node1
   *            one node (can be null)
   * @param node2
   *            another node (can be null)
   * @return true if node1 and node2 are on the same node group; false
   *         otherwise
   * @exception IllegalArgumentException
   *                when either node1 or node2 is null, or node1 or node2 do
   *                not belong to the cluster
   */
  @Override
  public boolean isOnSameNodeGroup(Node node1, Node node2) {
    if (node1 == null || node2 == null) {
      return false;
    }
    netlock.readLock().lock();
    try {
      return isSameParents(node1, node2);
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * Check if network topology is aware of NodeGroup
   */
  @Override
  public boolean isNodeGroupAware() {
    return true;
  }

  /** Add a leaf node
   * Update node counter & rack counter if necessary
   * @param node node to be added; can be null
   * @exception IllegalArgumentException if add a node to a leave 
   *                                     or node to be added is not a leaf
   */
  @Override
  public void add(Node node) {
    if (node==null) return;
    if( node instanceof InnerNode ) {
      throw new IllegalArgumentException(
        "Not allow to add an inner node: "+NodeBase.getPath(node));
    }
    netlock.writeLock().lock();
    try {
      Node rack = null;

      // if node only with default rack info, here we need to add default 
      // nodegroup info
      if (NetworkTopology.DEFAULT_RACK.equals(node.getNetworkLocation())) {
        node.setNetworkLocation(node.getNetworkLocation() + 
            NetworkTopologyWithNodeGroup.DEFAULT_NODEGROUP);
      }
      Node nodeGroup = getNode(node.getNetworkLocation());
      if (nodeGroup == null) {
        nodeGroup = new InnerNodeWithNodeGroup(node.getNetworkLocation());
      }
      rack = getNode(nodeGroup.getNetworkLocation());

      // rack should be an innerNode and with parent.
      // note: rack's null parent case is: node's topology only has one layer, 
      //       so rack is recognized as "/" and no parent. 
      // This will be recognized as a node with fault topology.
      if (rack != null && 
          (!(rack instanceof InnerNode) || rack.getParent() == null)) {
        throw new IllegalArgumentException("Unexpected data node " 
            + node.toString() 
            + " at an illegal network location");
      }
      if (clusterMap.add(node)) {
        LOG.info("Adding a new node: " + NodeBase.getPath(node));
        if (rack == null) {
          // We only track rack number here
          incrementRacks();
        }
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("NetworkTopology became:\n" + this.toString());
      }
    } finally {
      netlock.writeLock().unlock();
    }
  }

  /** Remove a node
   * Update node counter and rack counter if necessary
   * @param node node to be removed; can be null
   */
  @Override
  public void remove(Node node) {
    if (node==null) return;
    if( node instanceof InnerNode ) {
      throw new IllegalArgumentException(
          "Not allow to remove an inner node: "+NodeBase.getPath(node));
    }
    LOG.info("Removing a node: "+NodeBase.getPath(node));
    netlock.writeLock().lock();
    try {
      if (clusterMap.remove(node)) {
        Node nodeGroup = getNode(node.getNetworkLocation());
        if (nodeGroup == null) {
          nodeGroup = factory.newInnerNode(node.getNetworkLocation());
        }
        InnerNode rack = (InnerNode)getNode(nodeGroup.getNetworkLocation());
        if (rack == null) {
          numOfRacks--;
        }
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("NetworkTopology became:\n" + this.toString());
      }
    } finally {
      netlock.writeLock().unlock();
    }
  }

  @Override
  protected int getWeight(Node reader, Node node) {
    // 0 is local, 1 is same node group, 2 is same rack, 3 is off rack
    // Start off by initializing to off rack
    int weight = 3;
    if (reader != null) {
      if (reader.equals(node)) {
        weight = 0;
      } else if (isOnSameNodeGroup(reader, node)) {
        weight = 1;
      } else if (isOnSameRack(reader, node)) {
        weight = 2;
      }
    }
    return weight;
  }

  /**
   * Sort nodes array by their distances to <i>reader</i>.
   * <p/>
   * This is the same as {@link NetworkTopology#sortByDistance(Node, Node[],
   * int)} except with a four-level network topology which contains the
   * additional network distance of a "node group" which is between local and
   * same rack.
   *
   * @param reader    Node where data will be read
   * @param nodes     Available replicas with the requested data
   * @param activeLen Number of active nodes at the front of the array
   */
  @Override
  public void sortByDistance(Node reader, Node[] nodes, int activeLen) {
    // If reader is not a datanode (not in NetworkTopology tree), we need to
    // replace this reader with a sibling leaf node in tree.
    if (reader != null && !this.contains(reader)) {
      Node nodeGroup = getNode(reader.getNetworkLocation());
      if (nodeGroup != null && nodeGroup instanceof InnerNode) {
        InnerNode parentNode = (InnerNode) nodeGroup;
        // replace reader with the first children of its parent in tree
        reader = parentNode.getLeaf(0, null);
      } else {
        return;
      }
    }
    super.sortByDistance(reader, nodes, activeLen);
  }

  /** InnerNodeWithNodeGroup represents a switch/router of a data center, rack
   * or physical host. Different from a leaf node, it has non-null children.
   */
  static class InnerNodeWithNodeGroup extends InnerNodeImpl {
    public InnerNodeWithNodeGroup(String path) {
      super(path);
    }

    @Override
    public boolean isRack() {
      // it is node group
      if (getChildren().isEmpty()) {
        return false;
      }

      Node firstChild = getChildren().get(0);

      if (firstChild instanceof InnerNode) {
        Node firstGrandChild = (((InnerNode) firstChild).getChildren()).get(0);
        if (firstGrandChild instanceof InnerNode) {
          // it is datacenter
          return false;
        } else {
          return true;
        }
      }
      return false;
    }

    /**
     * Judge if this node represents a node group
     * 
     * @return true if it has no child or its children are not InnerNodes
     */
    boolean isNodeGroup() {
      if (getChildren().isEmpty()) {
        return true;
      }
      Node firstChild = getChildren().get(0);
      if (firstChild instanceof InnerNode) {
        // it is rack or datacenter
        return false;
      }
      return true;
    }
  }
}
