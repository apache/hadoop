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
package org.apache.hadoop.hdfs.net;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * This class extends DFSNetworkTopology to provide weight-based chooseRandom method.
 * <p>
 * The basic idea of this class is very similar to add virtual nodes to the topology.
 * Multiple virtual nodes can map to the same real data node. The number of virtual nodes
 * that one data node has is determined by its weight.
 * <p>
 * Here is an example to show how this idea works:
 * <p>
 * Suppose we have 3 data nodes: dn1(/rack1), dn2(/rack1), dn3(/rack2).
 * dn1 has weight 3, dn2 has weight 2, dn3 has weight 1.
 * Here is what the topology tree looks like: (dn' means a virtual node)
 * <p>
 *                   Root(6)
 *                 /         \
 *            rack1(5)       rack2(1)
 *              |                |
 *  [dn1',dn1',dn1',dn2',dn2']  [dn3']
 * <p>
 * when chooseRandom is invoked on the root, all the virtual nodes have the same
 * probability to be chosen. And if a virtual node is chosen, it will be mapped to
 * the real data node, so the probability of dn1~3 to be chosen is 1/2, 1/3, 1/6.
 * <p>
 * In fact, we don't need to really add virtual nodes to the topology, as currently
 * the chooseRandom method in based on the node count of InnerNode. We can just
 * define a method to abstract how to get the num of virtual nodes of a data node.
 * In old DFSNetworkTopology implementation, it just returns 1, but in this class,
 * it will return the weight of the data node.
 */
public class DFSNetworkTopologyWithWeight extends DFSNetworkTopology {

  private DNSToWeightMapping weightMapping;

  /**
   * Store the weight of all nodes in this topology.
   * The weight of a node should not be changed after it is added to the topology.
   */
  private final Map<String, Integer> weightMap = new HashMap<>();

  public DFSNetworkTopologyWithWeight() {
    init(new DFSTopologyNodeImplWithWeight.Factory(this::getDataNodeWeight));
  }

  @Override
  public void init(Configuration conf) {
    this.weightMapping = createWeightMapping(conf);
    this.weightMapping.setConf(conf);
  }

  private static DNSToWeightMapping createWeightMapping(Configuration conf) {
    return ReflectionUtils.newInstance(conf.getClass(
        DFSConfigKeys.DFS_NET_TOPOLOGY_NODE_WEIGHT_MAPPING_IMPL_KEY,
        DFSConfigKeys.DFS_NET_TOPOLOGY_NODE_WEIGHT_MAPPING_IMPL_DEFAULT,
        DNSToWeightMapping.class), conf);
  }

  @Override
  public void remove(Node node) {
    netlock.writeLock().lock();
    try {
      super.remove(node);
      if (node != null) {
        weightMap.remove(node.getName());
      }
    } finally {
      netlock.writeLock().unlock();
    }
  }

  private int getDataNodeWeight(DatanodeDescriptor dn) {
    String key = dn.getName();
    Integer weight = weightMap.get(key);
    if (weight == null) {
      weight = weightMapping.resolve(dn);
      Preconditions.checkNotNull(weight, "weight is null for " + key);
      weightMap.put(key, weight);
    }
    return weight;
  }

  /**
   * We don't add virtual nodes as leaf nodes, so when we need to randomly pick
   * one node from rack inner node, we should use random weighted choose.
   */
  @Override
  protected Node randomPickFromCandidates(List<Node> candidates) {
    return weightedRandomPickFromCandidates(candidates,
        node -> getNodeCount((DatanodeDescriptor) node));
  }

  /**
   * Return the weight of the given data node to represent the number of
   * virtual nodes as described in DFSNetworkTopologyWithWeight.
   */
  @Override
  protected int getNodeCount(DatanodeDescriptor dn) {
    return getDataNodeWeight(dn);
  }

  /**
   * The superclass's chooseRandomWithStorageTypeTwoTrial will try old
   * chooseRandom method first, which doesn't support weighted choose.
   * So we override it here to use the new method directly.
   */
  @Override
  public Node chooseRandomWithStorageTypeTwoTrial(String scope,
      Collection<Node> excludedNodes, StorageType type) {
    return chooseRandomWithStorageType(scope, excludedNodes, type);
  }

  /**
   * Reload the weight mapping.
   * <p>
   * Note that the weightMap will not be updated.
   */
  public void reloadMapping() {
    weightMapping.reload();
  }

}
