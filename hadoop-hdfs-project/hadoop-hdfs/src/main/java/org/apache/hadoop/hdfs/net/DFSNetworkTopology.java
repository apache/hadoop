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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

/**
 * The HDFS specific network topology class. The main purpose of doing this
 * subclassing is to add storage-type-aware chooseRandom method. All the
 * remaining parts should be the same.
 *
 * Currently a placeholder to test storage type info.
 */
public class DFSNetworkTopology extends NetworkTopology {

  private static final Random RANDOM = new Random();

  public static DFSNetworkTopology getInstance(Configuration conf) {

    DFSNetworkTopology nt = ReflectionUtils.newInstance(conf.getClass(
        DFSConfigKeys.DFS_NET_TOPOLOGY_IMPL_KEY,
        DFSConfigKeys.DFS_NET_TOPOLOGY_IMPL_DEFAULT,
        DFSNetworkTopology.class), conf);
    return (DFSNetworkTopology) nt.init(DFSTopologyNodeImpl.FACTORY);
  }

  /**
   * Randomly choose one node from <i>scope</i>, with specified storage type.
   *
   * If scope starts with ~, choose one from the all nodes except for the
   * ones in <i>scope</i>; otherwise, choose one from <i>scope</i>.
   * If excludedNodes is given, choose a node that's not in excludedNodes.
   *
   * @param scope range of nodes from which a node will be chosen
   * @param excludedNodes nodes to be excluded from
   * @param type the storage type we search for
   * @return the chosen node
   */
  public Node chooseRandomWithStorageType(final String scope,
      final Collection<Node> excludedNodes, StorageType type) {
    netlock.readLock().lock();
    try {
      if (scope.startsWith("~")) {
        return chooseRandomWithStorageType(
            NodeBase.ROOT, scope.substring(1), excludedNodes, type);
      } else {
        return chooseRandomWithStorageType(
            scope, null, excludedNodes, type);
      }
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * Randomly choose one node from <i>scope</i> with the given storage type.
   *
   * If scope starts with ~, choose one from the all nodes except for the
   * ones in <i>scope</i>; otherwise, choose one from <i>scope</i>.
   * If excludedNodes is given, choose a node that's not in excludedNodes.
   *
   * This call would make up to two calls. It first tries to get a random node
   * (with old method) and check if it satisfies. If yes, simply return it.
   * Otherwise, it make a second call (with the new method) by passing in a
   * storage type.
   *
   * This is for better performance reason. Put in short, the key note is that
   * the old method is faster but may take several runs, while the new method
   * is somewhat slower, and always succeed in one trial.
   * See HDFS-11535 for more detail.
   *
   * @param scope range of nodes from which a node will be chosen
   * @param excludedNodes nodes to be excluded from
   * @param type the storage type we search for
   * @return the chosen node
   */
  public Node chooseRandomWithStorageTypeTwoTrial(final String scope,
      final Collection<Node> excludedNodes, StorageType type) {
    netlock.readLock().lock();
    try {
      String searchScope;
      String excludedScope;
      if (scope.startsWith("~")) {
        searchScope = NodeBase.ROOT;
        excludedScope = scope.substring(1);
      } else {
        searchScope = scope;
        excludedScope = null;
      }
      // next do a two-trial search
      // first trial, call the old method, inherited from NetworkTopology
      Node n = chooseRandom(searchScope, excludedScope, excludedNodes);
      if (n == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("No node to choose.");
        }
        // this means there is simply no node to choose from
        return null;
      }
      Preconditions.checkArgument(n instanceof DatanodeDescriptor);
      DatanodeDescriptor dnDescriptor = (DatanodeDescriptor)n;

      if (dnDescriptor.hasStorageType(type)) {
        // the first trial succeeded, just return
        return dnDescriptor;
      } else {
        // otherwise, make the second trial by calling the new method
        LOG.debug("First trial failed, node has no type {}, " +
            "making second trial carrying this type", type);
        return chooseRandomWithStorageType(searchScope, excludedScope,
            excludedNodes, type);
      }
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * Choose a random node based on given scope, excludedScope and excludedNodes
   * set. Although in general the topology has at most three layers, this class
   * will not impose such assumption.
   *
   * At high level, the idea is like this, say:
   *
   * R has two children A and B, and storage type is X, say:
   * A has X = 6 (rooted at A there are 6 datanodes with X) and B has X = 8.
   *
   * Then R will generate a random int between 1~14, if it's <= 6, recursively
   * call into A, otherwise B. This will maintain a uniformed randomness of
   * choosing datanodes.
   *
   * The tricky part is how to handle excludes.
   *
   * For excludedNodes, since this set is small: currently the main reason of
   * being an excluded node is because it already has a replica. So randomly
   * picking up this node again should be rare. Thus we only check that, if the
   * chosen node is excluded, we do chooseRandom again.
   *
   * For excludedScope, we locate the root of the excluded scope. Subtracting
   * all it's ancestors' storage counters accordingly, this way the excluded
   * root is out of the picture.
   *
   * @param scope the scope where we look for node.
   * @param excludedScope the scope where the node must NOT be from.
   * @param excludedNodes the returned node must not be in this set
   * @return a node with required storage type
   */
  @VisibleForTesting
  Node chooseRandomWithStorageType(final String scope,
      String excludedScope, final Collection<Node> excludedNodes,
      StorageType type) {
    if (excludedScope != null) {
      if (scope.startsWith(excludedScope)) {
        return null;
      }
      if (!excludedScope.startsWith(scope)) {
        excludedScope = null;
      }
    }
    Node node = getNode(scope);
    if (node == null) {
      LOG.debug("Invalid scope {}, non-existing node", scope);
      return null;
    }
    if (!(node instanceof DFSTopologyNodeImpl)) {
      // a node is either DFSTopologyNodeImpl, or a DatanodeDescriptor
      // if a node is DatanodeDescriptor and excludedNodes contains it,
      // return null;
      if (excludedNodes != null && excludedNodes.contains(node)) {
        LOG.debug("{} in excludedNodes", node);
        return null;
      }
      return ((DatanodeDescriptor) node).hasStorageType(type) ? node : null;
    }
    DFSTopologyNodeImpl root = (DFSTopologyNodeImpl)node;
    Node excludeRoot = excludedScope == null ? null : getNode(excludedScope);

    // check to see if there are nodes satisfying the condition at all
    int availableCount = root.getSubtreeStorageCount(type);
    if (excludeRoot != null && root.isAncestor(excludeRoot)) {
      if (excludeRoot instanceof DFSTopologyNodeImpl) {
        availableCount -= ((DFSTopologyNodeImpl)excludeRoot)
            .getSubtreeStorageCount(type);
      } else {
        availableCount -= ((DatanodeDescriptor)excludeRoot)
            .hasStorageType(type) ? 1 : 0;
      }
    }
    if (excludedNodes != null) {
      for (Node excludedNode : excludedNodes) {
        if (excludeRoot != null && isNodeInScope(excludedNode, excludedScope)) {
          continue;
        }
        if (excludedNode instanceof DatanodeDescriptor) {
          availableCount -= ((DatanodeDescriptor) excludedNode)
              .hasStorageType(type) ? 1 : 0;
        } else if (excludedNode instanceof DFSTopologyNodeImpl) {
          availableCount -= ((DFSTopologyNodeImpl) excludedNode)
              .getSubtreeStorageCount(type);
        } else if (excludedNode instanceof DatanodeInfo) {
          // find out the corresponding DatanodeDescriptor object, beacuse
          // we need to get its storage type info.
          // could be expensive operation, fortunately the size of excluded
          // nodes set is supposed to be very small.
          String nodeLocation = excludedNode.getNetworkLocation()
              + "/" + excludedNode.getName();
          DatanodeDescriptor dn = (DatanodeDescriptor)getNode(nodeLocation);
          if (dn == null) {
            continue;
          }
          availableCount -= dn.hasStorageType(type)? 1 : 0;
        } else {
          LOG.error("Unexpected node type: {}.", excludedNode.getClass());
        }
      }
    }
    if (availableCount <= 0) {
      // should never be <0 in general, adding <0 check for safety purpose
      return null;
    }
    // to this point, it is guaranteed that there is at least one node
    // that satisfies the requirement.
    Node chosen =
        chooseRandomWithStorageTypeAndExcludeRoot(root, excludeRoot, type,
            excludedNodes);
    LOG.debug("chooseRandom returning {}", chosen);
    return chosen;
  }

  private boolean isNodeInScope(Node node, String scope) {
    if (!scope.endsWith(NodeBase.PATH_SEPARATOR_STR)) {
      scope += NodeBase.PATH_SEPARATOR_STR;
    }
    String nodeLocation =
        node.getNetworkLocation() + NodeBase.PATH_SEPARATOR_STR;
    return nodeLocation.startsWith(scope);
  }

  /**
   * Choose a random node that has the required storage type, under the given
   * root, with an excluded subtree root (could also just be a leaf node).
   *
   * @param root the root node where we start searching for a datanode
   * @param excludeRoot the root of the subtree what should be excluded
   * @param type the expected storage type
   * @param excludedNodes the list of nodes to be excluded
   * @return a random datanode, with the storage type, and is not in excluded
   * scope
   */
  private Node chooseRandomWithStorageTypeAndExcludeRoot(
      DFSTopologyNodeImpl root, Node excludeRoot, StorageType type,
      Collection<Node> excludedNodes) {
    Node chosenNode;
    if (root.isRack()) {
      // children are datanode descriptor
      ArrayList<Node> candidates = new ArrayList<>();
      for (Node node : root.getChildren()) {
        if (node.equals(excludeRoot) || (excludedNodes != null && excludedNodes
            .contains(node))) {
          continue;
        }
        DatanodeDescriptor dnDescriptor = (DatanodeDescriptor)node;
        if (dnDescriptor.hasStorageType(type)) {
          candidates.add(node);
        }
      }
      if (candidates.size() == 0) {
        return null;
      }
      // to this point, all nodes in candidates are valid choices, and they are
      // all datanodes, pick a random one.
      chosenNode = candidates.get(RANDOM.nextInt(candidates.size()));
    } else {
      // the children are inner nodes
      ArrayList<DFSTopologyNodeImpl> candidates =
          getEligibleChildren(root, excludeRoot, type, excludedNodes);
      if (candidates.size() == 0) {
        return null;
      }
      // again, all children are also inner nodes, we can do this cast.
      // to maintain uniformality, the search needs to be based on the counts
      // of valid datanodes. Below is a random weighted choose.
      int totalCounts = 0;
      int[] countArray = new int[candidates.size()];
      for (int i = 0; i < candidates.size(); i++) {
        DFSTopologyNodeImpl innerNode = candidates.get(i);
        int subTreeCount = innerNode.getSubtreeStorageCount(type);
        totalCounts += subTreeCount;
        countArray[i] = subTreeCount;
      }
      // generate a random val between [1, totalCounts]
      int randomCounts = RANDOM.nextInt(totalCounts) + 1;
      int idxChosen = 0;
      // searching for the idxChosen can potentially be done with binary
      // search, but does not seem to worth it here.
      for (int i = 0; i < countArray.length; i++) {
        if (randomCounts <= countArray[i]) {
          idxChosen = i;
          break;
        }
        randomCounts -= countArray[i];
      }
      DFSTopologyNodeImpl nextRoot = candidates.get(idxChosen);
      chosenNode = chooseRandomWithStorageTypeAndExcludeRoot(
          nextRoot, excludeRoot, type, excludedNodes);
    }
    return chosenNode;
  }

  /**
   * Given root, excluded root and storage type. Find all the children of the
   * root, that has the storage type available. One check is that if the
   * excluded root is under a children, this children must subtract the storage
   * count of the excluded root.
   * @param root the subtree root we check.
   * @param excludeRoot the root of the subtree that should be excluded.
   * @param type the storage type we look for.
   * @param excludedNodes the list of excluded nodes.
   * @return a list of possible nodes, each of them is eligible as the next
   * level root we search.
   */
  private ArrayList<DFSTopologyNodeImpl> getEligibleChildren(
      DFSTopologyNodeImpl root, Node excludeRoot, StorageType type,
      Collection<Node> excludedNodes) {
    ArrayList<DFSTopologyNodeImpl> candidates = new ArrayList<>();
    int excludeCount = 0;
    if (excludeRoot != null && root.isAncestor(excludeRoot)) {
      // the subtree to be excluded is under the given root,
      // find out the number of nodes to be excluded.
      if (excludeRoot instanceof DFSTopologyNodeImpl) {
        // if excludedRoot is an inner node, get the counts of all nodes on
        // this subtree of that storage type.
        excludeCount = ((DFSTopologyNodeImpl) excludeRoot)
            .getSubtreeStorageCount(type);
      } else {
        // if excludedRoot is a datanode, simply ignore this one node
        if (((DatanodeDescriptor) excludeRoot).hasStorageType(type)) {
          excludeCount = 1;
        }
      }
    }
    // have calculated the number of storage counts to be excluded.
    // walk through all children to check eligibility.
    for (Node node : root.getChildren()) {
      DFSTopologyNodeImpl dfsNode = (DFSTopologyNodeImpl) node;
      int storageCount = dfsNode.getSubtreeStorageCount(type);
      if (excludeRoot != null && excludeCount != 0 &&
          (dfsNode.isAncestor(excludeRoot) || dfsNode.equals(excludeRoot))) {
        storageCount -= excludeCount;
      }
      if (excludedNodes != null) {
        for (Node excludedNode : excludedNodes) {
          if (excludeRoot != null && isNodeInScope(excludedNode,
              NodeBase.getPath(excludeRoot))) {
            continue;
          }
          if (isNodeInScope(excludedNode, NodeBase.getPath(node))) {
            if (excludedNode instanceof DatanodeDescriptor) {
              storageCount -=
                  ((DatanodeDescriptor) excludedNode).hasStorageType(type) ?
                      1 : 0;
            } else if (excludedNode instanceof DFSTopologyNodeImpl) {
              storageCount -= ((DFSTopologyNodeImpl) excludedNode)
                  .getSubtreeStorageCount(type);
            }
          }
        }
      }
      if (storageCount > 0) {
        candidates.add(dfsNode);
      }
    }
    return candidates;
  }
}
