/*
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

package org.apache.slider.server.appmaster.model.mock;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Models the cluster itself: a set of mock cluster nodes.
 *
 * nodes retain the slot model with a limit of 2^8 slots/host -this
 * lets us use 24 bits of the container ID for hosts, and so simulate
 * larger hosts.
 *
 * upper 32: index into nodes in the cluster
 * NodeID hostname is the index in hex format; this is parsed down to the index
 * to resolve the host
 *
 * Important: container IDs will be reused as containers get recycled. This
 * is not an attempt to realistically mimic a real YARN cluster, just
 * simulate it enough for Slider to explore node re-use and its handling
 * of successful and unsuccessful allocations.
 *
 * There is little or no checking of valid parameters in here -this is for
 * test use, not production.
 */
public class MockYarnCluster {
  protected static final Logger LOG =
      LoggerFactory.getLogger(MockYarnCluster.class);

  private final int clusterSize;
  private final int containersPerNode;
  private MockYarnClusterNode[] nodes;

  MockYarnCluster(int clusterSize, int containersPerNode) {
    this.clusterSize = clusterSize;
    this.containersPerNode = containersPerNode;
    build();
  }

  public int getClusterSize() {
    return clusterSize;
  }

  @Override
  public String toString() {
    return "MockYarnCluster size=" + clusterSize + ", capacity=" +
        totalClusterCapacity()+ ", in use=" + containersInUse();
  }

  /**
   * Build the cluster.
   */
  private void build() {
    nodes = new MockYarnClusterNode[clusterSize];
    for (int i = 0; i < clusterSize; i++) {
      nodes[i] = new MockYarnClusterNode(i, containersPerNode);
    }
  }

  public MockYarnClusterNode nodeAt(int index) {
    return nodes[index];
  }

  MockYarnClusterNode lookup(String hostname) {
    int index = Integer.valueOf(hostname, 16);
    return nodeAt(index);
  }

  MockYarnClusterNode lookup(NodeId nodeId) {
    return lookup(nodeId.getHost());
  }

  MockYarnClusterNode lookupOwner(ContainerId cid) {
    return nodeAt(extractHost(cid.getContainerId()));
  }

  /**
   * Release a container: return true if it was actually in use.
   * @param cid container ID
   * @return the container released
   */
  MockYarnClusterContainer release(ContainerId cid) {
    int host = extractHost(cid.getContainerId());
    MockYarnClusterContainer inUse = nodeAt(host).release(cid.getContainerId());
    LOG.debug("Released {} inuse={}", cid, inUse);
    return inUse;
  }

  int containersInUse() {
    int count = 0;
    for (MockYarnClusterNode it : nodes) {
      count += it.containersInUse();
    }
    return count;
  }

  /**
   * Containers free.
   * @return
   */
  int containersFree() {
    return totalClusterCapacity() - containersInUse();
  }

  int totalClusterCapacity() {
    return clusterSize * containersPerNode;
  }

  /**
   * Reset all the containers.
   */
  public void reset() {
    for (MockYarnClusterNode node : nodes) {
      node.reset();
    }
  }

  /**
   * Bulk allocate the specific number of containers on a range of the cluster.
   * @param startNode start of the range
   * @param endNode end of the range
   * @param count count
   * @return the number actually allocated -it will be less the count supplied
   * if the node was full
   */
  public int bulkAllocate(int startNode, int endNode, int count) {
    int total = 0;
    for (int i = startNode; i <= endNode; i++) {
      total += nodeAt(i).bulkAllocate(count).size();
    }
    return total;
  }

  /**
   * Get the list of node reports. These are not cloned; updates will persist
   * in the nodemap
   * @return current node report list
   */
  List<NodeReport> getNodeReports() {
    List<NodeReport> reports = new ArrayList<>();

    for (MockYarnClusterNode n : nodes) {
      reports.add(n.nodeReport);
    }
    return reports;
  }

  /**
   * Model cluster nodes on the simpler "slot" model than the YARN-era
   * resource allocation model. Why? Easier to implement scheduling.
   * Of course, if someone does want to implement the full process...
   *
   */
  public static class MockYarnClusterNode {

    private final int nodeIndex;
    private final String hostname;
    private List<String> labels = new ArrayList<>();
    private final MockNodeId nodeId;
    private final MockYarnClusterContainer[] containers;
    private boolean offline;
    private NodeReport nodeReport;

    public MockYarnClusterNode(int index, int size) {
      nodeIndex = index;
      hostname = String.format(Locale.ENGLISH, "%08x", index);
      nodeId = new MockNodeId(hostname, 0);

      containers = new MockYarnClusterContainer[size];
      for (int i = 0; i < size; i++) {
        int cid = makeCid(index, i);
        MockContainerId mci = new MockContainerId(cid);
        containers[i] = new MockYarnClusterContainer(mci);
      }

      nodeReport = MockFactory.INSTANCE.newNodeReport(hostname, nodeId,
          NodeState.RUNNING, "");
    }

    public String getHostname() {
      return hostname;
    }

    public NodeId getNodeId() {
      return nodeId;
    }

    /**
     * Look up a container.
     * @param containerId
     * @return
     */
    public MockYarnClusterContainer lookup(int containerId) {
      return containers[extractContainer(containerId)];
    }

    /**
     * Go offline; release all containers.
     */
    public void goOffline() {
      if (!offline) {
        offline = true;
        reset();
      }
    }

    public void goOnline() {
      offline = false;
    }

    /**
     * Allocate a container -if one is available.
     * @return the container or null for none free
     * -or the cluster node is offline
     */
    public MockYarnClusterContainer allocate() {
      if (!offline) {
        for (int i = 0; i < containers.length; i++) {
          MockYarnClusterContainer c = containers[i];
          if (!c.busy) {
            c.busy = true;
            return c;
          }
        }
      }
      return null;
    }

    /**
     * Bulk allocate the specific number of containers.
     * @param count count
     * @return the list actually allocated -it will be less the count supplied
     * if the node was full
     */
    public List<MockYarnClusterContainer> bulkAllocate(int count) {
      List<MockYarnClusterContainer> result = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        MockYarnClusterContainer allocation = allocate();
        if (allocation == null) {
          break;
        }
        result.add(allocation);
      }
      return result;
    }

    /**
     * Release a container.
     * @param cid container ID
     * @return the container if the container was busy before the release
     */
    public MockYarnClusterContainer release(long cid) {
      MockYarnClusterContainer container = containers[extractContainer(cid)];
      boolean b = container.busy;
      container.busy = false;
      return b? container: null;
    }

    public String httpAddress() {
      return "http://$hostname/";
    }

    /**
     * Reset all the containers.
     */
    public void reset() {
      for (MockYarnClusterContainer cont : containers) {
        cont.reset();
      }
    }

    public int containersInUse() {
      int c = 0;
      for (MockYarnClusterContainer cont : containers) {
        c += cont.busy ? 1 : 0;
      }
      return c;
    }

    public int containersFree() {
      return containers.length - containersInUse();
    }
  }

  /**
   * Cluster container.
   */
  public static class MockYarnClusterContainer {
    private MockContainerId cid;
    private boolean busy;

    MockYarnClusterContainer(MockContainerId cid) {
      this.cid = cid;
    }

    public MockContainerId getCid() {
      return cid;
    }

    void reset() {
      busy = false;
    }
  }

  public static int makeCid(int hostIndex, int containerIndex) {
    return (hostIndex << 8) | containerIndex & 0xff;
  }

  public static final int extractHost(long cid) {
    return (int)((cid >>> 8) & 0xffff);
  }

  public static final int extractContainer(long cid) {
    return (int)(cid & 0xff);
  }

}
