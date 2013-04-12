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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.net.Node;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;

import com.google.common.collect.Lists;

/**
 * Test helper to generate mock nodes
 */
public class MockNodes {
  private static int NODE_ID = 0;
  private static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  public static List<RMNode> newNodes(int racks, int nodesPerRack,
                                        Resource perNode) {
    List<RMNode> list = Lists.newArrayList();
    for (int i = 0; i < racks; ++i) {
      for (int j = 0; j < nodesPerRack; ++j) {
        if (j == (nodesPerRack - 1)) {
          // One unhealthy node per rack.
          list.add(nodeInfo(i, perNode, NodeState.UNHEALTHY));
        }
        list.add(newNodeInfo(i, perNode));
      }
    }
    return list;
  }
  
  public static List<RMNode> deactivatedNodes(int racks, int nodesPerRack,
      Resource perNode) {
    List<RMNode> list = Lists.newArrayList();
    for (int i = 0; i < racks; ++i) {
      for (int j = 0; j < nodesPerRack; ++j) {
        NodeState[] allStates = NodeState.values();
        list.add(nodeInfo(i, perNode, allStates[j % allStates.length]));
      }
    }
    return list;
  }

  public static NodeId newNodeID(String host, int port) {
    NodeId nid = recordFactory.newRecordInstance(NodeId.class);
    nid.setHost(host);
    nid.setPort(port);
    return nid;
  }

  public static Resource newResource(int mem) {
    Resource rs = recordFactory.newRecordInstance(Resource.class);
    rs.setMemory(mem);
    return rs;
  }

  public static Resource newUsedResource(Resource total) {
    Resource rs = recordFactory.newRecordInstance(Resource.class);
    rs.setMemory((int)(Math.random() * total.getMemory()));
    return rs;
  }

  public static Resource newAvailResource(Resource total, Resource used) {
    Resource rs = recordFactory.newRecordInstance(Resource.class);
    rs.setMemory(total.getMemory() - used.getMemory());
    return rs;
  }

  private static class MockRMNodeImpl implements RMNode {
    private NodeId nodeId;
    private String hostName;
    private String nodeAddr;
    private String httpAddress;
    private int cmdPort;
    private Resource perNode;
    private String rackName;
    private NodeHealthStatus nodeHealthStatus;
    private NodeState state;

    public MockRMNodeImpl(NodeId nodeId, String nodeAddr, String httpAddress,
        Resource perNode, String rackName, NodeHealthStatus nodeHealthStatus,
        int cmdPort, String hostName, NodeState state) {
      this.nodeId = nodeId;
      this.nodeAddr = nodeAddr;
      this.httpAddress = httpAddress;
      this.perNode = perNode;
      this.rackName = rackName;
      this.nodeHealthStatus = nodeHealthStatus;
      this.cmdPort = cmdPort;
      this.hostName = hostName;
      this.state = state;
    }

    @Override
    public NodeId getNodeID() {
      return this.nodeId;
    }

    @Override
    public String getHostName() {
      return this.hostName;
    }

    @Override
    public int getCommandPort() {
      return this.cmdPort;
    }

    @Override
    public int getHttpPort() {
      return 0;
    }

    @Override
    public String getNodeAddress() {
      return this.nodeAddr;
    }

    @Override
    public String getHttpAddress() {
      return this.httpAddress;
    }

    @Override
    public NodeHealthStatus getNodeHealthStatus() {
      return this.nodeHealthStatus;
    }

    @Override
    public Resource getTotalCapability() {
      return this.perNode;
    }

    @Override
    public String getRackName() {
      return this.rackName;
    }

    @Override
    public Node getNode() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public NodeState getState() {
      return this.state;
    }

    @Override
    public List<ContainerId> getContainersToCleanUp() {
      return null;
    }

    @Override
    public List<ApplicationId> getAppsToCleanup() {
      return null;
    }

    @Override
    public void updateHeartbeatResponseForCleanup(HeartbeatResponse response) {
    }

    @Override
    public HeartbeatResponse getLastHeartBeatResponse() {
      return null;
    }

    @Override
    public List<UpdatedContainerInfo> pullContainerUpdates() {
      return new ArrayList<UpdatedContainerInfo>();
    }
  };

  private static RMNode buildRMNode(int rack, final Resource perNode, NodeState state, String httpAddr) {
    return buildRMNode(rack, perNode, state, httpAddr, NODE_ID++);
  }

  private static RMNode buildRMNode(int rack, final Resource perNode, NodeState state, String httpAddr, int hostnum) {
    final String rackName = "rack"+ rack;
    final int nid = hostnum;
    final String hostName = "host"+ nid;
    final int port = 123;
    final NodeId nodeID = newNodeID(hostName, port);
    final String httpAddress = httpAddr;
    final NodeHealthStatus nodeHealthStatus =
        recordFactory.newRecordInstance(NodeHealthStatus.class);
    if (state != NodeState.UNHEALTHY) {
      nodeHealthStatus.setIsNodeHealthy(true);
      nodeHealthStatus.setHealthReport("HealthyMe");
    }
    return new MockRMNodeImpl(nodeID, hostName, httpAddress, perNode, rackName,
        nodeHealthStatus, nid, hostName, state); 
  }

  public static RMNode nodeInfo(int rack, final Resource perNode,
      NodeState state) {
    return buildRMNode(rack, perNode, state, "N/A");
  }

  public static RMNode newNodeInfo(int rack, final Resource perNode) {
    return buildRMNode(rack, perNode, NodeState.RUNNING, "localhost:0");
  }

  public static RMNode newNodeInfo(int rack, final Resource perNode, int hostnum) {
    return buildRMNode(rack, perNode, null, "localhost:0", hostnum);
  }
}
