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

package org.apache.hadoop.yarn.server.resourcemanager.resourcetracker;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemStore;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.RMResourceTrackerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceListener;
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;
import org.junit.After;
import org.junit.Before;

import junit.framework.TestCase;

public class TestRMNMRPCResponseId extends TestCase {
  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  RMResourceTrackerImpl rmResourceTrackerImpl;
  ContainerTokenSecretManager containerTokenSecretManager =
    new ContainerTokenSecretManager();
  ResourceListener listener = new DummyResourceListener();
  private NodeId nodeid;
  
  private class DummyResourceListener implements ResourceListener {

    @Override
    public void addNode(NodeInfo nodeManager) {
      nodeid = nodeManager.getNodeID();
    }

    @Override
    public void removeNode(NodeInfo node) {
      /* do nothing */
    }

    @Override
    public void nodeUpdate(NodeInfo nodeInfo,
        Map<String, List<Container>> containers) {
    }
  }
  
  @Before
  public void setUp() {
    RMContext context = new ResourceManager.RMContextImpl(new MemStore());
    rmResourceTrackerImpl = new RMResourceTrackerImpl(containerTokenSecretManager, context);
    rmResourceTrackerImpl.init(new Configuration());
    rmResourceTrackerImpl.addListener(listener);
  }
  
  @After
  public void tearDown() {
    /* do nothing */
  }
  
  public void testRPCResponseId() throws IOException {
    String node = "localhost";
    Resource capability = recordFactory.newRecordInstance(Resource.class);
    RegisterNodeManagerRequest request = recordFactory.newRecordInstance(RegisterNodeManagerRequest.class);
    request.setHost(node);
    request.setContainerManagerPort(0);
    request.setHttpPort(0);
    request.setResource(capability);
    rmResourceTrackerImpl.registerNodeManager(node, 0, 0, capability);
    org.apache.hadoop.yarn.server.api.records.NodeStatus nodeStatus = recordFactory.
      newRecordInstance(org.apache.hadoop.yarn.server.api.records.NodeStatus.class);
    nodeStatus.setNodeId(nodeid);
    nodeStatus.setResponseId(0);
    NodeHealthStatus nodeHealthStatus = recordFactory.newRecordInstance(NodeHealthStatus.class);
    nodeHealthStatus.setIsNodeHealthy(true);
    nodeStatus.setNodeHealthStatus(nodeHealthStatus);
    HeartbeatResponse response = rmResourceTrackerImpl.nodeHeartbeat(nodeStatus);
    assertTrue(response.getResponseId() == 1);
    nodeStatus.setResponseId(response.getResponseId());
    response = rmResourceTrackerImpl.nodeHeartbeat(nodeStatus);
    assertTrue(response.getResponseId() == 2);   
    /* try calling with less response id */
    response = rmResourceTrackerImpl.nodeHeartbeat(nodeStatus);
    assertTrue(response.getResponseId() == 2);   
    nodeStatus.setResponseId(0);
    response = rmResourceTrackerImpl.nodeHeartbeat(nodeStatus);
    assertTrue(response.getReboot() == true);
  }
}