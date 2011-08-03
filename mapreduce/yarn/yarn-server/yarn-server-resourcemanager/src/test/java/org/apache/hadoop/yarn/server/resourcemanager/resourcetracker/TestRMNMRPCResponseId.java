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

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceTrackerService;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemStore;
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Before;

public class TestRMNMRPCResponseId extends TestCase {
  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  ResourceTrackerService resourceTrackerService;
  ContainerTokenSecretManager containerTokenSecretManager =
    new ContainerTokenSecretManager();
  private NodeId nodeid;
  
  @Before
  public void setUp() {
    Dispatcher dispatcher = new AsyncDispatcher();
    RMContext context = new RMContextImpl(new MemStore(), dispatcher, null,
        null);
    resourceTrackerService = new ResourceTrackerService(context, null, null,
        containerTokenSecretManager);
    resourceTrackerService.init(new Configuration());
  }
  
  @After
  public void tearDown() {
    /* do nothing */
  }
  
  public void testRPCResponseId() throws IOException {
    String node = "localhost";
    Resource capability = recordFactory.newRecordInstance(Resource.class);
    RegisterNodeManagerRequest request = recordFactory.newRecordInstance(RegisterNodeManagerRequest.class);
    NodeId nodeId = Records.newRecord(NodeId.class);
    nodeId.setHost(node);
    nodeId.setPort(1234);
    request.setNodeId(nodeId);
    request.setHttpPort(0);
    request.setResource(capability);

    RegisterNodeManagerRequest request1 = recordFactory
        .newRecordInstance(RegisterNodeManagerRequest.class);
    request1.setNodeId(nodeId);
    request1.setHttpPort(0);
    request1.setResource(capability);
    resourceTrackerService.registerNodeManager(request1);

    org.apache.hadoop.yarn.server.api.records.NodeStatus nodeStatus = recordFactory.
      newRecordInstance(org.apache.hadoop.yarn.server.api.records.NodeStatus.class);
    nodeStatus.setNodeId(nodeid);
    NodeHealthStatus nodeHealthStatus = recordFactory.newRecordInstance(NodeHealthStatus.class);
    nodeHealthStatus.setIsNodeHealthy(true);
    nodeStatus.setNodeHealthStatus(nodeHealthStatus);
    NodeHeartbeatRequest nodeHeartBeatRequest = recordFactory
        .newRecordInstance(NodeHeartbeatRequest.class);
    nodeHeartBeatRequest.setNodeStatus(nodeStatus);

    nodeStatus.setResponseId(0);
    HeartbeatResponse response = resourceTrackerService.nodeHeartbeat(
        nodeHeartBeatRequest).getHeartbeatResponse();
    assertTrue(response.getResponseId() == 1);

    nodeStatus.setResponseId(response.getResponseId());
    response = resourceTrackerService.nodeHeartbeat(nodeHeartBeatRequest)
        .getHeartbeatResponse();
    assertTrue(response.getResponseId() == 2);   

    /* try calling with less response id */
    response = resourceTrackerService.nodeHeartbeat(nodeHeartBeatRequest)
        .getHeartbeatResponse();
    assertTrue(response.getResponseId() == 2);

    nodeStatus.setResponseId(0);
    response = resourceTrackerService.nodeHeartbeat(nodeHeartBeatRequest)
        .getHeartbeatResponse();
    assertTrue(response.getReboot() == true);
  }
}