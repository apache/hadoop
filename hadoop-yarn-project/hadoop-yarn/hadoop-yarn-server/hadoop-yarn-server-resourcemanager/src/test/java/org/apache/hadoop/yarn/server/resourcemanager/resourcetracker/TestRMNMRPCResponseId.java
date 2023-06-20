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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.resourcemanager.NMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceTrackerService;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("rawtypes")
public class TestRMNMRPCResponseId {
  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  ResourceTrackerService resourceTrackerService;
  private NodeId nodeId;

  @Before
  public void setUp() {
    Configuration conf = new Configuration();
    // Dispatcher that processes events inline
    Dispatcher dispatcher = new InlineDispatcher();
    dispatcher.register(SchedulerEventType.class, new EventHandler<Event>() {
      @Override
      public void handle(Event event) {
        ; // ignore
      }
    });
    RMContext context =
        new RMContextImpl(dispatcher, null, null, null, null,
          null, new RMContainerTokenSecretManager(conf),
          new NMTokenSecretManagerInRM(conf), null, null);
    dispatcher.register(RMNodeEventType.class,
        new ResourceManager.NodeEventDispatcher(context));
    NodesListManager nodesListManager = new NodesListManager(context);
    nodesListManager.init(conf);
    
    context.getContainerTokenSecretManager().rollMasterKey();
    context.getNMTokenSecretManager().rollMasterKey();
    resourceTrackerService = new ResourceTrackerService(context,
        nodesListManager, new NMLivelinessMonitor(dispatcher),
        context.getContainerTokenSecretManager(),
        context.getNMTokenSecretManager());
    resourceTrackerService.init(conf);
  }
  
  @After
  public void tearDown() {
    /* do nothing */
  }

  @Test
  public void testRPCResponseId() throws IOException, YarnException {
    String node = "localhost";
    Resource capability = Resources.createResource(1024);
    RegisterNodeManagerRequest request = recordFactory.newRecordInstance(RegisterNodeManagerRequest.class);
    nodeId = NodeId.newInstance(node, 1234);
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
    nodeStatus.setNodeId(nodeId);
    NodeHealthStatus nodeHealthStatus = recordFactory.newRecordInstance(NodeHealthStatus.class);
    nodeHealthStatus.setIsNodeHealthy(true);
    nodeStatus.setNodeHealthStatus(nodeHealthStatus);
    NodeHeartbeatRequest nodeHeartBeatRequest = recordFactory
        .newRecordInstance(NodeHeartbeatRequest.class);
    nodeHeartBeatRequest.setNodeStatus(nodeStatus);

    nodeStatus.setResponseId(0);
    NodeHeartbeatResponse response = resourceTrackerService.nodeHeartbeat(
        nodeHeartBeatRequest);
    Assert.assertTrue(response.getResponseId() == 1);

    nodeStatus.setResponseId(response.getResponseId());
    response = resourceTrackerService.nodeHeartbeat(nodeHeartBeatRequest);
    Assert.assertTrue(response.getResponseId() == 2);   

    /* try calling with less response id */
    response = resourceTrackerService.nodeHeartbeat(nodeHeartBeatRequest);
    Assert.assertTrue(response.getResponseId() == 2);

    nodeStatus.setResponseId(0);
    response = resourceTrackerService.nodeHeartbeat(nodeHeartBeatRequest);
    Assert.assertTrue(NodeAction.RESYNC.equals(response.getNodeAction()));
    Assert.assertEquals("Too far behind rm response id:2 nm response id:0",
      response.getDiagnosticsMessage());
  }
}