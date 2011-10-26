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

import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.records.RegistrationResponse;
import org.apache.hadoop.yarn.server.resourcemanager.NMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceTrackerService;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Before;
import org.junit.Test;

public class TestNMExpiry {
  private static final Log LOG = LogFactory.getLog(TestNMExpiry.class);
  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  
  ResourceTrackerService resourceTrackerService;
  ContainerTokenSecretManager containerTokenSecretManager = 
    new ContainerTokenSecretManager();
  AtomicInteger test = new AtomicInteger();
  AtomicInteger notify = new AtomicInteger();

  private class TestNmLivelinessMonitor extends NMLivelinessMonitor {
    public TestNmLivelinessMonitor(Dispatcher dispatcher) {
      super(dispatcher);
    }

    @Override
    public void init(Configuration conf) {
      conf.setLong(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS, 1000);
      super.init(conf);
    }
    @Override
    protected void expire(NodeId id) {
        LOG.info("Expired  " + id);
        if (test.addAndGet(1) == 2) {
          try {
            /* delay atleast 2 seconds to make sure the 3rd one does not expire
             * 
             */
            Thread.sleep(2000);
          } catch(InterruptedException ie){}
          synchronized(notify) {
            notify.addAndGet(1);
            notify.notifyAll();
          }
        }
    }
  }

  @Before
  public void setUp() {
    Configuration conf = new Configuration();
    // Dispatcher that processes events inline
    Dispatcher dispatcher = new InlineDispatcher();
    dispatcher.register(SchedulerEventType.class,
        new InlineDispatcher.EmptyEventHandler());
    dispatcher.register(RMNodeEventType.class,
        new InlineDispatcher.EmptyEventHandler());
    RMContext context = new RMContextImpl(new MemStore(), dispatcher, null,
        null, null);
    NMLivelinessMonitor nmLivelinessMonitor = new TestNmLivelinessMonitor(
        dispatcher);
    nmLivelinessMonitor.init(conf);
    nmLivelinessMonitor.start();
    NodesListManager nodesListManager = new NodesListManager();
    nodesListManager.init(conf);
    resourceTrackerService = new ResourceTrackerService(context,
        nodesListManager, nmLivelinessMonitor, containerTokenSecretManager);
    
    resourceTrackerService.init(conf);
    resourceTrackerService.start();
  }

  private class ThirdNodeHeartBeatThread extends Thread {
    public void run() {
      int lastResponseID = 0;
      while (!stopT) {
        try {
          org.apache.hadoop.yarn.server.api.records.NodeStatus nodeStatus =
              recordFactory
                  .newRecordInstance(org.apache.hadoop.yarn.server.api.records.NodeStatus.class);
          nodeStatus.setNodeId(request3.getNodeId());
          nodeStatus.setResponseId(lastResponseID);
          nodeStatus.setNodeHealthStatus(recordFactory.newRecordInstance(NodeHealthStatus.class));
          nodeStatus.getNodeHealthStatus().setIsNodeHealthy(true);

          NodeHeartbeatRequest request = recordFactory
              .newRecordInstance(NodeHeartbeatRequest.class);
          request.setNodeStatus(nodeStatus);
          lastResponseID = resourceTrackerService.nodeHeartbeat(request)
              .getHeartbeatResponse().getResponseId();

          Thread.sleep(1000);
        } catch(Exception e) {
          LOG.info("failed to heartbeat ", e);
        }
      }
    } 
  }

  boolean stopT = false;
  RegisterNodeManagerRequest request3;

  @Test
  public void testNMExpiry() throws Exception {
    String hostname1 = "localhost1";
    String hostname2 = "localhost2";
    String hostname3 = "localhost3";
    Resource capability = recordFactory.newRecordInstance(Resource.class);

    RegisterNodeManagerRequest request1 = recordFactory
        .newRecordInstance(RegisterNodeManagerRequest.class);
    NodeId nodeId1 = Records.newRecord(NodeId.class);
    nodeId1.setPort(0);
    nodeId1.setHost(hostname1);
    request1.setNodeId(nodeId1);
    request1.setHttpPort(0);
    request1.setResource(capability);
    resourceTrackerService.registerNodeManager(request1);

    RegisterNodeManagerRequest request2 = recordFactory
        .newRecordInstance(RegisterNodeManagerRequest.class);
    NodeId nodeId2 = Records.newRecord(NodeId.class);
    nodeId2.setPort(0);
    nodeId2.setHost(hostname2);
    request2.setNodeId(nodeId2);
    request2.setHttpPort(0);
    request2.setResource(capability);
    resourceTrackerService.registerNodeManager(request2);

    request3 = recordFactory
        .newRecordInstance(RegisterNodeManagerRequest.class);
    NodeId nodeId3 = Records.newRecord(NodeId.class);
    nodeId3.setPort(0);
    nodeId3.setHost(hostname3);
    request3.setNodeId(nodeId3);
    request3.setHttpPort(0);
    request3.setResource(capability);
    RegistrationResponse thirdNodeRegResponse = resourceTrackerService
        .registerNodeManager(request3).getRegistrationResponse();

    /* test to see if hostanme 3 does not expire */
    stopT = false;
    new ThirdNodeHeartBeatThread().start();
    int timeOut = 0;
    synchronized (notify) {
      while (notify.get() == 0 && timeOut++ < 30) {
        notify.wait(1000);
      }
    }
    Assert.assertEquals(2, test.get()); 

    stopT = true;
  }
}
