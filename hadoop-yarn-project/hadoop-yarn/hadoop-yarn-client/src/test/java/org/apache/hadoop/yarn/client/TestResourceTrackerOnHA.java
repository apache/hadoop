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

package org.apache.hadoop.yarn.client;

import java.io.IOException;

import org.junit.Assert;

import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.ServerRMProxy;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestResourceTrackerOnHA extends ProtocolHATestBase{

  private ResourceTracker resourceTracker = null;

  @Before
  public void initiate() throws Exception {
    startHACluster(0, false, true, false);
    this.resourceTracker = getRMClient();
  }

  @After
  public void shutDown() {
    if(this.resourceTracker != null) {
      RPC.stopProxy(this.resourceTracker);
    }
  }

  @Test(timeout = 15000)
  public void testResourceTrackerOnHA() throws Exception {
    NodeId nodeId = NodeId.newInstance("localhost", 0);
    Resource resource = Resource.newInstance(2048, 4);

    // make sure registerNodeManager works when failover happens
    RegisterNodeManagerRequest request =
        RegisterNodeManagerRequest.newInstance(nodeId, 0, resource,
            YarnVersionInfo.getVersion(), null, null);
    resourceTracker.registerNodeManager(request);
    Assert.assertTrue(waitForNodeManagerToConnect(10000, nodeId));

    // restart the failover thread, and make sure nodeHeartbeat works
    failoverThread = createAndStartFailoverThread();
    NodeStatus status =
        NodeStatus.newInstance(NodeId.newInstance("localhost", 0), 0, null,
            null, null);
    NodeHeartbeatRequest request2 =
        NodeHeartbeatRequest.newInstance(status, null, null);
    resourceTracker.nodeHeartbeat(request2);
  }

  private ResourceTracker getRMClient() throws IOException {
    return ServerRMProxy.createRMProxy(this.conf, ResourceTracker.class);
  }

  private boolean waitForNodeManagerToConnect(int timeout, NodeId nodeId)
      throws Exception {
    for (int i = 0; i < timeout / 100; i++) {
      if (getActiveRM().getRMContext().getRMNodes().containsKey(nodeId)) {
        return true;
      }
      Thread.sleep(100);
    }
    return false;
  }
}
