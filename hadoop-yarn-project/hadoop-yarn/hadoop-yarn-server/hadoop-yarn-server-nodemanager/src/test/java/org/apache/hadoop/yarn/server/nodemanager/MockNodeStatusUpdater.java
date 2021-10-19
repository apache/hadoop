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

package org.apache.hadoop.yarn.server.nodemanager;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;
import org.apache.hadoop.yarn.server.nodemanager.health.NodeHealthCheckerService;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.utils.YarnServerBuilderUtils;

/**
 * This class allows a node manager to run without without communicating with a
 * real RM.
 */
public class MockNodeStatusUpdater extends NodeStatusUpdaterImpl {
  static final Logger LOG =
       LoggerFactory.getLogger(MockNodeStatusUpdater.class);
  
  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private ResourceTracker resourceTracker;

  public MockNodeStatusUpdater(Context context, Dispatcher dispatcher,
      NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics) {
    super(context, dispatcher, healthChecker, metrics);
    resourceTracker = createResourceTracker();
  }

  protected ResourceTracker createResourceTracker() {
    return new MockResourceTracker();
  }

  @Override
  protected ResourceTracker getRMClient() {
    return resourceTracker;
  }
  @Override
  protected void stopRMProxy() {
    return;
  }
  
  protected static class MockResourceTracker implements ResourceTracker {
    private int heartBeatID;

    @Override
    public RegisterNodeManagerResponse registerNodeManager(
        RegisterNodeManagerRequest request) throws YarnException,
        IOException {
      RegisterNodeManagerResponse response = recordFactory
          .newRecordInstance(RegisterNodeManagerResponse.class);
      MasterKey masterKey = new MasterKeyPBImpl();
      masterKey.setKeyId(123);
      masterKey.setBytes(ByteBuffer.wrap(new byte[] { new Integer(123)
        .byteValue() }));
      response.setContainerTokenMasterKey(masterKey);
      response.setNMTokenMasterKey(masterKey);
      return response;
    }

    @Override
    public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
        throws YarnException, IOException {
      NodeStatus nodeStatus = request.getNodeStatus();
      LOG.info("Got heartbeat number " + heartBeatID);
      nodeStatus.setResponseId(heartBeatID++);

      NodeHeartbeatResponse nhResponse = YarnServerBuilderUtils
          .newNodeHeartbeatResponse(heartBeatID, null, null,
              null, null, null, 1000L);
      return nhResponse;
    }

    @Override
    public UnRegisterNodeManagerResponse unRegisterNodeManager(
        UnRegisterNodeManagerRequest request) throws YarnException, IOException {
      return recordFactory
          .newRecordInstance(UnRegisterNodeManagerResponse.class);
    }
  }
}
