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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.nodelabels.NodeLabelTestBase;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.NodeLabelsProvider;
import org.apache.hadoop.yarn.server.utils.YarnServerBuilderUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestNodeStatusUpdaterForLabels extends NodeLabelTestBase {
  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private NodeManager nm;
  protected DummyNodeLabelsProvider dummyLabelsProviderRef;

  @Before
  public void setup() {
    dummyLabelsProviderRef = new DummyNodeLabelsProvider();
  }

  @After
  public void tearDown() {
    if (null != nm) {
      ServiceOperations.stop(nm);
    }
  }

  private class ResourceTrackerForLabels implements ResourceTracker {
    int heartbeatID = 0;
    Set<NodeLabel> labels;

    private boolean receivedNMHeartbeat = false;
    private boolean receivedNMRegister = false;

    private MasterKey createMasterKey() {
      MasterKey masterKey = new MasterKeyPBImpl();
      masterKey.setKeyId(123);
      masterKey.setBytes(ByteBuffer.wrap(new byte[] { new Integer(123)
          .byteValue() }));
      return masterKey;
    }

    @Override
    public RegisterNodeManagerResponse registerNodeManager(
        RegisterNodeManagerRequest request) throws YarnException, IOException {
      labels = request.getNodeLabels();
      RegisterNodeManagerResponse response =
          recordFactory.newRecordInstance(RegisterNodeManagerResponse.class);
      response.setNodeAction(NodeAction.NORMAL);
      response.setContainerTokenMasterKey(createMasterKey());
      response.setNMTokenMasterKey(createMasterKey());
      response.setAreNodeLabelsAcceptedByRM(labels != null);
      synchronized (ResourceTrackerForLabels.class) {
        receivedNMRegister = true;
        ResourceTrackerForLabels.class.notifyAll();
      }
      return response;
    }

    public void waitTillHeartbeat() {
      if (receivedNMHeartbeat) {
        return;
      }
      int i = 500;
      while (!receivedNMHeartbeat && i > 0) {
        synchronized (ResourceTrackerForLabels.class) {
          if (!receivedNMHeartbeat) {
            try {
              System.out
                  .println("In ResourceTrackerForLabels waiting for heartbeat : "
                      + System.currentTimeMillis());
              ResourceTrackerForLabels.class.wait(500l);
              // to avoid race condition, i.e. sendOutofBandHeartBeat can be
              // sent before NSU thread has gone to sleep, hence we wait and try
              // to resend heartbeat again
              nm.getNodeStatusUpdater().sendOutofBandHeartBeat();
              ResourceTrackerForLabels.class.wait(500l);
              i--;
            } catch (InterruptedException e) {
              Assert.fail("Exception caught while waiting for Heartbeat");
              e.printStackTrace();
            }
          }
        }
      }
      if (!receivedNMHeartbeat) {
        Assert.fail("Heartbeat dint receive even after waiting");
      }
    }

    public void waitTillRegister() {
      if (receivedNMRegister) {
        return;
      }
      while (!receivedNMRegister) {
        synchronized (ResourceTrackerForLabels.class) {
          try {
            ResourceTrackerForLabels.class.wait();
          } catch (InterruptedException e) {
            Assert.fail("Exception caught while waiting for register");
            e.printStackTrace();
          }
        }
      }
    }

    /**
     * Flag to indicate received any
     */
    public void resetNMHeartbeatReceiveFlag() {
      synchronized (ResourceTrackerForLabels.class) {
        receivedNMHeartbeat = false;
      }
    }

    @Override
    public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
        throws YarnException, IOException {
      System.out.println("RTS receive heartbeat : "
          + System.currentTimeMillis());
      labels = request.getNodeLabels();
      NodeStatus nodeStatus = request.getNodeStatus();
      nodeStatus.setResponseId(heartbeatID++);

      NodeHeartbeatResponse nhResponse =
          YarnServerBuilderUtils.newNodeHeartbeatResponse(heartbeatID,
              NodeAction.NORMAL, null, null, null, null, 1000L);

      // to ensure that heartbeats are sent only when required.
      nhResponse.setNextHeartBeatInterval(Long.MAX_VALUE);
      nhResponse.setAreNodeLabelsAcceptedByRM(labels != null);

      synchronized (ResourceTrackerForLabels.class) {
        receivedNMHeartbeat = true;
        ResourceTrackerForLabels.class.notifyAll();
      }
      return nhResponse;
    }
  }

  public static class DummyNodeLabelsProvider extends NodeLabelsProvider {

    @SuppressWarnings("unchecked")
    private Set<NodeLabel> nodeLabels = CommonNodeLabelsManager.EMPTY_NODELABEL_SET;

    public DummyNodeLabelsProvider() {
      super(DummyNodeLabelsProvider.class.getName());
    }

    @Override
    public synchronized Set<NodeLabel> getNodeLabels() {
      return nodeLabels;
    }

    synchronized void setNodeLabels(Set<NodeLabel> nodeLabels) {
      this.nodeLabels = nodeLabels;
    }
  }

  private YarnConfiguration createNMConfigForDistributeNodeLabels() {
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.NODELABEL_CONFIGURATION_TYPE,
        YarnConfiguration.DISTRIBUTED_NODELABEL_CONFIGURATION_TYPE);
    return conf;
  }

  @Test
  public void testNodeStatusUpdaterForNodeLabels() throws InterruptedException,
      IOException {
    final ResourceTrackerForLabels resourceTracker =
        new ResourceTrackerForLabels();
    nm = new NodeManager() {
      @Override
      protected NodeLabelsProvider createNodeLabelsProvider(
          Configuration conf) throws IOException {
        return dummyLabelsProviderRef;
      }

      @Override
      protected NodeStatusUpdater createNodeStatusUpdater(Context context,
          Dispatcher dispatcher, NodeHealthCheckerService healthChecker,
          NodeLabelsProvider labelsProvider) {

        return new NodeStatusUpdaterImpl(context, dispatcher, healthChecker,
            metrics, labelsProvider) {
          @Override
          protected ResourceTracker getRMClient() {
            return resourceTracker;
          }

          @Override
          protected void stopRMProxy() {
            return;
          }
        };
      }
    };

    YarnConfiguration conf = createNMConfigForDistributeNodeLabels();
    nm.init(conf);
    resourceTracker.resetNMHeartbeatReceiveFlag();
    nm.start();
    resourceTracker.waitTillRegister();
    assertNLCollectionEquals(resourceTracker.labels,
        dummyLabelsProviderRef
            .getNodeLabels());

    resourceTracker.waitTillHeartbeat();// wait till the first heartbeat
    resourceTracker.resetNMHeartbeatReceiveFlag();

    // heartbeat with updated labels
    dummyLabelsProviderRef.setNodeLabels(toNodeLabelSet("P"));

    nm.getNodeStatusUpdater().sendOutofBandHeartBeat();
    resourceTracker.waitTillHeartbeat();
    assertNLCollectionEquals(resourceTracker.labels,
        dummyLabelsProviderRef
            .getNodeLabels());
    resourceTracker.resetNMHeartbeatReceiveFlag();

    // heartbeat without updating labels
    nm.getNodeStatusUpdater().sendOutofBandHeartBeat();
    resourceTracker.waitTillHeartbeat();
    resourceTracker.resetNMHeartbeatReceiveFlag();
    assertNull(
        "If no change in labels then null should be sent as part of request",
        resourceTracker.labels);
    
    // provider return with null labels
    dummyLabelsProviderRef.setNodeLabels(null);    
    nm.getNodeStatusUpdater().sendOutofBandHeartBeat();
    resourceTracker.waitTillHeartbeat();
    assertTrue("If provider sends null then empty labels should be sent",
        resourceTracker.labels.isEmpty());
    resourceTracker.resetNMHeartbeatReceiveFlag();

    nm.stop();
  }
}
