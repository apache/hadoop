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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.Thread.State;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.ServerSocketUtil;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeAttributeType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.nodelabels.NodeLabelTestBase;
import org.apache.hadoop.yarn.nodelabels.NodeLabelUtil;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;
import org.apache.hadoop.yarn.server.nodemanager.health.NodeHealthCheckerService;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.NodeAttributesProvider;
import org.apache.hadoop.yarn.server.utils.YarnServerBuilderUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test NodeStatusUpdater for node attributes.
 */
public class TestNodeStatusUpdaterForAttributes extends NodeLabelTestBase {
  private static final RecordFactory RECORD_FACTORY =
      RecordFactoryProvider.getRecordFactory(null);

  private NodeManager nm;
  private DummyNodeAttributesProvider dummyAttributesProviderRef;

  @Before
  public void setup() {
    dummyAttributesProviderRef = new DummyNodeAttributesProvider();
  }

  @After
  public void tearDown() {
    if (null != nm) {
      ServiceOperations.stop(nm);
    }
  }

  private class ResourceTrackerForAttributes implements ResourceTracker {
    private int heartbeatID = 0;
    private Set<NodeAttribute> attributes;

    private boolean receivedNMHeartbeat = false;
    private boolean receivedNMRegister = false;

    private MasterKey createMasterKey() {
      MasterKey masterKey = new MasterKeyPBImpl();
      masterKey.setKeyId(123);
      masterKey.setBytes(
          ByteBuffer.wrap(new byte[] {new Integer(123).byteValue() }));
      return masterKey;
    }

    @Override
    public RegisterNodeManagerResponse registerNodeManager(
        RegisterNodeManagerRequest request) throws YarnException, IOException {
      attributes = request.getNodeAttributes();
      RegisterNodeManagerResponse response =
          RECORD_FACTORY.newRecordInstance(RegisterNodeManagerResponse.class);
      response.setNodeAction(NodeAction.NORMAL);
      response.setContainerTokenMasterKey(createMasterKey());
      response.setNMTokenMasterKey(createMasterKey());
      response.setAreNodeAttributesAcceptedByRM(attributes != null);
      synchronized (ResourceTrackerForAttributes.class) {
        receivedNMRegister = true;
        ResourceTrackerForAttributes.class.notifyAll();
      }
      return response;
    }

    public void waitTillHeartbeat()
        throws InterruptedException, TimeoutException {
      GenericTestUtils.waitFor(() -> receivedNMHeartbeat, 100, 30000);
      if (!receivedNMHeartbeat) {
        Assert.fail("Heartbeat is not received even after waiting");
      }
    }

    public void waitTillRegister()
        throws InterruptedException, TimeoutException {
      GenericTestUtils.waitFor(() -> receivedNMRegister, 100, 30000);
      if (!receivedNMRegister) {
        Assert.fail("Registration is not received even after waiting");
      }
    }

    /**
     * Flag to indicate received any.
     */
    public void resetNMHeartbeatReceiveFlag() {
      synchronized (ResourceTrackerForAttributes.class) {
        receivedNMHeartbeat = false;
      }
    }

    @Override
    public NodeHeartbeatResponse nodeHeartbeat(
        NodeHeartbeatRequest request) {
      attributes = request.getNodeAttributes();
      NodeStatus nodeStatus = request.getNodeStatus();
      nodeStatus.setResponseId(heartbeatID++);

      NodeHeartbeatResponse nhResponse = YarnServerBuilderUtils
          .newNodeHeartbeatResponse(heartbeatID, NodeAction.NORMAL, null, null,
              null, null, 1000L);

      // to ensure that heartbeats are sent only when required.
      nhResponse.setNextHeartBeatInterval(Long.MAX_VALUE);
      nhResponse.setAreNodeAttributesAcceptedByRM(attributes != null);

      synchronized (ResourceTrackerForAttributes.class) {
        receivedNMHeartbeat = true;
        ResourceTrackerForAttributes.class.notifyAll();
      }
      return nhResponse;
    }

    @Override
    public UnRegisterNodeManagerResponse unRegisterNodeManager(
        UnRegisterNodeManagerRequest request) {
      return null;
    }
  }

  /**
   * A dummy NodeAttributesProvider class for tests.
   */
  public static class DummyNodeAttributesProvider
      extends NodeAttributesProvider {

    public DummyNodeAttributesProvider() {
      super("DummyNodeAttributesProvider");
      // disable the fetch timer.
      setIntervalTime(-1);
    }

    @Override
    protected void cleanUp() throws Exception {
      // fake implementation, nothing to cleanup
    }

    @Override
    public TimerTask createTimerTask() {
      return new TimerTask() {
        @Override
        public void run() {
          setDescriptors(Collections.unmodifiableSet(new HashSet<>(0)));
        }
      };
    }
  }

  private YarnConfiguration createNMConfigForDistributeNodeAttributes() {
    YarnConfiguration conf = new YarnConfiguration();
    return conf;
  }

  @Test(timeout = 20000)
  public void testNodeStatusUpdaterForNodeAttributes()
      throws InterruptedException, IOException, TimeoutException {
    final ResourceTrackerForAttributes resourceTracker =
        new ResourceTrackerForAttributes();
    nm = new NodeManager() {
      @Override
      protected NodeAttributesProvider createNodeAttributesProvider(
          Configuration conf) throws IOException {
        return dummyAttributesProviderRef;
      }

      @Override
      protected NodeStatusUpdater createNodeStatusUpdater(
          Context context, Dispatcher dispatcher,
          NodeHealthCheckerService healthChecker) {

        return new NodeStatusUpdaterImpl(context, dispatcher, healthChecker,
            metrics) {
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

    YarnConfiguration conf = createNMConfigForDistributeNodeAttributes();
    conf.setLong(YarnConfiguration.NM_NODE_ATTRIBUTES_RESYNC_INTERVAL, 2000);
    conf.set(YarnConfiguration.NM_LOCALIZER_ADDRESS,
        "0.0.0.0:" + ServerSocketUtil.getPort(8040, 10));

    nm.init(conf);
    resourceTracker.resetNMHeartbeatReceiveFlag();
    nm.start();
    resourceTracker.waitTillRegister();
    assertTrue(NodeLabelUtil
        .isNodeAttributesEquals(dummyAttributesProviderRef.getDescriptors(),
            resourceTracker.attributes));

    resourceTracker.waitTillHeartbeat(); // wait till the first heartbeat
    resourceTracker.resetNMHeartbeatReceiveFlag();

    // heartbeat with updated attributes
    NodeAttribute attribute1 = NodeAttribute
        .newInstance(NodeAttribute.PREFIX_DISTRIBUTED, "Attr1",
            NodeAttributeType.STRING, "V1");
    dummyAttributesProviderRef.setDescriptors(ImmutableSet.of(attribute1));

    sendOutofBandHeartBeat();
    resourceTracker.waitTillHeartbeat();
    assertTrue(NodeLabelUtil
        .isNodeAttributesEquals(dummyAttributesProviderRef.getDescriptors(),
            resourceTracker.attributes));
    resourceTracker.resetNMHeartbeatReceiveFlag();

    // heartbeat without updating attributes
    sendOutofBandHeartBeat();
    resourceTracker.waitTillHeartbeat();
    resourceTracker.resetNMHeartbeatReceiveFlag();
    assertNull("If no change in attributes"
            + " then null should be sent as part of request",
        resourceTracker.attributes);

    // provider return with null attributes
    dummyAttributesProviderRef.setDescriptors(null);
    sendOutofBandHeartBeat();
    resourceTracker.waitTillHeartbeat();
    assertNotNull("If provider sends null"
            + " then empty label set should be sent and not null",
        resourceTracker.attributes);
    assertTrue("If provider sends null then empty attributes should be sent",
        resourceTracker.attributes.isEmpty());
    resourceTracker.resetNMHeartbeatReceiveFlag();
    // Since the resync interval is set to 2 sec in every alternate heartbeat
    // the attributes will be send along with heartbeat.
    // In loop we sleep for 1 sec
    // so that every sec 1 heartbeat is send.
    int nullAttributes = 0;
    int nonNullAttributes = 0;
    dummyAttributesProviderRef.setDescriptors(ImmutableSet.of(attribute1));
    for (int i = 0; i < 5; i++) {
      sendOutofBandHeartBeat();
      resourceTracker.waitTillHeartbeat();
      if (null == resourceTracker.attributes) {
        nullAttributes++;
      } else {
        Assert.assertTrue("In heartbeat PI attributes should be send",
            NodeLabelUtil.isNodeAttributesEquals(ImmutableSet.of(attribute1),
                resourceTracker.attributes));
        nonNullAttributes++;
      }
      resourceTracker.resetNMHeartbeatReceiveFlag();
      Thread.sleep(1000);
    }
    Assert.assertTrue("More than one heartbeat with empty attributes expected",
        nullAttributes > 1);
    Assert.assertTrue("More than one heartbeat with attributes expected",
        nonNullAttributes > 1);
    nm.stop();
  }

  @Test(timeout = 20000)
  public void testInvalidNodeAttributesFromProvider()
      throws InterruptedException, IOException, TimeoutException {
    final ResourceTrackerForAttributes resourceTracker =
        new ResourceTrackerForAttributes();
    nm = new NodeManager() {
      @Override protected NodeAttributesProvider createNodeAttributesProvider(
          Configuration conf) throws IOException {
        return dummyAttributesProviderRef;
      }

      @Override protected NodeStatusUpdater createNodeStatusUpdater(
          Context context, Dispatcher dispatcher,
          NodeHealthCheckerService healthChecker) {

        return new NodeStatusUpdaterImpl(context, dispatcher, healthChecker,
            metrics) {
          @Override protected ResourceTracker getRMClient() {
            return resourceTracker;
          }

          @Override protected void stopRMProxy() {
            return;
          }
        };
      }
    };

    YarnConfiguration conf = createNMConfigForDistributeNodeAttributes();
    conf.set(YarnConfiguration.NM_LOCALIZER_ADDRESS,
        "0.0.0.0:" + ServerSocketUtil.getPort(8040, 10));
    nm.init(conf);
    resourceTracker.resetNMHeartbeatReceiveFlag();
    nm.start();
    resourceTracker.waitTillRegister();
    assertTrue(NodeLabelUtil
        .isNodeAttributesEquals(dummyAttributesProviderRef.getDescriptors(),
            resourceTracker.attributes));

    resourceTracker.waitTillHeartbeat(); // wait till the first heartbeat
    resourceTracker.resetNMHeartbeatReceiveFlag();

    // update attribute1
    NodeAttribute attribute1 = NodeAttribute
        .newInstance(NodeAttribute.PREFIX_DISTRIBUTED, "Attr1",
            NodeAttributeType.STRING, "V1");
    dummyAttributesProviderRef.setDescriptors(ImmutableSet.of(attribute1));
    sendOutofBandHeartBeat();
    resourceTracker.waitTillHeartbeat();
    assertTrue(NodeLabelUtil.isNodeAttributesEquals(ImmutableSet.of(attribute1),
        resourceTracker.attributes));
    resourceTracker.resetNMHeartbeatReceiveFlag();

    // update attribute2
    NodeAttribute attribute2 = NodeAttribute
        .newInstance(NodeAttribute.PREFIX_DISTRIBUTED, "Attr2",
            NodeAttributeType.STRING, "V2");
    dummyAttributesProviderRef.setDescriptors(ImmutableSet.of(attribute2));
    sendOutofBandHeartBeat();
    resourceTracker.waitTillHeartbeat();
    assertTrue(NodeLabelUtil.isNodeAttributesEquals(ImmutableSet.of(attribute2),
        resourceTracker.attributes));
    resourceTracker.resetNMHeartbeatReceiveFlag();

    // update attribute2 & attribute2
    dummyAttributesProviderRef
        .setDescriptors(ImmutableSet.of(attribute1, attribute2));
    sendOutofBandHeartBeat();
    resourceTracker.waitTillHeartbeat();
    assertTrue(NodeLabelUtil
        .isNodeAttributesEquals(ImmutableSet.of(attribute1, attribute2),
            resourceTracker.attributes));
    resourceTracker.resetNMHeartbeatReceiveFlag();

    // heartbeat with invalid attributes
    NodeAttribute invalidAttribute = NodeAttribute
        .newInstance("_.P", "Attr1", NodeAttributeType.STRING, "V1");
    dummyAttributesProviderRef
        .setDescriptors(ImmutableSet.of(invalidAttribute));
    sendOutofBandHeartBeat();
    resourceTracker.waitTillHeartbeat();
    assertNull("On Invalid Attributes we need to retain earlier attributes, HB"
        + " needs to send null", resourceTracker.attributes);
    resourceTracker.resetNMHeartbeatReceiveFlag();

    // on next heartbeat same invalid attributes will be given by the provider,
    // but again validation check and reset RM with invalid attributes set
    // should not happen
    sendOutofBandHeartBeat();
    resourceTracker.waitTillHeartbeat();
    assertNull("NodeStatusUpdater need not send repeatedly empty attributes on"
        + " invalid attributes from provider ", resourceTracker.attributes);
    resourceTracker.resetNMHeartbeatReceiveFlag();
  }

  /**
   * This is to avoid race condition in the test case. NodeStatusUpdater
   * heartbeat thread after sending the heartbeat needs some time to process the
   * response and then go wait state. But in the test case once the main test
   * thread returns back after resourceTracker.waitTillHeartbeat() we proceed
   * with next sendOutofBandHeartBeat before heartbeat thread is blocked on
   * wait.
   * @throws InterruptedException
   * @throws IOException
   */
  private void sendOutofBandHeartBeat()
      throws InterruptedException, IOException {
    int i = 0;
    do {
      State statusUpdaterThreadState =
          ((NodeStatusUpdaterImpl) nm.getNodeStatusUpdater())
              .getStatusUpdaterThreadState();
      if (statusUpdaterThreadState.equals(Thread.State.TIMED_WAITING)
          || statusUpdaterThreadState.equals(Thread.State.WAITING)) {
        nm.getNodeStatusUpdater().sendOutofBandHeartBeat();
        break;
      }
      if (++i <= 10) {
        Thread.sleep(50);
      } else {
        throw new IOException("Waited for 500 ms"
            + " but NodeStatusUpdaterThread not in waiting state");
      }
    } while (true);
  }
}
