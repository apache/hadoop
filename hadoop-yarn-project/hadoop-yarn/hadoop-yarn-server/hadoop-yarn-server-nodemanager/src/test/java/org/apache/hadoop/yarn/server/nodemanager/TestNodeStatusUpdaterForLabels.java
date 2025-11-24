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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.lang.Thread.State;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.TimerTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.ServerSocketUtil;
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
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;
import org.apache.hadoop.yarn.server.nodemanager.health.NodeHealthCheckerService;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.NodeLabelsProvider;
import org.apache.hadoop.yarn.server.utils.YarnServerBuilderUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestNodeStatusUpdaterForLabels extends NodeLabelTestBase {
  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private NodeManager nm;
  protected DummyNodeLabelsProvider dummyLabelsProviderRef;

  @BeforeEach
  public void setup() {
    dummyLabelsProviderRef = new DummyNodeLabelsProvider();
  }

  @AfterEach
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

    public void waitTillHeartbeat() throws InterruptedException {
      if (receivedNMHeartbeat) {
        return;
      }
      int i = 15;
      while (!receivedNMHeartbeat && i > 0) {
        synchronized (ResourceTrackerForLabels.class) {
          if (!receivedNMHeartbeat) {
            System.out
                .println("In ResourceTrackerForLabels waiting for heartbeat : "
                    + System.currentTimeMillis());
            ResourceTrackerForLabels.class.wait(200);
            i--;
          }
        }
      }
      if (!receivedNMHeartbeat) {
        fail("Heartbeat dint receive even after waiting");
      }
    }

    public void waitTillRegister() throws InterruptedException {
      if (receivedNMRegister) {
        return;
      }
      while (!receivedNMRegister) {
        synchronized (ResourceTrackerForLabels.class) {
            ResourceTrackerForLabels.class.wait();
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

    @Override
    public UnRegisterNodeManagerResponse unRegisterNodeManager(
        UnRegisterNodeManagerRequest request) throws YarnException, IOException {
      return null;
    }
  }

  /**
   * A dummy NodeLabelsProvider class for tests.
   */
  public static class DummyNodeLabelsProvider extends NodeLabelsProvider {

    public DummyNodeLabelsProvider() {
      super("DummyNodeLabelsProvider");
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
          setDescriptors(CommonNodeLabelsManager.EMPTY_NODELABEL_SET);
        }
      };
    }
  }

  private YarnConfiguration createNMConfigForDistributeNodeLabels() {
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.NODELABEL_CONFIGURATION_TYPE,
        YarnConfiguration.DISTRIBUTED_NODELABEL_CONFIGURATION_TYPE);
    return conf;
  }

  @Test
  @Timeout(value = 20)
  public void testNodeStatusUpdaterForNodeLabels() throws InterruptedException,
      IOException {
    final ResourceTrackerForLabels resourceTracker =
        new ResourceTrackerForLabels();
    nm = new NodeManager() {
      @Override
      protected NodeLabelsProvider createNodeLabelsProvider(Configuration conf)
          throws IOException {
        return dummyLabelsProviderRef;
      }

      @Override
      protected NodeStatusUpdater createNodeStatusUpdater(Context context,
          Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {

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

    YarnConfiguration conf = createNMConfigForDistributeNodeLabels();
    conf.setLong(YarnConfiguration.NM_NODE_LABELS_RESYNC_INTERVAL, 2000);
    conf.set(YarnConfiguration.NM_LOCALIZER_ADDRESS, "0.0.0.0:"
        + ServerSocketUtil.getPort(8040, 10));

    nm.init(conf);
    resourceTracker.resetNMHeartbeatReceiveFlag();
    nm.start();
    resourceTracker.waitTillRegister();
    assertNLCollectionEquals(dummyLabelsProviderRef.getDescriptors(),
        resourceTracker.labels);

    resourceTracker.waitTillHeartbeat();// wait till the first heartbeat
    resourceTracker.resetNMHeartbeatReceiveFlag();

    // heartbeat with updated labels
    dummyLabelsProviderRef.setDescriptors(toNodeLabelSet("P"));

    sendOutofBandHeartBeat();
    resourceTracker.waitTillHeartbeat();
    assertNLCollectionEquals(dummyLabelsProviderRef.getDescriptors(),
        resourceTracker.labels);
    resourceTracker.resetNMHeartbeatReceiveFlag();

    // heartbeat without updating labels
    sendOutofBandHeartBeat();
    resourceTracker.waitTillHeartbeat();
    resourceTracker.resetNMHeartbeatReceiveFlag();
    assertNull(resourceTracker.labels,
        "If no change in labels then null should be sent as part of request");

    // provider return with null labels
    dummyLabelsProviderRef.setDescriptors(null);
    sendOutofBandHeartBeat();
    resourceTracker.waitTillHeartbeat();
    assertNotNull(resourceTracker.labels,
        "If provider sends null then empty label set should be sent and not null");
    assertTrue(resourceTracker.labels.isEmpty(),
        "If provider sends null then empty labels should be sent");
    resourceTracker.resetNMHeartbeatReceiveFlag();
    // Since the resync interval is set to 2 sec in every alternate heartbeat
    // the labels will be send along with heartbeat.In loop we sleep for 1 sec
    // so that every sec 1 heartbeat is send.
    int nullLabels = 0;
    int nonNullLabels = 0;
    dummyLabelsProviderRef.setDescriptors(toNodeLabelSet("P1"));
    for (int i = 0; i < 5; i++) {
      sendOutofBandHeartBeat();
      resourceTracker.waitTillHeartbeat();
      if (null == resourceTracker.labels) {
        nullLabels++;
      } else {
        assertEquals(toNodeLabelSet("P1"), resourceTracker.labels,
            "In heartbeat PI labels should be send");
        nonNullLabels++;
      }
      resourceTracker.resetNMHeartbeatReceiveFlag();
      Thread.sleep(1000);
    }
    assertTrue(nullLabels > 1,
        "More than one heartbeat with empty labels expected");
    assertTrue(nonNullLabels > 1,
        "More than one heartbeat with labels expected");
    nm.stop();
  }

  @Test
  @Timeout(value = 20)
  public void testInvalidNodeLabelsFromProvider() throws InterruptedException,
      IOException {
    final ResourceTrackerForLabels resourceTracker =
        new ResourceTrackerForLabels();
    nm = new NodeManager() {
      @Override
      protected NodeLabelsProvider createNodeLabelsProvider(Configuration conf)
          throws IOException {
        return dummyLabelsProviderRef;
      }

      @Override
      protected NodeStatusUpdater createNodeStatusUpdater(Context context,
          Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {

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

    YarnConfiguration conf = createNMConfigForDistributeNodeLabels();
    conf.set(YarnConfiguration.NM_LOCALIZER_ADDRESS, "0.0.0.0:"
        + ServerSocketUtil.getPort(8040, 10));
    nm.init(conf);
    resourceTracker.resetNMHeartbeatReceiveFlag();
    nm.start();
    dummyLabelsProviderRef.setDescriptors(toNodeLabelSet("P"));
    resourceTracker.waitTillHeartbeat();// wait till the first heartbeat
    resourceTracker.resetNMHeartbeatReceiveFlag();

    // heartbeat with invalid labels
    dummyLabelsProviderRef.setDescriptors(toNodeLabelSet("_.P"));

    sendOutofBandHeartBeat();
    resourceTracker.waitTillHeartbeat();
    assertNull(resourceTracker.labels,
        "On Invalid Labels we need to retain earlier labels, HB "
        + "needs to send null");
    resourceTracker.resetNMHeartbeatReceiveFlag();

    // on next heartbeat same invalid labels will be given by the provider, but
    // again label validation check and reset RM with empty labels set should
    // not happen
    sendOutofBandHeartBeat();
    resourceTracker.waitTillHeartbeat();
    assertNull(resourceTracker.labels,
        "NodeStatusUpdater need not send repeatedly empty labels on "
        + "invalid labels from provider ");
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
      State statusUpdaterThreadState = ((NodeStatusUpdaterImpl) nm.getNodeStatusUpdater())
          .getStatusUpdaterThreadState();
      if (statusUpdaterThreadState.equals(Thread.State.TIMED_WAITING)
          || statusUpdaterThreadState.equals(Thread.State.WAITING)) {
        nm.getNodeStatusUpdater().sendOutofBandHeartBeat();
        break;
      }
      if (++i <= 10) {
        Thread.sleep(50);
      } else {
        throw new IOException(
            "Waited for 500 ms but NodeStatusUpdaterThread not in waiting state");
      }
    } while (true);
  }
}
