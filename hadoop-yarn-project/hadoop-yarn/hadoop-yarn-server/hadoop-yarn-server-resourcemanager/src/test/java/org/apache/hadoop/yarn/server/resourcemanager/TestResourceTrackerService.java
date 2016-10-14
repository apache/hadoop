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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.nodelabels.NodeLabelTestBase;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NodeLabelsUtils;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStatusEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.timelineservice.collector.PerNodeTimelineCollectorsAuxService;
import org.apache.hadoop.yarn.server.timelineservice.storage.FileSystemTimelineWriterImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineWriter;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestResourceTrackerService extends NodeLabelTestBase {

  private final static File TEMP_DIR = new File(System.getProperty(
      "test.build.data", "/tmp"), "decommision");
  private final File hostFile = new File(TEMP_DIR + File.separator + "hostFile.txt");
  private final File excludeHostFile = new File(TEMP_DIR + File.separator +
      "excludeHostFile.txt");

  private MockRM rm;

  /**
   * Test RM read NM next heartBeat Interval correctly from Configuration file,
   * and NM get next heartBeat Interval from RM correctly
   */
  @Test (timeout = 50000)
  public void testGetNextHeartBeatInterval() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, "4000");

    rm = new MockRM(conf);
    rm.start();

    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);

    NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertEquals(4000, nodeHeartbeat.getNextHeartBeatInterval());

    NodeHeartbeatResponse nodeHeartbeat2 = nm2.nodeHeartbeat(true);
    Assert.assertEquals(4000, nodeHeartbeat2.getNextHeartBeatInterval());

  }

  /**
   * Decommissioning using a pre-configured include hosts file
   */
  @Test
  public void testDecommissionWithIncludeHosts() throws Exception {

    writeToHostsFile("localhost", "host1", "host2");
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH, hostFile
        .getAbsolutePath());

    rm = new MockRM(conf);
    rm.start();

    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);
    MockNM nm3 = rm.registerNode("localhost:4433", 1024);
    
    ClusterMetrics metrics = ClusterMetrics.getMetrics();
    assert(metrics != null);
    int metricCount = metrics.getNumDecommisionedNMs();

    NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    nodeHeartbeat = nm3.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));

    // To test that IPs also work
    String ip = NetUtils.normalizeHostName("localhost");
    writeToHostsFile("host1", ip);

    rm.getNodesListManager().refreshNodes(conf);

    checkShutdownNMCount(rm, ++metricCount);

    nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    Assert
      .assertEquals(1, ClusterMetrics.getMetrics().getNumShutdownNMs());

    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertTrue("Node is not decommisioned.", NodeAction.SHUTDOWN
        .equals(nodeHeartbeat.getNodeAction()));

    nodeHeartbeat = nm3.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    Assert.assertEquals(metricCount, ClusterMetrics.getMetrics()
      .getNumShutdownNMs());
    rm.stop();
  }

  /**
   * Decommissioning using a pre-configured exclude hosts file
   */
  @Test
  public void testDecommissionWithExcludeHosts() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH, hostFile
        .getAbsolutePath());

    writeToHostsFile("");
    rm = new MockRM(conf);
    rm.start();

    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);
    MockNM nm3 = rm.registerNode("localhost:4433", 1024);


    int metricCount = ClusterMetrics.getMetrics().getNumDecommisionedNMs();
    NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    rm.drainEvents();

    // To test that IPs also work
    String ip = NetUtils.normalizeHostName("localhost");
    writeToHostsFile("host2", ip);

    rm.getNodesListManager().refreshNodes(conf);

    checkDecommissionedNMCount(rm, metricCount + 2);

    nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertTrue("The decommisioned metrics are not updated",
        NodeAction.SHUTDOWN.equals(nodeHeartbeat.getNodeAction()));

    nodeHeartbeat = nm3.nodeHeartbeat(true);
    Assert.assertTrue("The decommisioned metrics are not updated",
        NodeAction.SHUTDOWN.equals(nodeHeartbeat.getNodeAction()));
    rm.drainEvents();

    writeToHostsFile("");
    rm.getNodesListManager().refreshNodes(conf);

    nm3 = rm.registerNode("localhost:4433", 1024);
    nodeHeartbeat = nm3.nodeHeartbeat(true);
    rm.drainEvents();
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    // decommissined node is 1 since 1 node is rejoined after updating exclude
    // file
    checkDecommissionedNMCount(rm, metricCount + 1);
  }

  /**
   * Graceful decommission node with no running application.
   */
  @Test
  public void testGracefulDecommissionNoApp() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH, hostFile
        .getAbsolutePath());

    writeToHostsFile("");
    rm = new MockRM(conf);
    rm.start();

    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);
    MockNM nm3 = rm.registerNode("host3:4433", 5120);

    int metricCount = ClusterMetrics.getMetrics().getNumDecommisionedNMs();
    NodeHeartbeatResponse nodeHeartbeat1 = nm1.nodeHeartbeat(true);
    NodeHeartbeatResponse nodeHeartbeat2 = nm2.nodeHeartbeat(true);
    NodeHeartbeatResponse nodeHeartbeat3 = nm3.nodeHeartbeat(true);

    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat1.getNodeAction()));
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat2.getNodeAction()));
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat3.getNodeAction()));

    rm.waitForState(nm2.getNodeId(), NodeState.RUNNING);
    rm.waitForState(nm3.getNodeId(), NodeState.RUNNING);

    // Graceful decommission both host2 and host3.
    writeToHostsFile("host2", "host3");
    rm.getNodesListManager().refreshNodes(conf, true);

    rm.waitForState(nm2.getNodeId(), NodeState.DECOMMISSIONING);
    rm.waitForState(nm3.getNodeId(), NodeState.DECOMMISSIONING);

    nodeHeartbeat1 = nm1.nodeHeartbeat(true);
    nodeHeartbeat2 = nm2.nodeHeartbeat(true);
    nodeHeartbeat3 = nm3.nodeHeartbeat(true);

    checkDecommissionedNMCount(rm, metricCount + 2);
    rm.waitForState(nm2.getNodeId(), NodeState.DECOMMISSIONED);
    rm.waitForState(nm3.getNodeId(), NodeState.DECOMMISSIONED);

    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat1.getNodeAction()));
    Assert.assertEquals(NodeAction.SHUTDOWN, nodeHeartbeat2.getNodeAction());
    Assert.assertEquals(NodeAction.SHUTDOWN, nodeHeartbeat3.getNodeAction());
  }

  /**
   * Graceful decommission node with running application.
   */
  @Test
  public void testGracefulDecommissionWithApp() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH, hostFile
        .getAbsolutePath());

    writeToHostsFile("");
    rm = new MockRM(conf);
    rm.start();

    MockNM nm1 = rm.registerNode("host1:1234", 10240);
    MockNM nm2 = rm.registerNode("host2:5678", 20480);
    MockNM nm3 = rm.registerNode("host3:4433", 10240);
    NodeId id1 = nm1.getNodeId();
    NodeId id3 = nm3.getNodeId();
    rm.waitForState(id1, NodeState.RUNNING);
    rm.waitForState(id3, NodeState.RUNNING);

    // Create an app and launch two containers on host1.
    RMApp app = rm.submitApp(2000);
    MockAM am = MockRM.launchAndRegisterAM(app, rm, nm1);
    ApplicationAttemptId aaid = app.getCurrentAppAttempt().getAppAttemptId();
    nm1.nodeHeartbeat(aaid, 2, ContainerState.RUNNING);
    nm3.nodeHeartbeat(true);

    // Graceful decommission host1 and host3
    writeToHostsFile("host1", "host3");
    rm.getNodesListManager().refreshNodes(conf, true);
    rm.waitForState(id1, NodeState.DECOMMISSIONING);
    rm.waitForState(id3, NodeState.DECOMMISSIONING);

    // host1 should be DECOMMISSIONING due to running containers.
    // host3 should become DECOMMISSIONED.
    nm1.nodeHeartbeat(true);
    nm3.nodeHeartbeat(true);
    rm.waitForState(id1, NodeState.DECOMMISSIONING);
    rm.waitForState(id3, NodeState.DECOMMISSIONED);
    nm1.nodeHeartbeat(aaid, 2, ContainerState.RUNNING);

    // Complete containers on host1.
    // Since the app is still RUNNING, expect NodeAction.NORMAL.
    NodeHeartbeatResponse nodeHeartbeat1 =
        nm1.nodeHeartbeat(aaid, 2, ContainerState.COMPLETE);
    Assert.assertEquals(NodeAction.NORMAL, nodeHeartbeat1.getNodeAction());

    // Finish the app and verified DECOMMISSIONED.
    MockRM.finishAMAndVerifyAppState(app, rm, nm1, am);
    rm.waitForState(app.getApplicationId(), RMAppState.FINISHED);
    nodeHeartbeat1 = nm1.nodeHeartbeat(aaid, 2, ContainerState.COMPLETE);
    Assert.assertEquals(NodeAction.SHUTDOWN, nodeHeartbeat1.getNodeAction());
    rm.waitForState(id1, NodeState.DECOMMISSIONED);
  }

  /**
  * Decommissioning using a post-configured include hosts file
  */
  @Test
  public void testAddNewIncludePathToConfiguration() throws Exception {
    Configuration conf = new Configuration();
    rm = new MockRM(conf);
    rm.start();
    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);
    ClusterMetrics metrics = ClusterMetrics.getMetrics();
    assert(metrics != null);
    int initialMetricCount = metrics.getNumShutdownNMs();
    NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertEquals(
        NodeAction.NORMAL,
        nodeHeartbeat.getNodeAction());
    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertEquals(
        NodeAction.NORMAL,
        nodeHeartbeat.getNodeAction());
    writeToHostsFile("host1");
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH, hostFile
        .getAbsolutePath());
    rm.getNodesListManager().refreshNodes(conf);
    checkShutdownNMCount(rm, ++initialMetricCount);
    nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertEquals(
        "Node should not have been shutdown.",
        NodeAction.NORMAL,
        nodeHeartbeat.getNodeAction());
    NodeState nodeState =
        rm.getRMContext().getInactiveRMNodes().get(nm2.getNodeId()).getState();
    Assert.assertEquals("Node should have been shutdown but is in state" +
            nodeState, NodeState.SHUTDOWN, nodeState);
  }
  
  /**
   * Decommissioning using a post-configured exclude hosts file
   */
  @Test
  public void testAddNewExcludePathToConfiguration() throws Exception {
    Configuration conf = new Configuration();
    rm = new MockRM(conf);
    rm.start();
    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);
    ClusterMetrics metrics = ClusterMetrics.getMetrics();
    assert(metrics != null);
    int initialMetricCount = metrics.getNumDecommisionedNMs();
    NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertEquals(
        NodeAction.NORMAL,
        nodeHeartbeat.getNodeAction());
    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertEquals(
        NodeAction.NORMAL,
        nodeHeartbeat.getNodeAction());
    writeToHostsFile("host2");
    conf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH, hostFile
        .getAbsolutePath());
    rm.getNodesListManager().refreshNodes(conf);
    checkDecommissionedNMCount(rm, ++initialMetricCount);
    nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertEquals(
        "Node should not have been decomissioned.",
        NodeAction.NORMAL,
        nodeHeartbeat.getNodeAction());
    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertEquals("Node should have been decomissioned but is in state" +
        nodeHeartbeat.getNodeAction(),
        NodeAction.SHUTDOWN, nodeHeartbeat.getNodeAction());
  }

  @Test
  public void testNodeRegistrationSuccess() throws Exception {
    writeToHostsFile("host2");
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH, hostFile
        .getAbsolutePath());
    rm = new MockRM(conf);
    rm.start();

    ResourceTrackerService resourceTrackerService = rm.getResourceTrackerService();
    RegisterNodeManagerRequest req = Records.newRecord(
        RegisterNodeManagerRequest.class);
    NodeId nodeId = NodeId.newInstance("host2", 1234);
    Resource capability = BuilderUtils.newResource(1024, 1);
    req.setResource(capability);
    req.setNodeId(nodeId);
    req.setHttpPort(1234);
    req.setNMVersion(YarnVersionInfo.getVersion());
    // trying to register a invalid node.
    RegisterNodeManagerResponse response =
        resourceTrackerService.registerNodeManager(req);
    Assert.assertEquals(NodeAction.NORMAL, response.getNodeAction());
  }

  @Test
  public void testNodeRegistrationWithLabels() throws Exception {
    writeToHostsFile("host2");
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
        hostFile.getAbsolutePath());
    conf.set(YarnConfiguration.NODELABEL_CONFIGURATION_TYPE,
        YarnConfiguration.DISTRIBUTED_NODELABEL_CONFIGURATION_TYPE);

    final RMNodeLabelsManager nodeLabelsMgr = new NullRMNodeLabelsManager();

    rm = new MockRM(conf) {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        return nodeLabelsMgr;
      }
    };
    rm.start();

    try {
      nodeLabelsMgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("A", "B", "C"));
    } catch (IOException e) {
      Assert.fail("Caught Exception while intializing");
      e.printStackTrace();
    }

    ResourceTrackerService resourceTrackerService =
        rm.getResourceTrackerService();
    RegisterNodeManagerRequest registerReq =
        Records.newRecord(RegisterNodeManagerRequest.class);
    NodeId nodeId = NodeId.newInstance("host2", 1234);
    Resource capability = BuilderUtils.newResource(1024, 1);
    registerReq.setResource(capability);
    registerReq.setNodeId(nodeId);
    registerReq.setHttpPort(1234);
    registerReq.setNMVersion(YarnVersionInfo.getVersion());
    registerReq.setNodeLabels(toSet(NodeLabel.newInstance("A")));
    RegisterNodeManagerResponse response =
        resourceTrackerService.registerNodeManager(registerReq);

    Assert.assertEquals("Action should be normal on valid Node Labels",
        NodeAction.NORMAL, response.getNodeAction());
    assertCollectionEquals(nodeLabelsMgr.getNodeLabels().get(nodeId),
        NodeLabelsUtils.convertToStringSet(registerReq.getNodeLabels()));
    Assert.assertTrue("Valid Node Labels were not accepted by RM",
        response.getAreNodeLabelsAcceptedByRM());
    rm.stop();
  }

  @Test
  public void testNodeRegistrationWithInvalidLabels() throws Exception {
    writeToHostsFile("host2");
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
        hostFile.getAbsolutePath());
    conf.set(YarnConfiguration.NODELABEL_CONFIGURATION_TYPE,
        YarnConfiguration.DISTRIBUTED_NODELABEL_CONFIGURATION_TYPE);

    final RMNodeLabelsManager nodeLabelsMgr = new NullRMNodeLabelsManager();

    rm = new MockRM(conf) {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        return nodeLabelsMgr;
      }
    };
    rm.start();

    try {
      nodeLabelsMgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("X", "Y", "Z"));
    } catch (IOException e) {
      Assert.fail("Caught Exception while intializing");
      e.printStackTrace();
    }

    ResourceTrackerService resourceTrackerService =
        rm.getResourceTrackerService();
    RegisterNodeManagerRequest registerReq =
        Records.newRecord(RegisterNodeManagerRequest.class);
    NodeId nodeId = NodeId.newInstance("host2", 1234);
    Resource capability = BuilderUtils.newResource(1024, 1);
    registerReq.setResource(capability);
    registerReq.setNodeId(nodeId);
    registerReq.setHttpPort(1234);
    registerReq.setNMVersion(YarnVersionInfo.getVersion());
    registerReq.setNodeLabels(toNodeLabelSet("A", "B", "C"));
    RegisterNodeManagerResponse response =
        resourceTrackerService.registerNodeManager(registerReq);

    Assert.assertEquals(
        "On Invalid Node Labels action is expected to be normal",
        NodeAction.NORMAL, response.getNodeAction());
    Assert.assertNull(nodeLabelsMgr.getNodeLabels().get(nodeId));
    Assert.assertNotNull(response.getDiagnosticsMessage());
    Assert.assertFalse("Node Labels should not accepted by RM If Invalid",
        response.getAreNodeLabelsAcceptedByRM());

    if (rm != null) {
      rm.stop();
    }
  }

  @Test
  public void testNodeRegistrationWithInvalidLabelsSyntax() throws Exception {
    writeToHostsFile("host2");
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
        hostFile.getAbsolutePath());
    conf.set(YarnConfiguration.NODELABEL_CONFIGURATION_TYPE,
        YarnConfiguration.DISTRIBUTED_NODELABEL_CONFIGURATION_TYPE);

    final RMNodeLabelsManager nodeLabelsMgr = new NullRMNodeLabelsManager();

    rm = new MockRM(conf) {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        return nodeLabelsMgr;
      }
    };
    rm.start();

    try {
      nodeLabelsMgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("X", "Y", "Z"));
    } catch (IOException e) {
      Assert.fail("Caught Exception while intializing");
      e.printStackTrace();
    }

    ResourceTrackerService resourceTrackerService =
        rm.getResourceTrackerService();
    RegisterNodeManagerRequest req =
        Records.newRecord(RegisterNodeManagerRequest.class);
    NodeId nodeId = NodeId.newInstance("host2", 1234);
    Resource capability = BuilderUtils.newResource(1024, 1);
    req.setResource(capability);
    req.setNodeId(nodeId);
    req.setHttpPort(1234);
    req.setNMVersion(YarnVersionInfo.getVersion());
    req.setNodeLabels(toNodeLabelSet("#Y"));
    RegisterNodeManagerResponse response =
        resourceTrackerService.registerNodeManager(req);

    Assert.assertEquals(
        "On Invalid Node Labels action is expected to be normal",
        NodeAction.NORMAL, response.getNodeAction());
    Assert.assertNull(nodeLabelsMgr.getNodeLabels().get(nodeId));
    Assert.assertNotNull(response.getDiagnosticsMessage());
    Assert.assertFalse("Node Labels should not accepted by RM If Invalid",
        response.getAreNodeLabelsAcceptedByRM());

    if (rm != null) {
      rm.stop();
    }
  }

  @Test
  public void testNodeRegistrationWithCentralLabelConfig() throws Exception {
    writeToHostsFile("host2");
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
        hostFile.getAbsolutePath());
    conf.set(YarnConfiguration.NODELABEL_CONFIGURATION_TYPE,
        YarnConfiguration.DEFAULT_NODELABEL_CONFIGURATION_TYPE);

    final RMNodeLabelsManager nodeLabelsMgr = new NullRMNodeLabelsManager();

    rm = new MockRM(conf) {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        return nodeLabelsMgr;
      }
    };
    rm.start();
    try {
      nodeLabelsMgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("A", "B", "C"));
    } catch (IOException e) {
      Assert.fail("Caught Exception while intializing");
      e.printStackTrace();
    }
    ResourceTrackerService resourceTrackerService =
        rm.getResourceTrackerService();
    RegisterNodeManagerRequest req =
        Records.newRecord(RegisterNodeManagerRequest.class);
    NodeId nodeId = NodeId.newInstance("host2", 1234);
    Resource capability = BuilderUtils.newResource(1024, 1);
    req.setResource(capability);
    req.setNodeId(nodeId);
    req.setHttpPort(1234);
    req.setNMVersion(YarnVersionInfo.getVersion());
    req.setNodeLabels(toNodeLabelSet("A"));
    RegisterNodeManagerResponse response =
        resourceTrackerService.registerNodeManager(req);
    // registered to RM with central label config
    Assert.assertEquals(NodeAction.NORMAL, response.getNodeAction());
    Assert.assertNull(nodeLabelsMgr.getNodeLabels().get(nodeId));
    Assert
        .assertFalse(
            "Node Labels should not accepted by RM If its configured with " +
                "Central configuration",
            response.getAreNodeLabelsAcceptedByRM());
    if (rm != null) {
      rm.stop();
    }
  }

  @SuppressWarnings("unchecked")
  private NodeStatus getNodeStatusObject(NodeId nodeId) {
    NodeStatus status = Records.newRecord(NodeStatus.class);
    status.setNodeId(nodeId);
    status.setResponseId(0);
    status.setContainersStatuses(Collections.EMPTY_LIST);
    status.setKeepAliveApplications(Collections.EMPTY_LIST);
    return status;
  }

  @Test
  public void testNodeHeartBeatWithLabels() throws Exception {
    writeToHostsFile("host2");
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
        hostFile.getAbsolutePath());
    conf.set(YarnConfiguration.NODELABEL_CONFIGURATION_TYPE,
        YarnConfiguration.DISTRIBUTED_NODELABEL_CONFIGURATION_TYPE);

    final RMNodeLabelsManager nodeLabelsMgr = new NullRMNodeLabelsManager();

    rm = new MockRM(conf) {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        return nodeLabelsMgr;
      }
    };
    rm.start();
    // adding valid labels
    try {
      nodeLabelsMgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("A", "B", "C"));
    } catch (IOException e) {
      Assert.fail("Caught Exception while intializing");
      e.printStackTrace();
    }

    // Registering of labels and other required info to RM
    ResourceTrackerService resourceTrackerService =
        rm.getResourceTrackerService();
    RegisterNodeManagerRequest registerReq =
        Records.newRecord(RegisterNodeManagerRequest.class);
    NodeId nodeId = NodeId.newInstance("host2", 1234);
    Resource capability = BuilderUtils.newResource(1024, 1);
    registerReq.setResource(capability);
    registerReq.setNodeId(nodeId);
    registerReq.setHttpPort(1234);
    registerReq.setNMVersion(YarnVersionInfo.getVersion());
    registerReq.setNodeLabels(toNodeLabelSet("A")); // Node register label
    RegisterNodeManagerResponse registerResponse =
        resourceTrackerService.registerNodeManager(registerReq);

    // modification of labels during heartbeat
    NodeHeartbeatRequest heartbeatReq =
        Records.newRecord(NodeHeartbeatRequest.class);
    heartbeatReq.setNodeLabels(toNodeLabelSet("B")); // Node heartbeat label update
    NodeStatus nodeStatusObject = getNodeStatusObject(nodeId);
    heartbeatReq.setNodeStatus(nodeStatusObject);
    heartbeatReq.setLastKnownNMTokenMasterKey(registerResponse
        .getNMTokenMasterKey());
    heartbeatReq.setLastKnownContainerTokenMasterKey(registerResponse
        .getContainerTokenMasterKey());
    NodeHeartbeatResponse nodeHeartbeatResponse =
        resourceTrackerService.nodeHeartbeat(heartbeatReq);

    Assert.assertEquals("InValid Node Labels were not accepted by RM",
        NodeAction.NORMAL, nodeHeartbeatResponse.getNodeAction());
    assertCollectionEquals(nodeLabelsMgr.getNodeLabels().get(nodeId),
        NodeLabelsUtils.convertToStringSet(heartbeatReq.getNodeLabels()));
    Assert.assertTrue("Valid Node Labels were not accepted by RM",
        nodeHeartbeatResponse.getAreNodeLabelsAcceptedByRM());
    
    // After modification of labels next heartbeat sends null informing no update
    Set<String> oldLabels = nodeLabelsMgr.getNodeLabels().get(nodeId);
    int responseId = nodeStatusObject.getResponseId();
    heartbeatReq =
        Records.newRecord(NodeHeartbeatRequest.class);
    heartbeatReq.setNodeLabels(null); // Node heartbeat label update
    nodeStatusObject = getNodeStatusObject(nodeId);
    nodeStatusObject.setResponseId(responseId+2);
    heartbeatReq.setNodeStatus(nodeStatusObject);
    heartbeatReq.setLastKnownNMTokenMasterKey(registerResponse
        .getNMTokenMasterKey());
    heartbeatReq.setLastKnownContainerTokenMasterKey(registerResponse
        .getContainerTokenMasterKey());
    nodeHeartbeatResponse = resourceTrackerService.nodeHeartbeat(heartbeatReq);

    Assert.assertEquals("InValid Node Labels were not accepted by RM",
        NodeAction.NORMAL, nodeHeartbeatResponse.getNodeAction());
    assertCollectionEquals(nodeLabelsMgr.getNodeLabels().get(nodeId),
        oldLabels);
    Assert.assertFalse("Node Labels should not accepted by RM",
        nodeHeartbeatResponse.getAreNodeLabelsAcceptedByRM());
    rm.stop();
  }

  @Test
  public void testNodeHeartBeatWithInvalidLabels() throws Exception {
    writeToHostsFile("host2");
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
        hostFile.getAbsolutePath());
    conf.set(YarnConfiguration.NODELABEL_CONFIGURATION_TYPE,
        YarnConfiguration.DISTRIBUTED_NODELABEL_CONFIGURATION_TYPE);

    final RMNodeLabelsManager nodeLabelsMgr = new NullRMNodeLabelsManager();

    rm = new MockRM(conf) {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        return nodeLabelsMgr;
      }
    };
    rm.start();

    try {
      nodeLabelsMgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("A", "B", "C"));
    } catch (IOException e) {
      Assert.fail("Caught Exception while intializing");
      e.printStackTrace();
    }

    ResourceTrackerService resourceTrackerService =
        rm.getResourceTrackerService();
    RegisterNodeManagerRequest registerReq =
        Records.newRecord(RegisterNodeManagerRequest.class);
    NodeId nodeId = NodeId.newInstance("host2", 1234);
    Resource capability = BuilderUtils.newResource(1024, 1);
    registerReq.setResource(capability);
    registerReq.setNodeId(nodeId);
    registerReq.setHttpPort(1234);
    registerReq.setNMVersion(YarnVersionInfo.getVersion());
    registerReq.setNodeLabels(toNodeLabelSet("A"));
    RegisterNodeManagerResponse registerResponse =
        resourceTrackerService.registerNodeManager(registerReq);

    NodeHeartbeatRequest heartbeatReq =
        Records.newRecord(NodeHeartbeatRequest.class);
    heartbeatReq.setNodeLabels(toNodeLabelSet("B", "#C")); // Invalid heart beat labels
    heartbeatReq.setNodeStatus(getNodeStatusObject(nodeId));
    heartbeatReq.setLastKnownNMTokenMasterKey(registerResponse
        .getNMTokenMasterKey());
    heartbeatReq.setLastKnownContainerTokenMasterKey(registerResponse
        .getContainerTokenMasterKey());
    NodeHeartbeatResponse nodeHeartbeatResponse =
        resourceTrackerService.nodeHeartbeat(heartbeatReq);

    // response should be NORMAL when RM heartbeat labels are rejected
    Assert.assertEquals("Response should be NORMAL when RM heartbeat labels"
        + " are rejected", NodeAction.NORMAL,
        nodeHeartbeatResponse.getNodeAction());
    Assert.assertFalse(nodeHeartbeatResponse.getAreNodeLabelsAcceptedByRM());
    Assert.assertNotNull(nodeHeartbeatResponse.getDiagnosticsMessage());
    rm.stop();
  }

  @Test
  public void testNodeHeartbeatWithCentralLabelConfig() throws Exception {
    writeToHostsFile("host2");
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
        hostFile.getAbsolutePath());
    conf.set(YarnConfiguration.NODELABEL_CONFIGURATION_TYPE,
        YarnConfiguration.DEFAULT_NODELABEL_CONFIGURATION_TYPE);

    final RMNodeLabelsManager nodeLabelsMgr = new NullRMNodeLabelsManager();

    rm = new MockRM(conf) {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        return nodeLabelsMgr;
      }
    };
    rm.start();

    ResourceTrackerService resourceTrackerService =
        rm.getResourceTrackerService();
    RegisterNodeManagerRequest req =
        Records.newRecord(RegisterNodeManagerRequest.class);
    NodeId nodeId = NodeId.newInstance("host2", 1234);
    Resource capability = BuilderUtils.newResource(1024, 1);
    req.setResource(capability);
    req.setNodeId(nodeId);
    req.setHttpPort(1234);
    req.setNMVersion(YarnVersionInfo.getVersion());
    req.setNodeLabels(toNodeLabelSet("A", "B", "C"));
    RegisterNodeManagerResponse registerResponse =
        resourceTrackerService.registerNodeManager(req);

    NodeHeartbeatRequest heartbeatReq =
        Records.newRecord(NodeHeartbeatRequest.class);
    heartbeatReq.setNodeLabels(toNodeLabelSet("B")); // Valid heart beat labels
    heartbeatReq.setNodeStatus(getNodeStatusObject(nodeId));
    heartbeatReq.setLastKnownNMTokenMasterKey(registerResponse
        .getNMTokenMasterKey());
    heartbeatReq.setLastKnownContainerTokenMasterKey(registerResponse
        .getContainerTokenMasterKey());
    NodeHeartbeatResponse nodeHeartbeatResponse =
        resourceTrackerService.nodeHeartbeat(heartbeatReq);

    // response should be ok but the RMacceptNodeLabelsUpdate should be false
    Assert.assertEquals(NodeAction.NORMAL,
        nodeHeartbeatResponse.getNodeAction());
    // no change in the labels,
    Assert.assertNull(nodeLabelsMgr.getNodeLabels().get(nodeId));
    // heartbeat labels rejected
    Assert.assertFalse("Invalid Node Labels should not accepted by RM",
        nodeHeartbeatResponse.getAreNodeLabelsAcceptedByRM());
    if (rm != null) {
      rm.stop();
    }
  }

  @Test
  public void testNodeRegistrationVersionLessThanRM() throws Exception {
    writeToHostsFile("host2");
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH, hostFile
        .getAbsolutePath());
    conf.set(YarnConfiguration.RM_NODEMANAGER_MINIMUM_VERSION,"EqualToRM" );
    rm = new MockRM(conf);
    rm.start();
    String nmVersion = "1.9.9";

    ResourceTrackerService resourceTrackerService = rm.getResourceTrackerService();
    RegisterNodeManagerRequest req = Records.newRecord(
        RegisterNodeManagerRequest.class);
    NodeId nodeId = NodeId.newInstance("host2", 1234);
    Resource capability = BuilderUtils.newResource(1024, 1);
    req.setResource(capability);
    req.setNodeId(nodeId);
    req.setHttpPort(1234);
    req.setNMVersion(nmVersion);
    // trying to register a invalid node.
    RegisterNodeManagerResponse response = resourceTrackerService.registerNodeManager(req);
    Assert.assertEquals(NodeAction.SHUTDOWN,response.getNodeAction());
    Assert.assertTrue("Diagnostic message did not contain: 'Disallowed NodeManager " +
        "Version "+ nmVersion + ", is less than the minimum version'",
        response.getDiagnosticsMessage().contains("Disallowed NodeManager Version " +
            nmVersion + ", is less than the minimum version "));

  }

  @Test
  public void testNodeRegistrationFailure() throws Exception {
    writeToHostsFile("host1");
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH, hostFile
        .getAbsolutePath());
    rm = new MockRM(conf);
    rm.start();
    
    ResourceTrackerService resourceTrackerService = rm.getResourceTrackerService();
    RegisterNodeManagerRequest req = Records.newRecord(
        RegisterNodeManagerRequest.class);
    NodeId nodeId = NodeId.newInstance("host2", 1234);
    req.setNodeId(nodeId);
    req.setHttpPort(1234);
    // trying to register a invalid node.
    RegisterNodeManagerResponse response = resourceTrackerService.registerNodeManager(req);
    Assert.assertEquals(NodeAction.SHUTDOWN,response.getNodeAction());
    Assert
      .assertEquals(
        "Disallowed NodeManager from  host2, Sending SHUTDOWN signal to the NodeManager.",
        response.getDiagnosticsMessage());
  }

  @Test
  public void testSetRMIdentifierInRegistration() throws Exception {

    Configuration conf = new Configuration();
    rm = new MockRM(conf);
    rm.start();

    MockNM nm = new MockNM("host1:1234", 5120, rm.getResourceTrackerService());
    RegisterNodeManagerResponse response = nm.registerNode();

    // Verify the RMIdentifier is correctly set in RegisterNodeManagerResponse
    Assert.assertEquals(ResourceManager.getClusterTimeStamp(),
      response.getRMIdentifier());
  }

  @Test
  public void testNodeRegistrationWithMinimumAllocations() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, "2048");
    conf.set(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, "4");
    rm = new MockRM(conf);
    rm.start();

    ResourceTrackerService resourceTrackerService
      = rm.getResourceTrackerService();
    RegisterNodeManagerRequest req = Records.newRecord(
        RegisterNodeManagerRequest.class);
    NodeId nodeId = BuilderUtils.newNodeId("host", 1234);
    req.setNodeId(nodeId);

    Resource capability = BuilderUtils.newResource(1024, 1);
    req.setResource(capability);
    RegisterNodeManagerResponse response1 =
        resourceTrackerService.registerNodeManager(req);
    Assert.assertEquals(NodeAction.SHUTDOWN,response1.getNodeAction());
    
    capability.setMemorySize(2048);
    capability.setVirtualCores(1);
    req.setResource(capability);
    RegisterNodeManagerResponse response2 =
        resourceTrackerService.registerNodeManager(req);
    Assert.assertEquals(NodeAction.SHUTDOWN,response2.getNodeAction());
    
    capability.setMemorySize(1024);
    capability.setVirtualCores(4);
    req.setResource(capability);
    RegisterNodeManagerResponse response3 =
        resourceTrackerService.registerNodeManager(req);
    Assert.assertEquals(NodeAction.SHUTDOWN,response3.getNodeAction());
    
    capability.setMemorySize(2048);
    capability.setVirtualCores(4);
    req.setResource(capability);
    RegisterNodeManagerResponse response4 =
        resourceTrackerService.registerNodeManager(req);
    Assert.assertEquals(NodeAction.NORMAL,response4.getNodeAction());
  }

  @Test
  public void testReboot() throws Exception {
    Configuration conf = new Configuration();
    rm = new MockRM(conf);
    rm.start();

    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:1234", 2048);

    int initialMetricCount = ClusterMetrics.getMetrics().getNumRebootedNMs();
    NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));

    nodeHeartbeat = nm2.nodeHeartbeat(
      new HashMap<ApplicationId, List<ContainerStatus>>(), true, -100);
    Assert.assertTrue(NodeAction.RESYNC.equals(nodeHeartbeat.getNodeAction()));
    Assert.assertEquals("Too far behind rm response id:0 nm response id:-100",
      nodeHeartbeat.getDiagnosticsMessage());
    checkRebootedNMCount(rm, ++initialMetricCount);
  }

  @Test
  public void testNodeHeartbeatForAppCollectorsMap() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    // set version to 2
    conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);
    // enable aux-service based timeline collectors
    conf.set(YarnConfiguration.NM_AUX_SERVICES, "timeline_collector");
    conf.set(YarnConfiguration.NM_AUX_SERVICES + "."
        + "timeline_collector" + ".class",
        PerNodeTimelineCollectorsAuxService.class.getName());
    conf.setClass(YarnConfiguration.TIMELINE_SERVICE_WRITER_CLASS,
        FileSystemTimelineWriterImpl.class, TimelineWriter.class);

    rm = new MockRM(conf);
    rm.start();

    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:1234", 2048);

    NodeHeartbeatResponse nodeHeartbeat1 = nm1.nodeHeartbeat(true);
    NodeHeartbeatResponse nodeHeartbeat2 = nm2.nodeHeartbeat(true);

    RMNodeImpl node1 =
        (RMNodeImpl) rm.getRMContext().getRMNodes().get(nm1.getNodeId());

    RMNodeImpl node2 =
        (RMNodeImpl) rm.getRMContext().getRMNodes().get(nm2.getNodeId());

    RMAppImpl app1 = (RMAppImpl) rm.submitApp(1024);
    String collectorAddr1 = "1.2.3.4:5";
    app1.setCollectorData(AppCollectorData.newInstance(
        app1.getApplicationId(), collectorAddr1));

    String collectorAddr2 = "5.4.3.2:1";
    RMAppImpl app2 = (RMAppImpl) rm.submitApp(1024);
    app2.setCollectorData(AppCollectorData.newInstance(
        app2.getApplicationId(), collectorAddr2));

    String collectorAddr3 = "5.4.3.2:2";
    app2.setCollectorData(AppCollectorData.newInstance(
        app2.getApplicationId(), collectorAddr3, 0, 1));

    String collectorAddr4 = "5.4.3.2:3";
    app2.setCollectorData(AppCollectorData.newInstance(
        app2.getApplicationId(), collectorAddr4, 1, 0));

    // Create a running container for app1 running on nm1
    ContainerId runningContainerId1 = BuilderUtils.newContainerId(
        BuilderUtils.newApplicationAttemptId(
        app1.getApplicationId(), 0), 0);

    ContainerStatus status1 = ContainerStatus.newInstance(runningContainerId1,
        ContainerState.RUNNING, "", 0);
    List<ContainerStatus> statusList = new ArrayList<ContainerStatus>();
    statusList.add(status1);
    NodeHealthStatus nodeHealth = NodeHealthStatus.newInstance(true,
        "", System.currentTimeMillis());
    NodeStatus nodeStatus = NodeStatus.newInstance(nm1.getNodeId(), 0,
        statusList, null, nodeHealth, null, null, null);
    node1.handle(new RMNodeStatusEvent(nm1.getNodeId(), nodeStatus,
        nodeHeartbeat1));

    Assert.assertEquals(1, node1.getRunningApps().size());
    Assert.assertEquals(app1.getApplicationId(), node1.getRunningApps().get(0));

    // Create a running container for app2 running on nm2
    ContainerId runningContainerId2 = BuilderUtils.newContainerId(
        BuilderUtils.newApplicationAttemptId(
        app2.getApplicationId(), 0), 0);

    ContainerStatus status2 = ContainerStatus.newInstance(runningContainerId2,
        ContainerState.RUNNING, "", 0);
    statusList = new ArrayList<ContainerStatus>();
    statusList.add(status2);
    nodeStatus = NodeStatus.newInstance(nm1.getNodeId(), 0,
        statusList, null, nodeHealth, null, null, null);
    node2.handle(new RMNodeStatusEvent(nm2.getNodeId(), nodeStatus,
        nodeHeartbeat2));
    Assert.assertEquals(1, node2.getRunningApps().size());
    Assert.assertEquals(app2.getApplicationId(), node2.getRunningApps().get(0));

    nodeHeartbeat1 = nm1.nodeHeartbeat(true);
    Map<ApplicationId, AppCollectorData> map1
        = nodeHeartbeat1.getAppCollectors();
    Assert.assertEquals(1, map1.size());
    Assert.assertEquals(collectorAddr1,
        map1.get(app1.getApplicationId()).getCollectorAddr());

    nodeHeartbeat2 = nm2.nodeHeartbeat(true);
    Map<ApplicationId, AppCollectorData> map2
        = nodeHeartbeat2.getAppCollectors();
    Assert.assertEquals(1, map2.size());
    Assert.assertEquals(collectorAddr4,
        map2.get(app2.getApplicationId()).getCollectorAddr());
  }

  private void checkRebootedNMCount(MockRM rm2, int count)
      throws InterruptedException {
    
    int waitCount = 0;
    while (ClusterMetrics.getMetrics().getNumRebootedNMs() != count
        && waitCount++ < 20) {
      synchronized (this) {
        wait(100);
      }
    }
    Assert.assertEquals("The rebooted metrics are not updated", count,
        ClusterMetrics.getMetrics().getNumRebootedNMs());
  }

  @Test
  public void testUnhealthyNodeStatus() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH, hostFile
        .getAbsolutePath());

    rm = new MockRM(conf);
    rm.start();

    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    Assert.assertEquals(0, ClusterMetrics.getMetrics().getUnhealthyNMs());
    // node healthy
    nm1.nodeHeartbeat(true);

    // node unhealthy
    nm1.nodeHeartbeat(false);
    checkUnhealthyNMCount(rm, nm1, true, 1);

    // node healthy again
    nm1.nodeHeartbeat(true);
    checkUnhealthyNMCount(rm, nm1, false, 0);
  }
  
  private void checkUnhealthyNMCount(MockRM rm, MockNM nm1, boolean health,
                                     int count) throws Exception {
    
    int waitCount = 0;
    while((rm.getRMContext().getRMNodes().get(nm1.getNodeId())
        .getState() != NodeState.UNHEALTHY) == health
        && waitCount++ < 20) {
      synchronized (this) {
        wait(100);
      }
    }
    Assert.assertFalse((rm.getRMContext().getRMNodes().get(nm1.getNodeId())
        .getState() != NodeState.UNHEALTHY) == health);
    Assert.assertEquals("Unhealthy metrics not incremented", count,
        ClusterMetrics.getMetrics().getUnhealthyNMs());
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testHandleContainerStatusInvalidCompletions() throws Exception {
    rm = new MockRM(new YarnConfiguration());
    rm.start();

    EventHandler handler =
        spy(rm.getRMContext().getDispatcher().getEventHandler());

    // Case 1: Unmanaged AM
    RMApp app = rm.submitApp(1024, true);

    // Case 1.1: AppAttemptId is null
    NMContainerStatus report =
        NMContainerStatus.newInstance(
          ContainerId.newContainerId(
            ApplicationAttemptId.newInstance(app.getApplicationId(), 2), 1), 0,
          ContainerState.COMPLETE, Resource.newInstance(1024, 1),
          "Dummy Completed", 0, Priority.newInstance(10), 1234);
    rm.getResourceTrackerService().handleNMContainerStatus(report, null);
    verify(handler, never()).handle((Event) any());

    // Case 1.2: Master container is null
    RMAppAttemptImpl currentAttempt =
        (RMAppAttemptImpl) app.getCurrentAppAttempt();
    currentAttempt.setMasterContainer(null);
    report = NMContainerStatus.newInstance(
          ContainerId.newContainerId(currentAttempt.getAppAttemptId(), 0), 0,
          ContainerState.COMPLETE, Resource.newInstance(1024, 1),
          "Dummy Completed", 0, Priority.newInstance(10), 1234);
    rm.getResourceTrackerService().handleNMContainerStatus(report, null);
    verify(handler, never()).handle((Event)any());

    // Case 2: Managed AM
    app = rm.submitApp(1024);

    // Case 2.1: AppAttemptId is null
    report = NMContainerStatus.newInstance(
          ContainerId.newContainerId(
            ApplicationAttemptId.newInstance(app.getApplicationId(), 2), 1), 0,
          ContainerState.COMPLETE, Resource.newInstance(1024, 1),
          "Dummy Completed", 0, Priority.newInstance(10), 1234);
    try {
      rm.getResourceTrackerService().handleNMContainerStatus(report, null);
    } catch (Exception e) {
      // expected - ignore
    }
    verify(handler, never()).handle((Event)any());

    // Case 2.2: Master container is null
    currentAttempt =
        (RMAppAttemptImpl) app.getCurrentAppAttempt();
    currentAttempt.setMasterContainer(null);
    report = NMContainerStatus.newInstance(
      ContainerId.newContainerId(currentAttempt.getAppAttemptId(), 0), 0,
      ContainerState.COMPLETE, Resource.newInstance(1024, 1),
      "Dummy Completed", 0, Priority.newInstance(10), 1234);
    try {
      rm.getResourceTrackerService().handleNMContainerStatus(report, null);
    } catch (Exception e) {
      // expected - ignore
    }
    verify(handler, never()).handle((Event)any());
  }

  @Test
  public void testReconnectNode() throws Exception {
    rm = new MockRM() {
      @Override
      protected EventHandler<SchedulerEvent> createSchedulerEventDispatcher() {
        return new EventDispatcher<SchedulerEvent>(this.scheduler,
            this.scheduler.getClass().getName()) {
          @Override
          public void handle(SchedulerEvent event) {
            scheduler.handle(event);
          }
        };
      }
    };
    rm.start();

    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 5120);
    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(false);
    rm.drainEvents();
    checkUnhealthyNMCount(rm, nm2, true, 1);
    final int expectedNMs = ClusterMetrics.getMetrics().getNumActiveNMs();
    QueueMetrics metrics = rm.getResourceScheduler().getRootQueueMetrics();
    // TODO Metrics incorrect in case of the FifoScheduler
    Assert.assertEquals(5120, metrics.getAvailableMB());

    // reconnect of healthy node
    nm1 = rm.registerNode("host1:1234", 5120);
    NodeHeartbeatResponse response = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(response.getNodeAction()));
    rm.drainEvents();
    Assert.assertEquals(expectedNMs, ClusterMetrics.getMetrics().getNumActiveNMs());
    checkUnhealthyNMCount(rm, nm2, true, 1);

    // reconnect of unhealthy node
    nm2 = rm.registerNode("host2:5678", 5120);
    response = nm2.nodeHeartbeat(false);
    Assert.assertTrue(NodeAction.NORMAL.equals(response.getNodeAction()));
    rm.drainEvents();
    Assert.assertEquals(expectedNMs, ClusterMetrics.getMetrics().getNumActiveNMs());
    checkUnhealthyNMCount(rm, nm2, true, 1);
    
    // unhealthy node changed back to healthy
    nm2 = rm.registerNode("host2:5678", 5120);
    response = nm2.nodeHeartbeat(true);
    response = nm2.nodeHeartbeat(true);
    rm.drainEvents();
    Assert.assertEquals(5120 + 5120, metrics.getAvailableMB());

    // reconnect of node with changed capability
    nm1 = rm.registerNode("host2:5678", 10240);
    response = nm1.nodeHeartbeat(true);
    rm.drainEvents();
    Assert.assertTrue(NodeAction.NORMAL.equals(response.getNodeAction()));
    Assert.assertEquals(5120 + 10240, metrics.getAvailableMB());

    // reconnect of node with changed capability and running applications
    List<ApplicationId> runningApps = new ArrayList<ApplicationId>();
    runningApps.add(ApplicationId.newInstance(1, 0));
    nm1 = rm.registerNode("host2:5678", 15360, 2, runningApps);
    response = nm1.nodeHeartbeat(true);
    rm.drainEvents();
    Assert.assertTrue(NodeAction.NORMAL.equals(response.getNodeAction()));
    Assert.assertEquals(5120 + 15360, metrics.getAvailableMB());
    
    // reconnect healthy node changing http port
    nm1 = new MockNM("host1:1234", 5120, rm.getResourceTrackerService());
    nm1.setHttpPort(3);
    nm1.registerNode();
    response = nm1.nodeHeartbeat(true);
    response = nm1.nodeHeartbeat(true);
    rm.drainEvents();
    RMNode rmNode = rm.getRMContext().getRMNodes().get(nm1.getNodeId());
    Assert.assertEquals(3, rmNode.getHttpPort());
    Assert.assertEquals(5120, rmNode.getTotalCapability().getMemorySize());
    Assert.assertEquals(5120 + 15360, metrics.getAvailableMB());

  }

  @Test
  public void testNMUnregistration() throws Exception {
    Configuration conf = new Configuration();
    rm = new MockRM(conf);
    rm.start();

    ResourceTrackerService resourceTrackerService = rm
        .getResourceTrackerService();
    MockNM nm1 = rm.registerNode("host1:1234", 5120);

    int shutdownNMsCount = ClusterMetrics.getMetrics()
        .getNumShutdownNMs();
    NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));

    UnRegisterNodeManagerRequest request = Records
        .newRecord(UnRegisterNodeManagerRequest.class);
    request.setNodeId(nm1.getNodeId());
    resourceTrackerService.unRegisterNodeManager(request);
    checkShutdownNMCount(rm, ++shutdownNMsCount);

    // The RM should remove the node after unregistration, hence send a reboot
    // command.
    nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.RESYNC.equals(nodeHeartbeat.getNodeAction()));
  }

  @Test
  public void testUnhealthyNMUnregistration() throws Exception {
    Configuration conf = new Configuration();
    rm = new MockRM(conf);
    rm.start();

    ResourceTrackerService resourceTrackerService = rm
        .getResourceTrackerService();
    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    Assert.assertEquals(0, ClusterMetrics.getMetrics().getUnhealthyNMs());
    // node healthy
    nm1.nodeHeartbeat(true);
    int shutdownNMsCount = ClusterMetrics.getMetrics().getNumShutdownNMs();

    // node unhealthy
    nm1.nodeHeartbeat(false);
    checkUnhealthyNMCount(rm, nm1, true, 1);
    UnRegisterNodeManagerRequest request = Records
        .newRecord(UnRegisterNodeManagerRequest.class);
    request.setNodeId(nm1.getNodeId());
    resourceTrackerService.unRegisterNodeManager(request);
    checkShutdownNMCount(rm, ++shutdownNMsCount);
  }

  @Test
  public void testInvalidNMUnregistration() throws Exception {
    Configuration conf = new Configuration();
    rm = new MockRM(conf);
    rm.start();
    ResourceTrackerService resourceTrackerService = rm
        .getResourceTrackerService();
    int decommisionedNMsCount = ClusterMetrics.getMetrics()
        .getNumDecommisionedNMs();

    // Node not found for unregister
    UnRegisterNodeManagerRequest request = Records
        .newRecord(UnRegisterNodeManagerRequest.class);
    request.setNodeId(BuilderUtils.newNodeId("host", 1234));
    resourceTrackerService.unRegisterNodeManager(request);
    checkShutdownNMCount(rm, 0);
    checkDecommissionedNMCount(rm, 0);

    // 1. Register the Node Manager
    // 2. Exclude the same Node Manager host
    // 3. Give NM heartbeat to RM
    // 4. Unregister the Node Manager
    MockNM nm1 = new MockNM("host1:1234", 5120, resourceTrackerService);
    RegisterNodeManagerResponse response = nm1.registerNode();
    Assert.assertEquals(NodeAction.NORMAL, response.getNodeAction());
    int shutdownNMsCount = ClusterMetrics.getMetrics().getNumShutdownNMs();
    writeToHostsFile("host2");
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
        hostFile.getAbsolutePath());
    rm.getNodesListManager().refreshNodes(conf);
    NodeHeartbeatResponse heartbeatResponse = nm1.nodeHeartbeat(true);
    Assert.assertEquals(NodeAction.SHUTDOWN, heartbeatResponse.getNodeAction());
    checkDecommissionedNMCount(rm, decommisionedNMsCount);
    request.setNodeId(nm1.getNodeId());
    resourceTrackerService.unRegisterNodeManager(request);
    checkShutdownNMCount(rm, ++shutdownNMsCount);
    checkDecommissionedNMCount(rm, decommisionedNMsCount);

    // 1. Register the Node Manager
    // 2. Exclude the same Node Manager host
    // 3. Unregister the Node Manager
    MockNM nm2 = new MockNM("host2:1234", 5120, resourceTrackerService);
    RegisterNodeManagerResponse response2 = nm2.registerNode();
    Assert.assertEquals(NodeAction.NORMAL, response2.getNodeAction());
    writeToHostsFile("host1");
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
        hostFile.getAbsolutePath());
    rm.getNodesListManager().refreshNodes(conf);
    request.setNodeId(nm2.getNodeId());
    resourceTrackerService.unRegisterNodeManager(request);
    checkShutdownNMCount(rm, ++shutdownNMsCount);
    checkDecommissionedNMCount(rm, decommisionedNMsCount);
    rm.stop();
  }

  @Test(timeout = 30000)
  public void testInitDecommMetric() throws Exception {
    testInitDecommMetricHelper(true);
    testInitDecommMetricHelper(false);
  }

  public void testInitDecommMetricHelper(boolean hasIncludeList)
      throws Exception {
    Configuration conf = new Configuration();
    rm = new MockRM(conf);
    rm.start();
    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);
    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);

    writeToHostsFile(excludeHostFile, "host1");
    conf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH,
        excludeHostFile.getAbsolutePath());

    if (hasIncludeList) {
      writeToHostsFile(hostFile, "host1", "host2");
      conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
          hostFile.getAbsolutePath());
    }
    rm.getNodesListManager().refreshNodes(conf);
    rm.drainEvents();
    rm.stop();

    MockRM rm1 = new MockRM(conf);
    rm1.start();
    nm1 = rm1.registerNode("host1:1234", 5120);
    nm2 = rm1.registerNode("host2:5678", 10240);
    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);
    rm1.drainEvents();
    Assert.assertEquals("Number of Decommissioned nodes should be 1",
        1, ClusterMetrics.getMetrics().getNumDecommisionedNMs());
    Assert.assertEquals("The inactiveRMNodes should contain an entry for the" +
        "decommissioned node",
        1, rm1.getRMContext().getInactiveRMNodes().size());
    writeToHostsFile(excludeHostFile, "");
    conf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH,
        excludeHostFile.getAbsolutePath());
    rm1.getNodesListManager().refreshNodes(conf);
    nm1 = rm1.registerNode("host1:1234", 5120);
    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);
    rm1.drainEvents();
    Assert.assertEquals("The decommissioned nodes metric should have " +
            "decremented to 0",
        0, ClusterMetrics.getMetrics().getNumDecommisionedNMs());
    Assert.assertEquals("The active nodes metric should be 2",
        2, ClusterMetrics.getMetrics().getNumActiveNMs());
    Assert.assertEquals("The inactive RMNodes entry should have been removed",
        0, rm1.getRMContext().getInactiveRMNodes().size());
    rm1.drainEvents();
    rm1.stop();
  }

  @Test(timeout = 30000)
  public void testInitDecommMetricNoRegistration() throws Exception {
    Configuration conf = new Configuration();
    rm = new MockRM(conf);
    rm.start();
    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);
    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);
    //host3 will not register or heartbeat
    writeToHostsFile(excludeHostFile, "host3", "host2");
    conf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH,
        excludeHostFile.getAbsolutePath());
    writeToHostsFile(hostFile, "host1", "host2");
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
        hostFile.getAbsolutePath());
    rm.getNodesListManager().refreshNodes(conf);
    rm.drainEvents();
    Assert.assertEquals("The decommissioned nodes metric should be 1 ",
        1, ClusterMetrics.getMetrics().getNumDecommisionedNMs());
    rm.stop();

    MockRM rm1 = new MockRM(conf);
    rm1.start();
    rm1.getNodesListManager().refreshNodes(conf);
    rm1.drainEvents();
    Assert.assertEquals("The decommissioned nodes metric should be 2 ",
        2, ClusterMetrics.getMetrics().getNumDecommisionedNMs());
    rm1.stop();
  }

  @Test
  public void testIncorrectRecommission() throws Exception {
    //Check decommissioned node not get recommissioned with graceful refresh
    Configuration conf = new Configuration();
    rm = new MockRM(conf);
    rm.start();
    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);
    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);
    writeToHostsFile(excludeHostFile, "host3", "host2");
    conf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH,
        excludeHostFile.getAbsolutePath());
    writeToHostsFile(hostFile, "host1", "host2");
    writeToHostsFile(excludeHostFile, "host1");
    rm.getNodesListManager().refreshNodesGracefully(conf, null);
    rm.drainEvents();
    nm1.nodeHeartbeat(true);
    rm.drainEvents();
    Assert.assertTrue("Node " + nm1.getNodeId().getHost() +
        " should be Decommissioned", rm.getRMContext()
        .getInactiveRMNodes().get(nm1.getNodeId()).getState() == NodeState
        .DECOMMISSIONED);
    writeToHostsFile(excludeHostFile, "");
    rm.getNodesListManager().refreshNodesGracefully(conf, null);
    rm.drainEvents();
    Assert.assertTrue("Node " + nm1.getNodeId().getHost() +
        " should be Decommissioned", rm.getRMContext()
        .getInactiveRMNodes().get(nm1.getNodeId()).getState() == NodeState
        .DECOMMISSIONED);
    rm.stop();
  }

  /**
   * Remove a node from all lists and check if its forgotten.
   */
  @Test
  public void testNodeRemovalNormally() throws Exception {
    testNodeRemovalUtil(false);
    testNodeRemovalUtilLost(false);
    testNodeRemovalUtilRebooted(false);
    testNodeRemovalUtilUnhealthy(false);
  }

  @Test
  public void testNodeRemovalGracefully() throws Exception {
    testNodeRemovalUtil(true);
    testNodeRemovalUtilLost(true);
    testNodeRemovalUtilRebooted(true);
    testNodeRemovalUtilUnhealthy(true);
  }

  public void refreshNodesOption(boolean doGraceful, Configuration conf)
      throws Exception {
    if (doGraceful) {
      rm.getNodesListManager().refreshNodesGracefully(conf, null);
    } else {
      rm.getNodesListManager().refreshNodes(conf);
    }
  }

  public void testNodeRemovalUtil(boolean doGraceful) throws Exception {
    Configuration conf = new Configuration();
    int timeoutValue = 500;
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH, "");
    conf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH, "");
    conf.setInt(YarnConfiguration.RM_NODEMANAGER_UNTRACKED_REMOVAL_TIMEOUT_MSEC,
        timeoutValue);
    CountDownLatch latch = new CountDownLatch(1);
    rm = new MockRM(conf);
    rm.init(conf);
    rm.start();
    RMContext rmContext = rm.getRMContext();
    refreshNodesOption(doGraceful, conf);
    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);
    MockNM nm3 = rm.registerNode("localhost:4433", 1024);
    ClusterMetrics metrics = ClusterMetrics.getMetrics();
    assert (metrics != null);

    //check all 3 nodes joined in as NORMAL
    NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    nodeHeartbeat = nm3.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    rm.drainEvents();
    Assert.assertEquals("All 3 nodes should be active",
        metrics.getNumActiveNMs(), 3);

    //Remove nm2 from include list, should now be shutdown with timer test
    String ip = NetUtils.normalizeHostName("localhost");
    writeToHostsFile("host1", ip);
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH, hostFile
        .getAbsolutePath());
    refreshNodesOption(doGraceful, conf);
    if (doGraceful) {
      rm.waitForState(nm2.getNodeId(), NodeState.DECOMMISSIONING);
    }
    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);
    rm.drainEvents();
    Assert.assertTrue("Node should not be in active node list",
        !rmContext.getRMNodes().containsKey(nm2.getNodeId()));

    RMNode rmNode = rmContext.getInactiveRMNodes().get(nm2.getNodeId());
    Assert.assertEquals("Node should be in inactive node list",
        rmNode.getState(),
        doGraceful? NodeState.DECOMMISSIONED : NodeState.SHUTDOWN);
    Assert.assertEquals("Active nodes should be 2",
        metrics.getNumActiveNMs(), 2);
    Assert.assertEquals("Shutdown nodes should be expected",
        metrics.getNumShutdownNMs(), doGraceful? 0 : 1);

    int nodeRemovalTimeout =
        conf.getInt(
            YarnConfiguration.RM_NODEMANAGER_UNTRACKED_REMOVAL_TIMEOUT_MSEC,
            YarnConfiguration.
                DEFAULT_RM_NODEMANAGER_UNTRACKED_REMOVAL_TIMEOUT_MSEC);
    int nodeRemovalInterval =
        rmContext.getNodesListManager().getNodeRemovalCheckInterval();
    long maxThreadSleeptime = nodeRemovalInterval + nodeRemovalTimeout;
    latch.await(maxThreadSleeptime, TimeUnit.MILLISECONDS);

    rmNode = rmContext.getInactiveRMNodes().get(nm2.getNodeId());
    Assert.assertEquals("Node should have been forgotten!",
        rmNode, null);
    Assert.assertEquals("Shutdown nodes should be 0 now",
        metrics.getNumShutdownNMs(), 0);

    //Check node removal and re-addition before timer expires
    writeToHostsFile("host1", ip, "host2");
    refreshNodesOption(doGraceful, conf);
    nm2 = rm.registerNode("host2:5678", 10240);
    rm.drainEvents();
    writeToHostsFile("host1", ip);
    refreshNodesOption(doGraceful, conf);
    rm.waitForState(nm2.getNodeId(),
                    doGraceful? NodeState.DECOMMISSIONING : NodeState.SHUTDOWN);
    nm2.nodeHeartbeat(true);
    rm.drainEvents();
    rmNode = rmContext.getInactiveRMNodes().get(nm2.getNodeId());
    Assert.assertEquals("Node should be shutdown",
        rmNode.getState(),
        doGraceful? NodeState.DECOMMISSIONED : NodeState.SHUTDOWN);
    Assert.assertEquals("Active nodes should be 2",
        metrics.getNumActiveNMs(), 2);
    Assert.assertEquals("Shutdown nodes should be expected",
        metrics.getNumShutdownNMs(), doGraceful? 0 : 1);

    //add back the node before timer expires
    latch.await(maxThreadSleeptime - 2000, TimeUnit.MILLISECONDS);
    writeToHostsFile("host1", ip, "host2");
    refreshNodesOption(doGraceful, conf);
    nm2 = rm.registerNode("host2:5678", 10240);
    nodeHeartbeat = nm2.nodeHeartbeat(true);
    rm.drainEvents();
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    Assert.assertEquals("Shutdown nodes should be 0 now",
        metrics.getNumShutdownNMs(), 0);
    Assert.assertEquals("All 3 nodes should be active",
        metrics.getNumActiveNMs(), 3);

    //Decommission this node, check timer doesn't remove it
    writeToHostsFile("host1", "host2", ip);
    writeToHostsFile(excludeHostFile, "host2");
    conf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH, excludeHostFile
        .getAbsolutePath());
    refreshNodesOption(doGraceful, conf);
    rm.drainEvents();
    rmNode = doGraceful ? rmContext.getRMNodes().get(nm2.getNodeId()) :
             rmContext.getInactiveRMNodes().get(nm2.getNodeId());
    Assert.assertTrue("Node should be DECOMMISSIONED or DECOMMISSIONING",
        (rmNode.getState() == NodeState.DECOMMISSIONED) ||
            (rmNode.getState() == NodeState.DECOMMISSIONING));
    if (rmNode.getState() == NodeState.DECOMMISSIONED) {
      Assert.assertEquals("Decommissioned/ing nodes should be 1 now",
          metrics.getNumDecommisionedNMs(), 1);
    }
    latch.await(maxThreadSleeptime, TimeUnit.MILLISECONDS);

    rmNode = doGraceful ? rmContext.getRMNodes().get(nm2.getNodeId()) :
             rmContext.getInactiveRMNodes().get(nm2.getNodeId());
    Assert.assertTrue("Node should be DECOMMISSIONED or DECOMMISSIONING",
        (rmNode.getState() == NodeState.DECOMMISSIONED) ||
            (rmNode.getState() == NodeState.DECOMMISSIONING));
    if (rmNode.getState() == NodeState.DECOMMISSIONED) {
      Assert.assertEquals("Decommissioned/ing nodes should be 1 now",
          metrics.getNumDecommisionedNMs(), 1);
    }

    //Test decommed/ing node that transitions to untracked,timer should remove
    testNodeRemovalUtilDecomToUntracked(rmContext, conf, nm1, nm2, nm3,
        maxThreadSleeptime, doGraceful);
    rm.stop();
  }

  // A helper method used by testNodeRemovalUtil to avoid exceeding
  // max allowed length.
  private void testNodeRemovalUtilDecomToUntracked(
      RMContext rmContext, Configuration conf,
      MockNM nm1, MockNM nm2, MockNM nm3,
      long maxThreadSleeptime, boolean doGraceful) throws Exception {
    ClusterMetrics metrics = ClusterMetrics.getMetrics();
    String ip = NetUtils.normalizeHostName("localhost");
    CountDownLatch latch = new CountDownLatch(1);
    writeToHostsFile("host1", ip, "host2");
    writeToHostsFile(excludeHostFile, "host2");
    refreshNodesOption(doGraceful, conf);
    nm1.nodeHeartbeat(true);
    //nm2.nodeHeartbeat(true);
    nm3.nodeHeartbeat(true);
    latch.await(maxThreadSleeptime, TimeUnit.MILLISECONDS);
    RMNode rmNode = doGraceful ? rmContext.getRMNodes().get(nm2.getNodeId()) :
             rmContext.getInactiveRMNodes().get(nm2.getNodeId());
    Assert.assertNotEquals("Timer for this node was not canceled!",
        rmNode, null);
    Assert.assertTrue("Node should be DECOMMISSIONED or DECOMMISSIONING",
        (rmNode.getState() == NodeState.DECOMMISSIONED) ||
            (rmNode.getState() == NodeState.DECOMMISSIONING));

    writeToHostsFile("host1", ip);
    writeToHostsFile(excludeHostFile, "");
    refreshNodesOption(doGraceful, conf);
    nm2.nodeHeartbeat(true);
    latch.await(maxThreadSleeptime, TimeUnit.MILLISECONDS);
    rmNode = doGraceful ? rmContext.getRMNodes().get(nm2.getNodeId()) :
             rmContext.getInactiveRMNodes().get(nm2.getNodeId());
    Assert.assertEquals("Node should have been forgotten!",
        rmNode, null);
    Assert.assertEquals("Shutdown nodes should be 0 now",
        metrics.getNumDecommisionedNMs(), 0);
    Assert.assertEquals("Shutdown nodes should be 0 now",
        metrics.getNumShutdownNMs(), 0);
    Assert.assertEquals("Active nodes should be 2",
        metrics.getNumActiveNMs(), 2);
  }

  private void testNodeRemovalUtilLost(boolean doGraceful) throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS, 2000);
    int timeoutValue = 500;
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
        hostFile.getAbsolutePath());
    conf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH,
        excludeHostFile.getAbsolutePath());
    writeToHostsFile(hostFile, "host1", "localhost", "host2");
    writeToHostsFile(excludeHostFile, "");
    conf.setInt(YarnConfiguration.RM_NODEMANAGER_UNTRACKED_REMOVAL_TIMEOUT_MSEC,
        timeoutValue);

    rm = new MockRM(conf);
    rm.init(conf);
    rm.start();
    RMContext rmContext = rm.getRMContext();
    refreshNodesOption(doGraceful, conf);
    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);
    MockNM nm3 = rm.registerNode("localhost:4433", 1024);
    ClusterMetrics clusterMetrics = ClusterMetrics.getMetrics();
    ClusterMetrics metrics = clusterMetrics;
    assert (metrics != null);
    rm.drainEvents();
    //check all 3 nodes joined in as NORMAL
    NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    nodeHeartbeat = nm3.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    rm.drainEvents();
    Assert.assertEquals("All 3 nodes should be active",
        metrics.getNumActiveNMs(), 3);
    int waitCount = 0;
    while(waitCount++ < 20){
      synchronized (this) {
        wait(200);
      }
      nm3.nodeHeartbeat(true);
      nm1.nodeHeartbeat(true);
    }
    Assert.assertNotEquals("host2 should be a lost NM!",
        rmContext.getInactiveRMNodes().get(nm2.getNodeId()), null);
    Assert.assertEquals("host2 should be a lost NM!",
        rmContext.getInactiveRMNodes().get(nm2.getNodeId()).getState(),
        NodeState.LOST);
    Assert.assertEquals("There should be 1 Lost NM!",
        clusterMetrics.getNumLostNMs(), 1);
    Assert.assertEquals("There should be 2 Active NM!",
        clusterMetrics.getNumActiveNMs(), 2);
    int nodeRemovalTimeout =
        conf.getInt(
            YarnConfiguration.RM_NODEMANAGER_UNTRACKED_REMOVAL_TIMEOUT_MSEC,
            YarnConfiguration.
                DEFAULT_RM_NODEMANAGER_UNTRACKED_REMOVAL_TIMEOUT_MSEC);
    int nodeRemovalInterval =
        rmContext.getNodesListManager().getNodeRemovalCheckInterval();
    long maxThreadSleeptime = nodeRemovalInterval + nodeRemovalTimeout;
    writeToHostsFile(hostFile, "host1", "localhost");
    writeToHostsFile(excludeHostFile, "");
    refreshNodesOption(doGraceful, conf);
    nm1.nodeHeartbeat(true);
    nm3.nodeHeartbeat(true);
    rm.drainEvents();
    waitCount = 0;
    while(rmContext.getInactiveRMNodes().get(
        nm2.getNodeId()) != null && waitCount++ < 2){
      synchronized (this) {
        wait(maxThreadSleeptime);
        nm1.nodeHeartbeat(true);
        nm2.nodeHeartbeat(true);
      }
    }
    Assert.assertEquals("host2 should have been forgotten!",
        rmContext.getInactiveRMNodes().get(nm2.getNodeId()), null);
    Assert.assertEquals("There should be no Lost NMs!",
        clusterMetrics.getNumLostNMs(), 0);
    Assert.assertEquals("There should be 2 Active NM!",
        clusterMetrics.getNumActiveNMs(), 2);
    rm.stop();
  }

  private void testNodeRemovalUtilRebooted(boolean doGraceful)
      throws Exception {
    Configuration conf = new Configuration();
    int timeoutValue = 500;
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
        hostFile.getAbsolutePath());
    conf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH,
        excludeHostFile.getAbsolutePath());
    writeToHostsFile(hostFile, "host1", "localhost", "host2");
    writeToHostsFile(excludeHostFile, "");
    conf.setInt(YarnConfiguration.RM_NODEMANAGER_UNTRACKED_REMOVAL_TIMEOUT_MSEC,
        timeoutValue);

    rm = new MockRM(conf);
    rm.init(conf);
    rm.start();
    RMContext rmContext = rm.getRMContext();
    refreshNodesOption(doGraceful, conf);
    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);
    MockNM nm3 = rm.registerNode("localhost:4433", 1024);
    ClusterMetrics clusterMetrics = ClusterMetrics.getMetrics();
    ClusterMetrics metrics = clusterMetrics;
    assert (metrics != null);
    NodeHeartbeatResponse nodeHeartbeat = nm2.nodeHeartbeat(
        new HashMap<ApplicationId, List<ContainerStatus>>(), true, -100);
    rm.drainEvents();
    rm.drainEvents();

    Assert.assertNotEquals("host2 should be a rebooted NM!",
        rmContext.getInactiveRMNodes().get(nm2.getNodeId()), null);
    Assert.assertEquals("host2 should be a rebooted NM!",
        rmContext.getInactiveRMNodes().get(nm2.getNodeId()).getState(),
        NodeState.REBOOTED);
    Assert.assertEquals("There should be 1 Rebooted NM!",
        clusterMetrics.getNumRebootedNMs(), 1);
    Assert.assertEquals("There should be 2 Active NM!",
        clusterMetrics.getNumActiveNMs(), 2);

    int nodeRemovalTimeout =
        conf.getInt(
            YarnConfiguration.RM_NODEMANAGER_UNTRACKED_REMOVAL_TIMEOUT_MSEC,
            YarnConfiguration.
                DEFAULT_RM_NODEMANAGER_UNTRACKED_REMOVAL_TIMEOUT_MSEC);
    int nodeRemovalInterval =
        rmContext.getNodesListManager().getNodeRemovalCheckInterval();
    long maxThreadSleeptime = nodeRemovalInterval + nodeRemovalTimeout;
    writeToHostsFile(hostFile, "host1", "localhost");
    writeToHostsFile(excludeHostFile, "");
    refreshNodesOption(doGraceful, conf);
    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);
    nm3.nodeHeartbeat(true);
    rm.drainEvents();
    int waitCount = 0;
    while(rmContext.getInactiveRMNodes().get(
        nm2.getNodeId()) != null && waitCount++ < 2){
      synchronized (this) {
        wait(maxThreadSleeptime);
      }
    }
    Assert.assertEquals("host2 should have been forgotten!",
        rmContext.getInactiveRMNodes().get(nm2.getNodeId()), null);
    Assert.assertEquals("There should be no Rebooted NMs!",
        clusterMetrics.getNumRebootedNMs(), 0);
    Assert.assertEquals("There should be 2 Active NM!",
        clusterMetrics.getNumActiveNMs(), 2);
    rm.stop();
  }

  private void testNodeRemovalUtilUnhealthy(boolean doGraceful)
      throws Exception {
    Configuration conf = new Configuration();
    int timeoutValue = 500;
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
        hostFile.getAbsolutePath());
    conf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH,
        excludeHostFile.getAbsolutePath());
    writeToHostsFile(hostFile, "host1", "localhost", "host2");
    writeToHostsFile(excludeHostFile, "");
    conf.setInt(YarnConfiguration.RM_NODEMANAGER_UNTRACKED_REMOVAL_TIMEOUT_MSEC,
        timeoutValue);

    rm = new MockRM(conf);
    rm.init(conf);
    rm.start();
    RMContext rmContext = rm.getRMContext();
    refreshNodesOption(doGraceful, conf);
    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);
    MockNM nm3 = rm.registerNode("localhost:4433", 1024);
    ClusterMetrics clusterMetrics = ClusterMetrics.getMetrics();
    ClusterMetrics metrics = clusterMetrics;
    assert (metrics != null);
    rm.drainEvents();
    //check all 3 nodes joined in as NORMAL
    NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    nodeHeartbeat = nm3.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    rm.drainEvents();
    Assert.assertEquals("All 3 nodes should be active",
        metrics.getNumActiveNMs(), 3);
    // node healthy
    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(false);
    nm3.nodeHeartbeat(true);
    checkUnhealthyNMCount(rm, nm2, true, 1);
    writeToHostsFile(hostFile, "host1", "localhost");
    writeToHostsFile(excludeHostFile, "");
    refreshNodesOption(doGraceful, conf);
    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(false);
    nm3.nodeHeartbeat(true);
    rm.drainEvents();
    if (!doGraceful) {
      Assert.assertNotEquals("host2 should be a shutdown NM!",
          rmContext.getInactiveRMNodes().get(nm2.getNodeId()), null);
      Assert.assertEquals("host2 should be a shutdown NM!",
          rmContext.getInactiveRMNodes().get(nm2.getNodeId()).getState(),
          NodeState.SHUTDOWN);
    }
    Assert.assertEquals("There should be 2 Active NM!",
        clusterMetrics.getNumActiveNMs(), 2);
    if (!doGraceful) {
      Assert.assertEquals("There should be 1 Shutdown NM!",
          clusterMetrics.getNumShutdownNMs(), 1);
    }
    Assert.assertEquals("There should be 0 Unhealthy NM!",
        clusterMetrics.getUnhealthyNMs(), 0);
    int nodeRemovalTimeout =
        conf.getInt(
            YarnConfiguration.RM_NODEMANAGER_UNTRACKED_REMOVAL_TIMEOUT_MSEC,
            YarnConfiguration.
                DEFAULT_RM_NODEMANAGER_UNTRACKED_REMOVAL_TIMEOUT_MSEC);
    int nodeRemovalInterval =
        rmContext.getNodesListManager().getNodeRemovalCheckInterval();
    long maxThreadSleeptime = nodeRemovalInterval + nodeRemovalTimeout;
    int waitCount = 0;
    while(rmContext.getInactiveRMNodes().get(
        nm2.getNodeId()) != null && waitCount++ < 2){
      synchronized (this) {
        wait(maxThreadSleeptime);
      }
    }
    Assert.assertEquals("host2 should have been forgotten!",
        rmContext.getInactiveRMNodes().get(nm2.getNodeId()), null);
    Assert.assertEquals("There should be no Shutdown NMs!",
        clusterMetrics.getNumRebootedNMs(), 0);
    Assert.assertEquals("There should be 2 Active NM!",
        clusterMetrics.getNumActiveNMs(), 2);
    rm.stop();
  }

  private void writeToHostsFile(String... hosts) throws IOException {
    writeToHostsFile(hostFile, hosts);
  }

  private void writeToHostsFile(File file, String... hosts)
      throws IOException {
    if (!file.exists()) {
      TEMP_DIR.mkdirs();
      file.createNewFile();
    }
    FileOutputStream fStream = null;
    try {
      fStream = new FileOutputStream(file);
      for (int i = 0; i < hosts.length; i++) {
        fStream.write(hosts[i].getBytes());
        fStream.write("\n".getBytes());
      }
    } finally {
      if (fStream != null) {
        IOUtils.closeStream(fStream);
        fStream = null;
      }
    }
  }

  private void checkDecommissionedNMCount(MockRM rm, int count)
      throws InterruptedException {
    int waitCount = 0;
    while (ClusterMetrics.getMetrics().getNumDecommisionedNMs() != count
        && waitCount++ < 20) {
      synchronized (this) {
        wait(100);
      }
    }
    Assert.assertEquals(count, ClusterMetrics.getMetrics()
        .getNumDecommisionedNMs());
    Assert.assertEquals("The decommisioned metrics are not updated", count,
        ClusterMetrics.getMetrics().getNumDecommisionedNMs());
  }

  private void checkShutdownNMCount(MockRM rm, int count)
      throws InterruptedException {
    int waitCount = 0;
    while (ClusterMetrics.getMetrics().getNumShutdownNMs() != count
        && waitCount++ < 20) {
      synchronized (this) {
        wait(100);
      }
    }
    Assert.assertEquals("The shutdown metrics are not updated", count,
        ClusterMetrics.getMetrics().getNumShutdownNMs());
  }

  @After
  public void tearDown() {
    if (hostFile != null && hostFile.exists()) {
      hostFile.delete();
    }

    ClusterMetrics.destroy();
    if (rm != null) {
      rm.stop();
    }

    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms.getSource("ClusterMetrics") != null) {
      DefaultMetricsSystem.shutdown();
    }
  }

  @Test(timeout = 60000)
  public void testNodeHeartBeatResponseForUnknownContainerCleanUp()
      throws Exception {
    Configuration conf = new Configuration();
    rm = new MockRM(conf);
    rm.init(conf);
    rm.start();

    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    rm.drainEvents();

    // send 1st heartbeat
    nm1.nodeHeartbeat(true);

    // Create 2 unknown containers tracked by NM
    ApplicationId applicationId = BuilderUtils.newApplicationId(1, 1);
    ApplicationAttemptId applicationAttemptId = BuilderUtils
        .newApplicationAttemptId(applicationId, 1);
    ContainerId cid1 = BuilderUtils.newContainerId(applicationAttemptId, 2);
    ContainerId cid2 = BuilderUtils.newContainerId(applicationAttemptId, 3);
    ArrayList<ContainerStatus> containerStats =
        new ArrayList<ContainerStatus>();
    containerStats.add(
        ContainerStatus.newInstance(cid1, ContainerState.COMPLETE, "", -1));
    containerStats.add(
        ContainerStatus.newInstance(cid2, ContainerState.COMPLETE, "", -1));

    Map<ApplicationId, List<ContainerStatus>> conts =
        new HashMap<ApplicationId, List<ContainerStatus>>();
    conts.put(applicationAttemptId.getApplicationId(), containerStats);

    // add RMApp into context.
    RMApp app1 = mock(RMApp.class);
    when(app1.getApplicationId()).thenReturn(applicationId);
    rm.getRMContext().getRMApps().put(applicationId, app1);

    // Send unknown container status in heartbeat
    nm1.nodeHeartbeat(conts, true);
    rm.drainEvents();

    int containersToBeRemovedFromNM = 0;
    while (true) {
      NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
      rm.drainEvents();
      containersToBeRemovedFromNM +=
          nodeHeartbeat.getContainersToBeRemovedFromNM().size();
      // asserting for 2 since two unknown containers status has been sent
      if (containersToBeRemovedFromNM == 2) {
        break;
      }
    }
  }
}
