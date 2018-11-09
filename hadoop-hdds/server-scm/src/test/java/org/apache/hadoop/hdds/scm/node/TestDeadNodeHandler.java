/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.node;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.SCMContainerManager;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.SCMPipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
    .NodeReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventPublisher;

import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test DeadNodeHandler.
 */
public class TestDeadNodeHandler {

  private SCMNodeManager nodeManager;
  private ContainerManager containerManager;
  private NodeReportHandler nodeReportHandler;
  private DeadNodeHandler deadNodeHandler;
  private EventPublisher publisher;
  private EventQueue eventQueue;
  private String storageDir;

  @Before
  public void setup() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    storageDir = GenericTestUtils.getTempPath(
        TestDeadNodeHandler.class.getSimpleName() + UUID.randomUUID());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, storageDir);
    eventQueue = new EventQueue();
    nodeManager = new SCMNodeManager(conf, "cluster1", null, eventQueue);
    PipelineManager pipelineManager =
        new SCMPipelineManager(conf, nodeManager, eventQueue);
    containerManager = new SCMContainerManager(conf, nodeManager,
        pipelineManager, eventQueue);
    deadNodeHandler = new DeadNodeHandler(nodeManager, containerManager);
    eventQueue.addHandler(SCMEvents.DEAD_NODE, deadNodeHandler);
    publisher = Mockito.mock(EventPublisher.class);
    nodeReportHandler = new NodeReportHandler(nodeManager);
  }

  @After
  public void teardown() {
    FileUtil.fullyDelete(new File(storageDir));
  }

  @Test
  public void testOnMessage() throws IOException, NodeNotFoundException {
    //GIVEN
    DatanodeDetails datanode1 = TestUtils.randomDatanodeDetails();
    DatanodeDetails datanode2 = TestUtils.randomDatanodeDetails();
    DatanodeDetails datanode3 = TestUtils.randomDatanodeDetails();

    String storagePath = GenericTestUtils.getRandomizedTempPath()
        .concat("/" + datanode1.getUuidString());

    StorageReportProto storageOne = TestUtils.createStorageReport(
        datanode1.getUuid(), storagePath, 100, 10, 90, null);

    // Standalone pipeline now excludes the nodes which are already used,
    // is the a proper behavior. Adding 9 datanodes for now to make the
    // test case happy.

    nodeManager.register(datanode1,
        TestUtils.createNodeReport(storageOne), null);
    nodeManager.register(datanode2,
        TestUtils.createNodeReport(storageOne), null);
    nodeManager.register(datanode3,
        TestUtils.createNodeReport(storageOne), null);

    nodeManager.register(TestUtils.randomDatanodeDetails(),
        TestUtils.createNodeReport(storageOne), null);
    nodeManager.register(TestUtils.randomDatanodeDetails(),
        TestUtils.createNodeReport(storageOne), null);
    nodeManager.register(TestUtils.randomDatanodeDetails(),
        TestUtils.createNodeReport(storageOne), null);

    nodeManager.register(TestUtils.randomDatanodeDetails(),
        TestUtils.createNodeReport(storageOne), null);
    nodeManager.register(TestUtils.randomDatanodeDetails(),
        TestUtils.createNodeReport(storageOne), null);
    nodeManager.register(TestUtils.randomDatanodeDetails(),
        TestUtils.createNodeReport(storageOne), null);

    ContainerInfo container1 =
        TestUtils.allocateContainer(containerManager);
    ContainerInfo container2 =
        TestUtils.allocateContainer(containerManager);
    ContainerInfo container3 =
        TestUtils.allocateContainer(containerManager);

    registerReplicas(datanode1, container1, container2);
    registerReplicas(datanode2, container1, container3);

    registerReplicas(containerManager, container1, datanode1, datanode2);
    registerReplicas(containerManager, container2, datanode1);
    registerReplicas(containerManager, container3, datanode2);

    TestUtils.closeContainer(containerManager, container1.containerID());
    TestUtils.closeContainer(containerManager, container2.containerID());
    TestUtils.closeContainer(containerManager, container3.containerID());

    deadNodeHandler.onMessage(datanode1, publisher);

    Set<ContainerReplica> container1Replicas = containerManager
        .getContainerReplicas(new ContainerID(container1.getContainerID()));
    Assert.assertEquals(1, container1Replicas.size());
    Assert.assertEquals(datanode2,
        container1Replicas.iterator().next().getDatanodeDetails());

    Set<ContainerReplica> container2Replicas = containerManager
        .getContainerReplicas(new ContainerID(container2.getContainerID()));
    Assert.assertEquals(0, container2Replicas.size());

    Set<ContainerReplica> container3Replicas = containerManager
            .getContainerReplicas(new ContainerID(container3.getContainerID()));
    Assert.assertEquals(1, container3Replicas.size());
    Assert.assertEquals(datanode2,
        container3Replicas.iterator().next().getDatanodeDetails());
  }

  @Test
  public void testStatisticsUpdate() throws Exception {
    //GIVEN
    DatanodeDetails datanode1 = TestUtils.randomDatanodeDetails();
    DatanodeDetails datanode2 = TestUtils.randomDatanodeDetails();

    String storagePath1 = GenericTestUtils.getRandomizedTempPath()
        .concat("/" + datanode1.getUuidString());
    String storagePath2 = GenericTestUtils.getRandomizedTempPath()
        .concat("/" + datanode2.getUuidString());

    StorageReportProto storageOne = TestUtils.createStorageReport(
        datanode1.getUuid(), storagePath1, 100, 10, 90, null);
    StorageReportProto storageTwo = TestUtils.createStorageReport(
        datanode2.getUuid(), storagePath2, 200, 20, 180, null);

    nodeManager.register(datanode1,
        TestUtils.createNodeReport(storageOne), null);
    nodeManager.register(datanode2,
        TestUtils.createNodeReport(storageTwo), null);

    nodeReportHandler.onMessage(getNodeReport(datanode1, storageOne),
        Mockito.mock(EventPublisher.class));
    nodeReportHandler.onMessage(getNodeReport(datanode2, storageTwo),
        Mockito.mock(EventPublisher.class));

    SCMNodeStat stat = nodeManager.getStats();
    Assert.assertTrue(stat.getCapacity().get() == 300);
    Assert.assertTrue(stat.getRemaining().get() == 270);
    Assert.assertTrue(stat.getScmUsed().get() == 30);

    SCMNodeMetric nodeStat = nodeManager.getNodeStat(datanode1);
    Assert.assertTrue(nodeStat.get().getCapacity().get() == 100);
    Assert.assertTrue(nodeStat.get().getRemaining().get() == 90);
    Assert.assertTrue(nodeStat.get().getScmUsed().get() == 10);

    //WHEN datanode1 is dead.
    eventQueue.fireEvent(SCMEvents.DEAD_NODE, datanode1);
    Thread.sleep(100);

    //THEN statistics in SCM should changed.
    stat = nodeManager.getStats();
    Assert.assertTrue(stat.getCapacity().get() == 200);
    Assert.assertTrue(stat.getRemaining().get() == 180);
    Assert.assertTrue(stat.getScmUsed().get() == 20);

    nodeStat = nodeManager.getNodeStat(datanode1);
    Assert.assertTrue(nodeStat.get().getCapacity().get() == 0);
    Assert.assertTrue(nodeStat.get().getRemaining().get() == 0);
    Assert.assertTrue(nodeStat.get().getScmUsed().get() == 0);
  }

  @Test
  public void testOnMessageReplicaFailure() throws Exception {

    DatanodeDetails datanode1 = TestUtils.randomDatanodeDetails();
    DatanodeDetails datanode2 = TestUtils.randomDatanodeDetails();
    DatanodeDetails datanode3 = TestUtils.randomDatanodeDetails();

    String storagePath = GenericTestUtils.getRandomizedTempPath()
        .concat("/" + datanode1.getUuidString());

    StorageReportProto storageOne = TestUtils.createStorageReport(
        datanode1.getUuid(), storagePath, 100, 10, 90, null);

    nodeManager.register(datanode1,
        TestUtils.createNodeReport(storageOne), null);
    nodeManager.register(datanode2,
        TestUtils.createNodeReport(storageOne), null);
    nodeManager.register(datanode3,
        TestUtils.createNodeReport(storageOne), null);

    DatanodeDetails dn1 = TestUtils.randomDatanodeDetails();
    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(DeadNodeHandler.getLogger());

    nodeReportHandler.onMessage(getNodeReport(dn1, storageOne),
        Mockito.mock(EventPublisher.class));

    ContainerInfo container1 =
        TestUtils.allocateContainer(containerManager);
    TestUtils.closeContainer(containerManager, container1.containerID());

    deadNodeHandler.onMessage(dn1, eventQueue);
    Assert.assertTrue(logCapturer.getOutput().contains(
        "DeadNode event for a unregistered node"));
  }

  private void registerReplicas(ContainerManager containerManager,
      ContainerInfo container, DatanodeDetails... datanodes)
      throws ContainerNotFoundException {
    for (DatanodeDetails datanode : datanodes) {
      containerManager.updateContainerReplica(
          new ContainerID(container.getContainerID()),
          ContainerReplica.newBuilder()
              .setContainerState(ContainerReplicaProto.State.OPEN)
              .setContainerID(container.containerID())
              .setDatanodeDetails(datanode).build());
    }
  }

  private void registerReplicas(DatanodeDetails datanode,
      ContainerInfo... containers)
      throws NodeNotFoundException {
    nodeManager
        .setContainers(datanode,
            Arrays.stream(containers)
                .map(container -> new ContainerID(container.getContainerID()))
                .collect(Collectors.toSet()));
  }

  private NodeReportFromDatanode getNodeReport(DatanodeDetails dn,
      StorageReportProto... reports) {
    NodeReportProto nodeReportProto = TestUtils.createNodeReport(reports);
    return new NodeReportFromDatanode(dn, nodeReportProto);
  }
}
