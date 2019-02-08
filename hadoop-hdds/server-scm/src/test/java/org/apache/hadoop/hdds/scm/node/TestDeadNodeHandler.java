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
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
    .NodeReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;

import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.event.Level;

/**
 * Test DeadNodeHandler.
 */
public class TestDeadNodeHandler {

  private StorageContainerManager scm;
  private SCMNodeManager nodeManager;
  private ContainerManager containerManager;
  private NodeReportHandler nodeReportHandler;
  private DeadNodeHandler deadNodeHandler;
  private EventPublisher publisher;
  private EventQueue eventQueue;
  private String storageDir;

  @Before
  public void setup() throws IOException, AuthenticationException {
    OzoneConfiguration conf = new OzoneConfiguration();
    storageDir = GenericTestUtils.getTempPath(
        TestDeadNodeHandler.class.getSimpleName() + UUID.randomUUID());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, storageDir);
    conf.set(HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL, "100ms");
    conf.set(ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, "50ms");
    conf.set(ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL, "1s");
    conf.set(ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL, "2s");
    eventQueue = new EventQueue();
    scm = HddsTestUtils.getScm(conf);
    nodeManager = (SCMNodeManager) scm.getScmNodeManager();
    containerManager = scm.getContainerManager();
    deadNodeHandler = new DeadNodeHandler(nodeManager, containerManager);
    eventQueue.addHandler(SCMEvents.DEAD_NODE, deadNodeHandler);
    publisher = Mockito.mock(EventPublisher.class);
    nodeReportHandler = new NodeReportHandler(nodeManager);
  }

  @After
  public void teardown() {
    scm.stop();
    scm.join();
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
    ContainerInfo container4 =
        TestUtils.allocateContainer(containerManager);

    registerContainers(datanode1, container1, container2, container4);
    registerContainers(datanode2, container1, container2);
    registerContainers(datanode3, container3);

    registerReplicas(containerManager, container1, datanode1, datanode2);
    registerReplicas(containerManager, container2, datanode1, datanode2);
    registerReplicas(containerManager, container3, datanode3);
    registerReplicas(containerManager, container4, datanode1);

    TestUtils.closeContainer(containerManager, container1.containerID());
    TestUtils.closeContainer(containerManager, container2.containerID());
    TestUtils.quasiCloseContainer(containerManager, container3.containerID());

    GenericTestUtils.setLogLevel(DeadNodeHandler.getLogger(), Level.DEBUG);
    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(DeadNodeHandler.getLogger());

    deadNodeHandler.onMessage(datanode1, publisher);

    Set<ContainerReplica> container1Replicas = containerManager
        .getContainerReplicas(new ContainerID(container1.getContainerID()));
    Assert.assertEquals(1, container1Replicas.size());
    Assert.assertEquals(datanode2,
        container1Replicas.iterator().next().getDatanodeDetails());

    Set<ContainerReplica> container2Replicas = containerManager
        .getContainerReplicas(new ContainerID(container2.getContainerID()));
    Assert.assertEquals(1, container2Replicas.size());
    Assert.assertEquals(datanode2,
        container2Replicas.iterator().next().getDatanodeDetails());

    Set<ContainerReplica> container3Replicas = containerManager
            .getContainerReplicas(new ContainerID(container3.getContainerID()));
    Assert.assertEquals(1, container3Replicas.size());
    Assert.assertEquals(datanode3,
        container3Replicas.iterator().next().getDatanodeDetails());

    // Replicate should be fired for container 1 and container 2 as now
    // datanode 1 is dead, these 2 will not match with expected replica count
    // and their state is one of CLOSED/QUASI_CLOSE.
    Assert.assertTrue(logCapturer.getOutput().contains(
        "Replicate Request fired for container " +
            container1.getContainerID()));
    Assert.assertTrue(logCapturer.getOutput().contains(
        "Replicate Request fired for container " +
            container2.getContainerID()));

    // as container4 is still in open state, replicate event should not have
    // fired for this.
    Assert.assertFalse(logCapturer.getOutput().contains(
        "Replicate Request fired for container " +
            container4.getContainerID()));


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

    //TODO: Support logic to mark a node as dead in NodeManager.

    nodeManager.processHeartbeat(datanode2);
    Thread.sleep(1000);
    nodeManager.processHeartbeat(datanode2);
    Thread.sleep(1000);
    nodeManager.processHeartbeat(datanode2);
    Thread.sleep(1000);
    nodeManager.processHeartbeat(datanode2);
    //THEN statistics in SCM should changed.
    stat = nodeManager.getStats();
    Assert.assertEquals(200L, stat.getCapacity().get().longValue());
    Assert.assertEquals(180L,
        stat.getRemaining().get().longValue());
    Assert.assertEquals(20L, stat.getScmUsed().get().longValue());
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

  private void registerReplicas(ContainerManager contManager,
      ContainerInfo container, DatanodeDetails... datanodes)
      throws ContainerNotFoundException {
    for (DatanodeDetails datanode : datanodes) {
      contManager.updateContainerReplica(
          new ContainerID(container.getContainerID()),
          ContainerReplica.newBuilder()
              .setContainerState(ContainerReplicaProto.State.OPEN)
              .setContainerID(container.containerID())
              .setDatanodeDetails(datanode).build());
    }
  }

  /**
   * Update containers available on the datanode.
   * @param datanode
   * @param containers
   * @throws NodeNotFoundException
   */
  private void registerContainers(DatanodeDetails datanode,
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
