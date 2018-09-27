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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerStateManager;
import org.apache.hadoop.hdds.scm.container.Mapping;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationRequest;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.states.Node2ContainerMap;
import org.apache.hadoop.hdds.scm.pipelines.PipelineSelector;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.NodeReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventPublisher;

import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import static org.mockito.Matchers.eq;
import org.mockito.Mockito;

/**
 * Test DeadNodeHandler.
 */
public class TestDeadNodeHandler {

  private List<ReplicationRequest> sentEvents = new ArrayList<>();
  private SCMNodeManager nodeManager;
  private Node2ContainerMap node2ContainerMap;
  private ContainerStateManager containerStateManager;
  private NodeReportHandler nodeReportHandler;
  private DeadNodeHandler deadNodeHandler;
  private EventPublisher publisher;
  private EventQueue eventQueue;

  @Before
  public void setup() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    node2ContainerMap = new Node2ContainerMap();
    containerStateManager = new ContainerStateManager(conf,
        Mockito.mock(Mapping.class),
        Mockito.mock(PipelineSelector.class));
    eventQueue = new EventQueue();
    nodeManager = new SCMNodeManager(conf, "cluster1", null, eventQueue);
    deadNodeHandler = new DeadNodeHandler(node2ContainerMap,
        containerStateManager, nodeManager);
    eventQueue.addHandler(SCMEvents.DEAD_NODE, deadNodeHandler);
    publisher = Mockito.mock(EventPublisher.class);
    nodeReportHandler = new NodeReportHandler(nodeManager);
  }

  @Test
  public void testOnMessage() throws IOException {
    //GIVEN
    DatanodeDetails datanode1 = TestUtils.randomDatanodeDetails();
    DatanodeDetails datanode2 = TestUtils.randomDatanodeDetails();

    ContainerInfo container1 =
        TestUtils.allocateContainer(containerStateManager);
    ContainerInfo container2 =
        TestUtils.allocateContainer(containerStateManager);
    ContainerInfo container3 =
        TestUtils.allocateContainer(containerStateManager);

    registerReplicas(node2ContainerMap, datanode1, container1, container2);
    registerReplicas(node2ContainerMap, datanode2, container1, container3);

    registerReplicas(containerStateManager, container1, datanode1, datanode2);
    registerReplicas(containerStateManager, container2, datanode1);
    registerReplicas(containerStateManager, container3, datanode2);

    TestUtils.closeContainer(containerStateManager, container1);

    //WHEN datanode1 is dead
    deadNodeHandler.onMessage(datanode1, publisher);

    //THEN
    //node2ContainerMap has not been changed
    Assert.assertEquals(2, node2ContainerMap.size());

    Set<DatanodeDetails> container1Replicas =
        containerStateManager.getContainerStateMap()
            .getContainerReplicas(new ContainerID(container1.getContainerID()));
    Assert.assertEquals(1, container1Replicas.size());
    Assert.assertEquals(datanode2, container1Replicas.iterator().next());

    Set<DatanodeDetails> container2Replicas =
        containerStateManager.getContainerStateMap()
            .getContainerReplicas(new ContainerID(container2.getContainerID()));
    Assert.assertEquals(0, container2Replicas.size());

    Set<DatanodeDetails> container3Replicas =
        containerStateManager.getContainerStateMap()
            .getContainerReplicas(new ContainerID(container3.getContainerID()));
    Assert.assertEquals(1, container3Replicas.size());
    Assert.assertEquals(datanode2, container3Replicas.iterator().next());

    ArgumentCaptor<ReplicationRequest> replicationRequestParameter =
        ArgumentCaptor.forClass(ReplicationRequest.class);

    Mockito.verify(publisher)
        .fireEvent(eq(SCMEvents.REPLICATE_CONTAINER),
            replicationRequestParameter.capture());

    Assert
        .assertEquals(container1.getContainerID(),
            replicationRequestParameter.getValue().getContainerId());
    Assert
        .assertEquals(1,
            replicationRequestParameter.getValue().getReplicationCount());
    Assert
        .assertEquals(3,
            replicationRequestParameter.getValue().getExpecReplicationCount());
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
    nodeReportHandler.onMessage(getNodeReport(datanode1, storageOne),
        Mockito.mock(EventPublisher.class));
    nodeReportHandler.onMessage(getNodeReport(datanode2, storageTwo),
        Mockito.mock(EventPublisher.class));

    ContainerInfo container1 =
        TestUtils.allocateContainer(containerStateManager);
    registerReplicas(node2ContainerMap, datanode1, container1);

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

  private void registerReplicas(ContainerStateManager csm,
      ContainerInfo container, DatanodeDetails... datanodes) {
    csm.getContainerStateMap()
        .addContainerReplica(new ContainerID(container.getContainerID()),
            datanodes);
  }

  private void registerReplicas(Node2ContainerMap node2ConMap,
      DatanodeDetails datanode,
      ContainerInfo... containers)
      throws SCMException {
    node2ConMap
        .insertNewDatanode(datanode.getUuid(),
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
