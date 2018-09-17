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
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerStateManager;
import org.apache.hadoop.hdds.scm.container.Mapping;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationRequest;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.states.Node2ContainerMap;
import org.apache.hadoop.hdds.scm.pipelines.PipelineSelector;
import org.apache.hadoop.hdds.server.events.EventPublisher;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import static org.mockito.Matchers.eq;
import org.mockito.Mockito;

/**
 * Test DeadNodeHandler.
 */
public class TestDeadNodeHandler {

  private List<ReplicationRequest> sentEvents = new ArrayList<>();

  @Test
  public void testOnMessage() throws IOException {
    //GIVEN
    DatanodeDetails datanode1 = TestUtils.randomDatanodeDetails();
    DatanodeDetails datanode2 = TestUtils.randomDatanodeDetails();

    Node2ContainerMap node2ContainerMap = new Node2ContainerMap();
    ContainerStateManager containerStateManager = new ContainerStateManager(
        new OzoneConfiguration(),
        Mockito.mock(Mapping.class),
        Mockito.mock(PipelineSelector.class)
    );

    ContainerInfo container1 =
        TestUtils.allocateContainer(containerStateManager);
    ContainerInfo container2 =
        TestUtils.allocateContainer(containerStateManager);
    ContainerInfo container3 =
        TestUtils.allocateContainer(containerStateManager);

    DeadNodeHandler handler =
        new DeadNodeHandler(node2ContainerMap, containerStateManager);

    registerReplicas(node2ContainerMap, datanode1, container1, container2);
    registerReplicas(node2ContainerMap, datanode2, container1, container3);

    registerReplicas(containerStateManager, container1, datanode1, datanode2);
    registerReplicas(containerStateManager, container2, datanode1);
    registerReplicas(containerStateManager, container3, datanode2);

    TestUtils.closeContainer(containerStateManager, container1);

    EventPublisher publisher = Mockito.mock(EventPublisher.class);

    //WHEN datanode1 is dead
    handler.onMessage(datanode1, publisher);

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

  private void registerReplicas(ContainerStateManager containerStateManager,
      ContainerInfo container, DatanodeDetails... datanodes) {
    containerStateManager.getContainerStateMap()
        .addContainerReplica(new ContainerID(container.getContainerID()),
            datanodes);
  }

  private void registerReplicas(Node2ContainerMap node2ContainerMap,
      DatanodeDetails datanode,
      ContainerInfo... containers)
      throws SCMException {
    node2ContainerMap
        .insertNewDatanode(datanode.getUuid(),
            Arrays.stream(containers)
                .map(container -> new ContainerID(container.getContainerID()))
                .collect(Collectors.toSet()));
  }

}