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

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerInfo;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerStateManager;
import org.apache.hadoop.hdds.scm.container.Mapping;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.states.Node2ContainerMap;
import org.apache.hadoop.hdds.server.events.EventPublisher;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test DeadNodeHandler.
 */
public class TestDeadNodeHandler {
  @Test
  public void testOnMessage() throws SCMException {
    //GIVEN
    DatanodeDetails datanode1 = TestUtils.randomDatanodeDetails();
    DatanodeDetails datanode2 = TestUtils.randomDatanodeDetails();

    ContainerInfo container1 = TestUtils.getRandomContainerInfo(1);
    ContainerInfo container2 = TestUtils.getRandomContainerInfo(2);
    ContainerInfo container3 = TestUtils.getRandomContainerInfo(3);

    Node2ContainerMap node2ContainerMap = new Node2ContainerMap();
    ContainerStateManager containerStateManager = new ContainerStateManager(
        new OzoneConfiguration(),
        Mockito.mock(Mapping.class)
    );
    DeadNodeHandler handler =
        new DeadNodeHandler(node2ContainerMap, containerStateManager);

    node2ContainerMap
        .insertNewDatanode(datanode1.getUuid(), new HashSet<ContainerID>() {{
            add(new ContainerID(container1.getContainerID()));
            add(new ContainerID(container2.getContainerID()));
          }});

    node2ContainerMap
        .insertNewDatanode(datanode2.getUuid(), new HashSet<ContainerID>() {{
            add(new ContainerID(container1.getContainerID()));
            add(new ContainerID(container3.getContainerID()));
          }});

    containerStateManager.getContainerStateMap()
        .addContainerReplica(new ContainerID(container1.getContainerID()),
            datanode1, datanode2);

    containerStateManager.getContainerStateMap()
        .addContainerReplica(new ContainerID(container2.getContainerID()),
            datanode1);

    containerStateManager.getContainerStateMap()
        .addContainerReplica(new ContainerID(container3.getContainerID()),
            datanode2);

    //WHEN datanode1 is dead
    handler.onMessage(datanode1, Mockito.mock(EventPublisher.class));

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

  }
}