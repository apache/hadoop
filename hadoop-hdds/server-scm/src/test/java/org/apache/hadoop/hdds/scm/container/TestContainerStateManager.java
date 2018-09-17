/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.container;

import java.io.IOException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationRequest;

import org.apache.hadoop.hdds.scm.pipelines.PipelineSelector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Testing ContainerStatemanager.
 */
public class TestContainerStateManager {

  private ContainerStateManager containerStateManager;

  @Before
  public void init() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    Mapping mapping = Mockito.mock(Mapping.class);
    PipelineSelector selector =  Mockito.mock(PipelineSelector.class);
    containerStateManager = new ContainerStateManager(conf, mapping, selector);

  }

  @Test
  public void checkReplicationStateOK() throws IOException {
    //GIVEN
    ContainerInfo c1 = TestUtils.allocateContainer(containerStateManager);

    DatanodeDetails d1 = TestUtils.randomDatanodeDetails();
    DatanodeDetails d2 = TestUtils.randomDatanodeDetails();
    DatanodeDetails d3 = TestUtils.randomDatanodeDetails();

    addReplica(c1, d1);
    addReplica(c1, d2);
    addReplica(c1, d3);

    //WHEN
    ReplicationRequest replicationRequest = containerStateManager
        .checkReplicationState(new ContainerID(c1.getContainerID()));

    //THEN
    Assert.assertNull(replicationRequest);
  }

  @Test
  public void checkReplicationStateMissingReplica() throws IOException {
    //GIVEN

    ContainerInfo c1 = TestUtils.allocateContainer(containerStateManager);

    DatanodeDetails d1 = TestUtils.randomDatanodeDetails();
    DatanodeDetails d2 = TestUtils.randomDatanodeDetails();

    addReplica(c1, d1);
    addReplica(c1, d2);

    //WHEN
    ReplicationRequest replicationRequest = containerStateManager
        .checkReplicationState(new ContainerID(c1.getContainerID()));

    Assert
        .assertEquals(c1.getContainerID(), replicationRequest.getContainerId());
    Assert.assertEquals(2, replicationRequest.getReplicationCount());
    Assert.assertEquals(3, replicationRequest.getExpecReplicationCount());
  }

  private void addReplica(ContainerInfo c1, DatanodeDetails d1) {
    containerStateManager
        .addContainerReplica(new ContainerID(c1.getContainerID()), d1);
  }

}