/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.server;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.block.BlockManager;
import org.apache.hadoop.hdds.scm.block.BlockManagerImpl;
import org.apache.hadoop.hdds.scm.safemode.SafeModeHandler;
import org.apache.hadoop.hdds.scm.container.ReplicationManager;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.SCMPipelineManager;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test class for @{@link SCMClientProtocolServer}.
 * */
public class TestSCMClientProtocolServer {
  private SCMClientProtocolServer scmClientProtocolServer;
  private OzoneConfiguration config;
  private EventQueue eventQueue;

  @Before
  public void setUp() throws Exception {
    config = new OzoneConfiguration();
    eventQueue = new EventQueue();
    scmClientProtocolServer = new SCMClientProtocolServer(config, null);
    BlockManager blockManager = Mockito.mock(BlockManagerImpl.class);
    ReplicationManager replicationManager =
        Mockito.mock(ReplicationManager.class);
    PipelineManager pipelineManager = Mockito.mock(SCMPipelineManager.class);
    SafeModeHandler safeModeHandler = new SafeModeHandler(config,
        scmClientProtocolServer, blockManager, replicationManager,
        pipelineManager);
    eventQueue.addHandler(SCMEvents.SAFE_MODE_STATUS, safeModeHandler);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testAllocateContainerFailureInSafeMode() throws Exception {
    LambdaTestUtils.intercept(SCMException.class,
        "SafeModePrecheck failed for allocateContainer", () -> {
          scmClientProtocolServer.allocateContainer(
              ReplicationType.STAND_ALONE, ReplicationFactor.ONE, "");
        });
  }
}