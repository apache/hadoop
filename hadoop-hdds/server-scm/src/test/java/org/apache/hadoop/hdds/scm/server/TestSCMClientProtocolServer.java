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
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
    eventQueue.addHandler(SCMEvents.CHILL_MODE_STATUS, scmClientProtocolServer);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testAllocateContainerFailureInChillMode() throws Exception {
    LambdaTestUtils.intercept(SCMException.class,
        "hillModePrecheck failed for allocateContainer", () -> {
          scmClientProtocolServer.allocateContainer(
              ReplicationType.STAND_ALONE, ReplicationFactor.ONE, "");
        });
  }
}