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

package org.apache.hadoop.hdds.scm.safemode;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.block.BlockManager;
import org.apache.hadoop.hdds.scm.block.BlockManagerImpl;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.SCMPipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMClientProtocolServer;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.lock.LockManager;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashSet;

/**
 * Tests SafeModeHandler behavior.
 */
public class TestSafeModeHandler {


  private OzoneConfiguration configuration;
  private SCMClientProtocolServer scmClientProtocolServer;
  private ReplicationManager replicationManager;
  private BlockManager blockManager;
  private SafeModeHandler safeModeHandler;
  private EventQueue eventQueue;
  private SCMSafeModeManager.SafeModeStatus safeModeStatus;
  private PipelineManager scmPipelineManager;

  public void setup(boolean enabled) {
    configuration = new OzoneConfiguration();
    configuration.setBoolean(HddsConfigKeys.HDDS_SCM_SAFEMODE_ENABLED,
        enabled);
    configuration.set(HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
        "3s");
    scmClientProtocolServer =
        Mockito.mock(SCMClientProtocolServer.class);
    eventQueue = new EventQueue();
    final ContainerManager containerManager =
        Mockito.mock(ContainerManager.class);
    Mockito.when(containerManager.getContainerIDs())
        .thenReturn(new HashSet<>());
    replicationManager = new ReplicationManager(
        new ReplicationManagerConfiguration(),
        containerManager, Mockito.mock(PlacementPolicy.class),
        eventQueue, new LockManager(configuration));
    scmPipelineManager = Mockito.mock(SCMPipelineManager.class);
    blockManager = Mockito.mock(BlockManagerImpl.class);
    safeModeHandler =
        new SafeModeHandler(configuration, scmClientProtocolServer,
            blockManager, replicationManager, scmPipelineManager);

    eventQueue.addHandler(SCMEvents.SAFE_MODE_STATUS, safeModeHandler);
    safeModeStatus = new SCMSafeModeManager.SafeModeStatus(false);

  }

  @Test
  public void testSafeModeHandlerWithSafeModeEnabled() throws Exception {
    setup(true);

    Assert.assertTrue(safeModeHandler.getSafeModeStatus());

    eventQueue.fireEvent(SCMEvents.SAFE_MODE_STATUS, safeModeStatus);

    GenericTestUtils.waitFor(() -> !safeModeHandler.getSafeModeStatus(),
        1000, 5000);

    Assert.assertFalse(scmClientProtocolServer.getSafeModeStatus());
    Assert.assertFalse(((BlockManagerImpl) blockManager).isScmInSafeMode());
    GenericTestUtils.waitFor(() ->
            replicationManager.isRunning(), 1000, 5000);
  }


  @Test
  public void testSafeModeHandlerWithSafeModeDisbaled() throws Exception{

    setup(false);

    Assert.assertFalse(safeModeHandler.getSafeModeStatus());

    eventQueue.fireEvent(SCMEvents.SAFE_MODE_STATUS, safeModeStatus);

    Assert.assertFalse(safeModeHandler.getSafeModeStatus());
    Assert.assertFalse(scmClientProtocolServer.getSafeModeStatus());
    Assert.assertFalse(((BlockManagerImpl) blockManager).isScmInSafeMode());
    GenericTestUtils.waitFor(() ->
        replicationManager.isRunning(), 1000, 5000);
  }
}
