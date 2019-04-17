/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.safemode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.scm.block.BlockManager;
import org.apache.hadoop.hdds.scm.container.ReplicationManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMClientProtocolServer;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager.SafeModeStatus;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Class to handle the activities needed to be performed after exiting safe
 * mode.
 */
public class SafeModeHandler implements EventHandler<SafeModeStatus> {

  private static final Logger LOG =
      LoggerFactory.getLogger(SafeModeHandler.class);

  private final SCMClientProtocolServer scmClientProtocolServer;
  private final BlockManager scmBlockManager;
  private final long waitTime;
  private final AtomicBoolean isInSafeMode = new AtomicBoolean(true);
  private final ReplicationManager replicationManager;

  private final PipelineManager scmPipelineManager;

  /**
   * SafeModeHandler, to handle the logic once we exit safe mode.
   * @param configuration
   * @param clientProtocolServer
   * @param blockManager
   * @param replicationManager
   */
  public SafeModeHandler(Configuration configuration,
      SCMClientProtocolServer clientProtocolServer,
      BlockManager blockManager,
      ReplicationManager replicationManager, PipelineManager pipelineManager) {
    Objects.requireNonNull(configuration, "Configuration cannot be null");
    Objects.requireNonNull(clientProtocolServer, "SCMClientProtocolServer " +
        "object cannot be null");
    Objects.requireNonNull(blockManager, "BlockManager object cannot be null");
    Objects.requireNonNull(replicationManager, "ReplicationManager " +
        "object cannot be null");
    Objects.requireNonNull(pipelineManager, "PipelineManager object cannot " +
        "be" + "null");
    this.waitTime = configuration.getTimeDuration(
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.scmClientProtocolServer = clientProtocolServer;
    this.scmBlockManager = blockManager;
    this.replicationManager = replicationManager;
    this.scmPipelineManager = pipelineManager;

    final boolean safeModeEnabled = configuration.getBoolean(
        HddsConfigKeys.HDDS_SCM_SAFEMODE_ENABLED,
        HddsConfigKeys.HDDS_SCM_SAFEMODE_ENABLED_DEFAULT);
    isInSafeMode.set(safeModeEnabled);

  }



  /**
   * Set SafeMode status based on
   * {@link org.apache.hadoop.hdds.scm.events.SCMEvents#SAFE_MODE_STATUS}.
   *
   * Inform BlockManager, ScmClientProtocolServer and replicationAcitivity
   * status about safeMode status.
   *
   * @param safeModeStatus
   * @param publisher
   */
  @Override
  public void onMessage(SafeModeStatus safeModeStatus,
      EventPublisher publisher) {

    isInSafeMode.set(safeModeStatus.getSafeModeStatus());
    scmClientProtocolServer.setSafeModeStatus(isInSafeMode.get());
    scmBlockManager.setSafeModeStatus(isInSafeMode.get());

    if (!isInSafeMode.get()) {
      final Thread safeModeExitThread = new Thread(() -> {
        try {
          Thread.sleep(waitTime);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        replicationManager.start();
        cleanupPipelines();
      });

      safeModeExitThread.setDaemon(true);
      safeModeExitThread.start();
    }

  }

  private void cleanupPipelines() {
    List<Pipeline> pipelineList = scmPipelineManager.getPipelines();
    pipelineList.forEach((pipeline) -> {
      try {
        if (pipeline.getPipelineState() == Pipeline.PipelineState.ALLOCATED) {
          scmPipelineManager.finalizeAndDestroyPipeline(pipeline, false);
        }
      } catch (IOException ex) {
        LOG.error("Finalize and destroy pipeline failed for pipeline "
            + pipeline.toString(), ex);
      }
    });
  }

  public boolean getSafeModeStatus() {
    return isInSafeMode.get();
  }


}
