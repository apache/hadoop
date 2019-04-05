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

package org.apache.hadoop.hdds.scm.chillmode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.scm.block.BlockManager;
import org.apache.hadoop.hdds.scm.container.ReplicationManager;
import org.apache.hadoop.hdds.scm.server.SCMClientProtocolServer;
import org.apache.hadoop.hdds.scm.chillmode.SCMChillModeManager.ChillModeStatus;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Class to handle the activities needed to be performed after exiting chill
 * mode.
 */
public class ChillModeHandler implements EventHandler<ChillModeStatus> {

  private final SCMClientProtocolServer scmClientProtocolServer;
  private final BlockManager scmBlockManager;
  private final long waitTime;
  private final AtomicBoolean isInChillMode = new AtomicBoolean(true);
  private final ReplicationManager replicationManager;


  /**
   * ChillModeHandler, to handle the logic once we exit chill mode.
   * @param configuration
   * @param clientProtocolServer
   * @param blockManager
   * @param replicationManager
   */
  public ChillModeHandler(Configuration configuration,
      SCMClientProtocolServer clientProtocolServer,
      BlockManager blockManager,
      ReplicationManager replicationManager) {
    Objects.requireNonNull(configuration, "Configuration cannot be null");
    Objects.requireNonNull(clientProtocolServer, "SCMClientProtocolServer " +
        "object cannot be null");
    Objects.requireNonNull(blockManager, "BlockManager object cannot be null");
    Objects.requireNonNull(replicationManager, "ReplicationManager " +
        "object cannot be null");
    this.waitTime = configuration.getTimeDuration(
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_CHILL_MODE_EXIT,
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_CHILL_MODE_EXIT_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.scmClientProtocolServer = clientProtocolServer;
    this.scmBlockManager = blockManager;
    this.replicationManager = replicationManager;

    final boolean chillModeEnabled = configuration.getBoolean(
        HddsConfigKeys.HDDS_SCM_CHILLMODE_ENABLED,
        HddsConfigKeys.HDDS_SCM_CHILLMODE_ENABLED_DEFAULT);
    isInChillMode.set(chillModeEnabled);

  }

  /**
   * Set ChillMode status based on
   * {@link org.apache.hadoop.hdds.scm.events.SCMEvents#CHILL_MODE_STATUS}.
   *
   * Inform BlockManager, ScmClientProtocolServer and replicationAcitivity
   * status about chillMode status.
   *
   * @param chillModeStatus
   * @param publisher
   */
  @Override
  public void onMessage(ChillModeStatus chillModeStatus,
      EventPublisher publisher) {

    isInChillMode.set(chillModeStatus.getChillModeStatus());
    scmClientProtocolServer.setChillModeStatus(isInChillMode.get());
    scmBlockManager.setChillModeStatus(isInChillMode.get());

    if (!isInChillMode.get()) {
      final Thread chillModeExitThread = new Thread(() -> {
        try {
          Thread.sleep(waitTime);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        replicationManager.start();
      });

      chillModeExitThread.setDaemon(true);
      chillModeExitThread.start();
    }

  }

  public boolean getChillModeStatus() {
    return isInChillMode.get();
  }


}
