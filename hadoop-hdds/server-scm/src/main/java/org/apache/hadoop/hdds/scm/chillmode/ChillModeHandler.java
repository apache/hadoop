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
import org.apache.hadoop.hdds.scm.container.replication.
    ReplicationActivityStatus;
import org.apache.hadoop.hdds.scm.server.SCMClientProtocolServer;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Class to handle the activities needed to be performed after exiting chill
 * mode.
 */
public class ChillModeHandler implements EventHandler<Boolean> {

  private SCMClientProtocolServer scmClientProtocolServer;
  private BlockManager scmBlockManager;
  private final long waitTime;
  private AtomicBoolean isInChillMode = new AtomicBoolean(true);
  private final ReplicationActivityStatus replicationActivityStatus;


  /**
   * ChillModeHandler, to handle the logic once we exit chill mode.
   * @param configuration
   * @param clientProtocolServer
   * @param blockManager
   * @param replicationStatus
   */
  public ChillModeHandler(Configuration configuration,
      SCMClientProtocolServer clientProtocolServer,
      BlockManager blockManager,
      ReplicationActivityStatus replicationStatus) {
    this.waitTime = configuration.getTimeDuration(
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_CHILL_MODE_EXIT,
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_CHILL_MODE_EXIT_DEFAULT,
        TimeUnit.MILLISECONDS);
    scmClientProtocolServer = clientProtocolServer;
    scmBlockManager = blockManager;
    replicationActivityStatus = replicationStatus;

    boolean chillModeEnabled = configuration.getBoolean(
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
  public void onMessage(Boolean chillModeStatus, EventPublisher publisher) {
    isInChillMode.set(chillModeStatus);

    replicationActivityStatus.fireReplicationStart(chillModeStatus, waitTime);
    scmClientProtocolServer.setChillModeStatus(chillModeStatus);
    scmBlockManager.setChillModeStatus(chillModeStatus);

  }

  public boolean getChillModeStatus() {
    return isInChillMode.get();
  }


}
