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
package org.apache.hadoop.hdds.scm.chillmode;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer
    .NodeRegistrationContainerReport;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StorageContainerManager enters chill mode on startup to allow system to
 * reach a stable state before becoming fully functional. SCM will wait
 * for certain resources to be reported before coming out of chill mode.
 *
 * ChillModeExitRule defines format to define new rules which must be satisfied
 * to exit Chill mode.
 * ContainerChillModeRule defines the only exit criteria right now.
 * On every new datanode registration event this class adds replicas
 * for reported containers and validates if cutoff threshold for
 * containers is meet.
 */
public class SCMChillModeManager implements
    EventHandler<NodeRegistrationContainerReport> {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMChillModeManager.class);
  private final boolean isChillModeEnabled;
  private AtomicBoolean inChillMode = new AtomicBoolean(true);

  private Map<String, ChillModeExitRule> exitRules = new HashMap(1);
  private Configuration config;
  private static final String CONT_EXIT_RULE = "ContainerChillModeRule";
  private static final String DN_EXIT_RULE = "DataNodeChillModeRule";
  private static final String PIPELINE_EXIT_RULE = "PipelineChillModeRule";

  private final EventQueue eventPublisher;

  public SCMChillModeManager(Configuration conf,
      List<ContainerInfo> allContainers, PipelineManager pipelineManager,
      EventQueue eventQueue) {
    this.config = conf;
    this.eventPublisher = eventQueue;
    this.isChillModeEnabled = conf.getBoolean(
        HddsConfigKeys.HDDS_SCM_CHILLMODE_ENABLED,
        HddsConfigKeys.HDDS_SCM_CHILLMODE_ENABLED_DEFAULT);
    if (isChillModeEnabled) {
      exitRules.put(CONT_EXIT_RULE,
          new ContainerChillModeRule(config, allContainers, this));
      exitRules.put(DN_EXIT_RULE, new DataNodeChillModeRule(config, this));

      if (conf.getBoolean(
          HddsConfigKeys.HDDS_SCM_CHILLMODE_PIPELINE_AVAILABILITY_CHECK,
          HddsConfigKeys.HDDS_SCM_CHILLMODE_PIPELINE_AVAILABILITY_CHECK_DEFAULT)
          && pipelineManager != null) {
        PipelineChillModeRule rule = new PipelineChillModeRule(pipelineManager,
            this);
        exitRules.put(PIPELINE_EXIT_RULE, rule);
        eventPublisher.addHandler(SCMEvents.PIPELINE_REPORT, rule);
      }
      emitChillModeStatus();
    } else {
      exitChillMode(eventQueue);
    }
  }

  /**
   * Emit Chill mode status.
   */
  @VisibleForTesting
  public void emitChillModeStatus() {
    eventPublisher.fireEvent(SCMEvents.CHILL_MODE_STATUS, getInChillMode());
  }

  public void validateChillModeExitRules(EventPublisher eventQueue) {
    for (ChillModeExitRule exitRule : exitRules.values()) {
      if (!exitRule.validate()) {
        return;
      }
    }
    exitChillMode(eventQueue);
  }

  /**
   * Exit chill mode. It does following actions:
   * 1. Set chill mode status to false.
   * 2. Emits START_REPLICATION for ReplicationManager.
   * 3. Cleanup resources.
   * 4. Emit chill mode status.
   * @param eventQueue
   */
  @VisibleForTesting
  public void exitChillMode(EventPublisher eventQueue) {
    LOG.info("SCM exiting chill mode.");
    setInChillMode(false);

    // TODO: Remove handler registration as there is no need to listen to
    // register events anymore.

    for (ChillModeExitRule e : exitRules.values()) {
      e.cleanup();
    }
    emitChillModeStatus();
  }

  @Override
  public void onMessage(
      NodeRegistrationContainerReport nodeRegistrationContainerReport,
      EventPublisher publisher) {
    if (getInChillMode()) {
      exitRules.get(CONT_EXIT_RULE).process(nodeRegistrationContainerReport);
      exitRules.get(DN_EXIT_RULE).process(nodeRegistrationContainerReport);
      validateChillModeExitRules(publisher);
    }
  }

  public boolean getInChillMode() {
    if (!isChillModeEnabled) {
      return false;
    }
    return inChillMode.get();
  }

  /**
   * Set chill mode status.
   */
  public void setInChillMode(boolean inChillMode) {
    this.inChillMode.set(inChillMode);
  }

  public static Logger getLogger() {
    return LOG;
  }

  @VisibleForTesting
  public double getCurrentContainerThreshold() {
    return ((ContainerChillModeRule) exitRules.get(CONT_EXIT_RULE))
        .getCurrentContainerThreshold();
  }

}
