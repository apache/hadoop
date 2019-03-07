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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
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
 *
 * Current ChillMode rules:
 * 1. ContainerChillModeRule:
 * On every new datanode registration, it fires
 * {@link SCMEvents#NODE_REGISTRATION_CONT_REPORT}.  This rule handles this
 * event. This rule process this report, increment the
 * containerWithMinReplicas count when this reported replica is in the
 * containerMap. Then validates if cutoff threshold for containers is meet.
 *
 * 2. DatanodeChillModeRule:
 * On every new datanode registration, it fires
 * {@link SCMEvents#NODE_REGISTRATION_CONT_REPORT}. This rule handles this
 * event. This rule process this report, and check if this is new node, add
 * to its reported node list. Then validate it cutoff threshold for minimum
 * number of datanode registered is met or not.
 *
 * 3. HealthyPipelineChillModeRule:
 * Once the pipelineReportHandler processes the
 * {@link SCMEvents#PIPELINE_REPORT}, it fires
 * {@link SCMEvents#PROCESSED_PIPELINE_REPORT}. This rule handles this
 * event. This rule processes this report, and check if pipeline is healthy
 * and increments current healthy pipeline count. Then validate it cutoff
 * threshold for healthy pipeline is met or not.
 *
 * 4. OneReplicaPipelineChillModeRule:
 * Once the pipelineReportHandler processes the
 * {@link SCMEvents#PIPELINE_REPORT}, it fires
 * {@link SCMEvents#PROCESSED_PIPELINE_REPORT}. This rule handles this
 * event. This rule processes this report, and add the reported pipeline to
 * reported pipeline set. Then validate it cutoff threshold for one replica
 * per pipeline is met or not.
 *
 */
public class SCMChillModeManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMChillModeManager.class);
  private final boolean isChillModeEnabled;
  private AtomicBoolean inChillMode = new AtomicBoolean(true);

  private Map<String, ChillModeExitRule> exitRules = new HashMap(1);
  private Configuration config;
  private static final String CONT_EXIT_RULE = "ContainerChillModeRule";
  private static final String DN_EXIT_RULE = "DataNodeChillModeRule";
  private static final String HEALTHY_PIPELINE_EXIT_RULE =
      "HealthyPipelineChillModeRule";
  private static final String ATLEAST_ONE_DATANODE_REPORTED_PIPELINE_EXIT_RULE =
      "AtleastOneDatanodeReportedRule";

  private Set<String> validatedRules = new HashSet<>();

  private final EventQueue eventPublisher;
  private final PipelineManager pipelineManager;

  public SCMChillModeManager(Configuration conf,
      List<ContainerInfo> allContainers, PipelineManager pipelineManager,
      EventQueue eventQueue) {
    this.config = conf;
    this.pipelineManager = pipelineManager;
    this.eventPublisher = eventQueue;
    this.isChillModeEnabled = conf.getBoolean(
        HddsConfigKeys.HDDS_SCM_CHILLMODE_ENABLED,
        HddsConfigKeys.HDDS_SCM_CHILLMODE_ENABLED_DEFAULT);

    if (isChillModeEnabled) {
      ContainerChillModeRule containerChillModeRule =
          new ContainerChillModeRule(CONT_EXIT_RULE, eventQueue, config,
              allContainers, this);
      DataNodeChillModeRule dataNodeChillModeRule =
          new DataNodeChillModeRule(DN_EXIT_RULE, eventQueue, config, this);
      exitRules.put(CONT_EXIT_RULE, containerChillModeRule);
      exitRules.put(DN_EXIT_RULE, dataNodeChillModeRule);
      if (conf.getBoolean(
          HddsConfigKeys.HDDS_SCM_CHILLMODE_PIPELINE_AVAILABILITY_CHECK,
          HddsConfigKeys.HDDS_SCM_CHILLMODE_PIPELINE_AVAILABILITY_CHECK_DEFAULT)
          && pipelineManager != null) {
        HealthyPipelineChillModeRule healthyPipelineChillModeRule =
            new HealthyPipelineChillModeRule(HEALTHY_PIPELINE_EXIT_RULE,
                eventQueue, pipelineManager,
                this, config);
        OneReplicaPipelineChillModeRule oneReplicaPipelineChillModeRule =
            new OneReplicaPipelineChillModeRule(
                ATLEAST_ONE_DATANODE_REPORTED_PIPELINE_EXIT_RULE, eventQueue,
                pipelineManager, this, conf);
        exitRules.put(HEALTHY_PIPELINE_EXIT_RULE, healthyPipelineChillModeRule);
        exitRules.put(ATLEAST_ONE_DATANODE_REPORTED_PIPELINE_EXIT_RULE,
            oneReplicaPipelineChillModeRule);
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
    eventPublisher.fireEvent(SCMEvents.CHILL_MODE_STATUS,
        new ChillModeStatus(getInChillMode()));
  }


  public synchronized void validateChillModeExitRules(String ruleName,
      EventPublisher eventQueue) {

    if (exitRules.get(ruleName) != null) {
      validatedRules.add(ruleName);
    } else {
      // This should never happen
      LOG.error("No Such Exit rule {}", ruleName);
    }


    if (validatedRules.size() == exitRules.size()) {
      // All rules are satisfied, we can exit chill mode.
      LOG.info("ScmChillModeManager, all rules are successfully validated");
      exitChillMode(eventQueue);
    }

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

    emitChillModeStatus();
    // TODO: #CLUTIL if we reenter chill mode the fixed interval pipeline
    // creation job needs to stop
    pipelineManager.startPipelineCreator();
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

  @VisibleForTesting
  public HealthyPipelineChillModeRule getHealthyPipelineChillModeRule() {
    return (HealthyPipelineChillModeRule)
        exitRules.get(HEALTHY_PIPELINE_EXIT_RULE);
  }

  @VisibleForTesting
  public OneReplicaPipelineChillModeRule getOneReplicaPipelineChillModeRule() {
    return (OneReplicaPipelineChillModeRule)
        exitRules.get(ATLEAST_ONE_DATANODE_REPORTED_PIPELINE_EXIT_RULE);
  }


  /**
   * Class used during ChillMode status event.
   */
  public static class ChillModeStatus {

    private boolean chillModeStatus;
    public ChillModeStatus(boolean chillModeState) {
      this.chillModeStatus = chillModeState;
    }

    public boolean getChillModeStatus() {
      return chillModeStatus;
    }
  }

}
