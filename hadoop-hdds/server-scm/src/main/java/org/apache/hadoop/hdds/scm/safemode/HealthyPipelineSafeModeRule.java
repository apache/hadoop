/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.safemode;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.server.events.TypedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class defining Safe mode exit criteria for Pipelines.
 *
 * This rule defines percentage of healthy pipelines need to be reported.
 * Once safe mode exit happens, this rules take care of writes can go
 * through in a cluster.
 */
public class HealthyPipelineSafeModeRule
    extends SafeModeExitRule<Pipeline>{

  public static final Logger LOG =
      LoggerFactory.getLogger(HealthyPipelineSafeModeRule.class);
  private final int healthyPipelineThresholdCount;
  private int currentHealthyPipelineCount = 0;

  HealthyPipelineSafeModeRule(String ruleName, EventQueue eventQueue,
      PipelineManager pipelineManager,
      SCMSafeModeManager manager, Configuration configuration) {
    super(manager, ruleName, eventQueue);
    double healthyPipelinesPercent =
        configuration.getDouble(HddsConfigKeys.
                HDDS_SCM_SAFEMODE_HEALTHY_PIPELINE_THRESHOLD_PCT,
            HddsConfigKeys.
                HDDS_SCM_SAFEMODE_HEALTHY_PIPELINE_THRESHOLD_PCT_DEFAULT);

    int minHealthyPipelines = 0;

    boolean createPipelineInSafemode = configuration.getBoolean(
        HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION,
        HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION_DEFAULT);

    if (createPipelineInSafemode) {
      minHealthyPipelines =
          configuration.getInt(HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_PIPELINE,
              HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_PIPELINE_DEFAULT);
    }

    Preconditions.checkArgument(
        (healthyPipelinesPercent >= 0.0 && healthyPipelinesPercent <= 1.0),
        HddsConfigKeys.
            HDDS_SCM_SAFEMODE_HEALTHY_PIPELINE_THRESHOLD_PCT
            + " value should be >= 0.0 and <= 1.0");

    // As we want to wait for RATIS write pipelines, no matter ONE or THREE
    int pipelineCount = pipelineManager.getPipelines(
        HddsProtos.ReplicationType.RATIS, Pipeline.PipelineState.OPEN).size() +
        pipelineManager.getPipelines(HddsProtos.ReplicationType.RATIS,
            Pipeline.PipelineState.ALLOCATED).size();

    // This value will be zero when pipeline count is 0.
    // On a fresh installed cluster, there will be zero pipelines in the SCM
    // pipeline DB.
    healthyPipelineThresholdCount = Math.max(minHealthyPipelines,
        (int) Math.ceil(healthyPipelinesPercent * pipelineCount));

    LOG.info(" Total pipeline count is {}, healthy pipeline " +
        "threshold count is {}", pipelineCount, healthyPipelineThresholdCount);

    getSafeModeMetrics().setNumHealthyPipelinesThreshold(
        healthyPipelineThresholdCount);
  }

  @Override
  protected TypedEvent<Pipeline> getEventType() {
    return SCMEvents.OPEN_PIPELINE;
  }

  @Override
  protected boolean validate() {
    if (currentHealthyPipelineCount >= healthyPipelineThresholdCount) {
      return true;
    }
    return false;
  }

  @Override
  protected void process(Pipeline pipeline) {

    // When SCM is in safe mode for long time, already registered
    // datanode can send pipeline report again, or SCMPipelineManager will
    // create new pipelines.
    Preconditions.checkNotNull(pipeline);
    if (pipeline.getType() == HddsProtos.ReplicationType.RATIS) {
      getSafeModeMetrics().incCurrentHealthyPipelinesCount();
      currentHealthyPipelineCount++;
    }

    if (scmInSafeMode()) {
      SCMSafeModeManager.getLogger().info(
          "SCM in safe mode. Healthy pipelines reported count is {}, " +
              "required healthy pipeline reported count is {}",
          currentHealthyPipelineCount, healthyPipelineThresholdCount);
    }
  }

  @Override
  protected void cleanup() {
  }

  @VisibleForTesting
  public int getCurrentHealthyPipelineCount() {
    return currentHealthyPipelineCount;
  }

  @VisibleForTesting
  public int getHealthyPipelineThresholdCount() {
    return healthyPipelineThresholdCount;
  }
}