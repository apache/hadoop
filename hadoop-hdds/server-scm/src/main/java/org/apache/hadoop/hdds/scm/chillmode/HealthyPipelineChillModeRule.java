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
package org.apache.hadoop.hdds.scm.chillmode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.PipelineReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Class defining Chill mode exit criteria for Pipelines.
 *
 * This rule defines percentage of healthy pipelines need to be reported.
 * Once chill mode exit happens, this rules take care of writes can go
 * through in a cluster.
 */
public class HealthyPipelineChillModeRule
    implements ChillModeExitRule<PipelineReportFromDatanode>,
    EventHandler<PipelineReportFromDatanode> {

  public static final Logger LOG =
      LoggerFactory.getLogger(HealthyPipelineChillModeRule.class);
  private final PipelineManager pipelineManager;
  private final SCMChillModeManager chillModeManager;
  private final int healthyPipelineThresholdCount;
  private int currentHealthyPipelineCount = 0;
  private final Set<DatanodeDetails> processedDatanodeDetails =
      new HashSet<>();

  HealthyPipelineChillModeRule(PipelineManager pipelineManager,
      SCMChillModeManager manager, Configuration configuration) {
    this.pipelineManager = pipelineManager;
    this.chillModeManager = manager;
    double healthyPipelinesPercent =
        configuration.getDouble(HddsConfigKeys.
                HDDS_SCM_CHILLMODE_HEALTHY_PIPELINE_THRESHOLD_PCT,
            HddsConfigKeys.
                HDDS_SCM_CHILLMODE_HEALTHY_PIPELINE_THRESHOLD_PCT_DEFAULT);

    // As we want to wait for 3 node pipelines
    int pipelineCount =
        pipelineManager.getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE).size();

    // This value will be zero when pipeline count is 0.
    // On a fresh installed cluster, there will be zero pipelines in the SCM
    // pipeline DB.
    healthyPipelineThresholdCount =
        (int) Math.ceil(healthyPipelinesPercent * pipelineCount);

    LOG.info(" Total pipeline count is {}, healthy pipeline " +
        "threshold count is {}", pipelineCount, healthyPipelineThresholdCount);
  }

  @Override
  public boolean validate() {
    if (currentHealthyPipelineCount >= healthyPipelineThresholdCount) {
      return true;
    }
    return false;
  }

  @Override
  public void process(PipelineReportFromDatanode pipelineReportFromDatanode) {
    Pipeline pipeline;
    Preconditions.checkNotNull(pipelineReportFromDatanode);
    PipelineReportsProto pipelineReport =
        pipelineReportFromDatanode.getReport();

    for (PipelineReport report : pipelineReport.getPipelineReportList()) {
      PipelineID pipelineID = PipelineID
          .getFromProtobuf(report.getPipelineID());
      try {
        pipeline = pipelineManager.getPipeline(pipelineID);
      } catch (PipelineNotFoundException e) {
        continue;
      }

      if (pipeline.getFactor() == HddsProtos.ReplicationFactor.THREE &&
          pipeline.getPipelineState() == Pipeline.PipelineState.OPEN) {
        // If the pipeline is open state mean, all 3 datanodes are reported
        // for this pipeline.
        currentHealthyPipelineCount++;
      }
    }
  }

  @Override
  public void cleanup() {
    // No need to deal with
  }

  @Override
  public void onMessage(PipelineReportFromDatanode pipelineReportFromDatanode,
      EventPublisher publisher) {
    // If we have already reached healthy pipeline threshold, skip processing
    // pipeline report from datanode.

    if (validate()) {
      chillModeManager.validateChillModeExitRules(publisher);
      return;
    }


    // When SCM is in chill mode for long time, already registered
    // datanode can send pipeline report again, then pipeline handler fires
    // processed report event, we should not consider this pipeline report
    // from datanode again during threshold calculation.
    DatanodeDetails dnDetails = pipelineReportFromDatanode.getDatanodeDetails();
    if (!processedDatanodeDetails.contains(
        pipelineReportFromDatanode.getDatanodeDetails())) {

      // Process pipeline report from datanode
      process(pipelineReportFromDatanode);

      if (chillModeManager.getInChillMode()) {
        SCMChillModeManager.getLogger().info(
            "SCM in chill mode. Healthy pipelines reported count is {}, " +
                "required healthy pipeline reported count is {}",
            currentHealthyPipelineCount, healthyPipelineThresholdCount);
      }

      processedDatanodeDetails.add(dnDetails);
    }

    if (validate()) {
      chillModeManager.validateChillModeExitRules(publisher);
    }
  }
}