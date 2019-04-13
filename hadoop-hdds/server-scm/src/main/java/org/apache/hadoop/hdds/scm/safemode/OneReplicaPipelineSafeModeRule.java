/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.safemode;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.
    PipelineReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.server.events.TypedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * This rule covers whether we have at least one datanode is reported for each
 * pipeline. This rule is for all open containers, we have at least one
 * replica available for read when we exit safe mode.
 */
public class OneReplicaPipelineSafeModeRule extends
    SafeModeExitRule<PipelineReportFromDatanode> {

  private static final Logger LOG =
      LoggerFactory.getLogger(OneReplicaPipelineSafeModeRule.class);

  private int thresholdCount;
  private Set<PipelineID> reportedPipelineIDSet = new HashSet<>();
  private final PipelineManager pipelineManager;
  private int currentReportedPipelineCount = 0;


  public OneReplicaPipelineSafeModeRule(String ruleName, EventQueue eventQueue,
      PipelineManager pipelineManager,
      SCMSafeModeManager safeModeManager, Configuration configuration) {
    super(safeModeManager, ruleName, eventQueue);
    this.pipelineManager = pipelineManager;

    double percent =
        configuration.getDouble(
            HddsConfigKeys.HDDS_SCM_SAFEMODE_ONE_NODE_REPORTED_PIPELINE_PCT,
            HddsConfigKeys.
                HDDS_SCM_SAFEMODE_ONE_NODE_REPORTED_PIPELINE_PCT_DEFAULT);

    Preconditions.checkArgument((percent >= 0.0 && percent <= 1.0),
        HddsConfigKeys.
            HDDS_SCM_SAFEMODE_ONE_NODE_REPORTED_PIPELINE_PCT  +
            " value should be >= 0.0 and <= 1.0");

    int totalPipelineCount =
        pipelineManager.getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE).size();

    thresholdCount = (int) Math.ceil(percent * totalPipelineCount);

    LOG.info(" Total pipeline count is {}, pipeline's with atleast one " +
        "datanode reported threshold count is {}", totalPipelineCount,
        thresholdCount);

  }

  @Override
  protected TypedEvent<PipelineReportFromDatanode> getEventType() {
    return SCMEvents.PROCESSED_PIPELINE_REPORT;
  }

  @Override
  protected boolean validate() {
    if (currentReportedPipelineCount >= thresholdCount) {
      return true;
    }
    return false;
  }

  @Override
  protected void process(PipelineReportFromDatanode
      pipelineReportFromDatanode) {
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
          !reportedPipelineIDSet.contains(pipelineID)) {
        reportedPipelineIDSet.add(pipelineID);
      }
    }

    currentReportedPipelineCount = reportedPipelineIDSet.size();

    if (scmInSafeMode()) {
      SCMSafeModeManager.getLogger().info(
          "SCM in safe mode. Pipelines with atleast one datanode reported " +
              "count is {}, required atleast one datanode reported per " +
              "pipeline count is {}",
          currentReportedPipelineCount, thresholdCount);
    }

  }

  @Override
  protected void cleanup() {
    reportedPipelineIDSet.clear();
  }

  @VisibleForTesting
  public int getThresholdCount() {
    return thresholdCount;
  }

  @VisibleForTesting
  public int getCurrentReportedPipelineCount() {
    return currentReportedPipelineCount;
  }

}
