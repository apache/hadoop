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

package org.apache.hadoop.hdds.scm.chillmode;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.
    PipelineReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * This rule covers whether we have atleast one datanode is reported for each
 * pipeline. This rule is for all open containers, we have at least one
 * replica available for read when we exit chill mode.
 */
public class OneReplicaPipelineChillModeRule implements
    ChillModeExitRule<PipelineReportFromDatanode>,
    EventHandler<PipelineReportFromDatanode> {

  private static final Logger LOG =
      LoggerFactory.getLogger(OneReplicaPipelineChillModeRule.class);

  private int thresholdCount;
  private Set<PipelineID> reportedPipelineIDSet = new HashSet<>();
  private final PipelineManager pipelineManager;
  private final SCMChillModeManager chillModeManager;

  public OneReplicaPipelineChillModeRule(PipelineManager pipelineManager,
      SCMChillModeManager chillModeManager,
      Configuration configuration) {
    this.chillModeManager = chillModeManager;
    this.pipelineManager = pipelineManager;

    double percent =
        configuration.getDouble(
            HddsConfigKeys.HDDS_SCM_CHILLMODE_ONE_NODE_REPORTED_PIPELINE_PCT,
            HddsConfigKeys.
                HDDS_SCM_CHILLMODE_ONE_NODE_REPORTED_PIPELINE_PCT_DEFAULT);

    int totalPipelineCount =
        pipelineManager.getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE).size();

    thresholdCount = (int) Math.ceil(percent * totalPipelineCount);

    LOG.info(" Total pipeline count is {}, pipeline's with atleast one " +
        "datanode reported threshold count is {}", totalPipelineCount,
        thresholdCount);

  }
  @Override
  public boolean validate() {
    if (reportedPipelineIDSet.size() >= thresholdCount) {
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
          !reportedPipelineIDSet.contains(pipelineID)) {
        reportedPipelineIDSet.add(pipelineID);
      }
    }
  }

  @Override
  public void cleanup() {
    reportedPipelineIDSet.clear();
  }

  @Override
  public void onMessage(PipelineReportFromDatanode pipelineReportFromDatanode,
      EventPublisher publisher) {

    if (validate()) {
      chillModeManager.validateChillModeExitRules(publisher);
      return;
    }

    // Process pipeline report from datanode
    process(pipelineReportFromDatanode);

    if (chillModeManager.getInChillMode()) {
      SCMChillModeManager.getLogger().info(
          "SCM in chill mode. Pipelines with atleast one datanode reported " +
              "count is {}, required atleast one datanode reported per " +
              "pipeline count is {}",
          reportedPipelineIDSet.size(), thresholdCount);
    }

    if (validate()) {
      chillModeManager.validateChillModeExitRules(publisher);
    }
  }
}
