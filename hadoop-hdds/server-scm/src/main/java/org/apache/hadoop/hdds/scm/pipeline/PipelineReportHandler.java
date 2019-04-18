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

package org.apache.hadoop.hdds.scm.pipeline;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.server
    .SCMDatanodeHeartbeatDispatcher.PipelineReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * Handles Pipeline Reports from datanode.
 */
public class PipelineReportHandler implements
    EventHandler<PipelineReportFromDatanode> {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(PipelineReportHandler.class);
  private final PipelineManager pipelineManager;
  private final Configuration conf;
  private final SCMSafeModeManager scmSafeModeManager;
  private final boolean pipelineAvailabilityCheck;

  public PipelineReportHandler(SCMSafeModeManager scmSafeModeManager,
      PipelineManager pipelineManager,
      Configuration conf) {
    Preconditions.checkNotNull(pipelineManager);
    Objects.requireNonNull(scmSafeModeManager);
    this.scmSafeModeManager = scmSafeModeManager;
    this.pipelineManager = pipelineManager;
    this.conf = conf;
    this.pipelineAvailabilityCheck = conf.getBoolean(
        HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_AVAILABILITY_CHECK,
        HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_AVAILABILITY_CHECK_DEFAULT);

  }

  @Override
  public void onMessage(PipelineReportFromDatanode pipelineReportFromDatanode,
      EventPublisher publisher) {
    Preconditions.checkNotNull(pipelineReportFromDatanode);
    DatanodeDetails dn = pipelineReportFromDatanode.getDatanodeDetails();
    PipelineReportsProto pipelineReport =
        pipelineReportFromDatanode.getReport();
    Preconditions.checkNotNull(dn, "Pipeline Report is "
        + "missing DatanodeDetails.");
    LOGGER.trace("Processing pipeline report for dn: {}", dn);
    for (PipelineReport report : pipelineReport.getPipelineReportList()) {
      try {
        processPipelineReport(report, dn);
      } catch (IOException e) {
        LOGGER.error("Could not process pipeline report={} from dn={} {}",
            report, dn, e);
      }
    }
    if (pipelineAvailabilityCheck && scmSafeModeManager.getInSafeMode()) {
      publisher.fireEvent(SCMEvents.PROCESSED_PIPELINE_REPORT,
          pipelineReportFromDatanode);
    }

  }

  private void processPipelineReport(PipelineReport report, DatanodeDetails dn)
      throws IOException {
    PipelineID pipelineID = PipelineID.getFromProtobuf(report.getPipelineID());
    Pipeline pipeline;
    try {
      pipeline = pipelineManager.getPipeline(pipelineID);
    } catch (PipelineNotFoundException e) {
      RatisPipelineUtils.destroyPipeline(dn, pipelineID, conf);
      return;
    }

    if (pipeline.getPipelineState() == Pipeline.PipelineState.ALLOCATED) {
      LOGGER.info("Pipeline {} reported by {}", pipeline.getId(), dn);
      pipeline.reportDatanode(dn);
      if (pipeline.isHealthy()) {
        // if all the dns have reported, pipeline can be moved to OPEN state
        pipelineManager.openPipeline(pipelineID);
      }
    } else {
      // In OPEN state case just report the datanode
      pipeline.reportDatanode(dn);
    }
  }
}
