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

import java.util.concurrent.atomic.AtomicBoolean;

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

/**
 * Class defining Chill mode exit criteria for Pipelines.
 */
public class PipelineChillModeRule
    implements ChillModeExitRule<PipelineReportFromDatanode>,
    EventHandler<PipelineReportFromDatanode> {
  /** Pipeline availability.*/
  private AtomicBoolean isPipelineAvailable = new AtomicBoolean(false);

  private final PipelineManager pipelineManager;
  private final SCMChillModeManager chillModeManager;

  PipelineChillModeRule(PipelineManager pipelineManager,
      SCMChillModeManager manager) {
    this.pipelineManager = pipelineManager;
    this.chillModeManager = manager;
  }

  @Override
  public boolean validate() {
    return isPipelineAvailable.get();
  }

  @Override
  public void process(PipelineReportFromDatanode report) {
    // No need to deal with
  }

  @Override
  public void cleanup() {
    // No need to deal with
  }

  @Override
  public void onMessage(PipelineReportFromDatanode pipelineReportFromDatanode,
      EventPublisher publisher) {
    // If we are already in pipeline available state,
    // skipping following check.
    if (validate()) {
      chillModeManager.validateChillModeExitRules(publisher);
      return;
    }

    Pipeline pipeline;
    Preconditions.checkNotNull(pipelineReportFromDatanode);
    PipelineReportsProto pipelineReport = pipelineReportFromDatanode
        .getReport();

    for (PipelineReport report : pipelineReport.getPipelineReportList()) {
      PipelineID pipelineID = PipelineID
          .getFromProtobuf(report.getPipelineID());
      try {
        pipeline = pipelineManager.getPipeline(pipelineID);
      } catch (PipelineNotFoundException e) {
        continue;
      }

      if (pipeline.getPipelineState() == Pipeline.PipelineState.OPEN) {
        // ensure there is an OPEN state pipeline and then allowed
        // to exit chill mode
        isPipelineAvailable.set(true);

        if (chillModeManager.getInChillMode()) {
          SCMChillModeManager.getLogger()
              .info("SCM in chill mode. 1 Pipeline reported, 1 required.");
        }
        break;
      }
    }

    if (validate()) {
      chillModeManager.validateChillModeExitRules(publisher);
    }
  }
}