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

package org.apache.hadoop.hdds.scm.pipelines;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
        .StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.scm.server
        .SCMDatanodeHeartbeatDispatcher.PipelineReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles Node Reports from datanode.
 */
public class PipelineReportHandler implements
        EventHandler<PipelineReportFromDatanode> {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(PipelineReportHandler.class);
  private final PipelineSelector pipelineSelector;

  public PipelineReportHandler(PipelineSelector pipelineSelector) {
    Preconditions.checkNotNull(pipelineSelector);
    this.pipelineSelector = pipelineSelector;
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
    pipelineSelector.processPipelineReport(dn, pipelineReport);
  }
}
