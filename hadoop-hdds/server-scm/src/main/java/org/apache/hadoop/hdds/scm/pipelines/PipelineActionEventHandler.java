/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.pipelines;

import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.PipelineAction;
import org.apache.hadoop.hdds.scm.container.common.helpers.PipelineID;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
    .PipelineActionsFromDatanode;

import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles pipeline actions from datanode.
 */
public class PipelineActionEventHandler implements
    EventHandler<PipelineActionsFromDatanode> {

  public static final Logger LOG = LoggerFactory.getLogger(
      PipelineActionEventHandler.class);

  public PipelineActionEventHandler() {

  }

  @Override
  public void onMessage(PipelineActionsFromDatanode report,
      EventPublisher publisher) {
    for (PipelineAction action : report.getReport().getPipelineActionsList()) {
      switch (action.getAction()) {
      case CLOSE:
        PipelineID pipelineID = PipelineID.
            getFromProtobuf(action.getClosePipeline().getPipelineID());
        LOG.info("Closing pipeline " + pipelineID + " for reason:" + action
            .getClosePipeline().getDetailedReason());
        publisher.fireEvent(SCMEvents.PIPELINE_CLOSE, pipelineID);
        break;
      default:
        LOG.error("unknown pipeline action:{}" + action.getAction());
      }
    }
  }
}
