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

package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineAction;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.PipelineActionsFromDatanode;

import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Handles pipeline actions from datanode.
 */
public class PipelineActionHandler
    implements EventHandler<PipelineActionsFromDatanode> {

  public static final Logger LOG =
      LoggerFactory.getLogger(PipelineActionHandler.class);

  private final PipelineManager pipelineManager;
  private final Configuration ozoneConf;

  public PipelineActionHandler(PipelineManager pipelineManager,
      OzoneConfiguration conf) {
    this.pipelineManager = pipelineManager;
    this.ozoneConf = conf;
  }

  @Override
  public void onMessage(PipelineActionsFromDatanode report,
      EventPublisher publisher) {
    for (PipelineAction action : report.getReport().getPipelineActionsList()) {
      if (action.getAction() == PipelineAction.Action.CLOSE) {
        PipelineID pipelineID = null;
        try {
          pipelineID = PipelineID.
              getFromProtobuf(action.getClosePipeline().getPipelineID());
          Pipeline pipeline = pipelineManager.getPipeline(pipelineID);
          LOG.info("Received pipeline action {} for {} from datanode [}",
              action.getAction(), pipeline, report.getDatanodeDetails());
          pipelineManager.finalizeAndDestroyPipeline(pipeline, true);
        } catch (IOException ioe) {
          LOG.error("Could not execute pipeline action={} pipeline={} {}",
              action, pipelineID, ioe);
        }
      } else {
        LOG.error("unknown pipeline action:{}" + action.getAction());
      }
    }
  }
}
