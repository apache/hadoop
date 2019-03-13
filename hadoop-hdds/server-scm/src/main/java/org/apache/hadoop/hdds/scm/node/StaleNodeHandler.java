/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

/**
 * Handles Stale node event.
 */
public class StaleNodeHandler implements EventHandler<DatanodeDetails> {
  private static final Logger LOG =
      LoggerFactory.getLogger(StaleNodeHandler.class);

  private final NodeManager nodeManager;
  private final PipelineManager pipelineManager;
  private final Configuration conf;

  public StaleNodeHandler(NodeManager nodeManager,
      PipelineManager pipelineManager, OzoneConfiguration conf) {
    this.nodeManager = nodeManager;
    this.pipelineManager = pipelineManager;
    this.conf = conf;
  }

  @Override
  public void onMessage(DatanodeDetails datanodeDetails,
      EventPublisher publisher) {
    Set<PipelineID> pipelineIds =
        nodeManager.getPipelines(datanodeDetails);
    LOG.info("Datanode {} moved to stale state. Finalizing its pipelines {}",
        datanodeDetails, pipelineIds);
    for (PipelineID pipelineID : pipelineIds) {
      try {
        Pipeline pipeline = pipelineManager.getPipeline(pipelineID);
        pipelineManager.finalizeAndDestroyPipeline(pipeline, true);
      } catch (IOException e) {
        LOG.info("Could not finalize pipeline={} for dn={}", pipelineID,
            datanodeDetails);
      }
    }
  }
}
