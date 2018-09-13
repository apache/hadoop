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

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.Mapping;
import org.apache.hadoop.hdds.scm.container.common.helpers.PipelineID;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.node.states.Node2ContainerMap;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Handles Stale node event.
 */
public class StaleNodeHandler implements EventHandler<DatanodeDetails> {
  static final Logger LOG = LoggerFactory.getLogger(StaleNodeHandler.class);

  private final Node2ContainerMap node2ContainerMap;
  private final Mapping containerManager;

  public StaleNodeHandler(Node2ContainerMap node2ContainerMap,
      Mapping containerManager) {
    this.node2ContainerMap = node2ContainerMap;
    this.containerManager = containerManager;
  }

  @Override
  public void onMessage(DatanodeDetails datanodeDetails,
                        EventPublisher publisher) {
    Set<PipelineID> pipelineIDs =
        containerManager.getPipelineOnDatanode(datanodeDetails);
    for (PipelineID id : pipelineIDs) {
      LOG.info("closing pipeline {}.", id);
      publisher.fireEvent(SCMEvents.PIPELINE_CLOSE, id);
    }
  }
}
