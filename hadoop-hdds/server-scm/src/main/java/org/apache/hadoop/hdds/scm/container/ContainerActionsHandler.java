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

package org.apache.hadoop.hdds.scm.container;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerAction;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
    .ContainerActionsFromDatanode;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles container reports from datanode.
 */
public class ContainerActionsHandler implements
    EventHandler<ContainerActionsFromDatanode> {

  private static final Logger LOG = LoggerFactory.getLogger(
      ContainerActionsHandler.class);

  @Override
  public void onMessage(
      ContainerActionsFromDatanode containerReportFromDatanode,
      EventPublisher publisher) {
    DatanodeDetails dd = containerReportFromDatanode.getDatanodeDetails();
    for (ContainerAction action : containerReportFromDatanode.getReport()
        .getContainerActionsList()) {
      ContainerID containerId = ContainerID.valueof(action.getContainerID());
      switch (action.getAction()) {
      case CLOSE:
        LOG.debug("Closing container {} in datanode {} because the" +
            " container is {}.", containerId, dd, action.getReason());
        publisher.fireEvent(SCMEvents.CLOSE_CONTAINER, containerId);
        break;
      default:
        LOG.warn("Invalid action {} with reason {}, from datanode {}. ",
            action.getAction(), action.getReason(), dd); }
    }
  }
}
