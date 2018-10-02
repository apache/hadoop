/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.node;

import java.util.Set;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerStateManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationRequest;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles Dead Node event.
 */
public class DeadNodeHandler implements EventHandler<DatanodeDetails> {

  private final ContainerStateManager containerStateManager;

  private final NodeManager nodeManager;

  private static final Logger LOG =
      LoggerFactory.getLogger(DeadNodeHandler.class);

  public DeadNodeHandler(NodeManager nodeManager,
      ContainerStateManager containerStateManager) {
    this.containerStateManager = containerStateManager;
    this.nodeManager = nodeManager;
  }

  @Override
  public void onMessage(DatanodeDetails datanodeDetails,
      EventPublisher publisher) {
    nodeManager.processDeadNode(datanodeDetails.getUuid());

    Set<ContainerID> containers =
        nodeManager.getContainers(datanodeDetails.getUuid());
    if (containers == null) {
      LOG.info("There's no containers in dead datanode {}, no replica will be"
          + " removed from the in-memory state.", datanodeDetails.getUuid());
      return;
    }
    LOG.info(
        "Datanode {}  is dead. Removing replications from the in-memory state.",
        datanodeDetails.getUuid());
    for (ContainerID container : containers) {
      try {
        try {
          containerStateManager.removeContainerReplica(container,
              datanodeDetails);
        } catch (SCMException ex) {
          LOG.info("DataNode {} doesn't have replica for container {}.",
              datanodeDetails.getUuid(), container.getId());
        }

        if (!containerStateManager.isOpen(container)) {
          ReplicationRequest replicationRequest =
              containerStateManager.checkReplicationState(container);

          if (replicationRequest != null) {
            publisher.fireEvent(SCMEvents.REPLICATE_CONTAINER,
                replicationRequest);
          }
        }
      } catch (SCMException e) {
        LOG.error("Can't remove container from containerStateMap {}", container
            .getId(), e);
      }
    }
  }

  /**
   * Returns logger.
   * */
  public static Logger getLogger() {
    return LOG;
  }
}
