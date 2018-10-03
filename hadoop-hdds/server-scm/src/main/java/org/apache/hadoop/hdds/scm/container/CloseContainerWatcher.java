/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.container;

import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.CommandStatus.Status;
import org.apache.hadoop.hdds.scm.command.CommandStatusReportHandler
    .CloseContainerStatus;

import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.server.events.Event;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventWatcher;
import org.apache.hadoop.hdds.scm.container.CloseContainerEventHandler
    .CloseContainerRetryableReq;
import org.apache.hadoop.ozone.lease.LeaseManager;
import org.apache.hadoop.ozone.lease.LeaseNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This watcher will watch for CLOSE_CONTAINER_STATUS events fired from
 * CommandStatusReport. If required it will re-trigger CloseContainer command
 * for DataNodes to CloseContainerEventHandler.
 */
public class CloseContainerWatcher extends
    EventWatcher<CloseContainerRetryableReq, CloseContainerStatus> {

  public static final Logger LOG =
      LoggerFactory.getLogger(CloseContainerWatcher.class);
  private final ContainerManager containerManager;

  public CloseContainerWatcher(Event<CloseContainerRetryableReq> startEvent,
      Event<CloseContainerStatus> completionEvent,
      LeaseManager<Long> leaseManager, ContainerManager containerManager) {
    super(startEvent, completionEvent, leaseManager);
    this.containerManager = containerManager;
  }

  @Override
  protected void onTimeout(EventPublisher publisher,
      CloseContainerRetryableReq payload) {
    // Let CloseContainerEventHandler handle this message.
    this.resendEventToHandler(payload.getId(), publisher);
  }

  @Override
  protected void onFinished(EventPublisher publisher,
      CloseContainerRetryableReq payload) {
    LOG.trace("CloseContainerCommand for containerId: {} executed ", payload
        .getContainerID().getId());
  }

  @Override
  protected synchronized void handleCompletion(CloseContainerStatus status,
      EventPublisher publisher) throws LeaseNotFoundException {
    // If status is PENDING then return without doing anything.
    if(status.getCmdStatus().getStatus().equals(Status.PENDING)){
      return;
    }

    CloseContainerRetryableReq closeCont = getTrackedEventbyId(status.getId());
    super.handleCompletion(status, publisher);
    // If status is FAILED then send a msg to Handler to resend the command.
    if (status.getCmdStatus().getStatus().equals(Status.FAILED) && closeCont
        != null) {
      this.resendEventToHandler(closeCont.getId(), publisher);
    }
  }

  private void resendEventToHandler(long containerID, EventPublisher
      publisher) {
    try {
      // Check if container is still open
      if (containerManager.getContainer(containerID).isContainerOpen()) {
        publisher.fireEvent(SCMEvents.CLOSE_CONTAINER,
            ContainerID.valueof(containerID));
      }
    } catch (IOException e) {
      LOG.warn("Error in CloseContainerWatcher while processing event " +
          "for containerId {} ExceptionMsg: ", containerID, e.getMessage());
    }
  }
}
