/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.container;

import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager
    .DeletionRequestToRepeat;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager
    .DeleteContainerCommandCompleted;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.server.events.Event;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventWatcher;
import org.apache.hadoop.ozone.lease.LeaseManager;

/**
 * Command watcher to track the delete container commands.
 */
public class DeleteContainerCommandWatcher extends
    EventWatcher<DeletionRequestToRepeat, DeleteContainerCommandCompleted> {

  public DeleteContainerCommandWatcher(
      Event<DeletionRequestToRepeat> startEvent,
      Event<DeleteContainerCommandCompleted> completionEvent,
      LeaseManager<Long> leaseManager) {
    super(startEvent, completionEvent, leaseManager);
  }

  @Override
  protected void onTimeout(EventPublisher publisher,
      DeletionRequestToRepeat payload) {
    //put back to the original queue
    publisher.fireEvent(SCMEvents.REPLICATE_CONTAINER,
        payload.getRequest());
  }


  @Override
  protected void onFinished(EventPublisher publisher,
      DeletionRequestToRepeat payload) {

  }
}
