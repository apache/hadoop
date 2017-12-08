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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .ParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .QueueManagementChange;

import java.util.List;

/**
 * Event to update scheduler of any queue management changes
 */
public class QueueManagementChangeEvent extends SchedulerEvent {

  private ParentQueue parentQueue;
  private List<QueueManagementChange> queueManagementChanges;

  public QueueManagementChangeEvent(ParentQueue parentQueue,
      List<QueueManagementChange> queueManagementChanges) {
    super(SchedulerEventType.MANAGE_QUEUE);
    this.parentQueue = parentQueue;
    this.queueManagementChanges = queueManagementChanges;
  }

  public ParentQueue getParentQueue() {
    return parentQueue;
  }

  public List<QueueManagementChange> getQueueManagementChanges() {
    return queueManagementChanges;
  }
}
