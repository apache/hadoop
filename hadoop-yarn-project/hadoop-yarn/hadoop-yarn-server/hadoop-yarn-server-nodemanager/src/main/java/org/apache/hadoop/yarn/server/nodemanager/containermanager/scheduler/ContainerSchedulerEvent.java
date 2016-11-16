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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler;

import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container
    .Container;

/**
 * Events consumed by the {@link ContainerScheduler}.
 */
public class ContainerSchedulerEvent extends
    AbstractEvent<ContainerSchedulerEventType> {

  private final Container container;

  /**
   * Create instance of Event.
   * @param container Container.
   * @param eventType EventType.
   */
  public ContainerSchedulerEvent(Container container,
      ContainerSchedulerEventType eventType) {
    super(eventType);
    this.container = container;
  }

  /**
   * Get the container associated with the event.
   * @return Container.
   */
  public Container getContainer() {
    return container;
  }
}
