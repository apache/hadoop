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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

/**
 * Simple event class used to communicate kill reserved containers, mark
 * containers for preemption and kill already preemption-marked containers.
 */
public class ContainerPreemptEvent extends SchedulerEvent {

  private final ApplicationAttemptId aid;
  private final RMContainer container;

  public ContainerPreemptEvent(ApplicationAttemptId aid, RMContainer container,
      SchedulerEventType type) {
    super(type);
    this.aid = aid;
    this.container = container;
  }

  public RMContainer getContainer(){
    return this.container;
  }

  public ApplicationAttemptId getAppId() {
    return aid;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(super.toString());
    sb.append(" ").append(getAppId())
        .append(" ").append(getContainer().getContainerId());
    return sb.toString();
  }

}
