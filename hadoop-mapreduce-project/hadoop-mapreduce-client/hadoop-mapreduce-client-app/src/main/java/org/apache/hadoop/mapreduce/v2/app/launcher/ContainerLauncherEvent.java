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

package org.apache.hadoop.mapreduce.v2.app.launcher;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.event.AbstractEvent;

public class ContainerLauncherEvent 
    extends AbstractEvent<ContainerLauncher.EventType> {

  private TaskAttemptId taskAttemptID;
  private ContainerId containerID;
  private String containerMgrAddress;
  private Token containerToken;
  private boolean dumpContainerThreads;

  public ContainerLauncherEvent(TaskAttemptId taskAttemptID, 
      ContainerId containerID,
      String containerMgrAddress,
      Token containerToken,
      ContainerLauncher.EventType type) {
    this(taskAttemptID, containerID, containerMgrAddress, containerToken, type,
        false);
  }

  public ContainerLauncherEvent(TaskAttemptId taskAttemptID,
      ContainerId containerID,
      String containerMgrAddress,
      Token containerToken,
      ContainerLauncher.EventType type,
      boolean dumpContainerThreads) {
    super(type);
    this.taskAttemptID = taskAttemptID;
    this.containerID = containerID;
    this.containerMgrAddress = containerMgrAddress;
    this.containerToken = containerToken;
    this.dumpContainerThreads = dumpContainerThreads;
  }

  public TaskAttemptId getTaskAttemptID() {
    return this.taskAttemptID;
  }

  public ContainerId getContainerID() {
    return containerID;
  }

  public String getContainerMgrAddress() {
    return containerMgrAddress;
  }

  public Token getContainerToken() {
    return containerToken;
  }

  public boolean getDumpContainerThreads() {
    return dumpContainerThreads;
  }

  @Override
  public String toString() {
    return super.toString() + " for container " + containerID + " taskAttempt "
        + taskAttemptID;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((containerID == null) ? 0 : containerID.hashCode());
    result = prime * result
        + ((containerMgrAddress == null) ? 0 : containerMgrAddress.hashCode());
    result = prime * result
        + ((containerToken == null) ? 0 : containerToken.hashCode());
    result = prime * result
        + ((taskAttemptID == null) ? 0 : taskAttemptID.hashCode());
    result = prime * result
        + (dumpContainerThreads ? 1 : 0);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ContainerLauncherEvent other = (ContainerLauncherEvent) obj;
    if (containerID == null) {
      if (other.containerID != null)
        return false;
    } else if (!containerID.equals(other.containerID))
      return false;
    if (containerMgrAddress == null) {
      if (other.containerMgrAddress != null)
        return false;
    } else if (!containerMgrAddress.equals(other.containerMgrAddress))
      return false;
    if (containerToken == null) {
      if (other.containerToken != null)
        return false;
    } else if (!containerToken.equals(other.containerToken))
      return false;
    if (taskAttemptID == null) {
      if (other.taskAttemptID != null)
        return false;
    } else if (!taskAttemptID.equals(other.taskAttemptID))
      return false;

    return dumpContainerThreads == other.dumpContainerThreads;
  }

}
