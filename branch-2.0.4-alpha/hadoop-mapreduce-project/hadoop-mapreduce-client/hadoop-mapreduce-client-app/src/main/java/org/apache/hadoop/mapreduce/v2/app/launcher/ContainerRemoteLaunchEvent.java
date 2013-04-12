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

import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerToken;

public class ContainerRemoteLaunchEvent extends ContainerLauncherEvent {

  private final ContainerLaunchContext container;
  private final Task task;

  public ContainerRemoteLaunchEvent(TaskAttemptId taskAttemptID,
      ContainerId containerID, String containerMgrAddress,
      ContainerToken containerToken,
      ContainerLaunchContext containerLaunchContext, Task remoteTask) {
    super(taskAttemptID, containerID, containerMgrAddress, containerToken,
        ContainerLauncher.EventType.CONTAINER_REMOTE_LAUNCH);
    this.container = containerLaunchContext;
    this.task = remoteTask;
  }

  public ContainerLaunchContext getContainer() {
    return this.container;
  }

  public Task getRemoteTask() {
    return this.task;
  }
  
  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }
}
