/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task;

import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor;

/**
 * {@link DeletionTask} handling the removal of Docker containers.
 */
public class DockerContainerDeletionTask extends DeletionTask
    implements Runnable {
  private String containerId;

  public DockerContainerDeletionTask(DeletionService deletionService,
      String user, String containerId) {
    this(INVALID_TASK_ID, deletionService, user, containerId);
  }

  public DockerContainerDeletionTask(int taskId,
      DeletionService deletionService, String user, String containerId) {
    super(taskId, deletionService, user, DeletionTaskType.DOCKER_CONTAINER);
    this.containerId = containerId;
  }

  /**
   * Get the id of the container to delete.
   *
   * @return the id of the container to delete.
   */
  public String getContainerId() {
    return containerId;
  }

  /**
   * Delete the specified Docker container.
   */
  @Override
  public void run() {
    if (LOG.isDebugEnabled()) {
      String msg = String.format("Running DeletionTask : %s", toString());
      LOG.debug(msg);
    }
    LinuxContainerExecutor exec = ((LinuxContainerExecutor)
        getDeletionService().getContainerExecutor());
    exec.removeDockerContainer(containerId);
  }

  /**
   * Convert the DockerContainerDeletionTask to a String representation.
   *
   * @return String representation of the DockerContainerDeletionTask.
   */
  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer("DockerContainerDeletionTask : ");
    sb.append("  id : ").append(this.getTaskId());
    sb.append("  containerId : ").append(this.containerId);
    return sb.toString().trim();
  }

  /**
   * Convert the DockerContainerDeletionTask to the Protobuf representation for
   * storing in the state store and recovery.
   *
   * @return the protobuf representation of the DockerContainerDeletionTask.
   */
  public DeletionServiceDeleteTaskProto convertDeletionTaskToProto() {
    DeletionServiceDeleteTaskProto.Builder builder =
        getBaseDeletionTaskProtoBuilder();
    builder.setTaskType(DeletionTaskType.DOCKER_CONTAINER.name());
    if (getContainerId() != null) {
      builder.setDockerContainerId(getContainerId());
    }
    return builder.build();
  }
}