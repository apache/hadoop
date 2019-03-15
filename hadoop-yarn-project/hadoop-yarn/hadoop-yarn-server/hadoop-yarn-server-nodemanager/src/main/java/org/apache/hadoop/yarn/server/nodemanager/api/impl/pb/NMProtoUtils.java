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
package org.apache.hadoop.yarn.server.nodemanager.api.impl.pb;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.recovery.DeletionTaskRecoveryInfo;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.DeletionTask;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.DeletionTaskType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.DockerContainerDeletionTask;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.FileDeletionTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Utilities for converting from PB representations.
 */
public final class NMProtoUtils {

  private static final Logger LOG =
       LoggerFactory.getLogger(NMProtoUtils.class);

  private NMProtoUtils() { }

  /**
   * Convert the Protobuf representation into a {@link DeletionTask}.
   *
   * @param proto             the Protobuf representation for the DeletionTask
   * @param deletionService   the {@link DeletionService}
   * @return the converted {@link DeletionTask}
   */
  public static DeletionTask convertProtoToDeletionTask(
      DeletionServiceDeleteTaskProto proto, DeletionService deletionService) {
    int taskId = proto.getId();
    if (proto.hasTaskType() && proto.getTaskType() != null) {
      if (proto.getTaskType().equals(DeletionTaskType.FILE.name())) {
        LOG.debug("Converting recovered FileDeletionTask");
        return convertProtoToFileDeletionTask(proto, deletionService, taskId);
      } else if (proto.getTaskType().equals(
          DeletionTaskType.DOCKER_CONTAINER.name())) {
        LOG.debug("Converting recovered DockerContainerDeletionTask");
        return convertProtoToDockerContainerDeletionTask(proto, deletionService,
            taskId);
      }
    }
    LOG.debug("Unable to get task type, trying FileDeletionTask");
    return convertProtoToFileDeletionTask(proto, deletionService, taskId);
  }

  /**
   * Convert the Protobuf representation into the {@link FileDeletionTask}.
   *
   * @param proto the Protobuf representation of the {@link FileDeletionTask}.
   * @param deletionService the {@link DeletionService}.
   * @param taskId the ID of the {@link DeletionTask}.
   * @return the populated {@link FileDeletionTask}.
   */
  public static FileDeletionTask convertProtoToFileDeletionTask(
      DeletionServiceDeleteTaskProto proto, DeletionService deletionService,
      int taskId) {
    String user = proto.hasUser() ? proto.getUser() : null;
    Path subdir = null;
    if (proto.hasSubdir()) {
      subdir = new Path(proto.getSubdir());
    }
    List<Path> basePaths = null;
    List<String> basedirs = proto.getBasedirsList();
    if (basedirs != null && basedirs.size() > 0) {
      basePaths = new ArrayList<>(basedirs.size());
      for (String basedir : basedirs) {
        basePaths.add(new Path(basedir));
      }
    }
    return new FileDeletionTask(taskId, deletionService, user, subdir,
        basePaths);
  }

  /**
   * Convert the Protobuf format into the {@link DockerContainerDeletionTask}.
   *
   * @param proto Protobuf format of the {@link DockerContainerDeletionTask}.
   * @param deletionService the {@link DeletionService}.
   * @param taskId the ID of the {@link DeletionTask}.
   * @return the populated {@link DockerContainerDeletionTask}.
   */
  public static DockerContainerDeletionTask
      convertProtoToDockerContainerDeletionTask(
      DeletionServiceDeleteTaskProto proto, DeletionService deletionService,
      int taskId) {
    String user = proto.hasUser() ? proto.getUser() : null;
    String containerId =
        proto.hasDockerContainerId() ? proto.getDockerContainerId() : null;
    return new DockerContainerDeletionTask(taskId, deletionService, user,
        containerId);
  }

  /**
   * Convert the Protobuf representation to the {@link DeletionTaskRecoveryInfo}
   * representation.
   *
   * @param proto the Protobuf representation of the {@link DeletionTask}
   * @param deletionService the {@link DeletionService}
   * @return the populated {@link DeletionTaskRecoveryInfo}
   */
  public static DeletionTaskRecoveryInfo convertProtoToDeletionTaskRecoveryInfo(
      DeletionServiceDeleteTaskProto proto, DeletionService deletionService) {
    DeletionTask deletionTask =
        NMProtoUtils.convertProtoToDeletionTask(proto, deletionService);
    List<Integer> successorTaskIds = new ArrayList<>();
    if (proto.getSuccessorIdsList() != null &&
        !proto.getSuccessorIdsList().isEmpty()) {
      successorTaskIds = proto.getSuccessorIdsList();
    }
    long deletionTimestamp = proto.getDeletionTime();
    return new DeletionTaskRecoveryInfo(deletionTask, successorTaskIds,
        deletionTimestamp);
  }
}
