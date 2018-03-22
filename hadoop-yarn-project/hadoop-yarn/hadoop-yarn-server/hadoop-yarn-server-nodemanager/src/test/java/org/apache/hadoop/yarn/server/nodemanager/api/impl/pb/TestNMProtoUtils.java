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
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * Test conversion to {@link DeletionTask}.
 */
public class TestNMProtoUtils {

  @Test
  public void testConvertProtoToDeletionTask() throws Exception {
    DeletionService deletionService = mock(DeletionService.class);
    DeletionServiceDeleteTaskProto.Builder protoBuilder =
        DeletionServiceDeleteTaskProto.newBuilder();
    int id = 0;
    protoBuilder.setId(id);
    DeletionServiceDeleteTaskProto proto = protoBuilder.build();
    DeletionTask deletionTask =
        NMProtoUtils.convertProtoToDeletionTask(proto, deletionService);
    assertEquals(DeletionTaskType.FILE, deletionTask.getDeletionTaskType());
    assertEquals(id, deletionTask.getTaskId());
  }

  @Test
  public void testConvertProtoToFileDeletionTask() throws Exception {
    DeletionService deletionService = mock(DeletionService.class);
    int id = 0;
    String user = "user";
    Path subdir = new Path("subdir");
    Path basedir = new Path("basedir");
    DeletionServiceDeleteTaskProto.Builder protoBuilder =
        DeletionServiceDeleteTaskProto.newBuilder();
    protoBuilder
        .setId(id)
        .setUser("user")
        .setSubdir(subdir.getName())
        .addBasedirs(basedir.getName());
    DeletionServiceDeleteTaskProto proto = protoBuilder.build();
    DeletionTask deletionTask =
        NMProtoUtils.convertProtoToFileDeletionTask(proto, deletionService, id);
    assertEquals(DeletionTaskType.FILE.name(),
        deletionTask.getDeletionTaskType().name());
    assertEquals(id, deletionTask.getTaskId());
    assertEquals(subdir, ((FileDeletionTask) deletionTask).getSubDir());
    assertEquals(basedir,
        ((FileDeletionTask) deletionTask).getBaseDirs().get(0));
  }

  @Test
  public void testConvertProtoToDockerContainerDeletionTask() throws Exception {
    DeletionService deletionService = mock(DeletionService.class);
    int id = 0;
    String user = "user";
    String dockerContainerId = "container_e123_12321231_00001";
    DeletionServiceDeleteTaskProto.Builder protoBuilder =
        DeletionServiceDeleteTaskProto.newBuilder();
    protoBuilder
        .setId(id)
        .setUser(user)
        .setDockerContainerId(dockerContainerId);
    DeletionServiceDeleteTaskProto proto = protoBuilder.build();
    DeletionTask deletionTask =
        NMProtoUtils.convertProtoToDockerContainerDeletionTask(proto,
            deletionService, id);
    assertEquals(DeletionTaskType.DOCKER_CONTAINER.name(),
        deletionTask.getDeletionTaskType().name());
    assertEquals(id, deletionTask.getTaskId());
    assertEquals(dockerContainerId,
        ((DockerContainerDeletionTask) deletionTask).getContainerId());
  }

  @Test
  public void testConvertProtoToDeletionTaskRecoveryInfo() throws Exception {
    long delTime = System.currentTimeMillis();
    List<Integer> successorTaskIds = Arrays.asList(1);
    DeletionTask deletionTask = mock(DeletionTask.class);
    DeletionTaskRecoveryInfo info =
        new DeletionTaskRecoveryInfo(deletionTask, successorTaskIds, delTime);
    assertEquals(deletionTask, info.getTask());
    assertEquals(successorTaskIds, info.getSuccessorTaskIds());
    assertEquals(delTime, info.getDeletionTimestamp());
  }

}