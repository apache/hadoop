/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.component;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.submarine.client.cli.param.RunJobParameters;
import org.apache.hadoop.yarn.submarine.common.api.TaskType;
import org.apache.hadoop.yarn.submarine.common.fs.RemoteDirectoryManager;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.AbstractComponent;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.FileSystemOperations;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command.LaunchCommandFactory;
import java.io.IOException;
import java.util.Objects;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConstants.CONTAINER_STATE_REPORT_AS_SERVICE_STATE;
import static org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.TensorFlowCommons.addCommonEnvironments;
import static org.apache.hadoop.yarn.submarine.utils.DockerUtilities.getDockerArtifact;
import static org.apache.hadoop.yarn.submarine.utils.SubmarineResourceUtils.convertYarnResourceToServiceResource;

/**
 * Component implementation for TensorFlow's Worker process.
 */
public class TensorFlowWorkerComponent extends AbstractComponent {
  public TensorFlowWorkerComponent(FileSystemOperations fsOperations,
      RemoteDirectoryManager remoteDirectoryManager,
      RunJobParameters parameters, TaskType taskType,
      LaunchCommandFactory launchCommandFactory,
      Configuration yarnConfig) {
    super(fsOperations, remoteDirectoryManager, parameters, taskType,
        yarnConfig, launchCommandFactory);
  }

  @Override
  public Component createComponent() throws IOException {
    Objects.requireNonNull(parameters.getWorkerResource(),
        "Worker resource must not be null!");
    if (parameters.getNumWorkers() < 1) {
      throw new IllegalArgumentException(
          "Number of workers should be at least 1!");
    }

    Component component = new Component();
    component.setName(taskType.getComponentName());

    if (taskType.equals(TaskType.PRIMARY_WORKER)) {
      component.setNumberOfContainers(1L);
      component.getConfiguration().setProperty(
          CONTAINER_STATE_REPORT_AS_SERVICE_STATE, "true");
    } else {
      component.setNumberOfContainers(
          (long) parameters.getNumWorkers() - 1);
    }

    if (parameters.getWorkerDockerImage() != null) {
      component.setArtifact(
          getDockerArtifact(parameters.getWorkerDockerImage()));
    }

    component.setResource(convertYarnResourceToServiceResource(
            parameters.getWorkerResource()));
    component.setRestartPolicy(Component.RestartPolicyEnum.NEVER);

    addCommonEnvironments(component, taskType);
    generateLaunchCommand(component);

    return component;
  }
}
