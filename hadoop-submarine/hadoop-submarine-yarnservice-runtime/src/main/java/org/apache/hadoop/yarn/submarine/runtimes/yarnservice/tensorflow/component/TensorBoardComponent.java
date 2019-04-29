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
import org.apache.hadoop.yarn.service.api.records.Component.RestartPolicyEnum;
import org.apache.hadoop.yarn.submarine.client.cli.param.RunJobParameters;
import org.apache.hadoop.yarn.submarine.common.api.TaskType;
import org.apache.hadoop.yarn.submarine.common.fs.RemoteDirectoryManager;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.AbstractComponent;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.FileSystemOperations;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.YarnServiceUtils;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command.LaunchCommandFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

import static org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.TensorFlowCommons.addCommonEnvironments;
import static org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.TensorFlowCommons.getDNSDomain;
import static org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.TensorFlowCommons.getUserName;
import static org.apache.hadoop.yarn.submarine.utils.DockerUtilities.getDockerArtifact;
import static org.apache.hadoop.yarn.submarine.utils.SubmarineResourceUtils.convertYarnResourceToServiceResource;

/**
 * Component implementation for Tensorboard's Tensorboard.
 */
public class TensorBoardComponent extends AbstractComponent {
  private static final Logger LOG =
      LoggerFactory.getLogger(TensorBoardComponent.class);

  public static final String TENSORBOARD_QUICKLINK_LABEL = "Tensorboard";
  private static final int DEFAULT_PORT = 6006;

  //computed fields
  private String tensorboardLink;

  public TensorBoardComponent(FileSystemOperations fsOperations,
      RemoteDirectoryManager remoteDirectoryManager,
      RunJobParameters parameters,
      LaunchCommandFactory launchCommandFactory,
      Configuration yarnConfig) {
    super(fsOperations, remoteDirectoryManager, parameters,
        TaskType.TENSORBOARD, yarnConfig, launchCommandFactory);
  }

  @Override
  public Component createComponent() throws IOException {
    Objects.requireNonNull(parameters.getTensorboardResource(),
        "TensorBoard resource must not be null!");

    Component component = new Component();
    component.setName(taskType.getComponentName());
    component.setNumberOfContainers(1L);
    component.setRestartPolicy(RestartPolicyEnum.NEVER);
    component.setResource(convertYarnResourceToServiceResource(
        parameters.getTensorboardResource()));

    if (parameters.getTensorboardDockerImage() != null) {
      component.setArtifact(
          getDockerArtifact(parameters.getTensorboardDockerImage()));
    }

    addCommonEnvironments(component, taskType);
    generateLaunchCommand(component);

    tensorboardLink = "http://" + YarnServiceUtils.getDNSName(
        parameters.getName(),
        taskType.getComponentName() + "-" + 0, getUserName(),
        getDNSDomain(yarnConfig), DEFAULT_PORT);
    LOG.info("Link to tensorboard:" + tensorboardLink);

    return component;
  }

  public String getTensorboardLink() {
    return tensorboardLink;
  }

}
