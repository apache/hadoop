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

package org.apache.hadoop.yarn.submarine.runtimes.yarnservice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.RunJobParameters;
import org.apache.hadoop.yarn.submarine.common.api.PyTorchRole;
import org.apache.hadoop.yarn.submarine.common.api.Role;
import org.apache.hadoop.yarn.submarine.common.api.TensorFlowRole;
import org.apache.hadoop.yarn.submarine.common.fs.RemoteDirectoryManager;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command.AbstractLaunchCommand;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command.LaunchCommandFactory;

import java.io.IOException;
import java.util.Objects;

import static org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.TensorFlowCommons.addCommonEnvironments;
import static org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.TensorFlowCommons.getScriptFileName;
import static org.apache.hadoop.yarn.submarine.utils.DockerUtilities.getDockerArtifact;
import static org.apache.hadoop.yarn.submarine.utils.SubmarineResourceUtils.convertYarnResourceToServiceResource;

/**
 * Abstract base class for Component classes.
 * The implementations of this class are act like factories for
 * {@link Component} instances.
 * All dependencies are passed to the constructor so that child classes
 * are obliged to provide matching constructors.
 */
public abstract class AbstractComponent {
  private final FileSystemOperations fsOperations;
  protected final RunJobParameters parameters;
  protected final Role role;
  private final RemoteDirectoryManager remoteDirectoryManager;
  protected final Configuration yarnConfig;
  private final LaunchCommandFactory launchCommandFactory;

  /**
   * This is only required for testing.
   */
  private String localScriptFile;

  public AbstractComponent(FileSystemOperations fsOperations,
      RemoteDirectoryManager remoteDirectoryManager,
      RunJobParameters parameters, Role role,
      Configuration yarnConfig,
      LaunchCommandFactory launchCommandFactory) {
    this.fsOperations = fsOperations;
    this.remoteDirectoryManager = remoteDirectoryManager;
    this.parameters = parameters;
    this.role = role;
    this.launchCommandFactory = launchCommandFactory;
    this.yarnConfig = yarnConfig;
  }

  protected abstract Component createComponent() throws IOException;

  protected Component createComponentInternal() throws IOException {
    Objects.requireNonNull(this.parameters.getWorkerResource(),
        "Worker resource must not be null!");
    if (parameters.getNumWorkers() < 1) {
      throw new IllegalArgumentException(
          "Number of workers should be at least 1!");
    }

    Component component = new Component();
    component.setName(role.getComponentName());

    if (role.equals(TensorFlowRole.PRIMARY_WORKER) ||
        role.equals(PyTorchRole.PRIMARY_WORKER)) {
      component.setNumberOfContainers(1L);
      // If the dependencies are upgraded to hadoop 3.3.0.
      // yarn.service.container-state-report-as-service-state can be replaced
      // with CONTAINER_STATE_REPORT_AS_SERVICE_STATE
      component.getConfiguration().setProperty(
          "yarn.service.container-state-report-as-service-state", "true");
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

    addCommonEnvironments(component, role);
    generateLaunchCommand(component);

    return component;
  }

  /**
   * Generates a command launch script on local disk,
   * returns path to the script.
   */
  protected void generateLaunchCommand(Component component)
      throws IOException {
    AbstractLaunchCommand launchCommand =
        launchCommandFactory.createLaunchCommand(role, component);
    this.localScriptFile = launchCommand.generateLaunchScript();

    String remoteLaunchCommand = uploadLaunchCommand(component);
    component.setLaunchCommand(remoteLaunchCommand);
  }

  private String uploadLaunchCommand(Component component)
      throws IOException {
    Objects.requireNonNull(localScriptFile, "localScriptFile should be " +
        "set before calling this method!");
    Path stagingDir =
        remoteDirectoryManager.getJobStagingArea(parameters.getName(), true);

    String destScriptFileName = getScriptFileName(role);
    fsOperations.uploadToRemoteFileAndLocalizeToContainerWorkDir(stagingDir,
        localScriptFile, destScriptFileName, component);

    return "./" + destScriptFileName;
  }

  String getLocalScriptFile() {
    return localScriptFile;
  }
}
