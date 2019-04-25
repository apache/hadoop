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
import org.apache.hadoop.yarn.submarine.client.cli.param.RunJobParameters;
import org.apache.hadoop.yarn.submarine.common.api.TaskType;
import org.apache.hadoop.yarn.submarine.common.fs.RemoteDirectoryManager;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command.AbstractLaunchCommand;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command.LaunchCommandFactory;

import java.io.IOException;
import java.util.Objects;

import static org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.TensorFlowCommons.getScriptFileName;

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
  protected final TaskType taskType;
  private final RemoteDirectoryManager remoteDirectoryManager;
  protected final Configuration yarnConfig;
  private final LaunchCommandFactory launchCommandFactory;

  /**
   * This is only required for testing.
   */
  private String localScriptFile;

  public AbstractComponent(FileSystemOperations fsOperations,
      RemoteDirectoryManager remoteDirectoryManager,
      RunJobParameters parameters, TaskType taskType,
      Configuration yarnConfig,
      LaunchCommandFactory launchCommandFactory) {
    this.fsOperations = fsOperations;
    this.remoteDirectoryManager = remoteDirectoryManager;
    this.parameters = parameters;
    this.taskType = taskType;
    this.launchCommandFactory = launchCommandFactory;
    this.yarnConfig = yarnConfig;
  }

  protected abstract Component createComponent() throws IOException;

  /**
   * Generates a command launch script on local disk,
   * returns path to the script.
   */
  protected void generateLaunchCommand(Component component)
      throws IOException {
    AbstractLaunchCommand launchCommand =
        launchCommandFactory.createLaunchCommand(taskType, component);
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

    String destScriptFileName = getScriptFileName(taskType);
    fsOperations.uploadToRemoteFileAndLocalizeToContainerWorkDir(stagingDir,
        localScriptFile, destScriptFileName, component);

    return "./" + destScriptFileName;
  }

  String getLocalScriptFile() {
    return localScriptFile;
  }
}
