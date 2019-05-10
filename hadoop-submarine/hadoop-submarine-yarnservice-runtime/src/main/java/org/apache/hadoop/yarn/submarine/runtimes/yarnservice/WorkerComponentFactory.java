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
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.PyTorchRunJobParameters;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.RunJobParameters;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.TensorFlowRunJobParameters;
import org.apache.hadoop.yarn.submarine.client.cli.runjob.Framework;
import org.apache.hadoop.yarn.submarine.common.api.Role;
import org.apache.hadoop.yarn.submarine.common.fs.RemoteDirectoryManager;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command.LaunchCommandFactory;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command.PyTorchLaunchCommandFactory;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command.TensorFlowLaunchCommandFactory;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.pytorch.component.PyTorchWorkerComponent;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.component.TensorFlowWorkerComponent;

/**
 * Factory class that helps creating Native Service components.
 */
public class WorkerComponentFactory {
  private final FileSystemOperations fsOperations;
  private final RemoteDirectoryManager remoteDirectoryManager;
  private final RunJobParameters parameters;
  private final LaunchCommandFactory launchCommandFactory;
  private final Configuration yarnConfig;

  WorkerComponentFactory(FileSystemOperations fsOperations,
      RemoteDirectoryManager remoteDirectoryManager,
      RunJobParameters parameters,
      LaunchCommandFactory launchCommandFactory,
      Configuration yarnConfig) {
    this.fsOperations = fsOperations;
    this.remoteDirectoryManager = remoteDirectoryManager;
    this.parameters = parameters;
    this.launchCommandFactory = launchCommandFactory;
    this.yarnConfig = yarnConfig;
  }

  /**
   * Creates either a TensorFlow or a PyTorch Native Service component.
   */
  public AbstractComponent create(Framework framework, Role role) {
    if (framework == Framework.TENSORFLOW) {
      return new TensorFlowWorkerComponent(fsOperations, remoteDirectoryManager,
          (TensorFlowRunJobParameters) parameters, role,
          (TensorFlowLaunchCommandFactory) launchCommandFactory, yarnConfig);
    } else if (framework == Framework.PYTORCH) {
      return new PyTorchWorkerComponent(fsOperations, remoteDirectoryManager,
          (PyTorchRunJobParameters) parameters, role,
          (PyTorchLaunchCommandFactory) launchCommandFactory, yarnConfig);
    } else {
      throw new UnsupportedOperationException("Only supported frameworks are: "
          + Framework.getValues());
    }
  }
}
