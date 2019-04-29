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

package org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.submarine.client.cli.param.RunJobParameters;
import org.apache.hadoop.yarn.submarine.common.api.TaskType;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.HadoopEnvironmentSetup;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.command.TensorBoardLaunchCommand;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.command.TensorFlowPsLaunchCommand;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.command.TensorFlowWorkerLaunchCommand;

import java.io.IOException;
import java.util.Objects;

/**
 * Simple factory to create instances of {@link AbstractLaunchCommand}
 * based on the {@link TaskType}.
 * All dependencies are passed to this factory that could be required
 * by any implementor of {@link AbstractLaunchCommand}.
 */
public class LaunchCommandFactory {
  private final HadoopEnvironmentSetup hadoopEnvSetup;
  private final RunJobParameters parameters;
  private final Configuration yarnConfig;

  public LaunchCommandFactory(HadoopEnvironmentSetup hadoopEnvSetup,
      RunJobParameters parameters, Configuration yarnConfig) {
    this.hadoopEnvSetup = hadoopEnvSetup;
    this.parameters = parameters;
    this.yarnConfig = yarnConfig;
  }

  public AbstractLaunchCommand createLaunchCommand(TaskType taskType,
      Component component) throws IOException {
    Objects.requireNonNull(taskType, "TaskType must not be null!");

    if (taskType == TaskType.WORKER || taskType == TaskType.PRIMARY_WORKER) {
      return new TensorFlowWorkerLaunchCommand(hadoopEnvSetup, taskType,
          component, parameters, yarnConfig);

    } else if (taskType == TaskType.PS) {
      return new TensorFlowPsLaunchCommand(hadoopEnvSetup, taskType, component,
          parameters, yarnConfig);

    } else if (taskType == TaskType.TENSORBOARD) {
      return new TensorBoardLaunchCommand(hadoopEnvSetup, taskType, component,
          parameters);
    }
    throw new IllegalStateException("Unknown task type: " + taskType);
  }
}
