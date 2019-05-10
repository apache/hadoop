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
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.TensorFlowRunJobParameters;
import org.apache.hadoop.yarn.submarine.common.api.Role;
import org.apache.hadoop.yarn.submarine.common.api.TensorFlowRole;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.HadoopEnvironmentSetup;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.command.TensorBoardLaunchCommand;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.command.TensorFlowPsLaunchCommand;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.command.TensorFlowWorkerLaunchCommand;

import java.io.IOException;
import java.util.Objects;

/**
 * Simple factory to create instances of {@link AbstractLaunchCommand}
 * based on the {@link Role}.
 * All dependencies are passed to this factory that could be required
 * by any implementor of {@link AbstractLaunchCommand}.
 */
public class TensorFlowLaunchCommandFactory implements LaunchCommandFactory {
  private final HadoopEnvironmentSetup hadoopEnvSetup;
  private final TensorFlowRunJobParameters parameters;
  private final Configuration yarnConfig;

  public TensorFlowLaunchCommandFactory(HadoopEnvironmentSetup hadoopEnvSetup,
      TensorFlowRunJobParameters parameters, Configuration yarnConfig) {
    this.hadoopEnvSetup = hadoopEnvSetup;
    this.parameters = parameters;
    this.yarnConfig = yarnConfig;
  }

  @Override
  public AbstractLaunchCommand createLaunchCommand(Role role,
      Component component) throws IOException {
    Objects.requireNonNull(role, "Role must not be null!");

    if (role == TensorFlowRole.WORKER ||
        role == TensorFlowRole.PRIMARY_WORKER) {
      return new TensorFlowWorkerLaunchCommand(hadoopEnvSetup, role,
          component, parameters, yarnConfig);

    } else if (role == TensorFlowRole.PS) {
      return new TensorFlowPsLaunchCommand(hadoopEnvSetup, role, component,
          parameters, yarnConfig);

    } else if (role == TensorFlowRole.TENSORBOARD) {
      return new TensorBoardLaunchCommand(hadoopEnvSetup, role, component,
          parameters);
    }
    throw new IllegalStateException("Unknown task type: " + role);
  }
}
