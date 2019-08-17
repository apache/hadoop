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

import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.PyTorchRunJobParameters;
import org.apache.hadoop.yarn.submarine.common.api.PyTorchRole;
import org.apache.hadoop.yarn.submarine.common.api.Role;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.HadoopEnvironmentSetup;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.pytorch.command.PyTorchWorkerLaunchCommand;

/**
 * Simple factory to create instances of {@link AbstractLaunchCommand}
 * based on the {@link Role}.
 * All dependencies are passed to this factory that could be required
 * by any implementor of {@link AbstractLaunchCommand}.
 */
public class PyTorchLaunchCommandFactory implements LaunchCommandFactory {
  private final HadoopEnvironmentSetup hadoopEnvSetup;
  private final PyTorchRunJobParameters parameters;
  private final Configuration yarnConfig;

  public PyTorchLaunchCommandFactory(HadoopEnvironmentSetup hadoopEnvSetup,
      PyTorchRunJobParameters parameters, Configuration yarnConfig) {
    this.hadoopEnvSetup = hadoopEnvSetup;
    this.parameters = parameters;
    this.yarnConfig = yarnConfig;
  }

  public AbstractLaunchCommand createLaunchCommand(Role role,
      Component component) throws IOException {
    Objects.requireNonNull(role, "Role must not be null!");

    if (role == PyTorchRole.WORKER ||
        role == PyTorchRole.PRIMARY_WORKER) {
      return new PyTorchWorkerLaunchCommand(hadoopEnvSetup, role,
          component, parameters, yarnConfig);

    } else {
      throw new IllegalStateException("Unknown task type: " + role);
    }
  }
}
