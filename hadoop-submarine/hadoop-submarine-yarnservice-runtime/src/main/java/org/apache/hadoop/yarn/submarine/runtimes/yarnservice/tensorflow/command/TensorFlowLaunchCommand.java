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

package org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.command;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.TensorFlowRunJobParameters;
import org.apache.hadoop.yarn.submarine.common.api.Role;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.HadoopEnvironmentSetup;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command.AbstractLaunchCommand;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command.LaunchScriptBuilder;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.TensorFlowCommons;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * Launch command implementation for
 * TensorFlow PS and Worker Service components.
 */
public abstract class TensorFlowLaunchCommand extends AbstractLaunchCommand {
  private static final Logger LOG =
      LoggerFactory.getLogger(TensorFlowLaunchCommand.class);
  private final Configuration yarnConfig;
  private final boolean distributed;
  private final int numberOfWorkers;
  private final int numberOfPS;
  private final String name;
  private final Role role;

  TensorFlowLaunchCommand(HadoopEnvironmentSetup hadoopEnvSetup,
      Role role, Component component,
      TensorFlowRunJobParameters parameters,
      Configuration yarnConfig) throws IOException {
    super(hadoopEnvSetup, component, parameters,
        role != null ? role.getName(): "");
    Objects.requireNonNull(role, "TensorFlowRole must not be null!");
    this.role = role;
    this.name = parameters.getName();
    this.distributed = parameters.isDistributed();
    this.numberOfWorkers = parameters.getNumWorkers();
    this.numberOfPS = parameters.getNumPS();
    this.yarnConfig = yarnConfig;
    logReceivedParameters();
  }

  private void logReceivedParameters() {
    if (this.numberOfWorkers <= 0) {
      LOG.warn("Received number of workers: {}", this.numberOfWorkers);
    }
    if (this.numberOfPS <= 0) {
      LOG.warn("Received number of PS: {}", this.numberOfPS);
    }
  }

  @Override
  public String generateLaunchScript() throws IOException {
    LaunchScriptBuilder builder = getBuilder();

    // When distributed training is required
    if (distributed) {
      String tfConfigEnvValue = TensorFlowCommons.getTFConfigEnv(
          role.getComponentName(), numberOfWorkers,
          numberOfPS, name,
          TensorFlowCommons.getUserName(),
          TensorFlowCommons.getDNSDomain(yarnConfig));
      String tfConfig = "export TF_CONFIG=\"" + tfConfigEnvValue + "\"\n";
      builder.append(tfConfig);
    }

    return builder
        .withLaunchCommand(createLaunchCommand())
        .build();
  }
}
