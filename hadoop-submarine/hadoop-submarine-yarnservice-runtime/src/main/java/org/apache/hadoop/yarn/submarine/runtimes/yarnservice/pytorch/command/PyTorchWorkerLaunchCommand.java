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

package org.apache.hadoop.yarn.submarine.runtimes.yarnservice.pytorch.command;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.PyTorchRunJobParameters;
import org.apache.hadoop.yarn.submarine.common.api.Role;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineLogs;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.HadoopEnvironmentSetup;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command.AbstractLaunchCommand;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command.LaunchScriptBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Launch command implementation for PyTorch components.
 */
public class PyTorchWorkerLaunchCommand extends AbstractLaunchCommand {
  private static final Logger LOG =
      LoggerFactory.getLogger(PyTorchWorkerLaunchCommand.class);
  private final Configuration yarnConfig;
  private final boolean distributed;
  private final int numberOfWorkers;
  private final String name;
  private final Role role;
  private final String launchCommand;

  public PyTorchWorkerLaunchCommand(HadoopEnvironmentSetup hadoopEnvSetup,
      Role role, Component component,
      PyTorchRunJobParameters parameters,
      Configuration yarnConfig) throws IOException {
    super(hadoopEnvSetup, component, parameters, role.getName());
    this.role = role;
    this.name = parameters.getName();
    this.distributed = parameters.isDistributed();
    this.numberOfWorkers = parameters.getNumWorkers();
    this.yarnConfig = yarnConfig;
    logReceivedParameters();

    this.launchCommand = parameters.getWorkerLaunchCmd();

    if (StringUtils.isEmpty(this.launchCommand)) {
      throw new IllegalArgumentException("LaunchCommand must not be null " +
          "or empty!");
    }
  }

  private void logReceivedParameters() {
    if (this.numberOfWorkers <= 0) {
      LOG.warn("Received number of workers: {}", this.numberOfWorkers);
    }
  }

  @Override
  public String generateLaunchScript() throws IOException {
    LaunchScriptBuilder builder = getBuilder();
    return builder
        .withLaunchCommand(createLaunchCommand())
        .build();
  }

  @Override
  public String createLaunchCommand() {
    if (SubmarineLogs.isVerbose()) {
      LOG.info("PyTorch Worker command =[" + launchCommand + "]");
    }
    return launchCommand + '\n';
  }
}
