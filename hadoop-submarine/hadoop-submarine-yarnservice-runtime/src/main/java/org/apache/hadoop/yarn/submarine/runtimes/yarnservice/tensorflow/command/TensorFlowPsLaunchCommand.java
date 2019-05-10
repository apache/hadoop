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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.TensorFlowRunJobParameters;
import org.apache.hadoop.yarn.submarine.common.api.Role;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineLogs;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.HadoopEnvironmentSetup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Launch command implementation for Tensorboard's PS component.
 */
public class TensorFlowPsLaunchCommand extends TensorFlowLaunchCommand {
  private static final Logger LOG =
      LoggerFactory.getLogger(TensorFlowPsLaunchCommand.class);
  private final String launchCommand;

  public TensorFlowPsLaunchCommand(HadoopEnvironmentSetup hadoopEnvSetup,
      Role role, Component component,
      TensorFlowRunJobParameters parameters,
      Configuration yarnConfig) throws IOException {
    super(hadoopEnvSetup, role, component, parameters, yarnConfig);
    this.launchCommand = parameters.getPSLaunchCmd();

    if (StringUtils.isEmpty(this.launchCommand)) {
      throw new IllegalArgumentException("LaunchCommand must not be null " +
          "or empty!");
    }
  }

  @Override
  public String createLaunchCommand() {
    if (SubmarineLogs.isVerbose()) {
      LOG.info("PS command =[" + launchCommand + "]");
    }
    return launchCommand + '\n';
  }
}
