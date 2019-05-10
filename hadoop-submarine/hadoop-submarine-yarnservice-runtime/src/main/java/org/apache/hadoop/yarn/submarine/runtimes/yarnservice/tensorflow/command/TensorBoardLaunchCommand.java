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
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.RunJobParameters;
import org.apache.hadoop.yarn.submarine.common.api.Role;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.HadoopEnvironmentSetup;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command.AbstractLaunchCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * Launch command implementation for Tensorboard.
 */
public class TensorBoardLaunchCommand extends AbstractLaunchCommand {
  private static final Logger LOG =
      LoggerFactory.getLogger(TensorBoardLaunchCommand.class);
  private final String checkpointPath;

  public TensorBoardLaunchCommand(HadoopEnvironmentSetup hadoopEnvSetup,
      Role role, Component component, RunJobParameters parameters)
      throws IOException {
    super(hadoopEnvSetup, component, parameters, role.getName());
    Objects.requireNonNull(parameters.getCheckpointPath(),
        "CheckpointPath must not be null as it is part "
            + "of the tensorboard command!");
    if (StringUtils.isEmpty(parameters.getCheckpointPath())) {
      throw new IllegalArgumentException("CheckpointPath must not be empty!");
    }

    this.checkpointPath = parameters.getCheckpointPath();
  }

  @Override
  public String generateLaunchScript() throws IOException {
    return getBuilder()
        .withLaunchCommand(createLaunchCommand())
        .build();
  }

  @Override
  public String createLaunchCommand() {
    String tbCommand = String.format("export LC_ALL=C && tensorboard " +
        "--logdir=%s%n", checkpointPath);
    LOG.info("Tensorboard command=" + tbCommand);
    return tbCommand;
  }
}
