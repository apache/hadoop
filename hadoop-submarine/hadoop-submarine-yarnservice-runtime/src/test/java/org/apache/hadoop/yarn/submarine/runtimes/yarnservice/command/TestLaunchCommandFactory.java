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
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * This class is to test the {@link LaunchCommandFactory}.
 */
public class TestLaunchCommandFactory {

  private LaunchCommandFactory createLaunchCommandFactory(
      RunJobParameters parameters) {
    HadoopEnvironmentSetup hadoopEnvSetup = mock(HadoopEnvironmentSetup.class);
    Configuration configuration = mock(Configuration.class);
    return new LaunchCommandFactory(hadoopEnvSetup, parameters, configuration);
  }

  @Test
  public void createLaunchCommandWorkerAndPrimaryWorker() throws IOException {
    RunJobParameters parameters = new RunJobParameters();
    parameters.setWorkerLaunchCmd("testWorkerLaunchCommand");
    LaunchCommandFactory launchCommandFactory = createLaunchCommandFactory(
        parameters);
    Component mockComponent = mock(Component.class);

    AbstractLaunchCommand launchCommand =
        launchCommandFactory.createLaunchCommand(TaskType.PRIMARY_WORKER,
            mockComponent);

    assertTrue(launchCommand instanceof TensorFlowWorkerLaunchCommand);

    launchCommand =
        launchCommandFactory.createLaunchCommand(TaskType.WORKER,
            mockComponent);
    assertTrue(launchCommand instanceof TensorFlowWorkerLaunchCommand);

  }

  @Test
  public void createLaunchCommandPs() throws IOException {
    RunJobParameters parameters = new RunJobParameters();
    parameters.setPSLaunchCmd("testPSLaunchCommand");
    LaunchCommandFactory launchCommandFactory = createLaunchCommandFactory(
        parameters);
    Component mockComponent = mock(Component.class);

    AbstractLaunchCommand launchCommand =
        launchCommandFactory.createLaunchCommand(TaskType.PS,
            mockComponent);

    assertTrue(launchCommand instanceof TensorFlowPsLaunchCommand);
  }

  @Test
  public void createLaunchCommandTensorboard() throws IOException {
    RunJobParameters parameters = new RunJobParameters();
    parameters.setCheckpointPath("testCheckpointPath");
    LaunchCommandFactory launchCommandFactory =
        createLaunchCommandFactory(parameters);
    Component mockComponent = mock(Component.class);

    AbstractLaunchCommand launchCommand =
        launchCommandFactory.createLaunchCommand(TaskType.TENSORBOARD,
            mockComponent);

    assertTrue(launchCommand instanceof TensorBoardLaunchCommand);
  }

}