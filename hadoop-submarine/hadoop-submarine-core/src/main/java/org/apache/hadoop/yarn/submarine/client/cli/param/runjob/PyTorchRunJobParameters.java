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

package org.apache.hadoop.yarn.submarine.client.cli.param.runjob;

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.submarine.client.cli.CliConstants;
import org.apache.hadoop.yarn.submarine.client.cli.CliUtils;
import org.apache.hadoop.yarn.submarine.client.cli.param.ParametersHolder;
import org.apache.hadoop.yarn.submarine.common.ClientContext;

import com.google.common.collect.Lists;

/**
 * Parameters for PyTorch job.
 */
public class PyTorchRunJobParameters extends RunJobParameters {

  private static final String CANNOT_BE_DEFINED_FOR_PYTORCH =
      "cannot be defined for PyTorch jobs!";

  @Override
  public void updateParameters(ParametersHolder parametersHolder,
      ClientContext clientContext)
      throws ParseException, IOException, YarnException {
    checkArguments(parametersHolder);

    super.updateParameters(parametersHolder, clientContext);

    String input = parametersHolder.getOptionValue(CliConstants.INPUT_PATH);
    this.workerParameters =
        getWorkerParameters(clientContext, parametersHolder, input);
    this.distributed = determineIfDistributed(workerParameters.getReplicas());
    executePostOperations(clientContext);
  }

  private void checkArguments(ParametersHolder parametersHolder)
      throws YarnException, ParseException {
    if (parametersHolder.getOptionValue(CliConstants.N_PS) != null) {
      throw new ParseException(getParamCannotBeDefinedErrorMessage(
          CliConstants.N_PS));
    } else if (parametersHolder.getOptionValue(CliConstants.PS_RES) != null) {
      throw new ParseException(getParamCannotBeDefinedErrorMessage(
          CliConstants.PS_RES));
    } else if (parametersHolder
        .getOptionValue(CliConstants.PS_DOCKER_IMAGE) != null) {
      throw new ParseException(getParamCannotBeDefinedErrorMessage(
          CliConstants.PS_DOCKER_IMAGE));
    } else if (parametersHolder
        .getOptionValue(CliConstants.PS_LAUNCH_CMD) != null) {
      throw new ParseException(getParamCannotBeDefinedErrorMessage(
          CliConstants.PS_LAUNCH_CMD));
    } else if (parametersHolder.hasOption(CliConstants.TENSORBOARD)) {
      throw new ParseException(getParamCannotBeDefinedErrorMessage(
          CliConstants.TENSORBOARD));
    } else if (parametersHolder
        .getOptionValue(CliConstants.TENSORBOARD_RESOURCES) != null) {
      throw new ParseException(getParamCannotBeDefinedErrorMessage(
          CliConstants.TENSORBOARD_RESOURCES));
    } else if (parametersHolder
        .getOptionValue(CliConstants.TENSORBOARD_DOCKER_IMAGE) != null) {
      throw new ParseException(getParamCannotBeDefinedErrorMessage(
          CliConstants.TENSORBOARD_DOCKER_IMAGE));
    }
  }

  private String getParamCannotBeDefinedErrorMessage(String cliName) {
    return String.format(
        "Parameter '%s' " + CANNOT_BE_DEFINED_FOR_PYTORCH, cliName);
  }

  @Override
  void executePostOperations(ClientContext clientContext) throws IOException {
    // Set default job dir / saved model dir, etc.
    setDefaultDirs(clientContext);
    replacePatternsInParameters(clientContext);
  }

  private void replacePatternsInParameters(ClientContext clientContext)
      throws IOException {
    if (StringUtils.isNotEmpty(getWorkerLaunchCmd())) {
      String afterReplace =
          CliUtils.replacePatternsInLaunchCommand(getWorkerLaunchCmd(), this,
              clientContext.getRemoteDirectoryManager());
      setWorkerLaunchCmd(afterReplace);
    }
  }

  @Override
  public List<String> getLaunchCommands() {
    return Lists.newArrayList(getWorkerLaunchCmd());
  }

  /**
   * We only support non-distributed PyTorch integration for now.
   * @param nWorkers
   * @return
   */
  private boolean determineIfDistributed(int nWorkers) {
    return false;
  }
}
