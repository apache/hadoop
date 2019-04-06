/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.hadoop.yarn.submarine.client.cli.param;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.submarine.client.cli.CliConstants;
import org.apache.hadoop.yarn.submarine.common.ClientContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Parameters required to run anything on cluster. Such as run job / serve model
 */
public abstract class RunParameters extends BaseParameters {
  private String savedModelPath;
  private String dockerImageName;
  private List<String> envars = new ArrayList<>();
  private String queue;

  @Override
  public void updateParameters(ParametersHolder parametersHolder,
      ClientContext clientContext) throws ParseException,
      IOException, YarnException {
    String savedModelPath = parametersHolder.getOptionValue(
        CliConstants.SAVED_MODEL_PATH);
    this.setSavedModelPath(savedModelPath);

    List<String> envVars = getEnvVars(parametersHolder);
    this.setEnvars(envVars);

    String queue = parametersHolder.getOptionValue(
        CliConstants.QUEUE);
    this.setQueue(queue);

    String dockerImage = parametersHolder.getOptionValue(
        CliConstants.DOCKER_IMAGE);
    this.setDockerImageName(dockerImage);

    super.updateParameters(parametersHolder, clientContext);
  }

  private List<String> getEnvVars(ParametersHolder parametersHolder)
      throws YarnException {
    List<String> result = new ArrayList<>();
    List<String> envVarsArray = parametersHolder.getOptionValues(
        CliConstants.ENV);
    if (envVarsArray != null) {
      result.addAll(envVarsArray);
    }
    return result;
  }

  public String getQueue() {
    return queue;
  }

  public RunParameters setQueue(String queue) {
    this.queue = queue;
    return this;
  }

  public String getDockerImageName() {
    return dockerImageName;
  }

  public RunParameters setDockerImageName(String dockerImageName) {
    this.dockerImageName = dockerImageName;
    return this;
  }


  public List<String> getEnvars() {
    return envars;
  }

  public RunParameters setEnvars(List<String> envars) {
    this.envars = envars;
    return this;
  }

  public String getSavedModelPath() {
    return savedModelPath;
  }

  public RunParameters setSavedModelPath(String savedModelPath) {
    this.savedModelPath = savedModelPath;
    return this;
  }
}
