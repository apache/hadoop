/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.yarn.submarine.runtimes.tony;

import com.linkedin.tony.Constants;
import com.linkedin.tony.TonyConfigurationKeys;
import com.linkedin.tony.util.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.TensorFlowRunJobParameters;
import org.apache.hadoop.yarn.submarine.common.resource.ResourceUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Utilities for Tony Runtime.
 */
public final class TonyUtils {
  private static final Log LOG = LogFactory.getLog(TonyUtils.class);

  public static Configuration tonyConfFromClientContext(
      TensorFlowRunJobParameters parameters) {
    Configuration tonyConf = new Configuration();
    tonyConf.setInt(
        TonyConfigurationKeys.getInstancesKey(Constants.WORKER_JOB_NAME),
        parameters.getNumWorkers());
    tonyConf.setInt(
        TonyConfigurationKeys.getInstancesKey(Constants.PS_JOB_NAME),
        parameters.getNumPS());
    // Resources for PS & Worker
    if (parameters.getPsResource() != null) {
      tonyConf.setInt(
          TonyConfigurationKeys.getResourceKey(Constants.PS_JOB_NAME,
              Constants.VCORES),
          parameters.getPsResource().getVirtualCores());
      tonyConf.setLong(
          TonyConfigurationKeys.getResourceKey(Constants.PS_JOB_NAME,
              Constants.MEMORY),
          ResourceUtils.getMemorySize(parameters.getPsResource()));
    }
    if (parameters.getWorkerResource() != null) {
      tonyConf.setInt(
          TonyConfigurationKeys.getResourceKey(Constants.WORKER_JOB_NAME,
              Constants.VCORES),
          parameters.getWorkerResource().getVirtualCores());
      tonyConf.setLong(
          TonyConfigurationKeys.getResourceKey(Constants.WORKER_JOB_NAME,
              Constants.MEMORY),
          ResourceUtils.getMemorySize(parameters.getWorkerResource()));
      tonyConf.setLong(
          TonyConfigurationKeys.getResourceKey(Constants.WORKER_JOB_NAME,
              Constants.GPUS),
          ResourceUtils.getResourceValue(parameters.getWorkerResource(),
              ResourceUtils.GPU_URI));
    }
    if (parameters.getQueue() != null) {
      tonyConf.set(
          TonyConfigurationKeys.YARN_QUEUE_NAME,
          parameters.getQueue());
    }
    // Set up Docker for PS & Worker
    if (parameters.getDockerImageName() != null) {
      tonyConf.set(TonyConfigurationKeys.getContainerDockerKey(),
          parameters.getDockerImageName());
      tonyConf.setBoolean(TonyConfigurationKeys.DOCKER_ENABLED, true);
    }
    if (parameters.getWorkerDockerImage() != null) {
      tonyConf.set(
          TonyConfigurationKeys.getDockerImageKey(Constants.WORKER_JOB_NAME),
          parameters.getWorkerDockerImage());
      tonyConf.setBoolean(TonyConfigurationKeys.DOCKER_ENABLED, true);
    }
    if (parameters.getPsDockerImage() != null) {
      tonyConf.set(
          TonyConfigurationKeys.getDockerImageKey(Constants.PS_JOB_NAME),
          parameters.getPsDockerImage());
      tonyConf.setBoolean(TonyConfigurationKeys.DOCKER_ENABLED, true);
    }

    // Set up container environment
    List<String> envs = parameters.getEnvars();
    tonyConf.setStrings(
        TonyConfigurationKeys.CONTAINER_LAUNCH_ENV,
        envs.toArray(new String[0]));
    tonyConf.setStrings(TonyConfigurationKeys.EXECUTION_ENV,
        envs.stream()
            .map(env -> env.replaceAll("DOCKER_", ""))
            .toArray(String[]::new));
    tonyConf.setStrings(TonyConfigurationKeys.CONTAINER_LAUNCH_ENV,
        envs.stream().map(env -> env.replaceAll("DOCKER_", ""))
            .toArray(String[]::new));

    // Set up running command
    if (parameters.getWorkerLaunchCmd() != null) {
      tonyConf.set(
          TonyConfigurationKeys.getExecuteCommandKey(Constants.WORKER_JOB_NAME),
          parameters.getWorkerLaunchCmd());
    }

    if (parameters.getPSLaunchCmd() != null) {
      tonyConf.set(
          TonyConfigurationKeys.getExecuteCommandKey(Constants.PS_JOB_NAME),
          parameters.getPSLaunchCmd());
    }

    tonyConf.setBoolean(TonyConfigurationKeys.SECURITY_ENABLED,
        !parameters.isSecurityDisabled());

    // Set up container resources
    if (parameters.getLocalizations() != null) {
      tonyConf.setStrings(TonyConfigurationKeys.getContainerResourcesKey(),
          parameters.getLocalizations().stream()
              .map(lo -> lo.getRemoteUri() + Constants.RESOURCE_DIVIDER
                  + lo.getLocalPath())
              .toArray(String[]::new));
    }

    if (parameters.getConfPairs() != null) {
      String[] confArray = parameters.getConfPairs().toArray(new String[0]);
      for (Map.Entry<String, String> cliConf : Utils
          .parseKeyValue(confArray).entrySet()) {
        String[] existingValue = tonyConf.getStrings(cliConf.getKey());
        if (existingValue != null
            && TonyConfigurationKeys
            .MULTI_VALUE_CONF.contains(cliConf.getKey())) {
          ArrayList<String> newValues = new ArrayList<>(Arrays
              .asList(existingValue));
          newValues.add(cliConf.getValue());
          tonyConf.setStrings(cliConf.getKey(),
                newValues.toArray(new String[0]));
        } else {
          tonyConf.set(cliConf.getKey(), cliConf.getValue());
        }
      }
    }

    LOG.info("Resources: " + tonyConf.get(
        TonyConfigurationKeys.getContainerResourcesKey()));
    return tonyConf;
  }

  private TonyUtils() {
  }
}
