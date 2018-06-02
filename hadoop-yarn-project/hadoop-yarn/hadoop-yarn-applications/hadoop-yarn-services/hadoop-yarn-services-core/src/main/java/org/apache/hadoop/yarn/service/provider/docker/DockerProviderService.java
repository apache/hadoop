/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.service.provider.docker;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;
import org.apache.hadoop.yarn.service.provider.AbstractProviderService;
import org.apache.hadoop.yarn.service.provider.ProviderUtils;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.utils.SliderFileSystem;
import org.apache.hadoop.yarn.service.containerlaunch.AbstractLauncher;
import org.apache.hadoop.yarn.service.containerlaunch.CommandLineBuilder;
import org.apache.hadoop.yarn.service.containerlaunch.ContainerLaunchService;
import org.apache.hadoop.yarn.service.exceptions.SliderException;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;

import java.io.IOException;
import java.util.Map;

public class DockerProviderService extends AbstractProviderService
    implements DockerKeys {

  public void processArtifact(AbstractLauncher launcher,
      ComponentInstance compInstance, SliderFileSystem fileSystem,
      Service service) throws IOException{
    launcher.setYarnDockerMode(true);
    launcher.setDockerImage(compInstance.getCompSpec().getArtifact().getId());
    launcher.setDockerNetwork(compInstance.getCompSpec().getConfiguration()
        .getProperty(DOCKER_NETWORK));
    launcher.setDockerHostname(compInstance.getHostname());
    launcher.setRunPrivilegedContainer(
        compInstance.getCompSpec().getRunPrivilegedContainer());
  }

  /**
   * Check if system is default to disable docker override or
   * user requested a Docker container with ENTRY_POINT support.
   *
   * @param component - YARN Service component
   * @return true if Docker launch command override is disabled
   */
  private boolean checkUseEntryPoint(Component component) {
    boolean overrideDisable = false;
    String overrideDisableKey = Environment.
        YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE.
            name();
    String overrideDisableValue = (component
        .getConfiguration().getEnv(overrideDisableKey) != null) ?
            component.getConfiguration().getEnv(overrideDisableKey) :
                System.getenv(overrideDisableKey);
    overrideDisable = Boolean.parseBoolean(overrideDisableValue);
    return overrideDisable;
  }

  @Override
  public void buildContainerLaunchCommand(AbstractLauncher launcher,
      Service service, ComponentInstance instance,
      SliderFileSystem fileSystem, Configuration yarnConf, Container container,
      ContainerLaunchService.ComponentLaunchContext compLaunchContext,
      Map<String, String> tokensForSubstitution)
          throws IOException, SliderException {
    Component component = instance.getComponent().getComponentSpec();
    boolean useEntryPoint = checkUseEntryPoint(component);
    if (useEntryPoint) {
      String launchCommand = component.getLaunchCommand();
      if (!StringUtils.isEmpty(launchCommand)) {
        launcher.addCommand(launchCommand);
      }
    } else {
      // substitute launch command
      String launchCommand = compLaunchContext.getLaunchCommand();
      // docker container may have empty commands
      if (!StringUtils.isEmpty(launchCommand)) {
        launchCommand = ProviderUtils
            .substituteStrWithTokens(launchCommand, tokensForSubstitution);
        CommandLineBuilder operation = new CommandLineBuilder();
        operation.add(launchCommand);
        operation.addOutAndErrFiles(OUT_FILE, ERR_FILE);
        launcher.addCommand(operation.build());
      }
    }
  }

}
