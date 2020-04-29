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
package org.apache.hadoop.yarn.service.provider;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.component.ComponentRestartPolicy;
import org.apache.hadoop.yarn.service.conf.YarnServiceConf;
import org.apache.hadoop.yarn.service.conf.YarnServiceConstants;
import org.apache.hadoop.yarn.service.containerlaunch.ContainerLaunchService;
import org.apache.hadoop.yarn.service.utils.SliderFileSystem;
import org.apache.hadoop.yarn.service.utils.ServiceUtils;
import org.apache.hadoop.yarn.service.exceptions.SliderException;
import org.apache.hadoop.yarn.service.containerlaunch.AbstractLauncher;
import org.apache.hadoop.yarn.service.containerlaunch.CommandLineBuilder;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;
import org.apache.hadoop.yarn.service.ServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.hadoop.yarn.service.conf.YarnServiceConf.CONTAINER_FAILURES_VALIDITY_INTERVAL;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConf.CONTAINER_RETRY_INTERVAL;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConf.CONTAINER_RETRY_MAX;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConf.DEFAULT_CONTAINER_FAILURES_VALIDITY_INTERVAL;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConf.DEFAULT_CONTAINER_RETRY_INTERVAL;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConf.DEFAULT_CONTAINER_RETRY_MAX;
import static org.apache.hadoop.yarn.service.utils.ServiceApiUtil.$;

public abstract class AbstractProviderService implements ProviderService,
    YarnServiceConstants {

  protected static final Logger log =
      LoggerFactory.getLogger(AbstractProviderService.class);

  public abstract void processArtifact(AbstractLauncher launcher,
      ComponentInstance compInstance, SliderFileSystem fileSystem,
      Service service,
      ContainerLaunchService.ComponentLaunchContext compLaunchCtx)
      throws IOException;

  public Map<String, String> buildContainerTokens(ComponentInstance instance,
      Container container,
      ContainerLaunchService.ComponentLaunchContext compLaunchContext) {
      // Generate tokens (key-value pair) for config substitution.
      // Get pre-defined tokens
      Map<String, String> globalTokens =
          instance.getComponent().getScheduler().globalTokens;
      Map<String, String> tokensForSubstitution = ProviderUtils
          .initCompTokensForSubstitute(instance, container,
              compLaunchContext);
      tokensForSubstitution.putAll(globalTokens);
      return tokensForSubstitution;
  }

  public void buildContainerEnvironment(AbstractLauncher launcher,
      Service service, ComponentInstance instance,
      SliderFileSystem fileSystem, Configuration yarnConf, Container container,
      ContainerLaunchService.ComponentLaunchContext compLaunchContext,
      Map<String, String> tokensForSubstitution)
          throws IOException, SliderException {
    // Set the environment variables in launcher
    launcher.putEnv(ServiceUtils.buildEnvMap(
        compLaunchContext.getConfiguration(), tokensForSubstitution));
    launcher.setEnv("WORK_DIR", ApplicationConstants.Environment.PWD.$());
    launcher.setEnv("LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    if (System.getenv(HADOOP_USER_NAME) != null) {
      launcher.setEnv(HADOOP_USER_NAME, System.getenv(HADOOP_USER_NAME));
    }
    launcher.setEnv("LANG", "en_US.UTF-8");
    launcher.setEnv("LC_ALL", "en_US.UTF-8");
    launcher.setEnv("LANGUAGE", "en_US.UTF-8");

    for (Entry<String, String> entry : launcher.getEnv().entrySet()) {
      tokensForSubstitution.put($(entry.getKey()), entry.getValue());
    }
  }

  public void buildContainerLaunchCommand(AbstractLauncher launcher,
      Service service, ComponentInstance instance,
      SliderFileSystem fileSystem, Configuration yarnConf, Container container,
      ContainerLaunchService.ComponentLaunchContext compLaunchContext,
      Map<String, String> tokensForSubstitution)
          throws IOException, SliderException {
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

  public void buildContainerRetry(AbstractLauncher launcher,
      Configuration yarnConf,
      ContainerLaunchService.ComponentLaunchContext compLaunchContext,
      ComponentInstance instance) {
    // By default retry forever every 30 seconds

    ComponentRestartPolicy restartPolicy = instance.getComponent()
        .getRestartPolicyHandler();
    if (restartPolicy.allowContainerRetriesForInstance(instance)) {
      launcher.setRetryContext(YarnServiceConf
          .getInt(CONTAINER_RETRY_MAX, DEFAULT_CONTAINER_RETRY_MAX,
              compLaunchContext.getConfiguration(), yarnConf), YarnServiceConf
          .getInt(CONTAINER_RETRY_INTERVAL, DEFAULT_CONTAINER_RETRY_INTERVAL,
              compLaunchContext.getConfiguration(), yarnConf), YarnServiceConf
          .getLong(CONTAINER_FAILURES_VALIDITY_INTERVAL,
              DEFAULT_CONTAINER_FAILURES_VALIDITY_INTERVAL,
              compLaunchContext.getConfiguration(), yarnConf));
    }
  }

  public ResolvedLaunchParams buildContainerLaunchContext(
      AbstractLauncher launcher,
      Service service, ComponentInstance instance,
      SliderFileSystem fileSystem, Configuration yarnConf, Container container,
      ContainerLaunchService.ComponentLaunchContext compLaunchContext)
      throws IOException, SliderException {
    ResolvedLaunchParams resolved = new ResolvedLaunchParams();
    processArtifact(launcher, instance, fileSystem, service, compLaunchContext);

    ServiceContext context =
        instance.getComponent().getScheduler().getContext();
    // Generate tokens (key-value pair) for config substitution.
    Map<String, String> tokensForSubstitution =
        buildContainerTokens(instance, container, compLaunchContext);

    // Setup launch context environment
    buildContainerEnvironment(launcher, service, instance,
        fileSystem, yarnConf, container, compLaunchContext,
        tokensForSubstitution);

    // create config file on hdfs and addResolvedRsrcPath local resource
    ProviderUtils.createConfigFileAndAddLocalResource(launcher, fileSystem,
        compLaunchContext, tokensForSubstitution, instance, context, resolved);

    // handles static files (like normal file / archive file) for localization.
    ProviderUtils.handleStaticFilesForLocalization(launcher, fileSystem,
        compLaunchContext, resolved);

    // replace launch command with token specific information
    buildContainerLaunchCommand(launcher, service, instance, fileSystem,
        yarnConf, container, compLaunchContext, tokensForSubstitution);

    // Setup container retry settings
    buildContainerRetry(launcher, yarnConf, compLaunchContext, instance);

    return resolved;
  }
}
