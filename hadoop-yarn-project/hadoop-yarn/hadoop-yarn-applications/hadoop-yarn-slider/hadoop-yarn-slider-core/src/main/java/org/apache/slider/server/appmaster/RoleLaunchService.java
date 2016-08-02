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

package org.apache.slider.server.appmaster;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.launch.ContainerLauncher;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.providers.ProviderService;
import org.apache.slider.providers.agent.AgentKeys;
import org.apache.slider.server.appmaster.actions.ActionStartContainer;
import org.apache.slider.server.appmaster.actions.QueueAccess;
import org.apache.slider.server.appmaster.state.ContainerAssignment;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.apache.slider.server.services.workflow.WorkflowExecutorService;
import org.apache.slider.server.services.workflow.ServiceThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A service for launching containers
 */
public class RoleLaunchService
    extends WorkflowExecutorService<ExecutorService> {
  protected static final Logger log =
    LoggerFactory.getLogger(RoleLaunchService.class);

  public static final String ROLE_LAUNCH_SERVICE = "RoleLaunchService";


  /**
   * Queue submission API
   */
  private final QueueAccess actionQueue;

  /**
   * Provider building up the command
   */
  private final ProviderService provider;
  
  /**
   * Filesystem to use for the launch
   */
  private final SliderFileSystem fs;

  /**
   * Path in the launch filesystem that refers to a configuration directory
   * -the interpretation of it is left to the Provider
   */
  private final Path generatedConfDirPath;
  /**
   * Path in the launch filesystem that refers to a temp directory
   * which will be cleaned up at (some) time in the future
   */
  private final Path launcherTmpDirPath;

  private Map<String, String> envVars;

  /**
   * Construct an instance of the launcher
   * @param queueAccess
   * @param provider the provider
   * @param fs filesystem
   * @param generatedConfDirPath path in the FS for the generated dir
   * @param envVars environment variables
   * @param launcherTmpDirPath path for a temporary data in the launch process
   */
  public RoleLaunchService(QueueAccess queueAccess,
      ProviderService provider,
      SliderFileSystem fs,
      Path generatedConfDirPath,
      Map<String, String> envVars,
      Path launcherTmpDirPath) {
    super(ROLE_LAUNCH_SERVICE);
    this.actionQueue = queueAccess;
    this.fs = fs;
    this.generatedConfDirPath = generatedConfDirPath;
    this.launcherTmpDirPath = launcherTmpDirPath;
    this.provider = provider;
    this.envVars = envVars;
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);
    setExecutor(Executors.newCachedThreadPool(
        new ServiceThreadFactory(ROLE_LAUNCH_SERVICE, true)));
  }

  /**
   * Start an asychronous launch operation
   * @param assignment container assignment
   * @param clusterSpec cluster spec to use for template
   * @param credentials credentials to use
   */
  public void launchRole(ContainerAssignment assignment,
      AggregateConf clusterSpec,
      Credentials credentials) {
    RoleStatus role = assignment.role;
    String roleName = role.getName();
    String roleGroup = role.getGroup();
    // prelaunch safety check
    Preconditions.checkArgument(provider.isSupportedRole(roleName));
    RoleLaunchService.RoleLauncher launcher =
      new RoleLaunchService.RoleLauncher(assignment,
         clusterSpec,
         clusterSpec.getResourceOperations().getOrAddComponent(roleGroup),
         clusterSpec.getAppConfOperations().getOrAddComponent(roleGroup),
         credentials);
    execute(launcher);
  }

  /**
   * Thread that runs on the AM to launch a container
   */
  private class RoleLauncher implements Runnable {

    private final ContainerAssignment assignment;
    // Allocated container
    public final Container container;
    private final MapOperations resourceComponent;
    private final MapOperations appComponent;
    private final AggregateConf instanceDefinition;
    public final ProviderRole role;
    private final Credentials credentials;
    private Exception raisedException;

    public RoleLauncher(ContainerAssignment assignment,
        AggregateConf instanceDefinition,
        MapOperations resourceComponent,
        MapOperations appComponent,
        Credentials credentials) {
      this.assignment = assignment;
      this.credentials = credentials;
      this.container = assignment.container;
      RoleStatus roleStatus = assignment.role;

      assert resourceComponent != null;
      assert appComponent != null;
      ProviderRole providerRole = roleStatus.getProviderRole();
      assert providerRole != null;
      this.role = providerRole;
      this.resourceComponent = resourceComponent;
      this.appComponent = appComponent;
      this.instanceDefinition = instanceDefinition;
    }

    public Exception getRaisedException() {
      return raisedException;
    }

    @Override
    public String toString() {
      return "RoleLauncher{" +
             "container=" + container.getId() +
             ", containerRole='" + role.name + '\'' +
             ", containerGroup='" + role.group + '\'' +
             '}';
    }

    @Override
    public void run() {
      try {
        ContainerLauncher containerLauncher =
            new ContainerLauncher(getConfig(), fs, container, credentials);
        containerLauncher.setupUGI();
        containerLauncher.putEnv(envVars);

        log.debug("Launching container {} into role {}",
                  container.getId(),
                  role.name);

        //now build up the configuration data
        Path containerTmpDirPath =
          new Path(launcherTmpDirPath, container.getId().toString());
        provider.buildContainerLaunchContext(containerLauncher,
            instanceDefinition,
            container,
            role,
            fs,
            generatedConfDirPath,
            resourceComponent,
            appComponent,
            containerTmpDirPath);

        RoleInstance instance = new RoleInstance(container);
        String[] envDescription = containerLauncher.dumpEnvToString();

        String commandsAsString = containerLauncher.getCommandsAsString();
        log.info("Starting container with command: {}",
                 commandsAsString);

        instance.command = commandsAsString;
        instance.role = role.name;
        instance.group = role.group;
        instance.roleId = role.id;
        instance.appVersion = instanceDefinition.getAppConfOperations()
            .getGlobalOptions().get(SliderKeys.APP_VERSION);
        instance.environment = envDescription;
        int delay = appComponent.getOptionInt(
            AgentKeys.KEY_CONTAINER_LAUNCH_DELAY, 0);
        int maxDelay =
            getConfig().getInt(YarnConfiguration.RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS,
                               YarnConfiguration.DEFAULT_RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS);
        if (delay > maxDelay/1000) {
          log.warn("Container launch delay of {} exceeds the maximum allowed of"
                   + " {} seconds.  Delay will not be utilized.",
                   delay, maxDelay/1000);
          delay = 0;
        }
        log.info("Container launch delay for {} set to {} seconds",
                 role.name, delay);
        actionQueue.schedule(new ActionStartContainer("starting " + role.name,
                                                      container,
                                                      containerLauncher.completeContainerLaunch(),
                                                      instance,
                                                      delay,
                                                      TimeUnit.SECONDS));
      } catch (Exception e) {
        log.error("Exception thrown while trying to start {}: {}",
            role.name, e, e);
        raisedException = e;
      }
    }

  }
}
