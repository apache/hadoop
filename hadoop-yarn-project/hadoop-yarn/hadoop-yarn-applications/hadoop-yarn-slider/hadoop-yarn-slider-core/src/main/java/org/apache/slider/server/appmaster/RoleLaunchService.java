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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.slider.api.resource.Application;
import org.apache.slider.api.resource.Component;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.core.launch.ContainerLauncher;
import org.apache.hadoop.yarn.service.provider.ProviderService;
import org.apache.hadoop.yarn.service.provider.ProviderFactory;
import org.apache.slider.server.appmaster.actions.QueueAccess;
import org.apache.hadoop.yarn.service.compinstance.ComponentInstance;
import org.apache.slider.server.appmaster.state.ContainerAssignment;
import org.apache.slider.server.services.workflow.ServiceThreadFactory;
import org.apache.slider.server.services.workflow.WorkflowExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.hadoop.yarn.service.conf.SliderKeys.KEY_CONTAINER_LAUNCH_DELAY;

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
  private  QueueAccess actionQueue;

  /**
   * Filesystem to use for the launch
   */
  private  SliderFileSystem fs;


  private Map<String, String> envVars = new HashMap<>();

  /**
   * Construct an instance of the launcher
   * @param queueAccess
   * @param fs filesystem
   * @param envVars environment variables
   */
  public RoleLaunchService(QueueAccess queueAccess, SliderFileSystem fs,
      Map<String, String> envVars) {
    super(ROLE_LAUNCH_SERVICE);
    this.actionQueue = queueAccess;
    this.fs = fs;
    this.envVars = envVars;
  }

  public RoleLaunchService(SliderFileSystem fs) {
    super(ROLE_LAUNCH_SERVICE);
    this.fs = fs;
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
   * @param credentials credentials to use
   */
  public void launchRole(ContainerAssignment assignment,
      Application application, Credentials credentials) {
  }

  public void launchComponent(Application application,
      ComponentInstance instance, Container container) {
    RoleLaunchService.RoleLauncher launcher =
        new RoleLaunchService.RoleLauncher(application, instance,
            container);
    execute(launcher);
  }

  /**
   * Thread that runs on the AM to launch a container
   */
  private class RoleLauncher implements Runnable {
    // Allocated container
    public final Container container;
    public final Application application;
    public ComponentInstance instance;

    public RoleLauncher(
        Application application,
        ComponentInstance instance, Container container) {
      this.container = container;
      this.application = application;
      this.instance = instance;
    }

    @Override
    public void run() {
      try {
        ContainerLauncher containerLauncher =
            new ContainerLauncher(null, fs, container, null);
        containerLauncher.putEnv(envVars);

        Component compSpec = instance.getCompSpec();
        ProviderService provider = ProviderFactory.getProviderService(
            compSpec.getArtifact());
        provider.buildContainerLaunchContext(containerLauncher, application,
            instance, fs);

        long delay = compSpec.getConfiguration()
                .getPropertyLong(KEY_CONTAINER_LAUNCH_DELAY, 0);
        long maxDelay = getConfig()
            .getLong(YarnConfiguration.RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS,
                YarnConfiguration.DEFAULT_RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS);
        if (delay > maxDelay/1000) {
          log.warn("Container launch delay of {} exceeds the maximum allowed of"
                   + " {} seconds.  Delay will not be utilized.",
                   delay, maxDelay/1000);
          delay = 0;
        }
        if (delay > 0) {
          Thread.sleep(delay * 1000);
        }
        instance.getComponent().getScheduler().getNmClient()
            .startContainerAsync(container,
                containerLauncher.completeContainerLaunch());
      } catch (Exception e) {
        log.error("Exception thrown while trying to start " + instance
            .getCompInstanceName()
            + " container = " + container.getId() + " on host " + container
            .getNodeId(), e);
      }
    }
  }
}
