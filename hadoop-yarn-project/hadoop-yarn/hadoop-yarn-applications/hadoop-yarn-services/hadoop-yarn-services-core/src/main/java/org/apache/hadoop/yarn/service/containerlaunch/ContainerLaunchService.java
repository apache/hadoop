/**
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

package org.apache.hadoop.yarn.service.containerlaunch;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.service.ServiceContext;
import org.apache.hadoop.yarn.service.api.records.Artifact;
import org.apache.hadoop.yarn.service.component.ComponentEvent;
import org.apache.hadoop.yarn.service.component.ComponentEventType;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;
import org.apache.hadoop.yarn.service.provider.ProviderService;
import org.apache.hadoop.yarn.service.provider.ProviderFactory;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.utils.SliderFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.hadoop.yarn.service.provider.ProviderService.FAILED_LAUNCH_PARAMS;

public class ContainerLaunchService extends AbstractService{

  protected static final Logger LOG =
      LoggerFactory.getLogger(ContainerLaunchService.class);

  private ExecutorService executorService;
  private SliderFileSystem fs;
  private ServiceContext context;
  public ContainerLaunchService(ServiceContext context) {
    super(ContainerLaunchService.class.getName());
    this.fs = context.fs;
    this.context = context;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    executorService = Executors.newCachedThreadPool();
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStop() throws Exception {
    if (executorService != null) {
      executorService.shutdownNow();
    }
    super.serviceStop();
  }

  public Future<ProviderService.ResolvedLaunchParams> launchCompInstance(
      Service service,
      ComponentInstance instance, Container container,
      ComponentLaunchContext componentLaunchContext) {
    ContainerLauncher launcher =
        new ContainerLauncher(service, instance, container,
            componentLaunchContext, false);
    return executorService.submit(launcher);
  }

  public Future<ProviderService.ResolvedLaunchParams> reInitCompInstance(
      Service service,
      ComponentInstance instance, Container container,
      ComponentLaunchContext componentLaunchContext) {
    ContainerLauncher reInitializer = new ContainerLauncher(service, instance,
        container, componentLaunchContext, true);
    return executorService.submit(reInitializer);
  }

  private class ContainerLauncher implements
      Callable<ProviderService.ResolvedLaunchParams> {
    public final Container container;
    public final Service service;
    public ComponentInstance instance;
    private final ComponentLaunchContext componentLaunchContext;
    private final boolean reInit;

    ContainerLauncher(Service service, ComponentInstance instance,
        Container container, ComponentLaunchContext componentLaunchContext,
        boolean reInit) {
      this.container = container;
      this.service = service;
      this.instance = instance;
      this.componentLaunchContext = componentLaunchContext;
      this.reInit = reInit;
    }

    @Override
    public ProviderService.ResolvedLaunchParams call() {
      ProviderService provider = ProviderFactory.getProviderService(
          componentLaunchContext.getArtifact());
      AbstractLauncher launcher = new AbstractLauncher(context);
      ProviderService.ResolvedLaunchParams resolvedParams = null;
      try {
        resolvedParams = provider.buildContainerLaunchContext(launcher, service,
            instance, fs, getConfig(), container, componentLaunchContext);
        if (!reInit) {
          LOG.info("launching container {}", container.getId());
          instance.getComponent().getScheduler().getNmClient()
              .startContainerAsync(container,
                  launcher.completeContainerLaunch());
        } else {
          LOG.info("reInitializing container {} with version {}",
              container.getId(), componentLaunchContext.getServiceVersion());
          instance.getComponent().getScheduler().getNmClient()
              .reInitializeContainerAsync(container.getId(),
                  launcher.completeContainerLaunch(), true);
        }
      } catch (Exception e) {
        LOG.error("{}: Failed to launch container.",
            instance.getCompInstanceId(), e);
        ComponentEvent event = new ComponentEvent(instance.getCompName(),
            ComponentEventType.CONTAINER_COMPLETED)
            .setInstance(instance).setContainerId(container.getId());
        context.scheduler.getDispatcher().getEventHandler().handle(event);
      }
      if (resolvedParams != null) {
        return resolvedParams;
      } else {
        return FAILED_LAUNCH_PARAMS;
      }
    }
  }

  /**
   * Launch context of a component.
   */
  public static class ComponentLaunchContext {
    private final String name;
    private final String serviceVersion;
    private Artifact artifact;
    private org.apache.hadoop.yarn.service.api.records.Configuration
        configuration;
    private String launchCommand;
    private boolean runPrivilegedContainer;

    public ComponentLaunchContext(String name, String serviceVersion) {
      this.name = Preconditions.checkNotNull(name);
      this.serviceVersion = Preconditions.checkNotNull(serviceVersion);
    }

    public String getName() {
      return name;
    }

    public String getServiceVersion() {
      return serviceVersion;
    }

    public Artifact getArtifact() {
      return artifact;
    }

    public org.apache.hadoop.yarn.service.api.records.
        Configuration getConfiguration() {
      return configuration;
    }

    public String getLaunchCommand() {
      return launchCommand;
    }

    public boolean isRunPrivilegedContainer() {
      return runPrivilegedContainer;
    }

    public ComponentLaunchContext setArtifact(Artifact artifact) {
      this.artifact = artifact;
      return this;
    }

    public ComponentLaunchContext setConfiguration(org.apache.hadoop.yarn.
        service.api.records.Configuration configuration) {
      this.configuration = configuration;
      return this;
    }

    public ComponentLaunchContext setLaunchCommand(String launchCommand) {
      this.launchCommand = launchCommand;
      return this;
    }

    public ComponentLaunchContext setRunPrivilegedContainer(
        boolean runPrivilegedContainer) {
      this.runPrivilegedContainer = runPrivilegedContainer;
      return this;
    }
  }
}
