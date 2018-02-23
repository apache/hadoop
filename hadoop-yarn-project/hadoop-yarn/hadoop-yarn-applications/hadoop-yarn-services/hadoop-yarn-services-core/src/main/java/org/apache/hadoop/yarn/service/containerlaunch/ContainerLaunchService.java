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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.service.ServiceContext;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;
import org.apache.hadoop.yarn.service.provider.ProviderService;
import org.apache.hadoop.yarn.service.provider.ProviderFactory;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.utils.SliderFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

  public void launchCompInstance(Service service,
      ComponentInstance instance, Container container) {
    ContainerLauncher launcher =
        new ContainerLauncher(service, instance, container);
    executorService.execute(launcher);
  }

  private class ContainerLauncher implements Runnable {
    public final Container container;
    public final Service service;
    public ComponentInstance instance;

    public ContainerLauncher(
        Service service,
        ComponentInstance instance, Container container) {
      this.container = container;
      this.service = service;
      this.instance = instance;
    }

    @Override public void run() {
      Component compSpec = instance.getCompSpec();
      ProviderService provider = ProviderFactory.getProviderService(
          compSpec.getArtifact());
      AbstractLauncher launcher = new AbstractLauncher(context);
      try {
        provider.buildContainerLaunchContext(launcher, service,
            instance, fs, getConfig(), container);
        instance.getComponent().getScheduler().getNmClient()
            .startContainerAsync(container,
                launcher.completeContainerLaunch());
      } catch (Exception e) {
        LOG.error(instance.getCompInstanceId()
            + ": Failed to launch container. ", e);

      }
    }
  }
}
