/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;

import java.util.Map;

/**
 * This class is a {@link ContainerRuntime} implementation that delegates all
 * operations to either a {@link DefaultLinuxContainerRuntime} instance or a
 * {@link DockerLinuxContainerRuntime} instance, depending on whether the
 * {@link DockerLinuxContainerRuntime} instance believes the operation to be
 * requesting a Docker container.
 *
 * @see DockerLinuxContainerRuntime#isDockerContainerRequested
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DelegatingLinuxContainerRuntime implements LinuxContainerRuntime {
  private static final Log LOG = LogFactory
      .getLog(DelegatingLinuxContainerRuntime.class);
  private DefaultLinuxContainerRuntime defaultLinuxContainerRuntime;
  private DockerLinuxContainerRuntime dockerLinuxContainerRuntime;

  @Override
  public void initialize(Configuration conf)
      throws ContainerExecutionException {
    PrivilegedOperationExecutor privilegedOperationExecutor =
        PrivilegedOperationExecutor.getInstance(conf);
    defaultLinuxContainerRuntime = new DefaultLinuxContainerRuntime(
        privilegedOperationExecutor);
    defaultLinuxContainerRuntime.initialize(conf);
    dockerLinuxContainerRuntime = new DockerLinuxContainerRuntime(
        privilegedOperationExecutor);
    dockerLinuxContainerRuntime.initialize(conf);
  }

  private LinuxContainerRuntime pickContainerRuntime(Container container) {
    Map<String, String> env = container.getLaunchContext().getEnvironment();
    LinuxContainerRuntime runtime;

    if (DockerLinuxContainerRuntime.isDockerContainerRequested(env)){
      runtime = dockerLinuxContainerRuntime;
    } else  {
      runtime = defaultLinuxContainerRuntime;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Using container runtime: " + runtime.getClass()
          .getSimpleName());
    }

    return runtime;
  }

  @Override
  public void prepareContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    LinuxContainerRuntime runtime = pickContainerRuntime(container);

    runtime.prepareContainer(ctx);
  }

  @Override
  public void launchContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    LinuxContainerRuntime runtime = pickContainerRuntime(container);

    runtime.launchContainer(ctx);
  }

  @Override
  public void signalContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    LinuxContainerRuntime runtime = pickContainerRuntime(container);

    runtime.signalContainer(ctx);
  }

  @Override
  public void reapContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    LinuxContainerRuntime runtime = pickContainerRuntime(container);

    runtime.reapContainer(ctx);
  }

  @Override
  public String[] getIpAndHost(Container container) {
    LinuxContainerRuntime runtime = pickContainerRuntime(container);
    return runtime.getIpAndHost(container);
  }
}