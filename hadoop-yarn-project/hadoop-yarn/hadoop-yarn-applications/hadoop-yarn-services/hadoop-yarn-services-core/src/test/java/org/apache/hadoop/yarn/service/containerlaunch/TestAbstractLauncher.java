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

package org.apache.hadoop.yarn.service.containerlaunch;

import org.apache.hadoop.yarn.service.ServiceContext;
import org.apache.hadoop.yarn.service.api.records.Configuration;
import org.apache.hadoop.yarn.service.component.AlwaysRestartPolicy;
import org.apache.hadoop.yarn.service.component.Component;
import org.apache.hadoop.yarn.service.component.NeverRestartPolicy;
import org.apache.hadoop.yarn.service.component.OnFailureRestartPolicy;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;
import org.apache.hadoop.yarn.service.provider.defaultImpl
    .DefaultProviderService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.fi.FiConfig.getConfig;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConf
    .DEFAULT_CONTAINER_FAILURES_VALIDITY_INTERVAL;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConf
    .DEFAULT_CONTAINER_RETRY_INTERVAL;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConf
    .DEFAULT_CONTAINER_RETRY_MAX;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link AbstractLauncher}.
 */
public class TestAbstractLauncher {

  private AbstractLauncher launcher;

  @Before
  public void setup() {
    launcher = new AbstractLauncher(mock(ServiceContext.class));
  }

  @Test
  public void testDockerContainerMounts() throws IOException {
    launcher.yarnDockerMode = true;
    launcher.envVars.put(AbstractLauncher.ENV_DOCKER_CONTAINER_MOUNTS,
        "s1:t1:ro");
    launcher.mountPaths.put("s2", "t2");
    launcher.completeContainerLaunch();
    String dockerContainerMounts = launcher.containerLaunchContext
        .getEnvironment().get(AbstractLauncher.ENV_DOCKER_CONTAINER_MOUNTS);

    Assert.assertEquals("s1:t1:ro,s2:t2:ro", dockerContainerMounts);
  }

  @Test
  public void testContainerRetries() throws Exception {

    DefaultProviderService providerService = new DefaultProviderService();
    AbstractLauncher mockLauncher = mock(AbstractLauncher.class);
    ContainerLaunchService.ComponentLaunchContext componentLaunchContext =
        mock(ContainerLaunchService.ComponentLaunchContext.class);

    ComponentInstance componentInstance = mock(ComponentInstance.class);

    //Never Restart Policy
    Component component = mock(Component.class);
    when(componentInstance.getComponent()).thenReturn(component);

    when(component.getRestartPolicyHandler()).thenReturn(NeverRestartPolicy
        .getInstance());

    providerService.buildContainerRetry(mockLauncher, getConfig(),
        componentLaunchContext, componentInstance);
    verifyZeroInteractions(mockLauncher);


    //OnFailure restart policy
    when(component.getRestartPolicyHandler()).thenReturn(OnFailureRestartPolicy
        .getInstance());
    when(componentLaunchContext.getConfiguration()).thenReturn(new
        Configuration());
    providerService.buildContainerRetry(mockLauncher, getConfig(),
        componentLaunchContext, componentInstance);
    verify(mockLauncher).setRetryContext(DEFAULT_CONTAINER_RETRY_MAX,
        DEFAULT_CONTAINER_RETRY_INTERVAL,
        DEFAULT_CONTAINER_FAILURES_VALIDITY_INTERVAL);

    reset(mockLauncher);

    //Always restart policy
    when(component.getRestartPolicyHandler()).thenReturn(AlwaysRestartPolicy
        .getInstance());
    providerService.buildContainerRetry(mockLauncher, getConfig(),
        componentLaunchContext, componentInstance);

    verify(mockLauncher).setRetryContext(DEFAULT_CONTAINER_RETRY_MAX,
        DEFAULT_CONTAINER_RETRY_INTERVAL,
        DEFAULT_CONTAINER_FAILURES_VALIDITY_INTERVAL);
  }
}
