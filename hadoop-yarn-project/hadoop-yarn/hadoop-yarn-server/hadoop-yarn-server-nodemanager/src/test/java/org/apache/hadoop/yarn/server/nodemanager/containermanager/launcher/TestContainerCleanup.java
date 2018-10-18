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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.SignalContainerCommand;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_SLEEP_DELAY_BEFORE_SIGKILL_MS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ContainerCleanup}.
 */
public class TestContainerCleanup {

  private YarnConfiguration conf;
  private ContainerId containerId;
  private ContainerExecutor executor;
  private ContainerLaunch launch;
  private ContainerCleanup cleanup;

  @Before
  public void setup() throws Exception {
    conf = new YarnConfiguration();
    conf.setLong(NM_SLEEP_DELAY_BEFORE_SIGKILL_MS, 60000);
    Context context = mock(Context.class);
    NMStateStoreService storeService = mock(NMStateStoreService.class);
    when(context.getNMStateStore()).thenReturn(storeService);

    Dispatcher dispatcher = new InlineDispatcher();
    executor = mock(ContainerExecutor.class);
    when(executor.signalContainer(Mockito.any(
        ContainerSignalContext.class))).thenReturn(true);

    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(),
        1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);
    containerId = ContainerId.newContainerId(attemptId, 1);
    Container container = mock(Container.class);

    when(container.getContainerId()).thenReturn(containerId);

    launch = mock(ContainerLaunch.class);
    launch.containerAlreadyLaunched = new AtomicBoolean(false);

    launch.pidFilePath = new Path("target/" + containerId.toString() + ".pid");
    when(launch.getContainerPid()).thenReturn(containerId.toString());

    cleanup = new ContainerCleanup(context, conf, dispatcher, executor,
        container, launch);
  }

  @Test
  public void testNoCleanupWhenContainerNotLaunched() throws IOException {
    cleanup.run();
    verify(launch, Mockito.times(0)).signalContainer(
        Mockito.any(SignalContainerCommand.class));
  }

  @Test
  public void testCleanup() throws Exception {
    launch.containerAlreadyLaunched.set(true);
    cleanup.run();
    ArgumentCaptor<ContainerSignalContext> captor =
        ArgumentCaptor.forClass(ContainerSignalContext.class);

    verify(executor, Mockito.times(1)).signalContainer(captor.capture());
    Assert.assertEquals("signal", ContainerExecutor.Signal.TERM,
        captor.getValue().getSignal());
  }
}
