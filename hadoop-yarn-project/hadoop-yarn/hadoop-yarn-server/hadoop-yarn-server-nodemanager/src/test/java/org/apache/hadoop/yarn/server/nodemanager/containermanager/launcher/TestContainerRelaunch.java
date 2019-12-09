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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** Unit tests for relaunching containers. */
public class TestContainerRelaunch {

  @Test
  public void testRelaunchContext() throws Exception {
    Configuration conf = new Configuration();

    Context mockContext = mock(Context.class);
    doReturn(new NMNullStateStoreService()).when(mockContext).getNMStateStore();
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId cid = ContainerId.newContainerId(appAttemptId, 1);
    Application mockApp = mock(Application.class);
    doReturn(appId).when(mockApp).getAppId();
    Container mockContainer = mock(Container.class);
    doReturn("somebody").when(mockContainer).getUser();
    doReturn(cid).when(mockContainer).getContainerId();
    doReturn("/foo").when(mockContainer).getWorkDir();
    doReturn("/bar").when(mockContainer).getLogDir();
    LocalDirsHandlerService mockDirsHandler =
        mock(LocalDirsHandlerService.class);
    doReturn(true).when(mockDirsHandler).isGoodLocalDir(any(String.class));
    doReturn(true).when(mockDirsHandler).isGoodLogDir(anyString());
    doReturn(true).when(mockDirsHandler).areDisksHealthy();
    doReturn(new Path("/some/file")).when(mockDirsHandler)
        .getLocalPathForRead(anyString());
    Dispatcher dispatcher = new InlineDispatcher();
    ContainerExecutor mockExecutor = mock(ContainerExecutor.class);
    ContainerRelaunch cr = new ContainerRelaunch(mockContext, conf, dispatcher,
        mockExecutor, mockApp, mockContainer, mockDirsHandler, null);
    int result = cr.call();
    assertEquals("relaunch failed", 0, result);
    ArgumentCaptor<ContainerStartContext> captor =
        ArgumentCaptor.forClass(ContainerStartContext.class);
    verify(mockExecutor).relaunchContainer(captor.capture());
    ContainerStartContext csc = captor.getValue();
    assertNotNull("app ID null", csc.getAppId());
    assertNotNull("container null", csc.getContainer());
    assertNotNull("container local dirs null", csc.getContainerLocalDirs());
    assertNotNull("container log dirs null", csc.getContainerLogDirs());
    assertNotNull("work dir null", csc.getContainerWorkDir());
    assertNotNull("filecache dirs null", csc.getFilecacheDirs());
    assertNotNull("local dirs null", csc.getLocalDirs());
    assertNotNull("localized resources null", csc.getLocalizedResources());
    assertNotNull("log dirs null", csc.getLogDirs());
    assertNotNull("script path null", csc.getNmPrivateContainerScriptPath());
    assertNotNull("tokens path null", csc.getNmPrivateTokensPath());
    assertNotNull("user null", csc.getUser());
    assertNotNull("user local dirs null", csc.getUserLocalDirs());
    assertNotNull("user filecache dirs null", csc.getUserFilecacheDirs());
    assertNotNull("application local dirs null", csc.getApplicationLocalDirs());
  }
}
