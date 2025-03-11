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
import org.apache.hadoop.security.Credentials;
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
import org.apache.hadoop.yarn.server.security.AMSecretKeys;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Unit tests for relaunching containers. */
public class TestContainerRelaunch {

  @Test
  public void testRelaunchContextWithoutHTTPS() throws Exception {
    testRelaunchContext(false);
  }

  @Test
  public void testRelaunchContextWithHTTPS() throws Exception {
    testRelaunchContext(true);
  }

  private void testRelaunchContext(boolean https) throws Exception {
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
    Credentials mockCredentials = mock(Credentials.class);
    when(mockContainer.getCredentials()).thenReturn(mockCredentials);
    if (https) {
      when(mockCredentials.getSecretKey(
          AMSecretKeys.YARN_APPLICATION_AM_KEYSTORE))
          .thenReturn("keystore".getBytes());
      when(mockCredentials.getSecretKey(
          AMSecretKeys.YARN_APPLICATION_AM_TRUSTSTORE))
          .thenReturn("truststore".getBytes());
    }
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
    assertEquals(0, result, "relaunch failed");
    ArgumentCaptor<ContainerStartContext> captor =
        ArgumentCaptor.forClass(ContainerStartContext.class);
    verify(mockExecutor).relaunchContainer(captor.capture());
    ContainerStartContext csc = captor.getValue();
    assertNotNull(csc.getAppId(), "app ID null");
    assertNotNull(csc.getContainer(), "container null");
    assertNotNull(csc.getContainerLocalDirs(), "container local dirs null");
    assertNotNull(csc.getContainerLogDirs(), "container log dirs null");
    assertNotNull(csc.getContainerWorkDir(), "work dir null");
    assertNotNull(csc.getFilecacheDirs(), "filecache dirs null");
    assertNotNull(csc.getLocalDirs(), "local dirs null");
    assertNotNull(csc.getLocalizedResources(), "localized resources null");
    assertNotNull(csc.getLogDirs(), "log dirs null");
    assertNotNull(csc.getNmPrivateContainerScriptPath(), "script path null");
    assertNotNull(csc.getNmPrivateTokensPath(), "tokens path null");
    if (https) {
      assertNotNull(csc.getNmPrivateKeystorePath(), "keystore path null");
      assertNotNull(csc.getNmPrivateTruststorePath(), "truststore path null");
    } else {
      assertNull(csc.getNmPrivateKeystorePath(), "keystore path not null");
      assertNull(csc.getNmPrivateTruststorePath(), "truststore path not null");
    }
    assertNotNull(csc.getUser(), "user null");
    assertNotNull(csc.getUserLocalDirs(), "user local dirs null");
    assertNotNull(csc.getUserFilecacheDirs(), "user filecache dirs null");
    assertNotNull(csc.getApplicationLocalDirs(), "application local dirs null");
  }
}
