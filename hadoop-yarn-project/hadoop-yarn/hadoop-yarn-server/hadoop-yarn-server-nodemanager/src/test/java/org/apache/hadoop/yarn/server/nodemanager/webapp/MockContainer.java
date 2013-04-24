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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.util.BuilderUtils;
import static org.mockito.Mockito.*;

public class MockContainer implements Container {

  private ContainerId id;
  private ContainerState state;
  private String user;
  private ContainerLaunchContext launchContext;
  private final Map<Path, List<String>> resource =
      new HashMap<Path, List<String>>();
  private RecordFactory recordFactory;
  private org.apache.hadoop.yarn.api.records.Container mockContainer;

  public MockContainer(ApplicationAttemptId appAttemptId,
      Dispatcher dispatcher, Configuration conf, String user,
      ApplicationId appId, int uniqId) {

    this.user = user;
    this.recordFactory = RecordFactoryProvider.getRecordFactory(conf);
    this.id = BuilderUtils.newContainerId(recordFactory, appId, appAttemptId,
        uniqId);
    this.launchContext = recordFactory
        .newRecordInstance(ContainerLaunchContext.class);
    launchContext.setUser(user);
    this.state = ContainerState.NEW;

    mockContainer = mock(org.apache.hadoop.yarn.api.records.Container.class);
    when(mockContainer.getId()).thenReturn(id);
  }

  public void setState(ContainerState state) {
    this.state = state;
  }

  @Override
  public String getUser() {
    return user;
  }

  @Override
  public ContainerState getContainerState() {
    return state;
  }

  @Override
  public ContainerLaunchContext getLaunchContext() {
    return launchContext;
  }

  @Override
  public Credentials getCredentials() {
    return null;
  }

  @Override
  public Map<Path, List<String>> getLocalizedResources() {
    return resource;
  }

  @Override
  public ContainerStatus cloneAndGetContainerStatus() {
    ContainerStatus containerStatus = recordFactory
        .newRecordInstance(ContainerStatus.class);
    containerStatus
        .setState(org.apache.hadoop.yarn.api.records.ContainerState.RUNNING);
    containerStatus.setDiagnostics("testing");
    containerStatus.setExitStatus(0);
    return containerStatus;
  }

  @Override
  public String toString() {
    return "";
  }

  @Override
  public void handle(ContainerEvent event) {
  }

  @Override
  public org.apache.hadoop.yarn.api.records.Container getContainer() {
    return this.mockContainer;
  }
}
