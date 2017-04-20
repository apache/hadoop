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

package org.apache.slider.server.appmaster.model.mock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.LifecycleEvent;
import org.apache.hadoop.service.ServiceStateChangeListener;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.slider.api.resource.Application;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.launch.ContainerLauncher;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.providers.ProviderService;
import org.apache.slider.server.appmaster.state.StateAccessForProviders;
import org.apache.slider.server.services.yarnregistry.YarnRegistryViewForProviders;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Mock provider service.
 */
public class MockProviderService implements ProviderService {

  @Override
  public String getName() {
    return null;
  }

  @Override
  public void init(Configuration config) {
  }

  @Override
  public void start() {
  }

  @Override
  public void stop() {
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void registerServiceListener(ServiceStateChangeListener listener) {
  }

  @Override
  public void unregisterServiceListener(ServiceStateChangeListener listener) {
  }

  @Override
  public Configuration getConfig() {
    return null;
  }

  public STATE getServiceState() {
    return null;
  }

  @Override
  public long getStartTime() {
    return 0;
  }

  @Override
  public boolean isInState(STATE state) {
    return false;
  }

  @Override
  public Throwable getFailureCause() {
    return null;
  }

  @Override
  public STATE getFailureState() {
    return null;
  }

  @Override
  public boolean waitForServiceToStop(long timeout) {
    return false;
  }

  @Override
  public List<LifecycleEvent> getLifecycleHistory() {
    return null;
  }

  @Override
  public Map<String, String> getBlockers() {
    return null;
  }

  @Override
  public void buildContainerLaunchContext(ContainerLauncher containerLauncher,
      Application application, Container container, ProviderRole providerRole,
      SliderFileSystem sliderFileSystem) throws IOException, SliderException {

  }

  @Override
  public void setAMState(StateAccessForProviders stateAccessForProviders) {

  }

  @Override
  public void bindToYarnRegistry(YarnRegistryViewForProviders yarnRegistry) {

  }

  @Override
  public boolean processContainerStatus(ContainerId containerId,
      ContainerStatus status) {
    return false;
  }
}
