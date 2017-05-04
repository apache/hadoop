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

package org.apache.slider.providers;

import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.slider.api.resource.Application;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.launch.ContainerLauncher;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.apache.slider.server.appmaster.state.StateAccessForProviders;
import org.apache.slider.server.appmaster.timelineservice.ServiceTimelinePublisher;
import org.apache.slider.server.services.yarnregistry.YarnRegistryViewForProviders;

import java.io.IOException;

public interface ProviderService extends Service {

  /**
   * Set up the entire container launch context
   */
  void buildContainerLaunchContext(ContainerLauncher containerLauncher,
      Application application, Container container, ProviderRole providerRole,
      SliderFileSystem sliderFileSystem, RoleInstance roleInstance)
      throws IOException, SliderException;


  void setAMState(StateAccessForProviders stateAccessForProviders);

  /**
   * Bind to the YARN registry
   * @param yarnRegistry YARN registry
   */
  void bindToYarnRegistry(YarnRegistryViewForProviders yarnRegistry);

  /**
   * Process container status
   * @return true if status needs to be requested again, false otherwise
   */
  boolean processContainerStatus(ContainerId containerId,
      ContainerStatus status);

  /**
   * Set service publisher.
   * @param serviceTimelinePublisher service publisher.
   */
  void setServiceTimelinePublisher(
      ServiceTimelinePublisher serviceTimelinePublisher);
}
