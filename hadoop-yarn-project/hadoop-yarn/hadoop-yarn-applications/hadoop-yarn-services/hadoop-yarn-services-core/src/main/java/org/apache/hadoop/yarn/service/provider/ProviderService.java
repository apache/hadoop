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

package org.apache.hadoop.yarn.service.provider;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.containerlaunch.ContainerLaunchService;
import org.apache.hadoop.yarn.service.utils.SliderFileSystem;
import org.apache.hadoop.yarn.service.exceptions.SliderException;
import org.apache.hadoop.yarn.service.containerlaunch.AbstractLauncher;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public interface ProviderService {

  /**
   * Set up the entire container launch context
   */
  ResolvedLaunchParams buildContainerLaunchContext(
      AbstractLauncher containerLauncher,
      Service service, ComponentInstance instance,
      SliderFileSystem sliderFileSystem, Configuration yarnConf,
      Container container,
      ContainerLaunchService.ComponentLaunchContext componentLaunchContext)
      throws IOException, SliderException;

  /**
   * This holds any information that is resolved during building the launch
   * context for a container.
   * <p>
   * Right now it contains a mapping of resource keys to destination files
   * for resources that need to be localized.
   */
  class ResolvedLaunchParams {
    private Map<String, String> resolvedRsrcPaths = new HashMap<>();

    void addResolvedRsrcPath(String resourceKey, String destFile) {
      Preconditions.checkNotNull(destFile, "dest file cannot be null");
      Preconditions.checkNotNull(resourceKey,
          "local resource cannot be null");
      resolvedRsrcPaths.put(resourceKey, destFile);
    }

    public Map<String, String> getResolvedRsrcPaths() {
      return this.resolvedRsrcPaths;
    }

    public boolean didLaunchFail() {
      return false;
    }
  }

  ResolvedLaunchParams FAILED_LAUNCH_PARAMS = new ResolvedLaunchParams() {
    @Override
    public boolean didLaunchFail() {
      return true;
    }
  };

}
