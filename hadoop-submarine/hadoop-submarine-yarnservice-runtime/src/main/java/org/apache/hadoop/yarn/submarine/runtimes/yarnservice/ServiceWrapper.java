/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.runtimes.yarnservice;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.Service;

import java.io.IOException;
import java.util.Map;

/**
 * This class is only existing because we need a component name to
 * local launch command mapping from the test code.
 * Once this is solved in more clean or different way, we can delete this class.
 */
public class ServiceWrapper {
  private final Service service;

  @VisibleForTesting
  private Map<String, String> componentToLocalLaunchCommand = Maps.newHashMap();

  public ServiceWrapper(Service service) {
    this.service = service;
  }

  public void addComponent(AbstractComponent abstractComponent)
      throws IOException {
    Component component = abstractComponent.createComponent();
    service.addComponent(component);
    storeComponentName(abstractComponent, component.getName());
  }

  private void storeComponentName(
      AbstractComponent component, String name) {
    componentToLocalLaunchCommand.put(name,
        component.getLocalScriptFile());
  }

  public Service getService() {
    return service;
  }

  public String getLocalLaunchCommandPathForComponent(String componentName) {
    return componentToLocalLaunchCommand.get(componentName);
  }
}
