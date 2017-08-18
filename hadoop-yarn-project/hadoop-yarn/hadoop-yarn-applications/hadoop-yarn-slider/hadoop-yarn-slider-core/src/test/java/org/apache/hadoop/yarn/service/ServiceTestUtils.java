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

package org.apache.hadoop.yarn.service;

import org.apache.slider.api.resource.Application;
import org.apache.slider.api.resource.Component;
import org.apache.slider.api.resource.Resource;

public class ServiceTestUtils {

  // Example service definition
  // 2 components, each of which has 2 containers.
  protected Application createExampleApplication() {
    Application exampleApp = new Application();
    exampleApp.setName("example-app");
    exampleApp.addComponent(createComponent("compa"));
    exampleApp.addComponent(createComponent("compb"));
    return exampleApp;
  }

  protected Component createComponent(String name) {
    return createComponent(name, 2L, "sleep 1000");
  }

  protected Component createComponent(String name, long numContainers,
      String command) {
    Component comp1 = new Component();
    comp1.setNumberOfContainers(numContainers);
    comp1.setLaunchCommand(command);
    comp1.setName(name);
    Resource resource = new Resource();
    comp1.setResource(resource);
    resource.setMemory("128");
    resource.setCpus(1);
    return comp1;
  }
}
