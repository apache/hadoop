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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.Resource;
import org.apache.hadoop.yarn.service.utils.JsonSerDeser;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.apache.hadoop.yarn.service.utils.SliderFileSystem;
import org.codehaus.jackson.map.PropertyNamingStrategy;

import java.io.IOException;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ServiceTestUtils {

  public static final JsonSerDeser<Service> JSON_SER_DESER =
      new JsonSerDeser<>(Service.class,
          PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

  // Example service definition
  // 2 components, each of which has 2 containers.
  protected Service createExampleApplication() {
    Service exampleApp = new Service();
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

  public static SliderFileSystem initMockFs() throws IOException {
    return initMockFs(null);
  }

  public static SliderFileSystem initMockFs(Service ext) throws IOException {
    SliderFileSystem sfs = mock(SliderFileSystem.class);
    FileSystem mockFs = mock(FileSystem.class);
    JsonSerDeser<Service> jsonSerDeser = mock(JsonSerDeser.class);
    when(sfs.getFileSystem()).thenReturn(mockFs);
    when(sfs.buildClusterDirPath(anyObject())).thenReturn(
        new Path("cluster_dir_path"));
    if (ext != null) {
      when(jsonSerDeser.load(anyObject(), anyObject())).thenReturn(ext);
    }
    ServiceApiUtil.setJsonSerDeser(jsonSerDeser);
    return sfs;
  }
}
