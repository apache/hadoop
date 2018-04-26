/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.api.records.Artifact;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.Container;
import org.apache.hadoop.yarn.service.api.records.ContainerState;
import org.apache.hadoop.yarn.service.api.records.Resource;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.client.ServiceClient;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.apache.hadoop.yarn.service.utils.SliderFileSystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A mock version of ServiceClient - This class is design
 * to simulate various error conditions that will happen
 * when a consumer class calls ServiceClient.
 */
public class ServiceClientTest extends ServiceClient {

  private Configuration conf = new Configuration();
  private Service goodServiceStatus = buildLiveGoodService();
  private boolean initialized;

  public ServiceClientTest() {
    super();
  }

  @Override
  public void init(Configuration conf) {
    if (!initialized) {
      super.init(conf);
      initialized = true;
    }
  }

  @Override
  public void stop() {
    // This is needed for testing  API Server which use client to get status
    // and then perform an action.
  }

  public void forceStop() {
    super.stop();
  }

  @Override
  public Configuration getConfig() {
    return conf;
  }

  @Override
  public ApplicationId actionCreate(Service service) throws IOException {
    ServiceApiUtil.validateAndResolveService(service,
        new SliderFileSystem(conf), getConfig());
    return ApplicationId.newInstance(System.currentTimeMillis(), 1);
  }

  @Override
  public Service getStatus(String appName) {
    if (appName != null && appName.equals("jenkins")) {
      return goodServiceStatus;
    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public int actionStart(String serviceName)
      throws YarnException, IOException {
    if (serviceName != null && serviceName.equals("jenkins")) {
      return EXIT_SUCCESS;
    } else {
      throw new ApplicationNotFoundException("");
    }
  }

  @Override
  public int actionStop(String serviceName, boolean waitForAppStopped)
      throws YarnException, IOException {
    if (serviceName == null) {
      throw new NullPointerException();
    }
    if (serviceName.equals("jenkins")) {
      return EXIT_SUCCESS;
    } else if (serviceName.equals("jenkins-second-stop")) {
      return EXIT_COMMAND_ARGUMENT_ERROR;
    } else {
      throw new ApplicationNotFoundException("");
    }
  }

  @Override
  public int actionDestroy(String serviceName) {
    if (serviceName != null) {
      if (serviceName.equals("jenkins")) {
        return EXIT_SUCCESS;
      } else if (serviceName.equals("jenkins-already-stopped")) {
        return EXIT_SUCCESS;
      } else if (serviceName.equals("jenkins-doesn't-exist")) {
        return EXIT_NOT_FOUND;
      } else if (serviceName.equals("jenkins-error-cleaning-registry")) {
        return EXIT_OTHER_FAILURE;
      }
    }
    throw new IllegalArgumentException();
  }

  @Override
  public int initiateUpgrade(Service service) throws YarnException,
      IOException {
    if (service.getName() != null && service.getName().equals("jenkins")) {
      return EXIT_SUCCESS;
    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public int actionUpgrade(Service service, List<Container> compInstances)
      throws IOException, YarnException {
    if (service.getName() != null && service.getName().equals("jenkins")) {
      return EXIT_SUCCESS;
    } else {
      throw new IllegalArgumentException();
    }
  }

  Service getGoodServiceStatus() {
    return goodServiceStatus;
  }

  static Service buildGoodService() {
    Service service = new Service();
    service.setName("jenkins");
    service.setVersion("v1");
    Artifact artifact = new Artifact();
    artifact.setType(Artifact.TypeEnum.DOCKER);
    artifact.setId("jenkins:latest");
    Resource resource = new Resource();
    resource.setCpus(1);
    resource.setMemory("2048");
    List<Component> components = new ArrayList<>();
    Component c = new Component();
    c.setName("jenkins");
    c.setNumberOfContainers(2L);
    c.setArtifact(artifact);
    c.setLaunchCommand("");
    c.setResource(resource);
    components.add(c);
    service.setComponents(components);
    return service;
  }

  static Service buildLiveGoodService() {
    Service service = buildGoodService();
    Component comp = service.getComponents().iterator().next();
    List<Container> containers = new ArrayList<>();
    for (int i = 0; i < comp.getNumberOfContainers(); i++) {
      Container container = new Container();
      container.setComponentInstanceName(comp.getName() + "-" + (i + 1));
      container.setState(ContainerState.READY);
      containers.add(container);
    }
    comp.setContainers(containers);
    return service;
  }
}
