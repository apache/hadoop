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

import static org.junit.Assert.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.service.api.records.Artifact;
import org.apache.hadoop.yarn.service.api.records.Artifact.TypeEnum;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.ComponentState;
import org.apache.hadoop.yarn.service.api.records.Container;
import org.apache.hadoop.yarn.service.api.records.ContainerState;
import org.apache.hadoop.yarn.service.api.records.Resource;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
import org.apache.hadoop.yarn.service.api.records.ServiceStatus;
import org.apache.hadoop.yarn.service.conf.RestApiConstants;
import org.apache.hadoop.yarn.service.webapp.ApiServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test case for ApiServer REST API.
 *
 */
public class TestApiServer {
  private ApiServer apiServer;
  private HttpServletRequest request;
  private ServiceClientTest mockServerClient;

  @Before
  public void setup() throws Exception {
    request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getRemoteUser())
        .thenReturn(System.getProperty("user.name"));
    mockServerClient = new ServiceClientTest();
    Configuration conf = new Configuration();
    conf.set("yarn.api-service.service.client.class",
        ServiceClientTest.class.getName());
    apiServer = new ApiServer(conf);
    apiServer.setServiceClient(mockServerClient);
  }

  @After
  public void teardown() {
    mockServerClient.forceStop();
  }

  @Test
  public void testPathAnnotation() {
    assertNotNull(this.apiServer.getClass().getAnnotation(Path.class));
    assertTrue("The controller has the annotation Path",
        this.apiServer.getClass().isAnnotationPresent(Path.class));
    final Path path = this.apiServer.getClass()
        .getAnnotation(Path.class);
    assertEquals("The path has /v1 annotation", "/v1", path.value());
  }

  @Test
  public void testGetVersion() {
    final Response actual = apiServer.getVersion();
    assertEquals("Version number is", Response.ok().build().getStatus(),
        actual.getStatus());
  }

  @Test
  public void testBadCreateService() {
    Service service = new Service();
    // Test for invalid argument
    final Response actual = apiServer.createService(request, service);
    assertEquals("Create service is ",
        Response.status(Status.BAD_REQUEST).build().getStatus(),
        actual.getStatus());
  }

  @Test
  public void testGoodCreateService() throws Exception {
    String json = "{\"auths\": "
        + "{\"https://index.docker.io/v1/\": "
        + "{\"auth\": \"foobarbaz\"},"
        + "\"registry.example.com\": "
        + "{\"auth\": \"bazbarfoo\"}}}";
    File dockerTmpDir = new File("target", "docker-tmp");
    FileUtils.deleteQuietly(dockerTmpDir);
    dockerTmpDir.mkdirs();
    String dockerConfig = dockerTmpDir + "/config.json";
    BufferedWriter bw = new BufferedWriter(new FileWriter(dockerConfig));
    bw.write(json);
    bw.close();
    Service service = ServiceClientTest.buildGoodService();
    final Response actual = apiServer.createService(request, service);
    assertEquals("Create service is ",
        Response.status(Status.ACCEPTED).build().getStatus(),
        actual.getStatus());
  }

  @Test
  public void testInternalServerErrorDockerClientConfigMissingCreateService() {
    Service service = new Service();
    service.setName("jenkins");
    service.setVersion("v1");
    service.setDockerClientConfig("/does/not/exist/config.json");
    Artifact artifact = new Artifact();
    artifact.setType(TypeEnum.DOCKER);
    artifact.setId("jenkins:latest");
    Resource resource = new Resource();
    resource.setCpus(1);
    resource.setMemory("2048");
    List<Component> components = new ArrayList<>();
    Component c = new Component();
    c.setName("jenkins");
    c.setNumberOfContainers(1L);
    c.setArtifact(artifact);
    c.setLaunchCommand("");
    c.setResource(resource);
    components.add(c);
    service.setComponents(components);
    final Response actual = apiServer.createService(request, service);
    assertEquals("Create service is ",
        Response.status(Status.BAD_REQUEST).build().getStatus(),
        actual.getStatus());
  }

  @Test
  public void testBadGetService() {
    final String serviceName = "nonexistent-jenkins";
    final Response actual = apiServer.getService(request, serviceName);
    assertEquals("Get service is ",
        Response.status(Status.NOT_FOUND).build().getStatus(),
        actual.getStatus());
    ServiceStatus serviceStatus = (ServiceStatus) actual.getEntity();
    assertEquals("Response code don't match",
        RestApiConstants.ERROR_CODE_APP_NAME_INVALID, serviceStatus.getCode());
    assertEquals("Response diagnostics don't match",
        "Service " + serviceName + " not found",
        serviceStatus.getDiagnostics());
  }

  @Test
  public void testBadGetService2() {
    final Response actual = apiServer.getService(request, null);
    assertEquals("Get service is ",
        Response.status(Status.NOT_FOUND).build().getStatus(),
        actual.getStatus());
    ServiceStatus serviceStatus = (ServiceStatus) actual.getEntity();
    assertEquals("Response code don't match",
        RestApiConstants.ERROR_CODE_APP_NAME_INVALID, serviceStatus.getCode());
    assertEquals("Response diagnostics don't match",
        "Service name cannot be null.", serviceStatus.getDiagnostics());
  }

  @Test
  public void testGoodGetService() {
    final Response actual = apiServer.getService(request, "jenkins");
    assertEquals("Get service is ",
        Response.status(Status.OK).build().getStatus(), actual.getStatus());
  }

  @Test
  public void testBadDeleteService() {
    final Response actual = apiServer.deleteService(request, "no-jenkins");
    assertEquals("Delete service is ",
        Response.status(Status.BAD_REQUEST).build().getStatus(),
        actual.getStatus());
  }

  @Test
  public void testBadDeleteService2() {
    final Response actual = apiServer.deleteService(request, null);
    assertEquals("Delete service is ",
        Response.status(Status.BAD_REQUEST).build().getStatus(),
        actual.getStatus());
  }

  @Test
  public void testBadDeleteService3() {
    final Response actual = apiServer.deleteService(request,
        "jenkins-doesn't-exist");
    assertEquals("Delete service is ",
        Response.status(Status.BAD_REQUEST).build().getStatus(),
        actual.getStatus());
  }

  @Test
  public void testBadDeleteService4() {
    final Response actual = apiServer.deleteService(request,
        "jenkins-error-cleaning-registry");
    assertEquals("Delete service is ",
        Response.status(Status.INTERNAL_SERVER_ERROR).build().getStatus(),
        actual.getStatus());
  }

  @Test
  public void testGoodDeleteService() {
    final Response actual = apiServer.deleteService(request, "jenkins");
    assertEquals("Delete service is ",
        Response.status(Status.OK).build().getStatus(), actual.getStatus());
  }

  @Test
  public void testDeleteStoppedService() {
    final Response actual = apiServer.deleteService(request,
        "jenkins-already-stopped");
    assertEquals("Delete service is ",
        Response.status(Status.OK).build().getStatus(), actual.getStatus());
  }

  @Test
  public void testDecreaseContainerAndStop() {
    Service service = new Service();
    service.setState(ServiceState.STOPPED);
    service.setName("jenkins");
    Artifact artifact = new Artifact();
    artifact.setType(TypeEnum.DOCKER);
    artifact.setId("jenkins:latest");
    Resource resource = new Resource();
    resource.setCpus(1);
    resource.setMemory("2048");
    List<Component> components = new ArrayList<Component>();
    Component c = new Component();
    c.setName("jenkins");
    c.setNumberOfContainers(0L);
    c.setArtifact(artifact);
    c.setLaunchCommand("");
    c.setResource(resource);
    components.add(c);
    service.setComponents(components);
    final Response actual = apiServer.updateService(request, "jenkins",
        service);
    assertEquals("update service is ",
        Response.status(Status.OK).build().getStatus(), actual.getStatus());
  }

  @Test
  public void testBadDecreaseContainerAndStop() {
    Service service = new Service();
    service.setState(ServiceState.STOPPED);
    service.setName("no-jenkins");
    Artifact artifact = new Artifact();
    artifact.setType(TypeEnum.DOCKER);
    artifact.setId("jenkins:latest");
    Resource resource = new Resource();
    resource.setCpus(1);
    resource.setMemory("2048");
    List<Component> components = new ArrayList<Component>();
    Component c = new Component();
    c.setName("no-jenkins");
    c.setNumberOfContainers(-1L);
    c.setArtifact(artifact);
    c.setLaunchCommand("");
    c.setResource(resource);
    components.add(c);
    service.setComponents(components);
    System.out.println("before stop");
    final Response actual = apiServer.updateService(request, "no-jenkins",
        service);
    assertEquals("flex service is ",
        Response.status(Status.BAD_REQUEST).build().getStatus(),
        actual.getStatus());
  }

  @Test
  public void testIncreaseContainersAndStart() {
    Service service = new Service();
    service.setState(ServiceState.STARTED);
    service.setName("jenkins");
    Artifact artifact = new Artifact();
    artifact.setType(TypeEnum.DOCKER);
    artifact.setId("jenkins:latest");
    Resource resource = new Resource();
    resource.setCpus(1);
    resource.setMemory("2048");
    List<Component> components = new ArrayList<Component>();
    Component c = new Component();
    c.setName("jenkins");
    c.setNumberOfContainers(2L);
    c.setArtifact(artifact);
    c.setLaunchCommand("");
    c.setResource(resource);
    components.add(c);
    service.setComponents(components);
    final Response actual = apiServer.updateService(request, "jenkins",
        service);
    assertEquals("flex service is ",
        Response.status(Status.OK).build().getStatus(), actual.getStatus());
  }

  @Test
  public void testBadStartServices() {
    Service service = new Service();
    service.setState(ServiceState.STARTED);
    service.setName("no-jenkins");
    Artifact artifact = new Artifact();
    artifact.setType(TypeEnum.DOCKER);
    artifact.setId("jenkins:latest");
    Resource resource = new Resource();
    resource.setCpus(1);
    resource.setMemory("2048");
    List<Component> components = new ArrayList<Component>();
    Component c = new Component();
    c.setName("jenkins");
    c.setNumberOfContainers(2L);
    c.setArtifact(artifact);
    c.setLaunchCommand("");
    c.setResource(resource);
    components.add(c);
    service.setComponents(components);
    final Response actual = apiServer.updateService(request, "no-jenkins",
        service);
    assertEquals("start service is ",
        Response.status(Status.BAD_REQUEST).build().getStatus(),
        actual.getStatus());
  }

  @Test
  public void testGoodStartServices() {
    Service service = new Service();
    service.setState(ServiceState.STARTED);
    service.setName("jenkins");
    Artifact artifact = new Artifact();
    artifact.setType(TypeEnum.DOCKER);
    artifact.setId("jenkins:latest");
    Resource resource = new Resource();
    resource.setCpus(1);
    resource.setMemory("2048");
    List<Component> components = new ArrayList<Component>();
    Component c = new Component();
    c.setName("jenkins");
    c.setNumberOfContainers(2L);
    c.setArtifact(artifact);
    c.setLaunchCommand("");
    c.setResource(resource);
    components.add(c);
    service.setComponents(components);
    final Response actual = apiServer.updateService(request, "jenkins",
        service);
    assertEquals("start service is ",
        Response.status(Status.OK).build().getStatus(), actual.getStatus());
  }

  @Test
  public void testBadStopServices() {
    Service service = new Service();
    service.setState(ServiceState.STOPPED);
    service.setName("no-jenkins");
    Artifact artifact = new Artifact();
    artifact.setType(TypeEnum.DOCKER);
    artifact.setId("jenkins:latest");
    Resource resource = new Resource();
    resource.setCpus(1);
    resource.setMemory("2048");
    List<Component> components = new ArrayList<Component>();
    Component c = new Component();
    c.setName("no-jenkins");
    c.setNumberOfContainers(-1L);
    c.setArtifact(artifact);
    c.setLaunchCommand("");
    c.setResource(resource);
    components.add(c);
    service.setComponents(components);
    System.out.println("before stop");
    final Response actual = apiServer.updateService(request, "no-jenkins",
        service);
    assertEquals("stop service is ",
        Response.status(Status.BAD_REQUEST).build().getStatus(),
        actual.getStatus());
  }

  @Test
  public void testGoodStopServices() {
    Service service = new Service();
    service.setState(ServiceState.STOPPED);
    service.setName("jenkins");
    System.out.println("before stop");
    final Response actual = apiServer.updateService(request, "jenkins",
        service);
    assertEquals("stop service is ",
        Response.status(Status.OK).build().getStatus(), actual.getStatus());
  }

  @Test
  public void testBadSecondStopServices() throws Exception {
    Service service = new Service();
    service.setState(ServiceState.STOPPED);
    service.setName("jenkins-second-stop");
    // simulates stop on an already stopped service
    System.out.println("before second stop");
    final Response actual = apiServer.updateService(request,
        "jenkins-second-stop", service);
    assertEquals("stop service should have thrown 400 Bad Request: ",
        Response.status(Status.BAD_REQUEST).build().getStatus(),
        actual.getStatus());
    ServiceStatus serviceStatus = (ServiceStatus) actual.getEntity();
    assertEquals("Stop service should have failed with service already stopped",
        "Service jenkins-second-stop is already stopped",
        serviceStatus.getDiagnostics());
  }

  @Test
  public void testUpdateService() {
    Service service = new Service();
    service.setState(ServiceState.STARTED);
    service.setName("no-jenkins");
    Artifact artifact = new Artifact();
    artifact.setType(TypeEnum.DOCKER);
    artifact.setId("jenkins:latest");
    Resource resource = new Resource();
    resource.setCpus(1);
    resource.setMemory("2048");
    List<Component> components = new ArrayList<Component>();
    Component c = new Component();
    c.setName("no-jenkins");
    c.setNumberOfContainers(-1L);
    c.setArtifact(artifact);
    c.setLaunchCommand("");
    c.setResource(resource);
    components.add(c);
    service.setComponents(components);
    System.out.println("before stop");
    final Response actual = apiServer.updateService(request, "no-jenkins",
        service);
    assertEquals("update service is ",
        Response.status(Status.BAD_REQUEST)
            .build().getStatus(), actual.getStatus());
  }

  @Test
  public void testUpdateComponent() {
    Response actual = apiServer.updateComponent(request, "jenkins",
        "jenkins-master", null);
    ServiceStatus serviceStatus = (ServiceStatus) actual.getEntity();
    assertEquals("Update component should have failed with 400 bad request",
        Response.status(Status.BAD_REQUEST).build().getStatus(),
        actual.getStatus());
    assertEquals("Update component should have failed with no data error",
        "No component data provided", serviceStatus.getDiagnostics());

    Component comp = new Component();
    actual = apiServer.updateComponent(request, "jenkins", "jenkins-master",
        comp);
    serviceStatus = (ServiceStatus) actual.getEntity();
    assertEquals("Update component should have failed with 400 bad request",
        Response.status(Status.BAD_REQUEST).build().getStatus(),
        actual.getStatus());
    assertEquals("Update component should have failed with no count error",
        "No container count provided", serviceStatus.getDiagnostics());

    comp.setNumberOfContainers(-1L);
    actual = apiServer.updateComponent(request, "jenkins", "jenkins-master",
        comp);
    serviceStatus = (ServiceStatus) actual.getEntity();
    assertEquals("Update component should have failed with 400 bad request",
        Response.status(Status.BAD_REQUEST).build().getStatus(),
        actual.getStatus());
    assertEquals("Update component should have failed with no count error",
        "Invalid number of containers specified -1", serviceStatus.getDiagnostics());

    comp.setName("jenkins-slave");
    comp.setNumberOfContainers(1L);
    actual = apiServer.updateComponent(request, "jenkins", "jenkins-master",
        comp);
    serviceStatus = (ServiceStatus) actual.getEntity();
    assertEquals("Update component should have failed with 400 bad request",
        Response.status(Status.BAD_REQUEST).build().getStatus(),
        actual.getStatus());
    assertEquals(
        "Update component should have failed with component name mismatch "
            + "error",
        "Component name in the request object (jenkins-slave) does not match "
            + "that in the URI path (jenkins-master)",
        serviceStatus.getDiagnostics());
  }

  @Test
  public void testInitiateUpgrade() {
    Service goodService = ServiceClientTest.buildLiveGoodService();
    goodService.setVersion("v2");
    goodService.setState(ServiceState.UPGRADING);
    final Response actual = apiServer.updateService(request,
        goodService.getName(), goodService);
    assertEquals("Initiate upgrade is ",
        Response.status(Status.ACCEPTED).build().getStatus(),
        actual.getStatus());
  }

  @Test
  public void testUpgradeSingleInstance() {
    Service goodService = ServiceClientTest.buildLiveGoodService();
    Component comp = goodService.getComponents().iterator().next();
    Container container = comp.getContainers().iterator().next();
    container.setState(ContainerState.UPGRADING);

    // To be able to upgrade, the service needs to be in UPGRADING
    // and container state needs to be in NEEDS_UPGRADE.
    Service serviceStatus = mockServerClient.getGoodServiceStatus();
    serviceStatus.setState(ServiceState.UPGRADING);
    Container liveContainer = serviceStatus.getComponents().iterator().next()
        .getContainers().iterator().next();
    liveContainer.setState(ContainerState.NEEDS_UPGRADE);
    mockServerClient.setExpectedInstances(Sets.newHashSet(
        liveContainer.getComponentInstanceName()));

    final Response actual = apiServer.updateComponentInstance(request,
        goodService.getName(), comp.getName(),
        container.getComponentInstanceName(), container);
    assertEquals("Instance upgrade is ",
        Response.status(Status.ACCEPTED).build().getStatus(),
        actual.getStatus());
  }

  @Test
  public void testUpgradeMultipleInstances() {
    Service goodService = ServiceClientTest.buildLiveGoodService();
    Component comp = goodService.getComponents().iterator().next();
    comp.getContainers().forEach(container ->
        container.setState(ContainerState.UPGRADING));

    // To be able to upgrade, the service needs to be in UPGRADING
    // and container state needs to be in NEEDS_UPGRADE.
    Service serviceStatus = mockServerClient.getGoodServiceStatus();
    serviceStatus.setState(ServiceState.UPGRADING);
    Set<String> expectedInstances = new HashSet<>();
    serviceStatus.getComponents().iterator().next().getContainers().forEach(
        container -> {
          container.setState(ContainerState.NEEDS_UPGRADE);
          expectedInstances.add(container.getComponentInstanceName());
        }
    );
    mockServerClient.setExpectedInstances(expectedInstances);

    final Response actual = apiServer.updateComponentInstances(request,
        goodService.getName(), comp.getContainers());
    assertEquals("Instance upgrade is ",
        Response.status(Status.ACCEPTED).build().getStatus(),
        actual.getStatus());
  }

  @Test
  public void testUpgradeComponent() {
    Service goodService = ServiceClientTest.buildLiveGoodService();
    Component comp = goodService.getComponents().iterator().next();
    comp.setState(ComponentState.UPGRADING);

    // To be able to upgrade, the service needs to be in UPGRADING
    // and component state needs to be in NEEDS_UPGRADE.
    Service serviceStatus = mockServerClient.getGoodServiceStatus();
    serviceStatus.setState(ServiceState.UPGRADING);
    Component liveComp = serviceStatus.getComponent(comp.getName());
    liveComp.setState(ComponentState.NEEDS_UPGRADE);
    Set<String> expectedInstances = new HashSet<>();
    liveComp.getContainers().forEach(container -> {
      expectedInstances.add(container.getComponentInstanceName());
      container.setState(ContainerState.NEEDS_UPGRADE);
    });
    mockServerClient.setExpectedInstances(expectedInstances);

    final Response actual = apiServer.updateComponent(request,
        goodService.getName(), comp.getName(), comp);
    assertEquals("Component upgrade is ",
        Response.status(Status.ACCEPTED).build().getStatus(),
        actual.getStatus());
  }

  @Test
  public void testUpgradeMultipleComps() {
    Service goodService = ServiceClientTest.buildLiveGoodService();
    goodService.getComponents().forEach(comp ->
        comp.setState(ComponentState.UPGRADING));

    // To be able to upgrade, the live service needs to be in UPGRADING
    // and component states needs to be in NEEDS_UPGRADE.
    Service serviceStatus = mockServerClient.getGoodServiceStatus();
    serviceStatus.setState(ServiceState.UPGRADING);
    Set<String> expectedInstances = new HashSet<>();
    serviceStatus.getComponents().forEach(liveComp -> {
      liveComp.setState(ComponentState.NEEDS_UPGRADE);
      liveComp.getContainers().forEach(liveContainer -> {
        expectedInstances.add(liveContainer.getComponentInstanceName());
        liveContainer.setState(ContainerState.NEEDS_UPGRADE);
      });
    });
    mockServerClient.setExpectedInstances(expectedInstances);

    final Response actual = apiServer.updateComponents(request,
        goodService.getName(), goodService.getComponents());
    assertEquals("Component upgrade is ",
        Response.status(Status.ACCEPTED).build().getStatus(),
        actual.getStatus());
  }
}
