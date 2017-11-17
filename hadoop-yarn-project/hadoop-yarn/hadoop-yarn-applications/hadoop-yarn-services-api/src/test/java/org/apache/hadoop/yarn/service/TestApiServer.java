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
import org.apache.hadoop.yarn.service.api.records.Artifact;
import org.apache.hadoop.yarn.service.api.records.Artifact.TypeEnum;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.Resource;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
import org.apache.hadoop.yarn.service.client.ServiceClient;
import org.apache.hadoop.yarn.service.webapp.ApiServer;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Test case for ApiServer REST API.
 *
 */
public class TestApiServer {
  private ApiServer apiServer;

  @Before
  public void setup() throws Exception {
    ServiceClient mockServerClient = new ServiceClientTest();
    Configuration conf = new Configuration();
    conf.set("yarn.api-service.service.client.class",
        ServiceClientTest.class.getName());
    ApiServer.setServiceClient(mockServerClient);
    this.apiServer = new ApiServer(conf);
  }

  @Test
  public void testPathAnnotation() {
    assertNotNull(this.apiServer.getClass().getAnnotation(Path.class));
    assertTrue("The controller has the annotation Path",
        this.apiServer.getClass().isAnnotationPresent(Path.class));
    final Path path = this.apiServer.getClass()
        .getAnnotation(Path.class);
    assertEquals("The path has /v1 annotation", path.value(),
        "/v1");
  }

  @Test
  public void testGetVersion() {
    final Response actual = apiServer.getVersion();
    assertEquals("Version number is", actual.getStatus(),
        Response.ok().build().getStatus());
  }

  @Test
  public void testBadCreateService() {
    Service service = new Service();
    // Test for invalid argument
    final Response actual = apiServer.createService(service);
    assertEquals("Create service is ", actual.getStatus(),
        Response.status(Status.BAD_REQUEST).build().getStatus());
  }

  @Test
  public void testGoodCreateService() {
    Service service = new Service();
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
    c.setNumberOfContainers(1L);
    c.setArtifact(artifact);
    c.setLaunchCommand("");
    c.setResource(resource);
    components.add(c);
    service.setComponents(components);
    final Response actual = apiServer.createService(service);
    assertEquals("Create service is ", actual.getStatus(),
        Response.status(Status.ACCEPTED).build().getStatus());
  }

  @Test
  public void testBadGetService() {
    final Response actual = apiServer.getService("no-jenkins");
    assertEquals("Get service is ", actual.getStatus(),
        Response.status(Status.NOT_FOUND).build().getStatus());
  }

  @Test
  public void testBadGetService2() {
    final Response actual = apiServer.getService(null);
    assertEquals("Get service is ", actual.getStatus(),
        Response.status(Status.INTERNAL_SERVER_ERROR)
            .build().getStatus());
  }

  @Test
  public void testGoodGetService() {
    final Response actual = apiServer.getService("jenkins");
    assertEquals("Get service is ", actual.getStatus(),
        Response.status(Status.OK).build().getStatus());
  }

  @Test
  public void testBadDeleteService() {
    final Response actual = apiServer.deleteService("no-jenkins");
    assertEquals("Delete service is ", actual.getStatus(),
        Response.status(Status.BAD_REQUEST).build().getStatus());
  }

  @Test
  public void testBadDeleteService2() {
    final Response actual = apiServer.deleteService(null);
    assertEquals("Delete service is ", actual.getStatus(),
        Response.status(Status.INTERNAL_SERVER_ERROR)
            .build().getStatus());
  }

  @Test
  public void testGoodDeleteService() {
    final Response actual = apiServer.deleteService("jenkins");
    assertEquals("Delete service is ", actual.getStatus(),
        Response.status(Status.OK).build().getStatus());
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
    final Response actual = apiServer.updateService("jenkins",
        service);
    assertEquals("update service is ", actual.getStatus(),
        Response.status(Status.OK).build().getStatus());
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
    final Response actual = apiServer.updateService("no-jenkins",
        service);
    assertEquals("flex service is ", actual.getStatus(),
        Response.status(Status.BAD_REQUEST).build().getStatus());
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
    final Response actual = apiServer.updateService("jenkins",
        service);
    assertEquals("flex service is ", actual.getStatus(),
        Response.status(Status.OK).build().getStatus());
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
    final Response actual = apiServer.updateService("no-jenkins",
        service);
    assertEquals("start service is ", actual.getStatus(),
        Response.status(Status.INTERNAL_SERVER_ERROR).build()
            .getStatus());
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
    final Response actual = apiServer.updateService("jenkins",
        service);
    assertEquals("start service is ", actual.getStatus(),
        Response.status(Status.OK).build().getStatus());
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
    final Response actual = apiServer.updateService("no-jenkins",
        service);
    assertEquals("stop service is ", actual.getStatus(),
        Response.status(Status.BAD_REQUEST).build().getStatus());
  }

  @Test
  public void testGoodStopServices() {
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
    c.setNumberOfContainers(-1L);
    c.setArtifact(artifact);
    c.setLaunchCommand("");
    c.setResource(resource);
    components.add(c);
    service.setComponents(components);
    System.out.println("before stop");
    final Response actual = apiServer.updateService("jenkins",
        service);
    assertEquals("stop service is ", actual.getStatus(),
        Response.status(Status.OK).build().getStatus());
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
    final Response actual = apiServer.updateService("no-jenkins",
        service);
    assertEquals("update service is ", actual.getStatus(),
        Response.status(Status.INTERNAL_SERVER_ERROR)
            .build().getStatus());
  }
}
