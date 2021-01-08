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

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingCluster;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ResourceTypeInfo;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.security.DockerCredentialTokenIdentifier;
import org.apache.hadoop.yarn.service.api.records.Artifact;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.ResourceInformation;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
import org.apache.hadoop.yarn.service.component.ComponentState;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstanceState;
import org.apache.hadoop.yarn.service.conf.YarnServiceConf;
import org.apache.hadoop.yarn.util.DockerClientConfigHandler;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.registry.client.api.RegistryConstants.KEY_REGISTRY_ZK_QUORUM;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestServiceAM extends ServiceTestUtils{

  private static final Logger LOG =
      LoggerFactory.getLogger(TestServiceAM.class);

  private File basedir;
  YarnConfiguration conf = new YarnConfiguration();
  TestingCluster zkCluster;
  @Rule
  public ServiceTestUtils.ServiceFSWatcher rule =
      new ServiceTestUtils.ServiceFSWatcher();

  @Before
  public void setup() throws Exception {
    basedir = new File("target", "apps");
    if (basedir.exists()) {
      FileUtils.deleteDirectory(basedir);
    } else {
      basedir.mkdirs();
    }
    zkCluster = new TestingCluster(1);
    zkCluster.start();
    conf.set(KEY_REGISTRY_ZK_QUORUM, zkCluster.getConnectString());
    LOG.info("ZK cluster: {}", zkCluster.getConnectString());
  }

  @After
  public void tearDown() throws IOException {
    if (basedir != null) {
      FileUtils.deleteDirectory(basedir);
    }
    if (zkCluster != null) {
      zkCluster.stop();
    }
  }

  // Race condition YARN-7486
  // 1. Allocate 1 container to compa and wait it to be started
  // 2. Fail this container, and in the meanwhile allocate the 2nd container.
  // 3. The 2nd container should not be assigned to compa-0 instance, because
  //   the compa-0 instance is not stopped yet.
  // 4. check compa still has the instance in the pending list.
  @Test
  public void testContainerCompleted() throws TimeoutException,
      InterruptedException {
    ApplicationId applicationId = ApplicationId.newInstance(123456, 1);
    Service exampleApp = new Service();
    exampleApp.setId(applicationId.toString());
    exampleApp.setVersion("v1");
    exampleApp.setName("testContainerCompleted");
    exampleApp.addComponent(createComponent("compa", 1, "pwd"));

    MockServiceAM am = new MockServiceAM(exampleApp);
    am.init(conf);
    am.start();

    ComponentInstance compa0 = am.getCompInstance("compa", "compa-0");
    // allocate a container
    am.feedContainerToComp(exampleApp, 1, "compa");
    am.waitForCompInstanceState(compa0, ComponentInstanceState.STARTED);

    LOG.info("Fail the container 1");
    // fail the container
    am.feedFailedContainerToComp(exampleApp, 1, "compa");

    // allocate the second container immediately, this container will not be
    // assigned to comp instance
    // because the instance is not yet added to the pending list.
    am.feedContainerToComp(exampleApp, 2, "compa");

    am.waitForCompInstanceState(compa0, ComponentInstanceState.INIT);
    // still 1 pending instance
    Assert.assertEquals(1,
        am.getComponent("compa").getPendingInstances().size());
    am.stop();
  }

  // Test to verify that the containers of previous attempt are not prematurely
  // released. These containers are sent by the RM to the AM in the
  // heartbeat response.
  @Test(timeout = 200000)
  public void testContainersFromPreviousAttemptsWithRMRestart()
      throws Exception {
    ApplicationId applicationId = ApplicationId.newInstance(
        System.currentTimeMillis(), 1);
    Service exampleApp = new Service();
    exampleApp.setId(applicationId.toString());
    exampleApp.setVersion("v1");
    exampleApp.setName("testContainersRecovers");
    String comp1Name = "comp1";
    String comp1InstName = "comp1-0";

    org.apache.hadoop.yarn.service.api.records.Component compA =
        createComponent(comp1Name, 1, "sleep");
    exampleApp.addComponent(compA);

    MockServiceAM am = new MockServiceAM(exampleApp);
    ContainerId containerId = am.createContainerId(1);
    am.feedRegistryComponent(containerId, comp1Name, comp1InstName);
    am.init(conf);
    am.start();

    ComponentInstance comp10 = am.getCompInstance(comp1Name, comp1InstName);
    am.feedRecoveredContainer(containerId, comp1Name);
    am.waitForCompInstanceState(comp10, ComponentInstanceState.STARTED);

    // 0 pending instance
    Assert.assertEquals(0,
        am.getComponent(comp1Name).getPendingInstances().size());

    GenericTestUtils.waitFor(() -> am.getCompInstance(comp1Name, comp1InstName)
        .getContainerStatus() != null, 2000, 200000);

    Assert.assertEquals("container state",
        org.apache.hadoop.yarn.api.records.ContainerState.RUNNING,
        am.getCompInstance(comp1Name, comp1InstName).getContainerStatus()
            .getState());
    am.stop();
  }

  // Test to verify that the containers of previous attempt are released and the
  // component instance is added to the pending queue when the recovery wait
  // time interval elapses.
  @Test(timeout = 200000)
  public void testContainersReleasedWhenExpired()
      throws Exception {
    ApplicationId applicationId = ApplicationId.newInstance(
        System.currentTimeMillis(), 1);
    Service exampleApp = new Service();
    exampleApp.setId(applicationId.toString());
    exampleApp.setName("testContainersRecovers");
    exampleApp.setVersion("v1");
    String comp1Name = "comp1";
    String comp1InstName = "comp1-0";

    org.apache.hadoop.yarn.service.api.records.Component compA =
        createComponent(comp1Name, 1, "sleep");
    exampleApp.addComponent(compA);

    MockServiceAM am = new MockServiceAM(exampleApp);
    ContainerId containerId = am.createContainerId(1);
    am.feedRegistryComponent(containerId, comp1Name, comp1InstName);
    conf.setLong(YarnServiceConf.CONTAINER_RECOVERY_TIMEOUT_MS, 10);
    am.init(conf);
    am.start();
    Thread.sleep(100);
    GenericTestUtils.waitFor(() -> am.getComponent(comp1Name).getState()
        .equals(ComponentState.FLEXING), 100, 2000);

    // 1 pending instance
    Assert.assertEquals(1, am.getComponent(comp1Name).getPendingInstances()
        .size());

    am.feedContainerToComp(exampleApp, 2, comp1Name);

    GenericTestUtils.waitFor(() -> am.getCompInstance(comp1Name, comp1InstName)
        .getContainerStatus() != null, 2000, 200000);
    Assert.assertEquals("container state",
        org.apache.hadoop.yarn.api.records.ContainerState.RUNNING,
        am.getCompInstance(comp1Name, comp1InstName).getContainerStatus()
            .getState());
  }

  // Test to verify that the AM doesn't wait for containers of a different app
  // even though it corresponds to the same service.
  @Test(timeout = 200000)
  public void testContainersFromDifferentApp()
      throws Exception {
    ApplicationId applicationId = ApplicationId.newInstance(
        System.currentTimeMillis(), 1);
    Service exampleApp = new Service();
    exampleApp.setId(applicationId.toString());
    exampleApp.setName("testContainersFromDifferentApp");
    exampleApp.setVersion("v1");
    String comp1Name = "comp1";
    String comp1InstName = "comp1-0";

    org.apache.hadoop.yarn.service.api.records.Component compA =
        createComponent(comp1Name, 1, "sleep");
    exampleApp.addComponent(compA);

    MockServiceAM am = new MockServiceAM(exampleApp);
    ContainerId containerId = am.createContainerId(1);
    // saves the container in the registry
    am.feedRegistryComponent(containerId, comp1Name, comp1InstName);

    ApplicationId changedAppId = ApplicationId.newInstance(
        System.currentTimeMillis(), 2);
    exampleApp.setId(changedAppId.toString());
    am.init(conf);
    am.start();
    // 1 pending instance since the container in registry belongs to a different
    // app.
    Assert.assertEquals(1,
        am.getComponent(comp1Name).getPendingInstances().size());

    am.feedContainerToComp(exampleApp, 1, comp1Name);
    GenericTestUtils.waitFor(() -> am.getCompInstance(comp1Name, comp1InstName)
        .getContainerStatus() != null, 2000, 200000);

    Assert.assertEquals("container state",
        org.apache.hadoop.yarn.api.records.ContainerState.RUNNING,
        am.getCompInstance(comp1Name, comp1InstName).getContainerStatus()
            .getState());
    am.stop();
  }

  @Test
  public void testScheduleWithMultipleResourceTypes()
      throws TimeoutException, InterruptedException, IOException {
    ApplicationId applicationId = ApplicationId.newInstance(123456, 1);
    Service exampleApp = new Service();
    exampleApp.setId(applicationId.toString());
    exampleApp.setName("testScheduleWithMultipleResourceTypes");
    exampleApp.setVersion("v1");

    List<ResourceTypeInfo> resourceTypeInfos = new ArrayList<>(
        ResourceUtils.getResourcesTypeInfo());
    // Add 3rd resource type.
    resourceTypeInfos.add(ResourceTypeInfo
        .newInstance("resource-1", "", ResourceTypes.COUNTABLE));
    // Reinitialize resource types
    ResourceUtils.reinitializeResources(resourceTypeInfos);

    Component serviceCompoent = createComponent("compa", 1, "pwd");
    serviceCompoent.getResource().setResourceInformations(ImmutableMap
        .of("resource-1", new ResourceInformation().value(3333L).unit("Gi")));
    exampleApp.addComponent(serviceCompoent);

    MockServiceAM am = new MockServiceAM(exampleApp);
    am.init(conf);
    am.start();

    ServiceScheduler serviceScheduler = am.context.scheduler;
    AMRMClientAsync<AMRMClient.ContainerRequest> amrmClientAsync =
        serviceScheduler.getAmRMClient();

    Collection<AMRMClient.ContainerRequest> rr =
        amrmClientAsync.getMatchingRequests(0);
    Assert.assertEquals(1, rr.size());

    org.apache.hadoop.yarn.api.records.Resource capability =
        rr.iterator().next().getCapability();
    Assert.assertEquals(3333L, capability.getResourceValue("resource-1"));
    Assert.assertEquals("Gi",
        capability.getResourceInformation("resource-1").getUnits());

    am.stop();
  }

  @Test
  public void testContainerCompletedEventProcessed() throws Exception {
    ServiceContext context = createServiceContext("abc");
    MockServiceScheduler scheduler = new MockServiceScheduler(context);
    scheduler.init(conf);
    ApplicationId appId = ApplicationId.newInstance(0, 0);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId,
        1);
    ContainerId containerId1 = ContainerId.newContainerId(appAttemptId, 0);
    ContainerStatus containerStatus1 = ContainerStatus.newInstance(containerId1,
        org.apache.hadoop.yarn.api.records.ContainerState.COMPLETE,
        "successful", 0);
    ContainerId containerId2 = ContainerId.newContainerId(appAttemptId, 1);
    ContainerStatus containerStatus2 = ContainerStatus.newInstance(containerId2,
        org.apache.hadoop.yarn.api.records.ContainerState.COMPLETE,
        "successful", 0);
    ComponentInstance instance = Mockito.mock(ComponentInstance.class);
    Mockito.doReturn("componentInstance").when(instance).getCompName();
    scheduler.addLiveCompInstance(containerId2, instance);
    List<ContainerStatus> statuses = new ArrayList<>();
    // First container instance will be null
    statuses.add(containerStatus1);
    // Second container instance is added
    scheduler.addLiveCompInstance(containerId2, instance);
    statuses.add(containerStatus2);
    scheduler.callbackHandler.onContainersCompleted(statuses);
    // For second container event should be dispatched.
    verify(scheduler.dispatcher, times(1)).getEventHandler();
    DefaultMetricsSystem.shutdown();
  }

  private ServiceContext createServiceContext(String name)
      throws Exception {
    Artifact artifact = new Artifact();
    artifact.setId("1");
    artifact.setType(Artifact.TypeEnum.TARBALL);
    Service serviceDef = ServiceTestUtils.createExampleApplication();
    ApplicationId applicationId = ApplicationId.newInstance(
        System.currentTimeMillis(), 1);
    serviceDef.setId(applicationId.toString());
    serviceDef.setName(name);
    serviceDef.setState(ServiceState.STARTED);
    serviceDef.getComponents().forEach(component ->
        component.setArtifact(artifact));
    ServiceContext context = new MockRunningServiceContext(rule,
        serviceDef);
    context.scheduler.getDispatcher().setDrainEventsOnStop();
    context.scheduler.getDispatcher().start();
    return context;
  }

  class MockServiceScheduler extends ServiceScheduler {
    private AsyncDispatcher dispatcher;
    private AMRMClientCallback callbackHandler = new AMRMClientCallback();

    MockServiceScheduler(ServiceContext context) {
      super(context);
    }

    @Override
    protected AsyncDispatcher createAsyncDispatcher() {
      dispatcher = Mockito.mock(AsyncDispatcher.class);
      EventHandler<Event> handler = Mockito.mock(EventHandler.class);
      Mockito.doReturn(handler).when(dispatcher).getEventHandler();
      return dispatcher;
    }

    @Override
    protected AMRMClientAsync<AMRMClient.ContainerRequest> createAMRMClient() {
      return AMRMClientAsync.createAMRMClientAsync(1000, callbackHandler);
    }

  }

  @Test
  public void testRecordTokensForContainers() throws Exception {
    ApplicationId applicationId = ApplicationId.newInstance(123456, 1);
    Service exampleApp = new Service();
    exampleApp.setId(applicationId.toString());
    exampleApp.setName("testContainerCompleted");
    exampleApp.addComponent(createComponent("compa", 1, "pwd"));

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
    Credentials dockerCred =
        DockerClientConfigHandler.readCredentialsFromConfigFile(
            new Path(dockerConfig), conf, applicationId.toString());


    MockServiceAM am = new MockServiceAM(exampleApp, dockerCred);
    ByteBuffer amCredBuffer = am.recordTokensForContainers();
    Credentials amCreds =
        DockerClientConfigHandler.getCredentialsFromTokensByteBuffer(
            amCredBuffer);

    assertEquals(2, amCreds.numberOfTokens());
    for (Token<? extends TokenIdentifier> tk : amCreds.getAllTokens()) {
      Assert.assertTrue(
          tk.getKind().equals(DockerCredentialTokenIdentifier.KIND));
    }

    am.stop();
  }

  @Test
  public void testIPChange() throws TimeoutException,
      InterruptedException {
    ApplicationId applicationId = ApplicationId.newInstance(123456, 1);
    String comp1Name = "comp1";
    String comp1InstName = "comp1-0";
    Service exampleApp = new Service();
    exampleApp.setId(applicationId.toString());
    exampleApp.setVersion("v1");
    exampleApp.setName("testIPChange");
    Component comp1 = createComponent(comp1Name, 1, "sleep 60");
    comp1.setArtifact(new Artifact().type(Artifact.TypeEnum.DOCKER));
    exampleApp.addComponent(comp1);

    MockServiceAM am = new MockServiceAM(exampleApp);
    am.init(conf);
    am.start();

    ComponentInstance comp1inst0 = am.getCompInstance(comp1Name, comp1InstName);
    // allocate a container
    am.feedContainerToComp(exampleApp, 1, comp1Name);
    GenericTestUtils.waitFor(() -> comp1inst0.getContainerStatus() != null,
        2000, 200000);
    // first host status will match the container nodeId
    Assert.assertEquals("localhost",
        comp1inst0.getContainerStatus().getHost());

    LOG.info("Change the IP and host");
    // change the container status
    am.updateContainerStatus(exampleApp, 1, comp1Name, "new.host");
    GenericTestUtils.waitFor(() -> comp1inst0.getContainerStatus().getHost()
        .equals("new.host"), 2000, 200000);

    LOG.info("Change the IP and host again");
    // change the container status
    am.updateContainerStatus(exampleApp, 1, comp1Name, "newer.host");
    GenericTestUtils.waitFor(() -> comp1inst0.getContainerStatus().getHost()
        .equals("newer.host"), 2000, 200000);
    am.stop();
  }

  // Test to verify that the containers are released and the
  // component instance is added to the pending queue when building the launch
  // context fails.
  @Test(timeout = 30000)
  public void testContainersReleasedWhenPreLaunchFails()
      throws Exception {
    ApplicationId applicationId = ApplicationId.newInstance(
        System.currentTimeMillis(), 1);
    Service exampleApp = new Service();
    exampleApp.setId(applicationId.toString());
    exampleApp.setVersion("v1");
    exampleApp.setName("testContainersReleasedWhenPreLaunchFails");

    Component compA = createComponent("compa", 1, "pwd");
    Artifact artifact = new Artifact();
    artifact.setType(Artifact.TypeEnum.TARBALL);
    compA.artifact(artifact);
    exampleApp.addComponent(compA);

    MockServiceAM am = new MockServiceAM(exampleApp);
    am.init(conf);
    am.start();

    ContainerId containerId = am.createContainerId(1);

    // allocate a container
    am.feedContainerToComp(exampleApp, containerId, "compa");
    am.waitForContainerToRelease(containerId);
    ComponentInstance compAinst0 = am.getCompInstance(compA.getName(),
        "compa-0");
    GenericTestUtils.waitFor(() ->
        am.getComponent(compA.getName()).getPendingInstances()
        .contains(compAinst0), 2000, 30000);

    Assert.assertEquals(1,
        am.getComponent("compa").getPendingInstances().size());
    am.stop();
  }

  @Test(timeout = 30000)
  public void testSyncSysFS() {
    ApplicationId applicationId = ApplicationId.newInstance(
        System.currentTimeMillis(), 1);
    Service exampleApp = new Service();
    exampleApp.setId(applicationId.toString());
    exampleApp.setVersion("v1");
    exampleApp.setName("tensorflow");

    Component compA = createComponent("compa", 1, "pwd");
    compA.getConfiguration().getEnv().put(
        "YARN_CONTAINER_RUNTIME_YARN_SYSFS_ENABLE", "true");
    Artifact artifact = new Artifact();
    artifact.setType(Artifact.TypeEnum.TARBALL);
    compA.artifact(artifact);
    exampleApp.addComponent(compA);
    try {
      MockServiceAM am = new MockServiceAM(exampleApp);
      am.init(conf);
      am.start();
      ServiceScheduler scheduler = am.context.scheduler;
      scheduler.syncSysFs(exampleApp);
      scheduler.close();
      am.stop();
      am.close();
    } catch (Exception e) {
      LOG.error("Fail to sync sysfs: {}", e);
      Assert.fail("Fail to sync sysfs.");
    }
  }

  @Test
  public void testScheduleWithResourceAttributes() throws Exception {
    ApplicationId applicationId = ApplicationId.newInstance(123456, 1);
    Service exampleApp = new Service();
    exampleApp.setId(applicationId.toString());
    exampleApp.setName("testScheduleWithResourceAttributes");
    exampleApp.setVersion("v1");

    List<ResourceTypeInfo> resourceTypeInfos = new ArrayList<>(
        ResourceUtils.getResourcesTypeInfo());
    // Add 3rd resource type.
    resourceTypeInfos.add(ResourceTypeInfo
        .newInstance("test-resource", "", ResourceTypes.COUNTABLE));
    // Reinitialize resource types
    ResourceUtils.reinitializeResources(resourceTypeInfos);

    Component serviceCompoent = createComponent("compa", 1, "pwd");
    serviceCompoent.getResource().setResourceInformations(
        ImmutableMap.of("test-resource",
            new ResourceInformation()
                .value(1234L)
                .unit("Gi")
                .attributes(ImmutableMap.of("k1", "v1", "k2", "v2"))));
    exampleApp.addComponent(serviceCompoent);

    MockServiceAM am = new MockServiceAM(exampleApp);
    am.init(conf);
    am.start();

    ServiceScheduler serviceScheduler = am.context.scheduler;
    AMRMClientAsync<AMRMClient.ContainerRequest> amrmClientAsync =
        serviceScheduler.getAmRMClient();

    Collection<AMRMClient.ContainerRequest> rr =
        amrmClientAsync.getMatchingRequests(0);
    Assert.assertEquals(1, rr.size());

    org.apache.hadoop.yarn.api.records.Resource capability =
        rr.iterator().next().getCapability();
    Assert.assertEquals(1234L, capability.getResourceValue("test-resource"));
    Assert.assertEquals("Gi",
        capability.getResourceInformation("test-resource").getUnits());
    Assert.assertEquals(2, capability.getResourceInformation("test-resource")
        .getAttributes().size());
    am.stop();
  }
}
