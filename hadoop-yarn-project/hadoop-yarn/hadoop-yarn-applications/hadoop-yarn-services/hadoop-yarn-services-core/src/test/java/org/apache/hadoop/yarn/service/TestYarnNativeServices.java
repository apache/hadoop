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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.ComponentState;
import org.apache.hadoop.yarn.service.api.records.Container;
import org.apache.hadoop.yarn.service.api.records.ContainerState;
import org.apache.hadoop.yarn.service.api.records.PlacementConstraint;
import org.apache.hadoop.yarn.service.api.records.PlacementPolicy;
import org.apache.hadoop.yarn.service.api.records.PlacementScope;
import org.apache.hadoop.yarn.service.api.records.PlacementType;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
import org.apache.hadoop.yarn.service.client.ServiceClient;
import org.apache.hadoop.yarn.service.conf.YarnServiceConstants;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.apache.hadoop.yarn.service.utils.SliderFileSystem;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.yarn.api.records.YarnApplicationState.FINISHED;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConf.YARN_SERVICE_BASE_PATH;
import static org.apache.hadoop.yarn.service.exceptions.LauncherExitCodes.EXIT_COMMAND_ARGUMENT_ERROR;
import static org.apache.hadoop.yarn.service.exceptions.LauncherExitCodes.EXIT_NOT_FOUND;

/**
 * End to end tests to test deploying services with MiniYarnCluster and a in-JVM
 * ZK testing cluster.
 */
public class TestYarnNativeServices extends ServiceTestUtils {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestYarnNativeServices.class);

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Before
  public void setup() throws Exception {
    File tmpYarnDir = new File("target", "tmp");
    FileUtils.deleteQuietly(tmpYarnDir);
  }

  @After
  public void tearDown() throws IOException {
    shutdown();
  }

  // End-to-end test to use ServiceClient to deploy a service.
  // 1. Create a service with 2 components, each of which has 2 containers
  // 2. Flex up each component to 3 containers and check the component instance names
  // 3. Flex down each component to 1 container and check the component instance names
  // 4. Flex up each component to 2 containers and check the component instance names
  // 5. Stop the service
  // 6. Destroy the service
  @Test (timeout = 200000)
  public void testCreateFlexStopDestroyService() throws Exception {
    setupInternal(NUM_NMS);
    ServiceClient client = createClient(getConf());
    Service exampleApp = createExampleApplication();
    client.actionCreate(exampleApp);
    SliderFileSystem fileSystem = new SliderFileSystem(getConf());
    Path appDir = fileSystem.buildClusterDirPath(exampleApp.getName());
    // check app.json is persisted.
    Assert.assertTrue(
        getFS().exists(new Path(appDir, exampleApp.getName() + ".json")));
    waitForServiceToBeStable(client, exampleApp);

    // Flex two components, each from 2 container to 3 containers.
    flexComponents(client, exampleApp, 3L);
    // wait for flex to be completed, increase from 2 to 3 containers.
    waitForServiceToBeStable(client, exampleApp);
    // check all instances name for each component are in sequential order.
    checkCompInstancesInOrder(client, exampleApp);

    // flex down to 1
    flexComponents(client, exampleApp, 1L);
    waitForServiceToBeStable(client, exampleApp);
    checkCompInstancesInOrder(client, exampleApp);

    // check component dir and registry are cleaned up.

    // flex up again to 2
    flexComponents(client, exampleApp, 2L);
    waitForServiceToBeStable(client, exampleApp);
    checkCompInstancesInOrder(client, exampleApp);

    // stop the service
    LOG.info("Stop the service");
    client.actionStop(exampleApp.getName(), true);
    ApplicationReport report = client.getYarnClient()
        .getApplicationReport(ApplicationId.fromString(exampleApp.getId()));
    // AM unregisters with RM successfully
    Assert.assertEquals(FINISHED, report.getYarnApplicationState());
    Assert.assertEquals(FinalApplicationStatus.ENDED,
        report.getFinalApplicationStatus());
    String serviceZKPath = RegistryUtils.servicePath(RegistryUtils
        .currentUser(), YarnServiceConstants.APP_TYPE, exampleApp.getName());
    Assert.assertFalse("Registry ZK service path still exists after stop",
        getCuratorService().zkPathExists(serviceZKPath));

    LOG.info("Destroy the service");
    // destroy the service and check the app dir is deleted from fs.
    Assert.assertEquals(0, client.actionDestroy(exampleApp.getName()));
    // check the service dir on hdfs (in this case, local fs) are deleted.
    Assert.assertFalse(getFS().exists(appDir));

    // check that destroying again does not succeed
    Assert.assertEquals(EXIT_NOT_FOUND, client.actionDestroy(exampleApp.getName()));
  }

  // Save a service without starting it and ensure that stop does not NPE and
  // that service can be successfully destroyed
  @Test (timeout = 200000)
  public void testStopDestroySavedService() throws Exception {
    setupInternal(NUM_NMS);
    ServiceClient client = createClient(getConf());
    Service exampleApp = createExampleApplication();
    client.actionBuild(exampleApp);
    Assert.assertEquals(EXIT_COMMAND_ARGUMENT_ERROR, client.actionStop(
        exampleApp.getName()));
    Assert.assertEquals(0, client.actionDestroy(exampleApp.getName()));
  }

  // Create compa with 2 containers
  // Create compb with 2 containers which depends on compa
  // Create compc with 2 containers which depends on compb
  // Check containers for compa started before containers for compb before
  // containers for compc
  @Test (timeout = 200000)
  public void testComponentStartOrder() throws Exception {
    setupInternal(NUM_NMS);
    ServiceClient client = createClient(getConf());
    Service exampleApp = new Service();
    exampleApp.setName("teststartorder");
    exampleApp.setVersion("v1");
    exampleApp.addComponent(createComponent("compa", 2, "sleep 1000"));

    // Let compb depend on compa
    Component compb = createComponent("compb", 2, "sleep 1000");
    compb.setDependencies(Collections.singletonList("compa"));
    exampleApp.addComponent(compb);

    // Let compc depend on compb
    Component compc = createComponent("compc", 2, "sleep 1000");
    compc.setDependencies(Collections.singletonList("compb"));
    exampleApp.addComponent(compc);

    client.actionCreate(exampleApp);
    waitForServiceToBeStable(client, exampleApp);

    // check that containers for compa are launched before containers for compb
    checkContainerLaunchDependencies(client, exampleApp, "compa", "compb",
        "compc");

    client.actionStop(exampleApp.getName(), true);
    client.actionDestroy(exampleApp.getName());
  }

  @Test(timeout = 200000)
  public void testCreateServiceSameNameDifferentUser() throws Exception {
    String sameAppName = "same-name";
    String userA = "usera";
    String userB = "userb";

    setupInternal(NUM_NMS);
    ServiceClient client = createClient(getConf());
    String origBasePath = getConf().get(YARN_SERVICE_BASE_PATH);

    Service userAApp = new Service();
    userAApp.setName(sameAppName);
    userAApp.setVersion("v1");
    userAApp.addComponent(createComponent("comp", 1, "sleep 1000"));

    Service userBApp = new Service();
    userBApp.setName(sameAppName);
    userBApp.setVersion("v1");
    userBApp.addComponent(createComponent("comp", 1, "sleep 1000"));

    File userABasePath = null, userBBasePath = null;
    try {
      userABasePath = new File(origBasePath, userA);
      userABasePath.mkdirs();
      getConf().set(YARN_SERVICE_BASE_PATH, userABasePath.getAbsolutePath());
      client.actionCreate(userAApp);
      waitForServiceToBeStarted(client, userAApp);

      userBBasePath = new File(origBasePath, userB);
      userBBasePath.mkdirs();
      getConf().set(YARN_SERVICE_BASE_PATH, userBBasePath.getAbsolutePath());
      client.actionBuild(userBApp);
    } catch (Exception e) {
      Assert
          .fail("Exception should not be thrown - " + e.getLocalizedMessage());
    } finally {
      if (userABasePath != null) {
        getConf().set(YARN_SERVICE_BASE_PATH, userABasePath.getAbsolutePath());
        client.actionStop(sameAppName, true);
        client.actionDestroy(sameAppName);
      }
      if (userBBasePath != null) {
        getConf().set(YARN_SERVICE_BASE_PATH, userBBasePath.getAbsolutePath());
        client.actionDestroy(sameAppName);
      }
    }

    // Need to extend this test to validate that different users can create
    // apps of exact same name. So far only create followed by build is tested.
    // Need to test create followed by create.
  }

  @Test(timeout = 200000)
  public void testCreateServiceSameNameSameUser() throws Exception {
    String sameAppName = "same-name";
    String user = UserGroupInformation.getCurrentUser().getUserName();
    System.setProperty("user.name", user);

    setupInternal(NUM_NMS);
    ServiceClient client = createClient(getConf());

    Service appA = new Service();
    appA.setName(sameAppName);
    appA.setVersion("v1");
    appA.addComponent(createComponent("comp", 1, "sleep 1000"));

    Service appB = new Service();
    appB.setName(sameAppName);
    appB.setVersion("v1");
    appB.addComponent(createComponent("comp", 1, "sleep 1000"));

    try {
      client.actionBuild(appA);
      client.actionBuild(appB);
    } catch (Exception e) {
      String expectedMsg = "Service Instance dir already exists:";
      if (e.getLocalizedMessage() != null) {
        Assert.assertThat(e.getLocalizedMessage(),
            CoreMatchers.containsString(expectedMsg));
      } else {
        Assert.fail("Message cannot be null. It has to say - " + expectedMsg);
      }
    } finally {
      // cleanup
      client.actionDestroy(sameAppName);
    }

    try {
      client.actionCreate(appA);
      waitForServiceToBeStarted(client, appA);

      client.actionCreate(appB);
      waitForServiceToBeStarted(client, appB);
    } catch (Exception e) {
      String expectedMsg = "Failed to create service " + sameAppName
          + ", because it already exists.";
      if (e.getLocalizedMessage() != null) {
        Assert.assertThat(e.getLocalizedMessage(),
            CoreMatchers.containsString(expectedMsg));
      } else {
        Assert.fail("Message cannot be null. It has to say - " + expectedMsg);
      }
    } finally {
      // cleanup
      client.actionStop(sameAppName, true);
      client.actionDestroy(sameAppName);
    }
  }

  // Test to verify recovery of SeviceMaster after RM is restarted.
  // 1. Create an example service.
  // 2. Restart RM.
  // 3. Fail the application attempt.
  // 4. Verify ServiceMaster recovers.
  @Test(timeout = 200000)
  public void testRecoverComponentsAfterRMRestart() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    conf.setBoolean(
        YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED, true);
    conf.setLong(YarnConfiguration.NM_RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS,
        500L);

    conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
    conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_USE_RPC, true);
    setConf(conf);
    setupInternal(NUM_NMS);

    ServiceClient client = createClient(getConf());
    Service exampleApp = createExampleApplication();
    client.actionCreate(exampleApp);
    Multimap<String, String> containersBeforeFailure =
        waitForAllCompToBeReady(client, exampleApp);

    LOG.info("Restart the resource manager");
    getYarnCluster().restartResourceManager(
        getYarnCluster().getActiveRMIndex());
    GenericTestUtils.waitFor(() ->
        getYarnCluster().getResourceManager().getServiceState() ==
            org.apache.hadoop.service.Service.STATE.STARTED, 2000, 200000);
    Assert.assertTrue("node managers connected",
        getYarnCluster().waitForNodeManagersToConnect(5000));

    ApplicationId exampleAppId = ApplicationId.fromString(exampleApp.getId());
    ApplicationAttemptId applicationAttemptId = client.getYarnClient()
        .getApplicationReport(exampleAppId).getCurrentApplicationAttemptId();

    LOG.info("Fail the application attempt {}", applicationAttemptId);
    client.getYarnClient().failApplicationAttempt(applicationAttemptId);
    //wait until attempt 2 is running
    GenericTestUtils.waitFor(() -> {
      try {
        ApplicationReport ar = client.getYarnClient()
            .getApplicationReport(exampleAppId);
        return ar.getCurrentApplicationAttemptId().getAttemptId() == 2 &&
            ar.getYarnApplicationState() == YarnApplicationState.RUNNING;
      } catch (YarnException | IOException e) {
        throw new RuntimeException("while waiting", e);
      }
    }, 2000, 200000);

    Multimap<String, String> containersAfterFailure = waitForAllCompToBeReady(
        client, exampleApp);
    Assert.assertEquals("component container affected by restart",
        containersBeforeFailure, containersAfterFailure);

    LOG.info("Stop/destroy service {}", exampleApp);
    client.actionStop(exampleApp.getName(), true);
    client.actionDestroy(exampleApp.getName());
  }

  @Test(timeout = 200000)
  public void testUpgradeService() throws Exception {
    setupInternal(NUM_NMS);
    ServiceClient client = createClient(getConf());

    Service service = createExampleApplication();
    client.actionCreate(service);
    waitForServiceToBeStarted(client, service);

    //upgrade the service
    service.setVersion("v2");
    client.actionUpgrade(service);

    //wait for service to be in upgrade state
    waitForServiceToBeInState(client, service, ServiceState.UPGRADING);
    SliderFileSystem fs = new SliderFileSystem(getConf());
    Service fromFs = ServiceApiUtil.loadServiceUpgrade(fs,
        service.getName(), service.getVersion());
    Assert.assertEquals(service.getName(), fromFs.getName());
    Assert.assertEquals(service.getVersion(), fromFs.getVersion());
  }

  // Test to verify ANTI_AFFINITY placement policy
  // 1. Start mini cluster with 3 NMs and scheduler placement-constraint handler
  // 2. Create an example service with 3 containers
  // 3. Verify no more than 1 container comes up in each of the 3 NMs
  // 4. Flex the component to 4 containers
  // 5. Verify that the 4th container does not even get allocated since there
  //    are only 3 NMs
  @Test (timeout = 200000)
  public void testCreateServiceWithPlacementPolicy() throws Exception {
    // We need to enable scheduler placement-constraint at the cluster level to
    // let apps use placement policies.
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER);
    setConf(conf);
    setupInternal(3);
    ServiceClient client = createClient(getConf());
    Service exampleApp = new Service();
    exampleApp.setName("example-app");
    exampleApp.setVersion("v1");
    Component comp = createComponent("compa", 3L, "sleep 1000");
    PlacementPolicy pp = new PlacementPolicy();
    PlacementConstraint pc = new PlacementConstraint();
    pc.setName("CA1");
    pc.setTargetTags(Collections.singletonList("compa"));
    pc.setScope(PlacementScope.NODE);
    pc.setType(PlacementType.ANTI_AFFINITY);
    pp.setConstraints(Collections.singletonList(pc));
    comp.setPlacementPolicy(pp);
    exampleApp.addComponent(comp);
    client.actionCreate(exampleApp);
    waitForServiceToBeStable(client, exampleApp);

    // Check service is stable and all 3 containers are running
    Service service = client.getStatus(exampleApp.getName());
    Component component = service.getComponent("compa");
    Assert.assertEquals("Service state should be STABLE", ServiceState.STABLE,
        service.getState());
    Assert.assertEquals("3 containers are expected to be running", 3,
        component.getContainers().size());
    // Prepare a map of non-AM containers for later lookup
    Set<String> nonAMContainerIdSet = new HashSet<>();
    for (Container cont : component.getContainers()) {
      nonAMContainerIdSet.add(cont.getId());
    }

    // Verify that no more than 1 non-AM container came up on each of the 3 NMs
    Set<String> hosts = new HashSet<>();
    ApplicationReport report = client.getYarnClient()
        .getApplicationReport(ApplicationId.fromString(exampleApp.getId()));
    GetContainersRequest req = GetContainersRequest
        .newInstance(report.getCurrentApplicationAttemptId());
    ResourceManager rm = getYarnCluster().getResourceManager();
    for (ContainerReport contReport : rm.getClientRMService().getContainers(req)
        .getContainerList()) {
      if (!nonAMContainerIdSet
          .contains(contReport.getContainerId().toString())) {
        continue;
      }
      if (hosts.contains(contReport.getNodeHttpAddress())) {
        Assert.fail("Container " + contReport.getContainerId()
            + " came up in the same host as another container.");
      } else {
        hosts.add(contReport.getNodeHttpAddress());
      }
    }

    // Flex compa up to 4, which is more containers than the no of NMs
    Map<String, Long> compCounts = new HashMap<>();
    compCounts.put("compa", 4L);
    exampleApp.getComponent("compa").setNumberOfContainers(4L);
    client.flexByRestService(exampleApp.getName(), compCounts);
    try {
      // 10 secs is enough for the container to be started. The down side of
      // this test is that it has to wait that long. Setting a higher wait time
      // will add to the total time taken by tests to run.
      waitForServiceToBeStable(client, exampleApp, 10000);
      Assert.fail("Service should not be in a stable state. It should throw "
          + "a timeout exception.");
    } catch (Exception e) {
      // Check that service state is not STABLE and only 3 containers are
      // running and the fourth one should not get allocated.
      service = client.getStatus(exampleApp.getName());
      component = service.getComponent("compa");
      Assert.assertNotEquals("Service state should not be STABLE",
          ServiceState.STABLE, service.getState());
      Assert.assertEquals("Component state should be FLEXING",
          ComponentState.FLEXING, component.getState());
      Assert.assertEquals("3 containers are expected to be running", 3,
          component.getContainers().size());
    }

    LOG.info("Stop/destroy service {}", exampleApp);
    client.actionStop(exampleApp.getName(), true);
    client.actionDestroy(exampleApp.getName());
  }

  @Test(timeout = 200000)
  public void testAMSigtermDoesNotKillApplication() throws Exception {
    runAMSignalTest(SignalContainerCommand.GRACEFUL_SHUTDOWN);
  }

  @Test(timeout = 200000)
  public void testAMSigkillDoesNotKillApplication() throws Exception {
    runAMSignalTest(SignalContainerCommand.FORCEFUL_SHUTDOWN);
  }

  public void runAMSignalTest(SignalContainerCommand signal) throws Exception {
    setupInternal(NUM_NMS);
    ServiceClient client = createClient(getConf());
    Service exampleApp = createExampleApplication();
    client.actionCreate(exampleApp);
    waitForServiceToBeStable(client, exampleApp);
    Service appStatus1 = client.getStatus(exampleApp.getName());
    ApplicationId exampleAppId = ApplicationId.fromString(appStatus1.getId());

    YarnClient yarnClient = createYarnClient(getConf());
    ApplicationReport applicationReport = yarnClient.getApplicationReport(
        exampleAppId);

    ApplicationAttemptId firstAttemptId = applicationReport
        .getCurrentApplicationAttemptId();
    ApplicationAttemptReport attemptReport = yarnClient
        .getApplicationAttemptReport(firstAttemptId);

    // the AM should not perform a graceful shutdown since the operation was not
    // initiated through the service client
    yarnClient.signalToContainer(attemptReport.getAMContainerId(), signal);

    GenericTestUtils.waitFor(() -> {
      try {
        ApplicationReport ar = client.getYarnClient()
            .getApplicationReport(exampleAppId);
        YarnApplicationState state = ar.getYarnApplicationState();
        Assert.assertTrue(state == YarnApplicationState.RUNNING ||
            state == YarnApplicationState.ACCEPTED);
        if (state != YarnApplicationState.RUNNING) {
          return false;
        }
        if (ar.getCurrentApplicationAttemptId() == null ||
            ar.getCurrentApplicationAttemptId().equals(firstAttemptId)) {
          return false;
        }
        Service appStatus2 = client.getStatus(exampleApp.getName());
        if (appStatus2.getState() != ServiceState.STABLE) {
          return false;
        }
        Assert.assertEquals(getSortedContainerIds(appStatus1).toString(),
            getSortedContainerIds(appStatus2).toString());
        return true;
      } catch (YarnException | IOException e) {
        throw new RuntimeException("while waiting", e);
      }
    }, 2000, 200000);
  }

  private static List<String> getSortedContainerIds(Service s) {
    List<String> containerIds = new ArrayList<>();
    for (Component component : s.getComponents()) {
      for (Container container : component.getContainers()) {
        containerIds.add(container.getId());
      }
    }
    Collections.sort(containerIds);
    return containerIds;
  }

  // Check containers launched are in dependency order
  // Get all containers into a list and sort based on container launch time e.g.
  // compa-c1, compa-c2, compb-c1, compb-c2;
  // check that the container's launch time are align with the dependencies.
  private void checkContainerLaunchDependencies(ServiceClient client,
      Service exampleApp, String... compOrder)
      throws IOException, YarnException {
    Service retrievedApp = client.getStatus(exampleApp.getName());
    List<Container> containerList = new ArrayList<>();
    for (Component component : retrievedApp.getComponents()) {
      containerList.addAll(component.getContainers());
    }
    // sort based on launchTime
    containerList
        .sort((o1, o2) -> o1.getLaunchTime().compareTo(o2.getLaunchTime()));
    LOG.info("containerList: " + containerList);
    // check the containers are in the dependency order.
    int index = 0;
    for (String comp : compOrder) {
      long num = retrievedApp.getComponent(comp).getNumberOfContainers();
      for (int i = 0; i < num; i++) {
        String compInstanceName = containerList.get(index).getComponentInstanceName();
        String compName =
            compInstanceName.substring(0, compInstanceName.lastIndexOf('-'));
        Assert.assertEquals(comp, compName);
        index++;
      }
    }
  }


  private Map<String, Long> flexComponents(ServiceClient client,
      Service exampleApp, long count) throws YarnException, IOException {
    Map<String, Long> compCounts = new HashMap<>();
    compCounts.put("compa", count);
    compCounts.put("compb", count);
    // flex will update the persisted conf to reflect latest number of containers.
    exampleApp.getComponent("compa").setNumberOfContainers(count);
    exampleApp.getComponent("compb").setNumberOfContainers(count);
    client.flexByRestService(exampleApp.getName(), compCounts);
    return compCounts;
  }

  // Check each component's comp instances name are in sequential order.
  // E.g. If there are two instances compA-1 and compA-2
  // When flex up to 4 instances, it should be compA-1 , compA-2, compA-3, compA-4
  // When flex down to 3 instances,  it should be compA-1 , compA-2, compA-3.
  private void checkCompInstancesInOrder(ServiceClient client,
      Service exampleApp) throws IOException, YarnException,
      TimeoutException, InterruptedException {
    Service service = client.getStatus(exampleApp.getName());
    for (Component comp : service.getComponents()) {
      checkEachCompInstancesInOrder(comp, exampleApp.getName());
    }
  }

  private void checkEachCompInstancesInOrder(Component component, String
      serviceName) throws TimeoutException, InterruptedException {
    long expectedNumInstances = component.getNumberOfContainers();
    Assert.assertEquals(expectedNumInstances, component.getContainers().size());
    TreeSet<String> instances = new TreeSet<>();
    for (Container container : component.getContainers()) {
      instances.add(container.getComponentInstanceName());
      String componentZKPath = RegistryUtils.componentPath(RegistryUtils
          .currentUser(), YarnServiceConstants.APP_TYPE, serviceName,
          RegistryPathUtils.encodeYarnID(container.getId()));
      GenericTestUtils.waitFor(() -> {
        try {
          return getCuratorService().zkPathExists(componentZKPath);
        } catch (IOException e) {
          return false;
        }
      }, 1000, 60000);
    }

    int i = 0;
    for (String s : instances) {
      Assert.assertEquals(component.getName() + "-" + i, s);
      i++;
    }
  }

  /**
   * Wait until all the containers for all components become ready state.
   *
   * @param client
   * @param exampleApp
   * @return all ready containers of a service.
   * @throws TimeoutException
   * @throws InterruptedException
   */
  private Multimap<String, String> waitForAllCompToBeReady(ServiceClient client,
      Service exampleApp) throws TimeoutException, InterruptedException {
    int expectedTotalContainers = countTotalContainers(exampleApp);

    Multimap<String, String> allContainers = HashMultimap.create();

    GenericTestUtils.waitFor(() -> {
      try {
        Service retrievedApp = client.getStatus(exampleApp.getName());
        int totalReadyContainers = 0;
        allContainers.clear();
        LOG.info("Num Components " + retrievedApp.getComponents().size());
        for (Component component : retrievedApp.getComponents()) {
          LOG.info("looking for  " + component.getName());
          LOG.info(component.toString());
          if (component.getContainers() != null) {
            if (component.getContainers().size() == exampleApp
                .getComponent(component.getName()).getNumberOfContainers()) {
              for (Container container : component.getContainers()) {
                LOG.info(
                    "Container state " + container.getState() + ", component "
                        + component.getName());
                if (container.getState() == ContainerState.READY) {
                  totalReadyContainers++;
                  allContainers.put(component.getName(), container.getId());
                  LOG.info("Found 1 ready container " + container.getId());
                }
              }
            } else {
              LOG.info(component.getName() + " Expected number of containers "
                  + exampleApp.getComponent(component.getName())
                  .getNumberOfContainers() + ", current = " + component
                  .getContainers());
            }
          }
        }
        LOG.info("Exit loop, totalReadyContainers= " + totalReadyContainers
            + " expected = " + expectedTotalContainers);
        return totalReadyContainers == expectedTotalContainers;
      } catch (Exception e) {
        e.printStackTrace();
        return false;
      }
    }, 2000, 200000);
    return allContainers;
  }

  /**
   * Wait until service state becomes stable. A service is stable when all
   * requested containers of all components are running and in ready state.
   *
   * @param client
   * @param exampleApp
   * @throws TimeoutException
   * @throws InterruptedException
   */
  private void waitForServiceToBeStable(ServiceClient client,
      Service exampleApp) throws TimeoutException, InterruptedException {
    waitForServiceToBeStable(client, exampleApp, 200000);
  }

  private void waitForServiceToBeStable(ServiceClient client,
      Service exampleApp, int waitForMillis)
      throws TimeoutException, InterruptedException {
    waitForServiceToBeInState(client, exampleApp, ServiceState.STABLE,
        waitForMillis);
  }

  /**
   * Wait until service is started. It does not have to reach a stable state.
   *
   * @param client
   * @param exampleApp
   * @throws TimeoutException
   * @throws InterruptedException
   */
  private void waitForServiceToBeStarted(ServiceClient client,
      Service exampleApp) throws TimeoutException, InterruptedException {
    waitForServiceToBeInState(client, exampleApp, ServiceState.STARTED);
  }

  private void waitForServiceToBeInState(ServiceClient client,
      Service exampleApp, ServiceState desiredState) throws TimeoutException,
      InterruptedException {
    waitForServiceToBeInState(client, exampleApp, desiredState, 200000);
  }

  /**
   * Wait until service is started. It does not have to reach a stable state.
   *
   * @param client
   * @param exampleApp
   * @throws TimeoutException
   * @throws InterruptedException
   */
  private void waitForServiceToBeInState(ServiceClient client,
      Service exampleApp, ServiceState desiredState, int waitForMillis) throws
      TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> {
      try {
        Service retrievedApp = client.getStatus(exampleApp.getName());
        System.out.println(retrievedApp);
        return retrievedApp.getState() == desiredState;
      } catch (Exception e) {
        e.printStackTrace();
        return false;
      }
    }, 2000, waitForMillis);
  }

  private int countTotalContainers(Service service) {
    int totalContainers = 0;
    for (Component component : service.getComponents()) {
      totalContainers += component.getNumberOfContainers();
    }
    return totalContainers;
  }
}
