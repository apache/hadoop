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
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.Container;
import org.apache.hadoop.yarn.service.api.records.ContainerState;
import org.apache.hadoop.yarn.service.client.ServiceClient;
import org.apache.hadoop.yarn.service.exceptions.SliderException;
import org.apache.hadoop.yarn.service.utils.SliderFileSystem;
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
    ServiceClient client = createClient();
    Service exampleApp = createExampleApplication();
    client.actionCreate(exampleApp);
    SliderFileSystem fileSystem = new SliderFileSystem(getConf());
    Path appDir = fileSystem.buildClusterDirPath(exampleApp.getName());
    // check app.json is persisted.
    Assert.assertTrue(
        getFS().exists(new Path(appDir, exampleApp.getName() + ".json")));
    waitForAllCompToBeReady(client, exampleApp);

    // Flex two components, each from 2 container to 3 containers.
    flexComponents(client, exampleApp, 3L);
    // wait for flex to be completed, increase from 2 to 3 containers.
    waitForAllCompToBeReady(client, exampleApp);
    // check all instances name for each component are in sequential order.
    checkCompInstancesInOrder(client, exampleApp);

    // flex down to 1
    flexComponents(client, exampleApp, 1L);
    waitForAllCompToBeReady(client, exampleApp);
    checkCompInstancesInOrder(client, exampleApp);

    // check component dir and registry are cleaned up.

    // flex up again to 2
    flexComponents(client, exampleApp, 2L);
    waitForAllCompToBeReady(client, exampleApp);
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

    LOG.info("Destroy the service");
    //destroy the service and check the app dir is deleted from fs.
    client.actionDestroy(exampleApp.getName());
    // check the service dir on hdfs (in this case, local fs) are deleted.
    Assert.assertFalse(getFS().exists(appDir));
  }

  // Create compa with 2 containers
  // Create compb with 2 containers which depends on compa
  // Check containers for compa started before containers for compb
  @Test (timeout = 200000)
  public void testComponentStartOrder() throws Exception {
    setupInternal(NUM_NMS);
    ServiceClient client = createClient();
    Service exampleApp = new Service();
    exampleApp.setName("teststartorder");
    exampleApp.addComponent(createComponent("compa", 2, "sleep 1000"));
    Component compb = createComponent("compb", 2, "sleep 1000");

    // Let compb depedends on compa;
    compb.setDependencies(Collections.singletonList("compa"));
    exampleApp.addComponent(compb);

    client.actionCreate(exampleApp);
    waitForAllCompToBeReady(client, exampleApp);

    // check that containers for compa are launched before containers for compb
    checkContainerLaunchDependencies(client, exampleApp, "compa", "compb");

    client.actionStop(exampleApp.getName(), true);
    client.actionDestroy(exampleApp.getName());
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

    ServiceClient client = createClient();
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
      Service exampleApp) throws IOException, YarnException {
    Service service = client.getStatus(exampleApp.getName());
    for (Component comp : service.getComponents()) {
      checkEachCompInstancesInOrder(comp);
    }
  }

  private void checkRegistryAndCompDirDeleted() {

  }

  private void checkEachCompInstancesInOrder(Component component) {
    long expectedNumInstances = component.getNumberOfContainers();
    Assert.assertEquals(expectedNumInstances, component.getContainers().size());
    TreeSet<String> instances = new TreeSet<>();
    for (Container container : component.getContainers()) {
      instances.add(container.getComponentInstanceName());
    }

    int i = 0;
    for (String s : instances) {
      Assert.assertEquals(component.getName() + "-" + i, s);
      i++;
    }
  }

  private void waitForOneCompToBeReady(ServiceClient client,
      Service exampleApp, String readyComp)
      throws TimeoutException, InterruptedException {
    long numExpectedContainers =
        exampleApp.getComponent(readyComp).getNumberOfContainers();
    GenericTestUtils.waitFor(() -> {
      try {
        Service retrievedApp = client.getStatus(exampleApp.getName());
        Component retrievedComp = retrievedApp.getComponent(readyComp);

        if (retrievedComp.getContainers() != null
            && retrievedComp.getContainers().size() == numExpectedContainers) {
          LOG.info(readyComp + " found " + numExpectedContainers
              + " containers running");
          return true;
        } else {
          LOG.info(" Waiting for " + readyComp + "'s containers to be running");
          return false;
        }
      } catch (Exception e) {
        e.printStackTrace();
        return false;
      }
    }, 2000, 200000);
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

  private ServiceClient createClient() throws Exception {
    ServiceClient client = new ServiceClient() {
      @Override protected Path addJarResource(String appName,
          Map<String, LocalResource> localResources)
          throws IOException, SliderException {
        // do nothing, the Unit test will use local jars
        return null;
      }
    };
    client.init(getConf());
    client.start();
    return client;
  }


  private int countTotalContainers(Service service) {
    int totalContainers = 0;
    for (Component component : service.getComponents()) {
      totalContainers += component.getNumberOfContainers();
    }
    return totalContainers;
  }
}
