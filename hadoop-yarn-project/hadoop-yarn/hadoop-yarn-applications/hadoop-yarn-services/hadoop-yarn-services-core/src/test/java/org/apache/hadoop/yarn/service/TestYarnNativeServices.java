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

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.test.TestingCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.Container;
import org.apache.hadoop.yarn.service.api.records.ContainerState;
import org.apache.hadoop.yarn.service.client.ServiceClient;
import org.apache.hadoop.yarn.service.conf.YarnServiceConf;
import org.apache.hadoop.yarn.service.exceptions.SliderException;
import org.apache.hadoop.yarn.service.utils.SliderFileSystem;
import org.apache.hadoop.yarn.util.LinuxResourceCalculatorPlugin;
import org.apache.hadoop.yarn.util.ProcfsBasedProcessTree;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.registry.client.api.RegistryConstants.KEY_REGISTRY_ZK_QUORUM;
import static org.apache.hadoop.yarn.api.records.YarnApplicationState.FINISHED;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.*;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConf.AM_RESOURCE_MEM;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConf.YARN_SERVICE_BASE_PATH;

/**
 * End to end tests to test deploying services with MiniYarnCluster and a in-JVM
 * ZK testing cluster.
 */
public class TestYarnNativeServices extends ServiceTestUtils{

  private static final Log LOG =
      LogFactory.getLog(TestYarnNativeServices.class);

  private MiniYARNCluster yarnCluster = null;
  private MiniDFSCluster hdfsCluster = null;
  private FileSystem fs = null;
  protected Configuration conf = null;
  private static final int NUM_NMS = 1;
  private File basedir;

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Before
  public void setup() throws Exception {
    setupInternal(NUM_NMS);
  }

  private void setupInternal(int numNodeManager)
      throws Exception {
    LOG.info("Starting up YARN cluster");
//    Logger rootLogger = LogManager.getRootLogger();
//    rootLogger.setLevel(Level.DEBUG);
    conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
    // reduce the teardown waiting time
    conf.setLong(YarnConfiguration.DISPATCHER_DRAIN_EVENTS_TIMEOUT, 1000);
    conf.set("yarn.log.dir", "target");
    // mark if we need to launch the v1 timeline server
    // disable aux-service based timeline aggregators
    conf.set(YarnConfiguration.NM_AUX_SERVICES, "");
    conf.set(YarnConfiguration.NM_VMEM_PMEM_RATIO, "8");
    // Enable ContainersMonitorImpl
    conf.set(YarnConfiguration.NM_CONTAINER_MON_RESOURCE_CALCULATOR,
        LinuxResourceCalculatorPlugin.class.getName());
    conf.set(YarnConfiguration.NM_CONTAINER_MON_PROCESS_TREE,
        ProcfsBasedProcessTree.class.getName());
    conf.setBoolean(
        YarnConfiguration.YARN_MINICLUSTER_CONTROL_RESOURCE_MONITORING, true);
    conf.setBoolean(TIMELINE_SERVICE_ENABLED, false);
    conf.setInt(YarnConfiguration.NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE, 100);
    conf.setLong(DEBUG_NM_DELETE_DELAY_SEC, 60000);
    conf.setLong(AM_RESOURCE_MEM, 526);
    conf.setLong(YarnServiceConf.READINESS_CHECK_INTERVAL, 5);
    // Disable vmem check to disallow NM killing the container
    conf.setBoolean(NM_VMEM_CHECK_ENABLED, false);
    conf.setBoolean(NM_PMEM_CHECK_ENABLED, false);
    // setup zk cluster
    TestingCluster zkCluster;
    zkCluster = new TestingCluster(1);
    zkCluster.start();
    conf.set(YarnConfiguration.RM_ZK_ADDRESS, zkCluster.getConnectString());
    conf.set(KEY_REGISTRY_ZK_QUORUM, zkCluster.getConnectString());
    LOG.info("ZK cluster: " +  zkCluster.getConnectString());

    fs = FileSystem.get(conf);
    basedir = new File("target", "apps");
    if (basedir.exists()) {
      FileUtils.deleteDirectory(basedir);
    } else {
      basedir.mkdirs();
    }

    conf.set(YARN_SERVICE_BASE_PATH, basedir.getAbsolutePath());

    if (yarnCluster == null) {
      yarnCluster =
          new MiniYARNCluster(TestYarnNativeServices.class.getSimpleName(), 1,
              numNodeManager, 1, 1);
      yarnCluster.init(conf);
      yarnCluster.start();

      waitForNMsToRegister();

      URL url = Thread.currentThread().getContextClassLoader()
          .getResource("yarn-site.xml");
      if (url == null) {
        throw new RuntimeException(
            "Could not find 'yarn-site.xml' dummy file in classpath");
      }
      Configuration yarnClusterConfig = yarnCluster.getConfig();
      yarnClusterConfig.set(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
          new File(url.getPath()).getParent());
      //write the document to a buffer (not directly to the file, as that
      //can cause the file being written to get read -which will then fail.
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      yarnClusterConfig.writeXml(bytesOut);
      bytesOut.close();
      //write the bytes to the file in the classpath
      OutputStream os = new FileOutputStream(new File(url.getPath()));
      os.write(bytesOut.toByteArray());
      os.close();
      LOG.info("Write yarn-site.xml configs to: " + url);
    }
    if (hdfsCluster == null) {
      HdfsConfiguration hdfsConfig = new HdfsConfiguration();
      hdfsCluster = new MiniDFSCluster.Builder(hdfsConfig)
          .numDataNodes(1).build();
    }

    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      LOG.info("setup thread sleep interrupted. message=" + e.getMessage());
    }


  }

  private void waitForNMsToRegister() throws Exception {
    int sec = 60;
    while (sec >= 0) {
      if (yarnCluster.getResourceManager().getRMContext().getRMNodes().size()
          >= NUM_NMS) {
        break;
      }
      Thread.sleep(1000);
      sec--;
    }
  }

  @After
  public void tearDown() throws IOException {
    if (yarnCluster != null) {
      try {
        yarnCluster.stop();
      } finally {
        yarnCluster = null;
      }
    }
    if (hdfsCluster != null) {
      try {
        hdfsCluster.shutdown();
      } finally {
        hdfsCluster = null;
      }
    }
    if (basedir != null) {
      FileUtils.deleteDirectory(basedir);
    }
    SliderFileSystem sfs = new SliderFileSystem(conf);
    Path appDir = sfs.getBaseApplicationPath();
    sfs.getFileSystem().delete(appDir, true);
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
    ServiceClient client = createClient();
    Service exampleApp = createExampleApplication();
    client.actionCreate(exampleApp);
    SliderFileSystem fileSystem = new SliderFileSystem(conf);
    Path appDir = fileSystem.buildClusterDirPath(exampleApp.getName());
    // check app.json is persisted.
    Assert.assertTrue(
        fs.exists(new Path(appDir, exampleApp.getName() + ".json")));
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
    Assert.assertFalse(fs.exists(appDir));
  }

  // Create compa with 2 containers
  // Create compb with 2 containers which depends on compa
  // Check containers for compa started before containers for compb
  @Test (timeout = 200000)
  public void testComponentStartOrder() throws Exception {
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
        String compInstanceName = containerList.get(index).getComponentName();
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
      instances.add(container.getComponentName());
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

  // wait until all the containers for all components become ready state
  private void waitForAllCompToBeReady(ServiceClient client,
      Service exampleApp) throws TimeoutException, InterruptedException {
    int expectedTotalContainers = countTotalContainers(exampleApp);
    GenericTestUtils.waitFor(() -> {
      try {
        Service retrievedApp = client.getStatus(exampleApp.getName());
        int totalReadyContainers = 0;
        LOG.info("Num Components " + retrievedApp.getComponents().size());
        for (Component component : retrievedApp.getComponents()) {
          LOG.info("looking for  " + component.getName());
          LOG.info(component);
          if (component.getContainers() != null) {
            if (component.getContainers().size() == exampleApp
                .getComponent(component.getName()).getNumberOfContainers()) {
              for (Container container : component.getContainers()) {
                LOG.info(
                    "Container state " + container.getState() + ", component "
                        + component.getName());
                if (container.getState() == ContainerState.READY) {
                  totalReadyContainers++;
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
    client.init(conf);
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
