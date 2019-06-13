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

import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.registry.client.impl.zk.CuratorService;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.Container;
import org.apache.hadoop.yarn.service.api.records.ContainerState;
import org.apache.hadoop.yarn.service.api.records.Resource;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
import org.apache.hadoop.yarn.service.client.ServiceClient;
import org.apache.hadoop.yarn.service.conf.YarnServiceConf;
import org.apache.hadoop.yarn.service.exceptions.SliderException;
import org.apache.hadoop.yarn.service.utils.JsonSerDeser;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.apache.hadoop.yarn.service.utils.SliderFileSystem;
import org.apache.hadoop.yarn.util.LinuxResourceCalculatorPlugin;
import org.apache.hadoop.yarn.util.ProcfsBasedProcessTree;
import org.codehaus.jackson.map.PropertyNamingStrategy;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.registry.client.api.RegistryConstants.KEY_REGISTRY_ZK_QUORUM;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_PMEM_CHECK_ENABLED;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_VMEM_CHECK_ENABLED;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_ENABLED;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConf.AM_RESOURCE_MEM;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConf.YARN_SERVICE_BASE_PATH;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ServiceTestUtils {

  private static final Logger LOG =
      LoggerFactory.getLogger(ServiceTestUtils.class);

  private MiniYARNCluster yarnCluster = null;
  private MiniDFSCluster hdfsCluster = null;
  private TestingCluster zkCluster;
  private CuratorService curatorService;
  private FileSystem fs = null;
  private Configuration conf = null;
  public static final int NUM_NMS = 1;
  private File basedir;

  public static final JsonSerDeser<Service> JSON_SER_DESER =
      new JsonSerDeser<>(Service.class,
          PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

  // Example service definition
  // 2 components, each of which has 2 containers.
  public static Service createExampleApplication() {
    Service exampleApp = new Service();
    exampleApp.setName("example-app");
    exampleApp.setVersion("v1");
    exampleApp.addComponent(createComponent("compa"));
    exampleApp.addComponent(createComponent("compb"));
    return exampleApp;
  }

  // Example service definition
  // 2 components, each of which has 2 containers.
  public static Service createTerminatingJobExample(String serviceName) {
    Service exampleApp = new Service();
    exampleApp.setName(serviceName);
    exampleApp.setVersion("v1");
    exampleApp.addComponent(
        createComponent("terminating-comp1", 2, "sleep 1000",
            Component.RestartPolicyEnum.NEVER, null));
    exampleApp.addComponent(
        createComponent("terminating-comp2", 2, "sleep 1000",
            Component.RestartPolicyEnum.ON_FAILURE, null));
    exampleApp.addComponent(
        createComponent("terminating-comp3", 2, "sleep 1000",
            Component.RestartPolicyEnum.ON_FAILURE, null));

    return exampleApp;
  }

  public static Component createComponent(String name) {
    return createComponent(name, 2L, "sleep 1000",
        Component.RestartPolicyEnum.ALWAYS, null);
  }

  protected static Component createComponent(String name, long numContainers,
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

  protected static Component createComponent(String name, long numContainers,
      String command, Component.RestartPolicyEnum restartPolicyEnum,
      List<String> dependencies) {
    Component comp = createComponent(name, numContainers, command);
    comp.setRestartPolicy(restartPolicyEnum);

    if (dependencies != null) {
      comp.dependencies(dependencies);
    }
    return comp;
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

  protected void setConf(YarnConfiguration conf) {
    this.conf = conf;
  }

  protected Configuration getConf() {
    return conf;
  }

  protected FileSystem getFS() {
    return fs;
  }

  protected MiniYARNCluster getYarnCluster() {
    return yarnCluster;
  }

  protected void setupInternal(int numNodeManager)
      throws Exception {
    LOG.info("Starting up YARN cluster");
    if (conf == null) {
      setConf(new YarnConfiguration());
    }
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
    // set auth filters
    conf.set(HttpServer2.FILTER_INITIALIZER_PROPERTY,
        "org.apache.hadoop.security.AuthenticationFilterInitializer,"
            + "org.apache.hadoop.security.HttpCrossOriginFilterInitializer");
    // setup zk cluster
    zkCluster = new TestingCluster(1);
    zkCluster.start();
    conf.set(YarnConfiguration.RM_ZK_ADDRESS, zkCluster.getConnectString());
    conf.set(KEY_REGISTRY_ZK_QUORUM, zkCluster.getConnectString());
    LOG.info("ZK cluster: " + zkCluster.getConnectString());

    curatorService = new CuratorService("testCuratorService");
    curatorService.init(conf);
    curatorService.start();

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
          new MiniYARNCluster(this.getClass().getSimpleName(), 1,
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

  public void shutdown() throws IOException {
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
    if (curatorService != null) {
      ServiceOperations.stop(curatorService);
    }
    if (zkCluster != null) {
      zkCluster.stop();
    }
    if (basedir != null) {
      FileUtils.deleteDirectory(basedir);
    }
    SliderFileSystem sfs = new SliderFileSystem(conf);
    Path appDir = sfs.getBaseApplicationPath();
    sfs.getFileSystem().delete(appDir, true);
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

  /**
   * Creates a {@link ServiceClient} for test purposes.
   */
  public static ServiceClient createClient(Configuration conf)
      throws Exception {
    ServiceClient client = new ServiceClient() {
      @Override
      protected Path addJarResource(String appName,
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

  public static ServiceManager createServiceManager(ServiceContext context) {
    ServiceManager serviceManager = new ServiceManager(context);
    context.setServiceManager(serviceManager);
    return serviceManager;
  }

  /**
   * Creates a YarnClient for test purposes.
   */
  public static YarnClient createYarnClient(Configuration conf) {
    YarnClient client = YarnClient.createYarnClient();
    client.init(conf);
    client.start();
    return client;
  }

  protected CuratorService getCuratorService() throws IOException {
    return curatorService;
  }

  /**
   * Watcher to initialize yarn service base path under target and deletes the
   * the test directory when finishes.
   */
  public static class ServiceFSWatcher extends TestWatcher {
    private YarnConfiguration conf;
    private SliderFileSystem fs;
    private java.nio.file.Path serviceBasePath;

    @Override
    protected void starting(Description description) {
      conf = new YarnConfiguration();
      delete(description);
      serviceBasePath = Paths.get("target",
          description.getClassName(), description.getMethodName());
      conf.set(YARN_SERVICE_BASE_PATH, serviceBasePath.toString());
      try {
        fs = new SliderFileSystem(conf);
        fs.setAppDir(new Path(serviceBasePath.toString()));
      } catch (IOException e) {
        Throwables.throwIfUnchecked(e);
        throw new RuntimeException(e);
      }
    }

    @Override
    protected void finished(Description description) {
      delete(description);
    }

    private void delete(Description description) {
      FileUtils.deleteQuietly(Paths.get("target",
          description.getClassName()).toFile());
    }

    /**
     * Returns the yarn conf.
     */
    public YarnConfiguration getConf() {
      return conf;
    }

    /**
     * Returns the file system.
     */
    public SliderFileSystem getFs() {
      return fs;
    }

    /**
     * Returns the test service base path.
     */
    public java.nio.file.Path getServiceBasePath() {
      return serviceBasePath;
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
  protected Multimap<String, String> waitForAllCompToBeReady(ServiceClient
      client, Service exampleApp) throws TimeoutException,
      InterruptedException {
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
  protected void waitForServiceToBeStable(ServiceClient client,
      Service exampleApp) throws TimeoutException, InterruptedException {
    waitForServiceToBeStable(client, exampleApp, 200000);
  }

  protected void waitForServiceToBeStable(ServiceClient client,
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
  protected void waitForServiceToBeStarted(ServiceClient client,
      Service exampleApp) throws TimeoutException, InterruptedException {
    waitForServiceToBeInState(client, exampleApp, ServiceState.STARTED);
  }

  protected void waitForServiceToBeInState(ServiceClient client,
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
  protected void waitForServiceToBeInState(ServiceClient client,
      Service exampleApp, ServiceState desiredState, int waitForMillis) throws
      TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> {
      try {
        Service retrievedApp = client.getStatus(exampleApp.getName());
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
