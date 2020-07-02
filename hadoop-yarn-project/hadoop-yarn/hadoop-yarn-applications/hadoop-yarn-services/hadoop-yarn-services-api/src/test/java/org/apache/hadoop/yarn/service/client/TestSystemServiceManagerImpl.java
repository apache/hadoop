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

package org.apache.hadoop.yarn.service.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.conf.SliderExitCodes;
import org.apache.hadoop.yarn.service.conf.YarnServiceConf;
import org.apache.hadoop.yarn.service.exceptions.SliderException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.fail;

/**
 * Test class for system service manager.
 */
public class TestSystemServiceManagerImpl {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestSystemServiceManagerImpl.class);
  private SystemServiceManagerImpl systemService;
  private Configuration conf;
  private String resourcePath = "system-services";

  private String[] users = new String[] {"user1", "user2"};
  private static Map<String, Set<String>> loadedServices = new HashMap<>();
  private static Map<String, Set<String>> savedServices = new HashMap<>();
  private static Map<String, Set<String>> submittedServices = new HashMap<>();

  @Before
  public void setup() {
    File file = new File(
        getClass().getClassLoader().getResource(resourcePath).getFile());
    conf = new Configuration();
    conf.set(YarnServiceConf.YARN_SERVICES_SYSTEM_SERVICE_DIRECTORY,
        file.getAbsolutePath());
    systemService = new SystemServiceManagerImpl() {
      @Override ServiceClient getServiceClient() {
        return new TestServiceClient();
      }
    };
    systemService.init(conf); // do not call explicit start

    constructUserService(users[0], "example-app1");
    constructUserService(users[1], "example-app1", "example-app2");
  }

  @After
  public void tearDown() {
    systemService.stop();
  }

  @Test
  public void testSystemServiceSubmission() throws Exception {
    systemService.start();

    /* verify for ignored sevices count */
    Map<String, Integer> ignoredUserServices =
        systemService.getIgnoredUserServices();
    Assert.assertEquals(1, ignoredUserServices.size());
    Assert.assertTrue("User user1 doesn't exist.",
        ignoredUserServices.containsKey(users[0]));
    int count = ignoredUserServices.get(users[0]);
    Assert.assertEquals(1, count);
    Assert.assertEquals(1,
        systemService.getBadFileNameExtensionSkipCounter());
    Assert.assertEquals(1, systemService.getBadDirSkipCounter());

    Map<String, Set<Service>> userServices =
        systemService.getSyncUserServices();
    Assert.assertEquals(loadedServices.size(), userServices.size());
    verifyForScannedUserServices(userServices);

    verifyForLaunchedUserServices();

    // 2nd time launch service to handle if service exist scenario
    systemService.launchUserService(userServices);
    verifyForLaunchedUserServices();

    // verify start of stopped services
    submittedServices.clear();
    systemService.launchUserService(userServices);
    verifyForLaunchedUserServices();
  }

  private void verifyForScannedUserServices(
      Map<String, Set<Service>> userServices) {
    for (String user : users) {
      Set<Service> services = userServices.get(user);
      Set<String> serviceNames = loadedServices.get(user);
      Assert.assertEquals(serviceNames.size(), services.size());
      Iterator<Service> iterator = services.iterator();
      while (iterator.hasNext()) {
        Service next = iterator.next();
        Assert.assertTrue(
            "Service name doesn't exist in expected userService "
                + serviceNames, serviceNames.contains(next.getName()));
      }
    }
  }

  public void constructUserService(String user, String... serviceNames) {
    Set<String> service = loadedServices.get(user);
    if (service == null) {
      service = new HashSet<>();
      for (String serviceName : serviceNames) {
        service.add(serviceName);
      }
      loadedServices.put(user, service);
    }
  }

  class TestServiceClient extends ServiceClient {
    @Override
    protected void serviceStart() throws Exception {
      // do nothing
    }

    @Override
    protected void serviceStop() throws Exception {
      // do nothing
    }

    @Override
    protected void serviceInit(Configuration configuration)
        throws Exception {
      // do nothing
    }

    @Override
    public int actionBuild(Service service)
        throws YarnException, IOException {
      String userName =
          UserGroupInformation.getCurrentUser().getShortUserName();
      Set<String> services = savedServices.get(userName);
      if (services == null) {
        services = new HashSet<>();
        savedServices.put(userName, services);
      }
      if (services.contains(service.getName())) {
        String message = "Failed to save service " + service.getName()
            + ", because it already exists.";
        throw new SliderException(SliderExitCodes.EXIT_INSTANCE_EXISTS,
            message);
      }
      services.add(service.getName());
      return 0;
    }

    @Override
    public ApplicationId actionStartAndGetId(String serviceName)
        throws YarnException, IOException {
      String userName =
          UserGroupInformation.getCurrentUser().getShortUserName();
      Set<String> services = submittedServices.get(userName);
      if (services == null) {
        services = new HashSet<>();
        submittedServices.put(userName, services);
      }
      if (services.contains(serviceName)) {
        String message = "Failed to create service " + serviceName
            + ", because it is already running.";
        throw new YarnException(message);
      }
      services.add(serviceName);
      return ApplicationId.newInstance(System.currentTimeMillis(), 1);
    }
  }

  private void verifyForLaunchedUserServices() {
    Assert.assertEquals(loadedServices.size(), submittedServices.size());
    for (Map.Entry<String, Set<String>> entry : submittedServices.entrySet()) {
      String user = entry.getKey();
      Set<String> serviceSet = entry.getValue();
      Assert.assertTrue(loadedServices.containsKey(user));
      Set<String> services = loadedServices.get(user);
      Assert.assertEquals(services.size(), serviceSet.size());
      Assert.assertTrue(services.containsAll(serviceSet));
    }
  }

  @Test
  public void testFileSystemCloseWhenCleanUpService() throws Exception {
    FileSystem fs = null;
    Path path = new Path("/tmp/servicedir");

    HdfsConfiguration hdfsConfig = new HdfsConfiguration();
    MiniDFSCluster hdfsCluster = new MiniDFSCluster.Builder(hdfsConfig)
        .numDataNodes(1).build();

    fs = hdfsCluster.getFileSystem();
    if (!fs.exists(path)) {
      fs.mkdirs(path);
    }

    SystemServiceManagerImpl serviceManager = new SystemServiceManagerImpl();

    hdfsConfig.set(YarnServiceConf.YARN_SERVICES_SYSTEM_SERVICE_DIRECTORY,
        path.toString());
    serviceManager.init(hdfsConfig);

    // the FileSystem object owned by SystemServiceManager must not be closed
    // when cleanup a service
    hdfsConfig.set("hadoop.registry.zk.connection.timeout.ms", "100");
    hdfsConfig.set("hadoop.registry.zk.retry.times", "1");
    ApiServiceClient asc = new ApiServiceClient();
    asc.serviceInit(hdfsConfig);
    asc.actionCleanUp("testapp", "testuser");

    try {
      serviceManager.start();
    } catch (Exception e) {
      if (e.getMessage().contains("Filesystem closed")) {
        fail("SystemServiceManagerImpl failed to handle " +
            "FileSystem close");
      } else {
        fail("Should not get any exceptions");
      }
    } finally {
      serviceManager.stop();
      fs = hdfsCluster.getFileSystem();
      if (fs.exists(path)) {
        fs.delete(path, true);
      }
      if (hdfsCluster != null) {
        hdfsCluster.shutdown();
      }
    }
  }
}
