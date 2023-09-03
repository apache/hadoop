/**
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
package org.apache.hadoop.yarn.server.router.subcluster;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.commons.collections.CollectionUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.router.webapp.JavaProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static javax.servlet.http.HttpServletResponse.SC_OK;
import static javax.ws.rs.core.MediaType.APPLICATION_XML;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.RM_WEB_SERVICE_PATH;
import static org.apache.hadoop.yarn.server.router.webapp.TestRouterWebServicesREST.waitWebAppRunning;
import static org.junit.Assert.assertEquals;

public class TestFederationSubCluster {

  private static final Logger LOG = LoggerFactory.getLogger(TestFederationSubCluster.class);
  private static TestingServer curatorTestingServer;
  private static CuratorFramework curatorFramework;
  private static JavaProcess subCluster1;
  private static JavaProcess subCluster2;
  private static JavaProcess router;
  public static final String ZK_FEDERATION_STATESTORE =
      "org.apache.hadoop.yarn.server.federation.store.impl.ZookeeperFederationStateStore";
  private static String userName = "test";

  public void startFederationSubCluster(int zkPort, String sc1Param,
      String sc2Param, String routerParam) throws IOException, InterruptedException,
      YarnException, TimeoutException {

    // Step1. Initialize ZK's service.
    try {
      curatorTestingServer = new TestingServer(zkPort);
      curatorTestingServer.start();
      String connectString = curatorTestingServer.getConnectString();
      curatorFramework = CuratorFrameworkFactory.builder()
          .connectString(connectString)
          .retryPolicy(new RetryNTimes(100, 100))
          .build();
      curatorFramework.start();
      curatorFramework.getConnectionStateListenable().addListener((client, newState) -> {
        if (newState == ConnectionState.CONNECTED) {
          System.out.println("Connected to the ZooKeeper server!");
        }
      });
    } catch (Exception e) {
      LOG.error("Cannot initialize ZooKeeper store.", e);
      throw new IOException(e);
    }

    // Step2. Create a temporary directory output log.
    File baseDir = GenericTestUtils.getTestDir("processes");
    baseDir.mkdirs();
    String baseName = TestFederationSubCluster.class.getSimpleName();

    // Step3. Initialize subCluster SC-1
    String sc1WebAddress = getSCWebAddress(sc1Param);
    File rmOutput = new File(baseDir, baseName + "-" + Time.now() + "-rm.log");
    rmOutput.createNewFile();
    List<String> addClasspath = new LinkedList<>();
    addClasspath.add("../hadoop-yarn-server-timelineservice/target/classes");
    subCluster1 = new JavaProcess(TestMockSubCluster.class, addClasspath, rmOutput, sc1Param);
    waitWebAppRunning(sc1WebAddress, RM_WEB_SERVICE_PATH);

    // Step4. Initialize subCluster SC-2
    String sc2WebAddress = getSCWebAddress(sc2Param);
    File rmOutput2 = new File(baseDir, baseName + "-" + Time.now() + "-rm.log");
    rmOutput2.createNewFile();
    List<String> addClasspath2 = new LinkedList<>();
    addClasspath2.add("../hadoop-yarn-server-timelineservice/target/classes");
    subCluster2 = new JavaProcess(TestMockSubCluster.class, addClasspath2, rmOutput2, sc2Param);
    waitWebAppRunning(sc2WebAddress, RM_WEB_SERVICE_PATH);

    // Step5. Confirm that subClusters have been registered to ZK.
    String zkAddress = getZkAddress(zkPort);
    verifyRegistration(zkAddress);

    // Step6. Initialize router
    String routerWebAddress = getRouterWebAddress(routerParam);
    File routerOutput = new File(baseDir, baseName + "-" + Time.now() + "-router.log");
    routerOutput.createNewFile();
    router = new JavaProcess(TestMockRouter.class, null, routerOutput, routerParam);
    waitWebAppRunning(routerWebAddress, RM_WEB_SERVICE_PATH);
  }

  private String getSCWebAddress(String scParam) {
    String[] scParams = scParam.split(",");
    return "http://localhost:" + scParams[3];
  }

  private String getRouterWebAddress(String routerParam) {
    String[] routerParams = routerParam.split(",");
    return "http://localhost:" + routerParams[2];
  }

  private String getZkAddress(int port) {
    return "localhost:" + port;
  }

  public void stop() throws Exception {
    if (subCluster1 != null) {
      subCluster1.stop();
    }
    if (subCluster2 != null) {
      subCluster2.stop();
    }
    if (router != null) {
      router.stop();
    }
    if (curatorTestingServer != null) {
      curatorTestingServer.stop();
    }
  }

  private void verifyRegistration(String zkAddress)
      throws YarnException, InterruptedException, TimeoutException {
    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.FEDERATION_ENABLED, true);
    conf.set(YarnConfiguration.FEDERATION_STATESTORE_CLIENT_CLASS, ZK_FEDERATION_STATESTORE);
    conf.set(CommonConfigurationKeys.ZK_ADDRESS, zkAddress);
    RetryPolicy retryPolicy = FederationStateStoreFacade.createRetryPolicy(conf);
    FederationStateStore stateStore = (FederationStateStore)
        FederationStateStoreFacade.createRetryInstance(conf,
        YarnConfiguration.FEDERATION_STATESTORE_CLIENT_CLASS,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_CLIENT_CLASS,
        FederationStateStore.class, retryPolicy);
    stateStore.init(conf);
    FederationStateStoreFacade.getInstance().reinitialize(stateStore, conf);
    GetSubClustersInfoRequest request = GetSubClustersInfoRequest.newInstance(true);

    GenericTestUtils.waitFor(() -> {
      try {
        GetSubClustersInfoResponse response = stateStore.getSubClusters(request);
        List<SubClusterInfo> subClusters = response.getSubClusters();
        if (CollectionUtils.isNotEmpty(subClusters)) {
          return true;
        }
      } catch (Exception e) {
      }
      return false;
    }, 5000, 50 * 1000);
  }

  public static <T> T performGetCalls(final String routerAddress, final String path,
       final Class<T> returnType, final String queryName,
       final String queryValue) throws IOException, InterruptedException {

    Client clientToRouter = Client.create();
    WebResource toRouter = clientToRouter.resource(routerAddress).path(path);

    final WebResource.Builder toRouterBuilder;

    if (queryValue != null && queryName != null) {
      toRouterBuilder = toRouter.queryParam(queryName, queryValue).accept(APPLICATION_XML);
    } else {
      toRouterBuilder = toRouter.accept(APPLICATION_XML);
    }

    return UserGroupInformation.createRemoteUser(userName).doAs(
        (PrivilegedExceptionAction<T>) () -> {
          ClientResponse response = toRouterBuilder.get(ClientResponse.class);
          assertEquals(SC_OK, response.getStatus());
          return response.getEntity(returnType);
        });
  }
}
