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

package org.apache.hadoop.yarn.server.globalpolicygenerator.applicationcleaner;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.impl.FSRegistryOperationsService;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.utils.FederationRegistryClient;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GPGContext;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GPGContextImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for DefaultApplicationCleaner in GPG.
 */
public class TestDefaultApplicationCleaner {
  private Configuration conf;
  private MemoryFederationStateStore stateStore;
  private FederationStateStoreFacade facade;
  private ApplicationCleaner appCleaner;
  private GPGContext gpgContext;
  private RegistryOperations registry;
  private FederationRegistryClient registryClient;

  private List<ApplicationId> appIds;
  // The list of applications returned by mocked router
  private Set<ApplicationId> routerAppIds;

  private ApplicationId appIdToAddConcurrently;

  @Before
  public void setup() throws Exception {
    conf = new YarnConfiguration();

    // No Router query retry
    conf.set(YarnConfiguration.GPG_APPCLEANER_CONTACT_ROUTER_SPEC, "1,1,0");

    stateStore = new MemoryFederationStateStore();
    stateStore.init(conf);

    facade = FederationStateStoreFacade.getInstance();
    facade.reinitialize(stateStore, conf);

    registry = new FSRegistryOperationsService();
    registry.init(conf);
    registry.start();

    UserGroupInformation user = UserGroupInformation.getCurrentUser();
    registryClient = new FederationRegistryClient(conf, registry, user);
    registryClient.cleanAllApplications();
    Assert.assertEquals(0, registryClient.getAllApplications().size());

    gpgContext = new GPGContextImpl();
    gpgContext.setStateStoreFacade(facade);
    gpgContext.setRegistryClient(registryClient);

    appCleaner = new TestableDefaultApplicationCleaner();
    appCleaner.init(conf, gpgContext);

    routerAppIds = new HashSet<>();

    appIds = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      ApplicationId appId = ApplicationId.newInstance(0, i);
      appIds.add(appId);

      SubClusterId subClusterId =
          SubClusterId.newInstance("SUBCLUSTER-" + i);

      stateStore.addApplicationHomeSubCluster(
          AddApplicationHomeSubClusterRequest.newInstance(
              ApplicationHomeSubCluster.newInstance(appId, subClusterId)));

      // Write some registry entries for the app
      registryClient.writeAMRMTokenForUAM(appId, subClusterId.toString(),
          new Token<AMRMTokenIdentifier>());
    }
    Assert.assertEquals(3, registryClient.getAllApplications().size());
    appIdToAddConcurrently = null;
  }

  @After
  public void breakDown() {
    if (stateStore != null) {
      stateStore.close();
      stateStore = null;
    }
    if (registryClient != null) {
      registryClient.cleanAllApplications();
      registryClient = null;
    }
    if (registry != null) {
      registry.stop();
      registry = null;
    }
  }

  @Test
  public void testFederationStateStoreAppsCleanUp() throws YarnException {
    // Set first app to be still known by Router
    ApplicationId appId = appIds.get(0);
    routerAppIds.add(appId);

    // Another random app not in stateStore known by Router
    appId = ApplicationId.newInstance(100, 200);
    routerAppIds.add(appId);

    appCleaner.run();

    // Only one app should be left
    Assert.assertEquals(1,
        stateStore
            .getApplicationsHomeSubCluster(
                GetApplicationsHomeSubClusterRequest.newInstance())
            .getAppsHomeSubClusters().size());

    // The known app should not be cleaned in registry
    Assert.assertEquals(1, registryClient.getAllApplications().size());
  }

  /**
   * Testable version of DefaultApplicationCleaner.
   */
  public class TestableDefaultApplicationCleaner
      extends DefaultApplicationCleaner {
    @Override
    public Set<ApplicationId> getAppsFromRouter() throws YarnRuntimeException {
      if (appIdToAddConcurrently != null) {
        SubClusterId scId = SubClusterId.newInstance("MySubClusterId");
        try {
          ApplicationHomeSubCluster appHomeSubCluster =
              ApplicationHomeSubCluster.newInstance(appIdToAddConcurrently, scId);
          AddApplicationHomeSubClusterRequest request =
              AddApplicationHomeSubClusterRequest.newInstance(appHomeSubCluster);
          stateStore.addApplicationHomeSubCluster(request);
        } catch (YarnException e) {
          throw new YarnRuntimeException(e);
        }
        registryClient.writeAMRMTokenForUAM(appIdToAddConcurrently, scId.toString(),
            new Token<>());
      }
      return routerAppIds;
    }
  }

  @Test
  public void testConcurrentNewApp() throws YarnException {
    appIdToAddConcurrently = ApplicationId.newInstance(1, 1);

    appCleaner.run();

    // The concurrently added app should be still there
    GetApplicationsHomeSubClusterRequest appHomeSubClusterRequest =
         GetApplicationsHomeSubClusterRequest.newInstance();
    GetApplicationsHomeSubClusterResponse applicationsHomeSubCluster =
        stateStore.getApplicationsHomeSubCluster(appHomeSubClusterRequest);
    Assert.assertNotNull(applicationsHomeSubCluster);
    List<ApplicationHomeSubCluster> appsHomeSubClusters =
        applicationsHomeSubCluster.getAppsHomeSubClusters();
    Assert.assertNotNull(appsHomeSubClusters);
    Assert.assertEquals(1, appsHomeSubClusters.size());

    // The concurrently added app should be still there
    Assert.assertEquals(1, registryClient.getAllApplications().size());
  }
}
