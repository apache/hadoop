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
package org.apache.hadoop.yarn.server.router;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.junit.Before;
import org.junit.Test;

public class TestRouterStoreCommands {

  ////////////////////////////////
  // Router Constants
  ////////////////////////////////
  private Configuration conf;
  private MemoryFederationStateStore stateStore;
  private FederationStateStoreFacade facade;

  @Before
  public void setup() throws YarnException {
    conf = new YarnConfiguration();
    stateStore = new MemoryFederationStateStore();
    stateStore.init(conf);
    facade = FederationStateStoreFacade.getInstance(conf);
    facade.reinitialize(stateStore, conf);
  }

  @Test
  public void testRemoveApplicationFromRouterStateStore() throws Exception {

    // We will design such a unit test.
    // We will write the applicationId and subCluster into the stateStore,
    // and then remove the application through Router.removeApplication.
    // At this time, if we continue to query through the stateStore,
    // We will get a prompt that application not exists.

    ApplicationId appId = ApplicationId.newInstance(Time.now(), 1);
    SubClusterId homeSubCluster = SubClusterId.newInstance("SC-1");
    ApplicationHomeSubCluster applicationHomeSubCluster =
        ApplicationHomeSubCluster.newInstance(appId, homeSubCluster);
    AddApplicationHomeSubClusterRequest request =
        AddApplicationHomeSubClusterRequest.newInstance(applicationHomeSubCluster);
    stateStore.addApplicationHomeSubCluster(request);
    Router.removeApplication(conf, appId.toString());

    GetApplicationHomeSubClusterRequest request1 =
        GetApplicationHomeSubClusterRequest.newInstance(appId);

    LambdaTestUtils.intercept(YarnException.class, "Application " + appId + " does not exist.",
        () -> stateStore.getApplicationHomeSubCluster(request1));
  }
}
