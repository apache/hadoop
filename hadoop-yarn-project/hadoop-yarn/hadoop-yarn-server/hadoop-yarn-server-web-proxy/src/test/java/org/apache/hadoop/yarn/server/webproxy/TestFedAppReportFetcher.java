/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.webproxy;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationHistoryProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.util.StringHelper;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.fail;

public class TestFedAppReportFetcher {

  private Configuration conf;
  private static ApplicationHistoryProtocol history;

  private SubClusterId subClusterId1 = SubClusterId.newInstance("subCluster1");
  private SubClusterId subClusterId2 = SubClusterId.newInstance("subCluster2");
  private SubClusterInfo clusterInfo1 = SubClusterInfo.newInstance(subClusterId1, "10.0.0.1:1000",
      "10.0.0.1:1000", "10.0.0.1:1000", "10.0.0.1:1000", SubClusterState.SC_RUNNING, 0L, "");
  private SubClusterInfo clusterInfo2 = SubClusterInfo.newInstance(subClusterId2, "10.0.0.2:1000",
      "10.0.0.2:1000", "10.0.0.2:1000", "10.0.0.2:1000", SubClusterState.SC_RUNNING, 0L, "");
  private ApplicationClientProtocol appManager1;
  private ApplicationClientProtocol appManager2;
  private ApplicationId appId1 = ApplicationId.newInstance(0, 1);
  private ApplicationId appId2 = ApplicationId.newInstance(0, 2);

  private static FedAppReportFetcher fetcher;
  private final String appNotFoundExceptionMsg = "APP NOT FOUND";

  @After
  public void cleanUp() {
    history = null;
    fetcher = null;
  }

  private void testHelper(boolean isAHSEnabled)
      throws YarnException, IOException {
    conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED, isAHSEnabled);

    FederationStateStoreFacade fedFacade = FederationStateStoreFacade.getInstance();
    FederationStateStore fss = new MemoryFederationStateStore();
    fss.init(conf);
    fedFacade.reinitialize(fss, conf);

    fss.registerSubCluster(SubClusterRegisterRequest.newInstance(clusterInfo1));
    fss.registerSubCluster(SubClusterRegisterRequest.newInstance(clusterInfo2));
    fss.addApplicationHomeSubCluster(AddApplicationHomeSubClusterRequest
        .newInstance(ApplicationHomeSubCluster.newInstance(appId1, subClusterId1)));
    fss.addApplicationHomeSubCluster(AddApplicationHomeSubClusterRequest
        .newInstance(ApplicationHomeSubCluster.newInstance(appId2, subClusterId2)));

    appManager1 = Mockito.mock(ApplicationClientProtocol.class);
    Mockito.when(appManager1.getApplicationReport(Mockito.any(GetApplicationReportRequest.class)))
        .thenThrow(new ApplicationNotFoundException(appNotFoundExceptionMsg));

    appManager2 = Mockito.mock(ApplicationClientProtocol.class);
    Mockito.when(appManager2.getApplicationReport(Mockito.any(GetApplicationReportRequest.class)))
        .thenThrow(new ApplicationNotFoundException(appNotFoundExceptionMsg));

    fetcher = new TestFedAppReportFetcher.FedAppReportFetcherForTest(conf);
    fetcher.registerSubCluster(clusterInfo1, appManager1);
    fetcher.registerSubCluster(clusterInfo2, appManager2);
  }

  @Test
  public void testFetchReportAHSEnabled() throws YarnException, IOException {
    testHelper(true);
    fetcher.getApplicationReport(appId1);
    fetcher.getApplicationReport(appId2);
    Mockito.verify(history, Mockito.times(2))
        .getApplicationReport(Mockito.any(GetApplicationReportRequest.class));
    Mockito.verify(appManager1, Mockito.times(1))
        .getApplicationReport(Mockito.any(GetApplicationReportRequest.class));
    Mockito.verify(appManager2, Mockito.times(1))
        .getApplicationReport(Mockito.any(GetApplicationReportRequest.class));
  }

  @Test
  public void testFetchReportAHSDisabled() throws Exception {
    testHelper(false);

    /* RM will not know of the app and Application History Service is disabled
     * So we will not try to get the report from AHS and RM will throw
     * ApplicationNotFoundException
     */
    LambdaTestUtils.intercept(ApplicationNotFoundException.class, appNotFoundExceptionMsg,
        () -> fetcher.getApplicationReport(appId1));
    LambdaTestUtils.intercept(ApplicationNotFoundException.class, appNotFoundExceptionMsg,
        () -> fetcher.getApplicationReport(appId2));

    Mockito.verify(appManager1, Mockito.times(1))
        .getApplicationReport(Mockito.any(GetApplicationReportRequest.class));
    Mockito.verify(appManager2, Mockito.times(1))
        .getApplicationReport(Mockito.any(GetApplicationReportRequest.class));
    Assert.assertNull("HistoryManager should be null as AHS is disabled", history);
  }

  @Test
  public void testGetRmAppPageUrlBase() throws IOException, YarnException {
    testHelper(true);
    String scheme = WebAppUtils.getHttpSchemePrefix(conf);
    Assert.assertEquals(fetcher.getRmAppPageUrlBase(appId1),
        StringHelper.pjoin(scheme + clusterInfo1.getRMWebServiceAddress(), "cluster", "app"));
    Assert.assertEquals(fetcher.getRmAppPageUrlBase(appId2),
        StringHelper.pjoin(scheme + clusterInfo2.getRMWebServiceAddress(), "cluster", "app"));
  }

  static class FedAppReportFetcherForTest extends FedAppReportFetcher {

    FedAppReportFetcherForTest(Configuration conf) {
      super(conf);
    }

    @Override
    protected ApplicationHistoryProtocol getAHSProxy(Configuration conf)
        throws IOException {
      GetApplicationReportResponse resp = Mockito.mock(GetApplicationReportResponse.class);
      history = Mockito.mock(ApplicationHistoryProtocol.class);
      try {
        Mockito.when(history.getApplicationReport(Mockito.any(GetApplicationReportRequest.class)))
            .thenReturn(resp);
      } catch (YarnException e) {
        // This should never happen
        fail("Found exception when getApplicationReport!");
      }
      return history;
    }
  }
}
