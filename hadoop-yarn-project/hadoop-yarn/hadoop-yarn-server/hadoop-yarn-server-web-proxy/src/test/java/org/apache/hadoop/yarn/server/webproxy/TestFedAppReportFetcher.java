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
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationHistoryProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
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
import org.apache.hadoop.yarn.server.federation.utils.FederationApplicationHistory;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.StringHelper;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.fail;

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

  /** Per-SubCluster history servers by RPC port. */
  private static final Map<Integer, ApplicationHistoryProtocol> HISTORIES = new HashMap<>();

  @AfterEach
  public void cleanUp() {
    history = null;
    HISTORIES.clear();
    fetcher = null;
  }

  private void testHelper(boolean isAHSEnabled)
      throws YarnException, IOException {
    Configuration configuration = new YarnConfiguration();
    configuration.setBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED, isAHSEnabled);
    testHelper(configuration);
  }

  private void testHelper(Configuration configuration)
      throws YarnException, IOException {
    conf = configuration;

    FederationStateStoreFacade fedFacade = FederationStateStoreFacade.getInstance(this.conf);
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
     * So we will not try to get the report from AHS
     */
    LambdaTestUtils.intercept(ApplicationNotFoundException.class, appId1.toString(),
        () -> fetcher.getApplicationReport(appId1));
    LambdaTestUtils.intercept(ApplicationNotFoundException.class, appId2.toString(),
        () -> fetcher.getApplicationReport(appId2));

    Mockito.verify(appManager1, Mockito.times(1))
        .getApplicationReport(Mockito.any(GetApplicationReportRequest.class));
    Mockito.verify(appManager2, Mockito.times(1))
        .getApplicationReport(Mockito.any(GetApplicationReportRequest.class));
    Assertions.assertNull(history, "HistoryManager should be null as AHS is disabled");
  }

  @Test
  public void testGetRmAppPageUrlBase() throws IOException, YarnException {
    testHelper(true);
    String scheme = WebAppUtils.getHttpSchemePrefix(conf);
    Assertions.assertEquals(fetcher.getRmAppPageUrlBase(appId1),
        StringHelper.pjoin(scheme + clusterInfo1.getRMWebServiceAddress(), "cluster", "app"));
    Assertions.assertEquals(fetcher.getRmAppPageUrlBase(appId2),
        StringHelper.pjoin(scheme + clusterInfo2.getRMWebServiceAddress(), "cluster", "app"));
  }

  /**
   * The home SubCluster is not in the StateStore, but the RM of another
   * SubCluster still knows the application.
   */
  @Test
  public void testFetchReportWithoutHomeSubCluster() throws Exception {
    testHelper(true);
    ApplicationId appId3 = ApplicationId.newInstance(0, 3);
    ApplicationReport report = Records.newRecord(ApplicationReport.class);
    report.setApplicationId(appId3);
    Mockito.doReturn(GetApplicationReportResponse.newInstance(report))
        .when(appManager2).getApplicationReport(Mockito.any(GetApplicationReportRequest.class));

    AppReportFetcher.FetchedAppReport fetched = fetcher.getApplicationReport(appId3);
    Assertions.assertEquals(AppReportFetcher.AppReportSource.RM, fetched.getAppReportSource());
    Assertions.assertEquals(appId3, fetched.getApplicationReport().getApplicationId());
    String scheme = WebAppUtils.getHttpSchemePrefix(conf);
    Assertions.assertEquals(
        StringHelper.pjoin(scheme + clusterInfo2.getRMWebServiceAddress(), "cluster", "app"),
        fetcher.getRmAppPageUrlBase(appId3));
    Mockito.verify(history, Mockito.never())
        .getApplicationReport(Mockito.any(GetApplicationReportRequest.class));
  }

  /**
   * No RM knows the application: it is fetched from the history server, or
   * reported missing if the history server is disabled.
   */
  @Test
  public void testFetchReportInNoSubCluster() throws Exception {
    ApplicationId appId3 = ApplicationId.newInstance(0, 3);
    testHelper(true);
    Assertions.assertEquals(AppReportFetcher.AppReportSource.AHS,
        fetcher.getApplicationReport(appId3).getAppReportSource());

    testHelper(false);
    LambdaTestUtils.intercept(ApplicationNotFoundException.class,
        "Application " + appId3 + " does not exist in any SubCluster or history server.",
        () -> fetcher.getApplicationReport(appId3));
  }

  /**
   * No RM knows the application any more: it is found in the history server
   * of its SubCluster, and the proxy redirects to that one.
   */
  @Test
  public void testFetchReportFromSubClusterHistory() throws Exception {
    Configuration configuration = new YarnConfiguration();
    configuration.set(YarnConfiguration.ROUTER_APPLICATION_HISTORY_ADDRESS_PREFIX
        + subClusterId1, "ahs1:1001");
    configuration.set(YarnConfiguration.ROUTER_APPLICATION_HISTORY_ADDRESS_PREFIX
        + subClusterId2, "ahs2:1002");
    configuration.set(YarnConfiguration.ROUTER_APPLICATION_HISTORY_WEBAPP_ADDRESS_PREFIX
        + subClusterId2, "ahs2:8188");
    ApplicationHistoryProtocol ahs1 = Mockito.mock(ApplicationHistoryProtocol.class);
    Mockito.when(ahs1.getApplicationReport(Mockito.any(GetApplicationReportRequest.class)))
        .thenThrow(new ApplicationNotFoundException(appNotFoundExceptionMsg));
    ApplicationHistoryProtocol ahs2 = Mockito.mock(ApplicationHistoryProtocol.class);
    Mockito.when(ahs2.getApplicationReport(Mockito.any(GetApplicationReportRequest.class)))
        .thenReturn(GetApplicationReportResponse.newInstance(newReport(appId2)));
    HISTORIES.put(1001, ahs1);
    HISTORIES.put(1002, ahs2);
    testHelper(configuration);

    AppReportFetcher.FetchedAppReport fetched = fetcher.getApplicationReport(appId2);
    Assertions.assertEquals(AppReportFetcher.AppReportSource.AHS, fetched.getAppReportSource());
    Assertions.assertEquals(appId2, fetched.getApplicationReport().getApplicationId());
    Assertions.assertEquals("http://ahs2:8188/applicationhistory/app",
        fetched.getAhsAppPageUrlBase(null));
    // Its home SubCluster is known: its history server is asked first
    Mockito.verify(ahs1, Mockito.never())
        .getApplicationReport(Mockito.any(GetApplicationReportRequest.class));
  }

  /**
   * No RM knows the application and its history server cannot be asked: the
   * proxy must not report it missing.
   */
  @Test
  public void testFetchReportWithUnreachableHistory() throws Exception {
    Configuration configuration = new YarnConfiguration();
    configuration.set(YarnConfiguration.ROUTER_APPLICATION_HISTORY_ADDRESS_PREFIX
        + subClusterId1, "ahs1:1001");
    ApplicationHistoryProtocol ahs1 = Mockito.mock(ApplicationHistoryProtocol.class);
    Mockito.when(ahs1.getApplicationReport(Mockito.any(GetApplicationReportRequest.class)))
        .thenThrow(new ConnectException("AHS is down"));
    HISTORIES.put(1001, ahs1);
    testHelper(configuration);

    YarnException e = Assertions.assertThrows(YarnException.class,
        () -> fetcher.getApplicationReport(appId1));
    Assertions.assertFalse(e instanceof ApplicationNotFoundException, e.toString());
    Assertions.assertTrue(e.getMessage().startsWith("Unable to ask every history server"),
        e.getMessage());
  }

  private static ApplicationReport newReport(ApplicationId appId) {
    ApplicationReport report = Records.newRecord(ApplicationReport.class);
    report.setApplicationId(appId);
    return report;
  }

  /**
   * No RM knows the application but one cannot be asked: the application may
   * be running there, so the fetcher must not report it missing.
   */
  @Test
  public void testFetchReportWithUnreachableSubCluster() throws Exception {
    testHelper(false);
    Mockito.doThrow(new ConnectException("RM is down"))
        .when(appManager2).getApplicationReport(Mockito.any(GetApplicationReportRequest.class));
    ApplicationId appId3 = ApplicationId.newInstance(0, 3);
    YarnException e = Assertions.assertThrows(YarnException.class,
        () -> fetcher.getApplicationReport(appId3));
    Assertions.assertFalse(e instanceof ApplicationNotFoundException, e.toString());
    Assertions.assertTrue(e.getMessage().startsWith("Unable to ask every SubCluster"),
        e.getMessage());
  }

  static class FedAppReportFetcherForTest extends FedAppReportFetcher {

    FedAppReportFetcherForTest(Configuration conf) {
      super(conf);
    }

    @Override
    protected FederationApplicationHistory createApplicationHistory(Configuration conf) {
      return new FederationApplicationHistory(conf, UserGroupInformation.createRemoteUser("proxy"),
          false) {
        @Override
        protected ApplicationHistoryProtocol createProxy(InetSocketAddress address) {
          return HISTORIES.get(address.getPort());
        }

        @Override
        public void close() {
          // The proxies are mocks
        }
      };
    }

    @Override
    protected ApplicationHistoryProtocol getAHSProxy(Configuration conf)
        throws IOException {
      history = Mockito.mock(ApplicationHistoryProtocol.class);
      try {
        Mockito.when(history.getApplicationReport(Mockito.any(GetApplicationReportRequest.class)))
            .thenAnswer(invocation -> GetApplicationReportResponse.newInstance(newReport(
                invocation.<GetApplicationReportRequest>getArgument(0).getApplicationId())));
      } catch (YarnException e) {
        // This should never happen
        fail("Found exception when getApplicationReport!");
      }
      return history;
    }
  }
}
