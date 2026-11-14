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

package org.apache.hadoop.yarn.server.federation.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationHistoryProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.utils.FederationApplicationHistory.HistoryReport;
import org.apache.hadoop.yarn.util.Records;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link FederationApplicationHistory}.
 */
public class TestFederationApplicationHistory {

  private final ApplicationId appId = ApplicationId.newInstance(1, 1);
  private final ApplicationReport report = newReport(appId);
  private final UserGroupInformation user =
      UserGroupInformation.createRemoteUser("alice");

  /** History servers by RPC port. */
  private final Map<Integer, ApplicationHistoryProtocol> servers =
      new HashMap<>();
  /** Users the proxies were created as. */
  private final List<String> proxyUsers = new ArrayList<>();

  private static ApplicationReport newReport(ApplicationId appId) {
    ApplicationReport report = Records.newRecord(ApplicationReport.class);
    report.setApplicationId(appId);
    return report;
  }

  private FederationApplicationHistory newHistory(Configuration conf) {
    return new FederationApplicationHistory(conf, user) {
      @Override
      protected ApplicationHistoryProtocol createProxy(
          InetSocketAddress address) throws IOException {
        proxyUsers.add(UserGroupInformation.getCurrentUser().getUserName());
        return servers.get(address.getPort());
      }

      @Override
      public void close() {
        // The proxies are mocks
      }
    };
  }

  private ApplicationHistoryProtocol server(int port, boolean knowsApp)
      throws Exception {
    ApplicationHistoryProtocol server = mock(ApplicationHistoryProtocol.class);
    if (knowsApp) {
      when(server.getApplicationReport(any(GetApplicationReportRequest.class)))
          .thenReturn(GetApplicationReportResponse.newInstance(report));
    } else {
      when(server.getApplicationReport(any(GetApplicationReportRequest.class)))
          .thenThrow(new ApplicationNotFoundException("not here"));
    }
    servers.put(port, server);
    return server;
  }

  private ApplicationHistoryProtocol downServer(int port) throws Exception {
    ApplicationHistoryProtocol server = mock(ApplicationHistoryProtocol.class);
    when(server.getApplicationReport(any(GetApplicationReportRequest.class)))
        .thenThrow(new ConnectException("down"));
    servers.put(port, server);
    return server;
  }

  private Configuration perSubClusterConf() {
    Configuration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.ROUTER_APPLICATION_HISTORY_ADDRESS_PREFIX
        + "SC1", "ahs1:1001");
    conf.set(YarnConfiguration.ROUTER_APPLICATION_HISTORY_ADDRESS_PREFIX
        + "SC2", "ahs2:1002");
    conf.set(YarnConfiguration.ROUTER_APPLICATION_HISTORY_WEBAPP_ADDRESS_PREFIX
        + "SC2", "ahs2:8188");
    return conf;
  }

  @Test
  public void testAsksEveryHistoryServer() throws Exception {
    ApplicationHistoryProtocol ahs1 = server(1001, false);
    server(1002, true);
    FederationApplicationHistory history = newHistory(perSubClusterConf());

    HistoryReport found = history.getApplicationReport(appId, null);
    assertSame(report, found.getApplicationReport());
    assertEquals("http://ahs2:8188/applicationhistory/app",
        found.getAppPageUrlBase());
    verify(ahs1).getApplicationReport(any(GetApplicationReportRequest.class));
  }

  /** The history servers apply their ACLs to the client of the Router. */
  @Test
  public void testAsksAsTheGivenUser() throws Exception {
    server(1001, false);
    server(1002, true);
    FederationApplicationHistory history = newHistory(perSubClusterConf());

    history.getApplicationReport(appId, null);
    assertEquals(2, proxyUsers.size());
    for (String proxyUser : proxyUsers) {
      assertEquals("alice", proxyUser);
    }
  }

  @Test
  public void testAsksHomeSubClusterFirst() throws Exception {
    ApplicationHistoryProtocol ahs1 = server(1001, true);
    server(1002, true);
    FederationApplicationHistory history = newHistory(perSubClusterConf());

    HistoryReport found =
        history.getApplicationReport(appId, SubClusterId.newInstance("SC2"));
    assertSame(report, found.getApplicationReport());
    verify(ahs1, never())
        .getApplicationReport(any(GetApplicationReportRequest.class));
  }

  @Test
  public void testSkipsUnreachableHistoryServer() throws Exception {
    downServer(1001);
    server(1002, true);
    FederationApplicationHistory history = newHistory(perSubClusterConf());

    HistoryReport found = history.getApplicationReport(appId, null);
    assertSame(report, found.getApplicationReport());
  }

  /** A history server that cannot be asked might have the application. */
  @Test
  public void testUnreachableHistoryServerIsNotNotFound() throws Exception {
    downServer(1001);
    server(1002, false);
    FederationApplicationHistory history = newHistory(perSubClusterConf());

    YarnException e = assertThrows(YarnException.class,
        () -> history.getApplicationReport(appId, null));
    assertTrue(e.getMessage().startsWith("Unable to ask every history server"),
        e.getMessage());
  }

  @Test
  public void testNotFound() throws Exception {
    server(1001, false);
    server(1002, false);
    FederationApplicationHistory history = newHistory(perSubClusterConf());
    assertNull(history.getApplicationReport(appId,
        SubClusterId.newInstance("SC1")));
  }

  @Test
  public void testNoWebAddress() throws Exception {
    server(1001, true);
    FederationApplicationHistory history = newHistory(perSubClusterConf());
    HistoryReport found =
        history.getApplicationReport(appId, SubClusterId.newInstance("SC1"));
    assertSame(report, found.getApplicationReport());
    assertNull(found.getAppPageUrlBase());
  }

  @Test
  public void testRouterHistoryServerWithoutPerSubClusterConf()
      throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED, true);
    conf.set(YarnConfiguration.TIMELINE_SERVICE_ADDRESS, "ahs0:1000");
    conf.set(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS, "ahs0:8188");
    server(1000, true);
    FederationApplicationHistory history = newHistory(conf);

    assertTrue(history.isEnabled());
    HistoryReport found =
        history.getApplicationReport(appId, SubClusterId.newInstance("SC1"));
    assertSame(report, found.getApplicationReport());
    assertEquals("http://ahs0:8188/applicationhistory/app",
        found.getAppPageUrlBase());

    // Unless the caller has its own way to ask that one
    assertFalse(new FederationApplicationHistory(conf, user, false).isEnabled());
  }

  @Test
  public void testDisabled() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED, false);
    FederationApplicationHistory history = newHistory(conf);
    assertFalse(history.isEnabled());
    assertNull(history.getApplicationReport(appId, null));
  }
}
