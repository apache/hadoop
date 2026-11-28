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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationHistoryProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.AHSProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.util.StringHelper;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Looks up applications in the Application History Servers of the
 * SubClusters, for the applications that no RM knows any more: the RM removes
 * a completed application once it keeps more than
 * yarn.resourcemanager.max-completed-applications, and its home SubCluster is
 * deleted from the Federation StateStore with it.
 * <p>
 * Each SubCluster may have its own history server, configured with
 * {@link YarnConfiguration#ROUTER_APPLICATION_HISTORY_ADDRESS_PREFIX}. Without
 * any, the Router's own yarn.timeline-service.address may be used instead if
 * the generic application history is enabled.
 * <p>
 * The history servers are asked as the given user, so that they apply their
 * application ACLs to the client of the Router as they would if it asked
 * them directly. A history server that is down or slow must not hold the
 * Router's handler: each one is tried once, with short timeouts.
 */
public class FederationApplicationHistory implements Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(FederationApplicationHistory.class);

  /** Key of the Router's own history server. */
  private static final String DEFAULT_HISTORY = "";

  /** Connect timeout towards a history server, in milliseconds. */
  private static final int CONNECT_TIMEOUT_MS = 3000;

  /** A report found in a history server. */
  public static final class HistoryReport {
    private final ApplicationReport report;
    private final String appPageUrlBase;

    HistoryReport(ApplicationReport report, String appPageUrlBase) {
      this.report = report;
      this.appPageUrlBase = appPageUrlBase;
    }

    public ApplicationReport getApplicationReport() {
      return report;
    }

    /**
     * @return the application page of the history server without the
     *         application ID, or null if its web address is not configured.
     */
    public String getAppPageUrlBase() {
      return appPageUrlBase;
    }
  }

  private final Configuration conf;
  private final UserGroupInformation user;
  /** History server RPC addresses by SubClusterId, in a stable order. */
  private final Map<String, InetSocketAddress> addresses = new TreeMap<>();
  private final Map<String, String> appPageUrlBases = new TreeMap<>();
  private final Map<String, ApplicationHistoryProtocol> proxies =
      new ConcurrentHashMap<>();

  /**
   * @param conf the Router configuration.
   * @param user the user to ask the history servers as.
   */
  public FederationApplicationHistory(Configuration conf,
      UserGroupInformation user) {
    this(conf, user, true);
  }

  /**
   * @param conf the Router configuration.
   * @param user the user to ask the history servers as.
   * @param useRouterHistory whether to use the Router's own history server
   *          when no SubCluster has one.
   */
  public FederationApplicationHistory(Configuration conf,
      UserGroupInformation user, boolean useRouterHistory) {
    this.user = user;
    String scheme = WebAppUtils.getHttpSchemePrefix(conf);
    for (Map.Entry<String, String> e : conf.getPropsWithPrefix(
        YarnConfiguration.ROUTER_APPLICATION_HISTORY_ADDRESS_PREFIX)
        .entrySet()) {
      addresses.put(e.getKey(), NetUtils.createSocketAddr(e.getValue().trim()));
    }
    for (Map.Entry<String, String> e : conf.getPropsWithPrefix(
        YarnConfiguration.ROUTER_APPLICATION_HISTORY_WEBAPP_ADDRESS_PREFIX)
        .entrySet()) {
      appPageUrlBases.put(e.getKey(), StringHelper.pjoin(
          scheme + e.getValue().trim(), "applicationhistory", "app"));
    }
    if (addresses.isEmpty() && useRouterHistory && conf.getBoolean(
        YarnConfiguration.APPLICATION_HISTORY_ENABLED,
        YarnConfiguration.DEFAULT_APPLICATION_HISTORY_ENABLED)) {
      addresses.put(DEFAULT_HISTORY, conf.getSocketAddr(
          YarnConfiguration.TIMELINE_SERVICE_ADDRESS,
          YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ADDRESS,
          YarnConfiguration.DEFAULT_TIMELINE_SERVICE_PORT));
      appPageUrlBases.put(DEFAULT_HISTORY, StringHelper.pjoin(
          scheme + WebAppUtils.getAHSWebAppURLWithoutScheme(conf),
          "applicationhistory", "app"));
    }

    this.conf = new Configuration(conf);
    this.conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);
    this.conf.setInt(CommonConfigurationKeysPublic
        .IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY, 0);
    this.conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_TIMEOUT_KEY,
        CONNECT_TIMEOUT_MS);
    this.conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY,
        (int) Math.min(FederationApplicationRMProbe.getTimeout(conf),
            Integer.MAX_VALUE));
  }

  /** @return whether any history server is configured. */
  public boolean isEnabled() {
    return !addresses.isEmpty();
  }

  /**
   * Ask the history servers for an application: the one of its home
   * SubCluster first if known, then the others.
   *
   * @param appId the application.
   * @param homeSubCluster the application's home SubCluster, or null.
   * @return the report, or null if every history server answered that it
   *         does not know the application.
   * @throws YarnException if none has it but some could not be asked.
   */
  public HistoryReport getApplicationReport(ApplicationId appId,
      SubClusterId homeSubCluster) throws YarnException {
    List<String> order = new ArrayList<>();
    if (homeSubCluster != null
        && addresses.containsKey(homeSubCluster.getId())) {
      order.add(homeSubCluster.getId());
    }
    for (String key : addresses.keySet()) {
      if (!order.contains(key)) {
        order.add(key);
      }
    }

    GetApplicationReportRequest request =
        GetApplicationReportRequest.newInstance(appId);
    int failed = 0;
    for (String key : order) {
      try {
        ApplicationReport report = getProxy(key)
            .getApplicationReport(request).getApplicationReport();
        if (report != null) {
          String appPageUrlBase = appPageUrlBases.get(key);
          if (appPageUrlBase == null) {
            LOG.warn("No {}{}: no page in the history server {} for {}.",
                YarnConfiguration.ROUTER_APPLICATION_HISTORY_WEBAPP_ADDRESS_PREFIX,
                key, addresses.get(key), appId);
          }
          return new HistoryReport(report, appPageUrlBase);
        }
      } catch (ApplicationNotFoundException e) {
        LOG.debug("{} is not in the history server {}.", appId,
            addresses.get(key));
      } catch (YarnException | IOException e) {
        LOG.warn("Unable to get {} from the history server {}.", appId,
            addresses.get(key), e);
        failed++;
      }
    }
    if (failed > 0) {
      // The one that failed may have it
      throw new YarnException(String.format("Unable to ask every history "
          + "server for %s: %d of %d failed", appId, failed, order.size()));
    }
    return null;
  }

  private ApplicationHistoryProtocol getProxy(String key) throws IOException {
    ApplicationHistoryProtocol proxy = proxies.get(key);
    if (proxy != null) {
      return proxy;
    }
    final InetSocketAddress address = addresses.get(key);
    try {
      proxy = user.doAs(
          (PrivilegedExceptionAction<ApplicationHistoryProtocol>)
              () -> createProxy(address));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
    ApplicationHistoryProtocol existing = proxies.putIfAbsent(key, proxy);
    if (existing != null) {
      RPC.stopProxy(proxy);
      return existing;
    }
    return proxy;
  }

  /**
   * Create the proxy of a history server, as the user given at construction.
   *
   * @param address the RPC address of the history server.
   * @return the proxy.
   * @throws IOException if the proxy cannot be created.
   */
  @VisibleForTesting
  protected ApplicationHistoryProtocol createProxy(InetSocketAddress address)
      throws IOException {
    return AHSProxy.createAHSProxy(conf, ApplicationHistoryProtocol.class,
        address);
  }

  @Override
  public void close() {
    for (ApplicationHistoryProtocol proxy : proxies.values()) {
      RPC.stopProxy(proxy);
    }
    proxies.clear();
  }
}
