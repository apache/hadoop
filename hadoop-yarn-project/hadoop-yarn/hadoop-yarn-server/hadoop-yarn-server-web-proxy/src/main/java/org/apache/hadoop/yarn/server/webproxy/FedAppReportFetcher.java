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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.failover.FederationProxyProviderUtil;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationApplicationHistory;
import org.apache.hadoop.yarn.server.federation.utils.FederationApplicationHistory.HistoryReport;
import org.apache.hadoop.yarn.server.federation.utils.FederationApplicationRMProbe;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.util.StringHelper;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FedAppReportFetcher extends AppReportFetcher {

  private static final Logger LOG = LoggerFactory.getLogger(FedAppReportFetcher.class);

  private final Map<SubClusterId, Pair<SubClusterInfo, ApplicationClientProtocol>> subClusters;
  private FederationStateStoreFacade federationFacade;
  private final FederationApplicationRMProbe applicationProbe;
  private final FederationApplicationHistory applicationHistory;
  private final Map<SubClusterId, ApplicationClientProtocol> probeRMProxies =
      new ConcurrentHashMap<>();

  /**
   * Create a new Connection to the RM/Application History Server to fetch
   * Application reports.
   *
   * @param conf the conf to use to know where the RM is.
   */
  public FedAppReportFetcher(Configuration conf) {
    super(conf);
    subClusters = new ConcurrentHashMap<>();
    federationFacade = FederationStateStoreFacade.getInstance(conf);
    applicationProbe = new FederationApplicationRMProbe(this::getProbeRMProxy,
        FederationApplicationRMProbe.getTimeout(conf));
    applicationHistory = createApplicationHistory(conf);
  }

  /**
   * Get an application report for the specified application id from the RM and
   * fall back to the Application History Server if not found in RM.
   *
   * @param appId id of the application to get.
   * @return the ApplicationReport for the appId.
   * @throws YarnException on any error.
   * @throws IOException connection exception.
   */
  @Override
  public FetchedAppReport getApplicationReport(ApplicationId appId)
      throws YarnException, IOException {
    SubClusterId scid = null;
    // Set if some SubCluster could not tell whether it knows the application
    YarnException unconfirmed = null;
    try {
      scid = getHomeSubCluster(appId);
    } catch (ApplicationNotFoundException e) {
      LOG.debug("{} is not in any SubCluster.", appId);
    } catch (YarnException e) {
      unconfirmed = e;
    }
    if (scid != null) {
      ApplicationClientProtocol applicationsManager = subClusters.get(scid).getRight();
      try {
        ApplicationReport appReport = applicationsManager.getApplicationReport(
            GetApplicationReportRequest.newInstance(appId)).getApplicationReport();
        return new FetchedAppReport(appReport, AppReportSource.RM);
      } catch (ApplicationNotFoundException e) {
        LOG.debug("{} is no longer in SubCluster {}.", appId, scid);
      }
    }

    // The RM removed the completed application, and its home SubCluster with
    // it: ask the history servers
    FetchedAppReport fetchedAppReport = getApplicationReportFromHistory(appId, scid);
    if (fetchedAppReport == null) {
      if (unconfirmed != null) {
        // Maybe running in a SubCluster that did not answer
        throw unconfirmed;
      }
      throw new ApplicationNotFoundException("Application " + appId
          + " does not exist in any SubCluster or history server.");
    }
    return fetchedAppReport;
  }

  /**
   * Ask the history servers of the SubClusters, the one of the home
   * SubCluster first, or else the history server of the web proxy.
   *
   * @return the report, or null if no history server knows the application.
   */
  private FetchedAppReport getApplicationReportFromHistory(ApplicationId appId,
      SubClusterId scid) throws YarnException, IOException {
    if (!applicationHistory.isEnabled()) {
      return getApplicationReportFromAHS(appId);
    }
    HistoryReport history = applicationHistory.getApplicationReport(appId, scid);
    if (history == null) {
      return null;
    }
    return new FetchedAppReport(history.getApplicationReport(), history.getAppPageUrlBase());
  }

  /**
   * Create the lookup of the history servers of the SubClusters. They are
   * asked as the login user, as the RMs are. Without any, the history server
   * of the web proxy is used, as without federation.
   *
   * @param conf the configuration.
   * @return the lookup.
   */
  @VisibleForTesting
  protected FederationApplicationHistory createApplicationHistory(Configuration conf) {
    try {
      return new FederationApplicationHistory(conf, UserGroupInformation.getLoginUser(), false);
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
  }

  @Override
  public String getRmAppPageUrlBase(ApplicationId appId)
      throws IOException, YarnException {
    SubClusterId scid = getHomeSubCluster(appId);

    SubClusterInfo subClusterInfo = subClusters.get(scid).getLeft();
    String scheme = WebAppUtils.getHttpSchemePrefix(getConf());
    return StringHelper.pjoin(scheme + subClusterInfo.getRMWebServiceAddress(), "cluster", "app");
  }

  /**
   * Get the home SubCluster of an application from the StateStore, or else
   * from the RMs of all the active SubClusters, e.g. after the RM removed the
   * completed application and its home SubCluster with it.
   *
   * @param appId the application.
   * @return the home SubCluster.
   * @throws ApplicationNotFoundException if every RM answered that it does
   *           not know the application.
   * @throws YarnException if no RM knows it but some did not answer.
   * @throws IOException if the RM proxy cannot be created.
   */
  private SubClusterId getHomeSubCluster(ApplicationId appId)
      throws YarnException, IOException {
    SubClusterId scid = null;
    try {
      scid = federationFacade.getApplicationHomeSubCluster(appId);
    } catch (YarnException e) {
      LOG.debug("{} is not in the StateStore, asking every SubCluster.", appId);
    }
    if (scid == null) {
      scid = applicationProbe.find(appId, federationFacade.getSubClusters(true).keySet());
      if (scid == null) {
        throw new ApplicationNotFoundException(
            "Application " + appId + " does not exist in any SubCluster.");
      }
    }
    createSubclusterIfAbsent(scid);
    return scid;
  }

  /**
   * Get the proxy, with short timeouts, to ask the RM of a SubCluster for an
   * application whose home SubCluster is not in the StateStore.
   */
  private ApplicationClientProtocol getProbeRMProxy(SubClusterId scId) throws IOException {
    ApplicationClientProtocol proxy = probeRMProxies.get(scId);
    if (proxy == null) {
      Configuration probeConf = FederationApplicationRMProbe.createProbeConf(getConf());
      FederationProxyProviderUtil.updateConfForFederation(probeConf, scId.getId());
      proxy = ClientRMProxy.createRMProxy(probeConf, ApplicationClientProtocol.class);
      ApplicationClientProtocol existing = probeRMProxies.putIfAbsent(scId, proxy);
      if (existing != null) {
        RPC.stopProxy(proxy);
        proxy = existing;
      }
    }
    return proxy;
  }

  private void createSubclusterIfAbsent(SubClusterId scId) throws YarnException, IOException {
    if (subClusters.containsKey(scId)) {
      return;
    }
    SubClusterInfo subClusterInfo = federationFacade.getSubCluster(scId);
    Configuration subClusterConf = new Configuration(getConf());
    FederationProxyProviderUtil
        .updateConfForFederation(subClusterConf, subClusterInfo.getSubClusterId().toString());
    ApplicationClientProtocol proxy =
        ClientRMProxy.createRMProxy(subClusterConf, ApplicationClientProtocol.class);
    subClusters.put(scId, Pair.of(subClusterInfo, proxy));
  }

  public void stop() {
    super.stop();
    for (Pair pair : this.subClusters.values()) {
      RPC.stopProxy(pair.getRight());
    }
    applicationProbe.close();
    applicationHistory.close();
    for (ApplicationClientProtocol proxy : probeRMProxies.values()) {
      RPC.stopProxy(proxy);
    }
  }

  @VisibleForTesting
  public void registerSubCluster(SubClusterInfo info, ApplicationClientProtocol proxy) {
    subClusters.put(info.getSubClusterId(), Pair.of(info, proxy));
    probeRMProxies.put(info.getSubClusterId(), proxy);
  }
}
