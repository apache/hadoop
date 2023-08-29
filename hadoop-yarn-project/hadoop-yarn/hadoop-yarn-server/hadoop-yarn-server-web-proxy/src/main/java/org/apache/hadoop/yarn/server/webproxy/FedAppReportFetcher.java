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
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.failover.FederationProxyProviderUtil;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.util.StringHelper;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

public class FedAppReportFetcher extends AppReportFetcher {

  private final Map<SubClusterId, Pair<SubClusterInfo, ApplicationClientProtocol>> subClusters;
  private FederationStateStoreFacade federationFacade;

  /**
   * Create a new Connection to the RM/Application History Server to fetch
   * Application reports.
   *
   * @param conf the conf to use to know where the RM is.
   */
  public FedAppReportFetcher(Configuration conf) {
    super(conf);
    subClusters = new ConcurrentHashMap<>();
    federationFacade = FederationStateStoreFacade.getInstance();
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
    SubClusterId scid = federationFacade.getApplicationHomeSubCluster(appId);
    createSubclusterIfAbsent(scid);
    ApplicationClientProtocol applicationsManager = subClusters.get(scid).getRight();

    return super.getApplicationReport(applicationsManager, appId);
  }

  @Override
  public String getRmAppPageUrlBase(ApplicationId appId)
      throws IOException, YarnException {
    SubClusterId scid = federationFacade.getApplicationHomeSubCluster(appId);
    createSubclusterIfAbsent(scid);

    SubClusterInfo subClusterInfo = subClusters.get(scid).getLeft();
    String scheme = WebAppUtils.getHttpSchemePrefix(getConf());
    return StringHelper.pjoin(scheme + subClusterInfo.getRMWebServiceAddress(), "cluster", "app");
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
  }

  @VisibleForTesting
  public void registerSubCluster(SubClusterInfo info, ApplicationClientProtocol proxy) {
    subClusters.put(info.getSubClusterId(), Pair.of(info, proxy));
  }
}
