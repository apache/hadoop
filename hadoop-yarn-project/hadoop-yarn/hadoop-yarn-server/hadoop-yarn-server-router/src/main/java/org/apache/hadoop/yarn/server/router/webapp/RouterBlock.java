/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.router.webapp;

import com.sun.jersey.api.client.Client;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Collections;
import java.util.Comparator;

public abstract class RouterBlock extends HtmlBlock {

  private final Router router;

  public RouterBlock(Router router, ViewContext ctx) {
    super(ctx);
    this.router = router;
  }

  /**
   * Get RouterClusterMetrics Info.
   *
   * @return Router ClusterMetricsInfo.
   */
  protected ClusterMetricsInfo getRouterClusterMetricsInfo() {
    Configuration conf = this.router.getConfig();
    boolean isEnabled = isYarnFederationEnabled();
    if(isEnabled) {
      String webAppAddress = WebAppUtils.getRouterWebAppURLWithScheme(conf);
      Client client = RouterWebServiceUtil.createJerseyClient(conf);
      ClusterMetricsInfo metrics = RouterWebServiceUtil
          .genericForward(webAppAddress, null, ClusterMetricsInfo.class, HTTPMethods.GET,
          RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.METRICS, null, null,
          conf, client);
      return metrics;
    }
    return null;
  }

  /**
   * Get a list of subclusters.
   *
   * @return subcluster List.
   * @throws YarnException if the call to the getSubClusters is unsuccessful.
   */
  protected List<SubClusterInfo> getSubClusterInfoList() throws YarnException {
    FederationStateStoreFacade facade = FederationStateStoreFacade.getInstance();
    Map<SubClusterId, SubClusterInfo> subClustersInfo = facade.getSubClusters(true);

    // Sort the SubClusters.
    List<SubClusterInfo> subclusters = new ArrayList<>();
    subclusters.addAll(subClustersInfo.values());
    Comparator<? super SubClusterInfo> cmp = Comparator.comparing(o -> o.getSubClusterId());
    Collections.sort(subclusters, cmp);

    return subclusters;
  }

  /**
   * Whether Yarn Federation is enabled.
   *
   * @return true, enable yarn federation; false, not enable yarn federation;
   */
  protected boolean isYarnFederationEnabled() {
    Configuration conf = this.router.getConfig();
    boolean isEnabled = conf.getBoolean(
        YarnConfiguration.FEDERATION_ENABLED,
        YarnConfiguration.DEFAULT_FEDERATION_ENABLED);
    return isEnabled;
  }
}
