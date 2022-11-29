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
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;

public abstract class RouterBlock extends HtmlBlock {

  private final Router router;
  private final ViewContext ctx;
  private final FederationStateStoreFacade facade;
  private final Configuration conf;

  public RouterBlock(Router router, ViewContext ctx) {
    super(ctx);
    this.ctx = ctx;
    this.router = router;
    this.facade = FederationStateStoreFacade.getInstance();
    this.conf = this.router.getConfig();
  }

  /**
   * Get RouterClusterMetrics Info.
   *
   * @return Router ClusterMetricsInfo.
   */
  protected ClusterMetricsInfo getRouterClusterMetricsInfo() {
    boolean isEnabled = isYarnFederationEnabled();
    if(isEnabled) {
      String webAppAddress = WebAppUtils.getRouterWebAppURLWithScheme(conf);
      Client client = RouterWebServiceUtil.createJerseyClient(conf);
      ClusterMetricsInfo metrics = RouterWebServiceUtil
          .genericForward(webAppAddress, null, ClusterMetricsInfo.class, HTTPMethods.GET,
          RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.METRICS, null, null,
          conf, client);
      client.destroy();
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
    boolean isEnabled = conf.getBoolean(
        YarnConfiguration.FEDERATION_ENABLED,
        YarnConfiguration.DEFAULT_FEDERATION_ENABLED);
    return isEnabled;
  }

  /**
   * Get a list of SubClusterIds for ActiveSubClusters.
   *
   * @return list of SubClusterIds.
   */
  protected List<String> getActiveSubClusterIds() {
    List<String> result = new ArrayList<>();
    try {
      Map<SubClusterId, SubClusterInfo> subClustersInfo = facade.getSubClusters(true);
      subClustersInfo.values().stream().forEach(subClusterInfo -> {
        result.add(subClusterInfo.getSubClusterId().getId());
      });
    } catch (Exception e) {
      LOG.error("getActiveSubClusters error.", e);
    }
    return result;
  }

  /**
   * init SubCluster MetricsOverviewTable.
   *
   * @param html HTML Object.
   * @param subclusterId subClusterId
   */
  protected void initSubClusterMetricsOverviewTable(Block html, String subclusterId) {
    MetricsOverviewTable metricsOverviewTable = new MetricsOverviewTable(this.router, this.ctx);
    metricsOverviewTable.render(html, subclusterId);
  }

  /**
   * Get ClusterMetricsInfo By SubClusterId.
   *
   * @param subclusterId subClusterId
   * @return SubCluster RM ClusterMetricsInfo
   */
  protected ClusterMetricsInfo getClusterMetricsInfoBySubClusterId(String subclusterId) {
    try {
      SubClusterId subClusterId = SubClusterId.newInstance(subclusterId);
      SubClusterInfo subClusterInfo = facade.getSubCluster(subClusterId);
      if (subClusterInfo != null) {
        Client client = RouterWebServiceUtil.createJerseyClient(this.conf);
        // Call the RM interface to obtain schedule information
        String webAppAddress =  WebAppUtils.getHttpSchemePrefix(this.conf) +
            subClusterInfo.getRMWebServiceAddress();
        ClusterMetricsInfo metrics = RouterWebServiceUtil
            .genericForward(webAppAddress, null, ClusterMetricsInfo.class, HTTPMethods.GET,
            RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.METRICS, null, null,
            conf, client);
        client.destroy();
        return metrics;
      }
    } catch (Exception e) {
      LOG.error("getClusterMetricsInfoBySubClusterId subClusterId = {} error.", subclusterId, e);
    }
    return null;
  }

  protected Collection<SubClusterInfo> getSubClusterInfoList(String subclusterId) {
    try {
      SubClusterId subClusterId = SubClusterId.newInstance(subclusterId);
      SubClusterInfo subClusterInfo = facade.getSubCluster(subClusterId);
      return Collections.singletonList(subClusterInfo);
    } catch (Exception e) {
      LOG.error("getSubClusterInfoList subClusterId = {} error.", subclusterId, e);
    }
    return null;
  }

  public FederationStateStoreFacade getFacade() {
    return facade;
  }

  /**
   * Initialize the Nodes menu.
   *
   * @param mainList HTML Object.
   * @param subClusterIds subCluster List.
   */
  protected void initNodesMenu(Hamlet.UL<Hamlet.DIV<Hamlet>> mainList,
      List<String> subClusterIds) {
    if (CollectionUtils.isNotEmpty(subClusterIds)) {
      Hamlet.UL<Hamlet.LI<Hamlet.UL<Hamlet.DIV<Hamlet>>>> nodesList =
          mainList.li().a(url("nodes"), "Nodes").ul().
          $style("padding:0.3em 1em 0.1em 2em");

      // ### nodes info
      nodesList.li().__();
      for (String subClusterId : subClusterIds) {
        nodesList.li().a(url("nodes", subClusterId), subClusterId).__();
      }
      nodesList.__().__();
    } else {
      mainList.li().a(url("nodes"), "Nodes").__();
    }
  }

  /**
   * Initialize the Applications menu.
   *
   * @param mainList HTML Object.
   * @param subClusterIds subCluster List.
   */
  protected void initApplicationsMenu(Hamlet.UL<Hamlet.DIV<Hamlet>> mainList,
      List<String> subClusterIds) {
    if (CollectionUtils.isNotEmpty(subClusterIds)) {
      Hamlet.UL<Hamlet.LI<Hamlet.UL<Hamlet.DIV<Hamlet>>>> apps =
          mainList.li().a(url("apps"), "Applications").ul();
      apps.li().__();
      for (String subClusterId : subClusterIds) {
        Hamlet.LI<Hamlet.UL<Hamlet.LI<Hamlet.UL<Hamlet.DIV<Hamlet>>>>> subClusterList = apps.
            li().a(url("apps", subClusterId), subClusterId);
        Hamlet.UL<Hamlet.LI<Hamlet.UL<Hamlet.LI<Hamlet.UL<Hamlet.DIV<Hamlet>>>>>> subAppStates =
            subClusterList.ul().$style("padding:0.3em 1em 0.1em 2em");
        subAppStates.li().__();
        for (YarnApplicationState state : YarnApplicationState.values()) {
          subAppStates.
              li().a(url("apps", subClusterId, state.toString()), state.toString()).__();
        }
        subAppStates.li().__().__();
        subClusterList.__();
      }
      apps.__().__();
    } else {
      mainList.li().a(url("apps"), "Applications").__();
    }
  }

  /**
   * Initialize the NodeLabels menu.
   *
   * @param mainList HTML Object.
   * @param subClusterIds subCluster List.
   */
  protected void initNodeLabelsMenu(Hamlet.UL<Hamlet.DIV<Hamlet>> mainList,
      List<String> subClusterIds) {

    if (CollectionUtils.isNotEmpty(subClusterIds)) {
      Hamlet.UL<Hamlet.LI<Hamlet.UL<Hamlet.DIV<Hamlet>>>> nodesList =
          mainList.li().a(url("nodelabels"), "Node Labels").ul().
          $style("padding:0.3em 1em 0.1em 2em");

      // ### nodelabels info
      nodesList.li().__();
      for (String subClusterId : subClusterIds) {
        nodesList.li().a(url("nodelabels", subClusterId), subClusterId).__();
      }
      nodesList.__().__();
    } else {
      mainList.li().a(url("nodelabels"), "Node Labels").__();
    }
  }
}
