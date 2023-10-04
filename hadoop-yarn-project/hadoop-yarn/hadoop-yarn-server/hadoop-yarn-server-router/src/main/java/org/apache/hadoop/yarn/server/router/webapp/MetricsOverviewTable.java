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

import com.google.inject.Inject;
import com.sun.jersey.api.client.Client;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerOverviewInfo;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.server.router.webapp.dao.RouterClusterMetrics;
import org.apache.hadoop.yarn.server.router.webapp.dao.RouterSchedulerMetrics;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class MetricsOverviewTable extends RouterBlock {

  private final Router router;

  @Inject
  MetricsOverviewTable(Router router, ViewContext ctx) {
    super(router, ctx);
    this.router = router;
  }

  @Override
  protected void render(Block html) {
    // Initialize page styles
    html.style(".metrics {margin-bottom:5px}");

    // get routerClusterMetrics Info
    ClusterMetricsInfo routerClusterMetricsInfo = getRouterClusterMetricsInfo();
    RouterClusterMetrics routerClusterMetrics = new RouterClusterMetrics(routerClusterMetricsInfo);

    // metrics div
    Hamlet.DIV<Hamlet> div = html.div().$class("metrics");
    try {
      initFederationClusterAppsMetrics(div, routerClusterMetrics);
      initFederationClusterNodesMetrics(div, routerClusterMetrics);
      List<SubClusterInfo> subClusters = getSubClusterInfoList();
      initFederationClusterSchedulersMetrics(div, routerClusterMetrics, subClusters);
    } catch (Exception e) {
      LOG.error("MetricsOverviewTable init error.", e);
    }
    div.__();
  }

  protected void render(Block html, String subClusterId) {
    // Initialize page styles
    html.style(".metrics {margin-bottom:5px}");

    // get subClusterId ClusterMetrics Info
    ClusterMetricsInfo clusterMetricsInfo =
        getClusterMetricsInfoBySubClusterId(subClusterId);
    RouterClusterMetrics routerClusterMetrics =
        new RouterClusterMetrics(clusterMetricsInfo, subClusterId);

    // metrics div
    Hamlet.DIV<Hamlet> div = html.div().$class("metrics");
    try {
      initFederationClusterAppsMetrics(div, routerClusterMetrics);
      initFederationClusterNodesMetrics(div, routerClusterMetrics);
      Collection<SubClusterInfo> subClusters = getSubClusterInfoList(subClusterId);
      initFederationClusterSchedulersMetrics(div, routerClusterMetrics, subClusters);
    } catch (Exception e) {
      LOG.error("MetricsOverviewTable init error.", e);
    }
    div.__();
  }

  /**
   * Init Federation Cluster Apps Metrics.
   * Contains App information, resource usage information.
   *
   * @param div data display div.
   * @param metrics data metric information.
   */
  private void initFederationClusterAppsMetrics(Hamlet.DIV<Hamlet> div,
      RouterClusterMetrics metrics) {
    div.h3(metrics.getWebPageTitlePrefix() + " Cluster Metrics").
        table("#metricsoverview").
        thead().$class("ui-widget-header").
        // Initialize table header information
        tr().
        th().$class("ui-state-default").__("Apps Submitted").__().
        th().$class("ui-state-default").__("Apps Pending").__().
        th().$class("ui-state-default").__("Apps Running").__().
        th().$class("ui-state-default").__("Apps Completed").__().
        th().$class("ui-state-default").__("Containers Running").__().
        th().$class("ui-state-default").__("Used Resources").__().
        th().$class("ui-state-default").__("Total Resources").__().
        th().$class("ui-state-default").__("Reserved Resources").__().
        th().$class("ui-state-default").__("Physical Mem Used %").__().
        th().$class("ui-state-default").__("Physical VCores Used %").__().
        __().
        __().
        // Initialize table data information
        tbody().$class("ui-widget-content").
        tr().
        td(metrics.getAppsSubmitted()).
        td(metrics.getAppsPending()).
        td(String.valueOf(metrics.getAppsRunning())).
        td(metrics.getAppsCompleted()).
        td(metrics.getAllocatedContainers()).
        td(metrics.getUsedResources()).
        td(metrics.getTotalResources()).
        td(metrics.getReservedResources()).
        td(metrics.getUtilizedMBPercent()).
        td(metrics.getUtilizedVirtualCoresPercent()).
        __().
        __().__();
  }

  /**
   * Init Federation Cluster Nodes Metrics.
   *
   * @param div data display div.
   * @param metrics data metric information.
   */
  private void initFederationClusterNodesMetrics(Hamlet.DIV<Hamlet> div,
      RouterClusterMetrics metrics) {
    div.h3(metrics.getWebPageTitlePrefix() + " Cluster Nodes Metrics").
        table("#nodemetricsoverview").
        thead().$class("ui-widget-header").
        // Initialize table header information
        tr().
        th().$class("ui-state-default").__("Active Nodes").__().
        th().$class("ui-state-default").__("Decommissioning Nodes").__().
        th().$class("ui-state-default").__("Decommissioned Nodes").__().
        th().$class("ui-state-default").__("Lost Nodes").__().
        th().$class("ui-state-default").__("Unhealthy Nodes").__().
        th().$class("ui-state-default").__("Rebooted Nodes").__().
        th().$class("ui-state-default").__("Shutdown Nodes").__().
        __().
        __().
        // Initialize table data information
        tbody().$class("ui-widget-content").
        tr().
        td().a(url("nodes"), String.valueOf(metrics.getActiveNodes())).__().
        td().a(url("nodes/router/?node.state=decommissioning"),
            String.valueOf(metrics.getDecommissioningNodes())).__().
        td().a(url("nodes/router/?node.state=decommissioned"),
            String.valueOf(metrics.getDecommissionedNodes())).__().
        td().a(url("nodes/router/?node.state=lost"),
            String.valueOf(metrics.getLostNodes())).__().
        td().a(url("nodes/router/?node.state=unhealthy"),
            String.valueOf(metrics.getUnhealthyNodes())).__().
        td().a(url("nodes/router/?node.state=rebooted"),
            String.valueOf(metrics.getRebootedNodes())).__().
        td().a(url("nodes/router/?node.state=shutdown"),
            String.valueOf(metrics.getShutdownNodes())).__().
        __().
        __().__();
  }

  /**
   * Init Federation Cluster SchedulersMetrics.
   *
   * @param div data display div.
   * @param metrics data metric information.
   * @param subclusters active subcluster List.
   * @throws YarnException yarn error.
   * @throws IOException io error.
   * @throws InterruptedException interrupt error.
   */
  private void initFederationClusterSchedulersMetrics(Hamlet.DIV<Hamlet> div,
      RouterClusterMetrics metrics, Collection<SubClusterInfo> subclusters)
      throws YarnException, IOException, InterruptedException {

    Hamlet.TBODY<Hamlet.TABLE<Hamlet.DIV<Hamlet>>> fsMetricsScheduleTr =
        div.h3(metrics.getWebPageTitlePrefix() + " Scheduler Metrics").
        table("#schedulermetricsoverview").
        thead().$class("ui-widget-header").
        tr().
        th().$class("ui-state-default").__("SubCluster").__().
        th().$class("ui-state-default").__("Scheduler Type").__().
        th().$class("ui-state-default").__("Scheduling Resource Type").__().
        th().$class("ui-state-default").__("Minimum Allocation").__().
        th().$class("ui-state-default").__("Maximum Allocation").__().
        th().$class("ui-state-default").__("Maximum Cluster Application Priority").__().
        th().$class("ui-state-default").__("Scheduler Busy %").__().
        th().$class("ui-state-default").__("RM Dispatcher EventQueue Size").__().
        th().$class("ui-state-default")
        .__("Scheduler Dispatcher EventQueue Size").__().
        __().
        __().
        tbody().$class("ui-widget-content");

    boolean isEnabled = isYarnFederationEnabled();

    // If Federation mode is not enabled or there is currently no SubCluster available,
    // each column in the list should be displayed as N/A
    if (!isEnabled) {
      initLocalClusterOverViewTable(fsMetricsScheduleTr);
    } else if (subclusters != null && !subclusters.isEmpty()) {
      initSubClusterOverViewTable(metrics, fsMetricsScheduleTr, subclusters);
    } else {
      showRouterSchedulerMetricsData(UNAVAILABLE, fsMetricsScheduleTr);
    }

    fsMetricsScheduleTr.__().__();
  }

  /**
   * We display Scheduler information for local cluster.
   *
   * @param fsMetricsScheduleTr MetricsScheduleTr.
   */
  private void initLocalClusterOverViewTable(
      Hamlet.TBODY<Hamlet.TABLE<Hamlet.DIV<Hamlet>>> fsMetricsScheduleTr) {
    // configuration
    Configuration config = this.router.getConfig();
    Client client = RouterWebServiceUtil.createJerseyClient(config);
    String webAppAddress = WebAppUtils.getRMWebAppURLWithScheme(config);

    // Get the name of the local cluster.
    String localClusterName = config.get(YarnConfiguration.RM_CLUSTER_ID, UNAVAILABLE);
    SchedulerOverviewInfo schedulerOverviewInfo =
        getSchedulerOverviewInfo(webAppAddress, config, client);
    if (schedulerOverviewInfo != null) {
      RouterSchedulerMetrics rsMetrics =
          new RouterSchedulerMetrics(localClusterName, schedulerOverviewInfo);
      // Basic information
      showRouterSchedulerMetricsData(rsMetrics, fsMetricsScheduleTr);
    } else {
      showRouterSchedulerMetricsData(localClusterName, fsMetricsScheduleTr);
    }
  }

  /**
   * We display Scheduler information for multiple subClusters.
   *
   * @param metrics RouterClusterMetrics.
   * @param fsMetricsScheduleTr MetricsScheduleTr.
   * @param subClusters subCluster list.
   */
  private void initSubClusterOverViewTable(RouterClusterMetrics metrics,
      Hamlet.TBODY<Hamlet.TABLE<Hamlet.DIV<Hamlet>>> fsMetricsScheduleTr,
      Collection<SubClusterInfo> subClusters) {

    // configuration
    Configuration config = this.router.getConfig();

    Client client = RouterWebServiceUtil.createJerseyClient(config);

    // Traverse all SubClusters to get cluster information.
    for (SubClusterInfo subcluster : subClusters) {
      // We need to make sure subCluster is not null
      if (subcluster != null && subcluster.getSubClusterId() != null) {
        // Call the RM interface to obtain schedule information
        String webAppAddress =  WebAppUtils.getHttpSchemePrefix(config) +
            subcluster.getRMWebServiceAddress();
        SchedulerOverviewInfo schedulerOverviewInfo =
            getSchedulerOverviewInfo(webAppAddress, config, client);

        // If schedulerOverviewInfo is not null,
        // We will display information from rsMetrics, otherwise we will not display information.
        if (schedulerOverviewInfo != null) {
          RouterSchedulerMetrics rsMetrics =
              new RouterSchedulerMetrics(subcluster, metrics, schedulerOverviewInfo);
          // Basic information
          showRouterSchedulerMetricsData(rsMetrics, fsMetricsScheduleTr);
        }
      }
    }

    client.destroy();
  }

  /**
   * Get SchedulerOverview information based on webAppAddress.
   *
   * @param webAppAddress webAppAddress.
   * @param config configuration.
   * @param client jersey Client.
   * @return SchedulerOverviewInfo.
   */
  private SchedulerOverviewInfo getSchedulerOverviewInfo(String webAppAddress,
      Configuration config, Client client) {
    try {
      SchedulerOverviewInfo schedulerOverviewInfo = RouterWebServiceUtil
          .genericForward(webAppAddress, null, SchedulerOverviewInfo.class, HTTPMethods.GET,
          RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.SCHEDULER_OVERVIEW, null, null,
           config, client);
      return schedulerOverviewInfo;
    } catch (Exception e) {
      LOG.error("get SchedulerOverviewInfo from webAppAddress = {} error.",
          webAppAddress, e);
      return null;
    }
  }

  /**
   * Show RouterSchedulerMetricsData.
   *
   * @param rsMetrics routerSchedulerMetrics.
   * @param fsMetricsScheduleTr MetricsScheduleTr.
   */
  private void showRouterSchedulerMetricsData(RouterSchedulerMetrics rsMetrics,
      Hamlet.TBODY<Hamlet.TABLE<Hamlet.DIV<Hamlet>>> fsMetricsScheduleTr) {
    // Basic information
    fsMetricsScheduleTr.tr().
        td(rsMetrics.getSubCluster()).
        td(rsMetrics.getSchedulerType()).
        td(rsMetrics.getSchedulingResourceType()).
        td(rsMetrics.getMinimumAllocation()).
        td(rsMetrics.getMaximumAllocation()).
        td(rsMetrics.getApplicationPriority()).
        td(rsMetrics.getSchedulerBusy()).
        td(rsMetrics.getRmDispatcherEventQueueSize()).
        td(rsMetrics.getSchedulerDispatcherEventQueueSize()).
        __();
  }

  /**
   * Show RouterSchedulerMetricsData.
   *
   * @param subClusterId subClusterId.
   * @param fsMetricsScheduleTr MetricsScheduleTr.
   */
  private void showRouterSchedulerMetricsData(String subClusterId,
      Hamlet.TBODY<Hamlet.TABLE<Hamlet.DIV<Hamlet>>> fsMetricsScheduleTr) {
    String subCluster = StringUtils.isNotBlank(subClusterId) ? subClusterId : UNAVAILABLE;
    fsMetricsScheduleTr.tr().
        td(subCluster).
        td(UNAVAILABLE).
        td(UNAVAILABLE).
        td(UNAVAILABLE).
        td(UNAVAILABLE).
        td(UNAVAILABLE).
        td(UNAVAILABLE).
        td(UNAVAILABLE).
        td(UNAVAILABLE)
        .__();
  }
}
