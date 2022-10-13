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
import org.apache.hadoop.conf.Configuration;
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
        td(String.valueOf(metrics.getActiveNodes())).
        td(String.valueOf(metrics.getDecommissioningNodes())).
        td(String.valueOf(metrics.getDecommissionedNodes())).
        td(String.valueOf(metrics.getLostNodes())).
        td(String.valueOf(metrics.getUnhealthyNodes())).
        td(String.valueOf(metrics.getRebootedNodes())).
        td(String.valueOf(metrics.getShutdownNodes())).
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
    if (!isEnabled || subclusters == null || subclusters.isEmpty()) {
      fsMetricsScheduleTr.tr().
          td(UNAVAILABLE).
          td(UNAVAILABLE).
          td(UNAVAILABLE).
          td(UNAVAILABLE).
          td(UNAVAILABLE).
          td(UNAVAILABLE).
          td(UNAVAILABLE).
          td(UNAVAILABLE).
          td(UNAVAILABLE)
          .__();
    } else {
      initSubClusterOverViewTable(metrics, fsMetricsScheduleTr, subclusters);
    }

    fsMetricsScheduleTr.__().__();
  }

  private void initSubClusterOverViewTable(RouterClusterMetrics metrics,
      Hamlet.TBODY<Hamlet.TABLE<Hamlet.DIV<Hamlet>>> fsMetricsScheduleTr,
      Collection<SubClusterInfo> subclusters) {

    // configuration
    Configuration config = this.router.getConfig();

    Client client = RouterWebServiceUtil.createJerseyClient(config);

    // Traverse all SubClusters to get cluster information.
    for (SubClusterInfo subcluster : subclusters) {

      // Call the RM interface to obtain schedule information
      String webAppAddress =  WebAppUtils.getHttpSchemePrefix(config) +
          subcluster.getRMWebServiceAddress();

      SchedulerOverviewInfo typeInfo = RouterWebServiceUtil
          .genericForward(webAppAddress, null, SchedulerOverviewInfo.class, HTTPMethods.GET,
          RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.SCHEDULER_OVERVIEW, null, null,
          config, client);
      RouterSchedulerMetrics rsMetrics = new RouterSchedulerMetrics(subcluster, metrics, typeInfo);

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
  }
}
