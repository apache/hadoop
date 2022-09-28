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
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.server.router.webapp.dao.RouterClusterMetrics;
import org.apache.hadoop.yarn.server.router.webapp.dao.RouterSchedulerMetrics;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    initFederationClusterAppsMetrics(div, routerClusterMetrics);
    initFederationClusterNodesMetrics(div, routerClusterMetrics);
    try {
      initFederationClusterSchedulersMetrics(div, routerClusterMetrics);
    } catch (Exception e) {
      e.printStackTrace();
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
    div.h3("Federation Cluster Metrics").
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
    div.h3("Federation Cluster Nodes Metrics").
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
   * @throws YarnException yarn error.
   * @throws IOException io error.
   * @throws InterruptedException interrupt error.
   */
  private void initFederationClusterSchedulersMetrics(Hamlet.DIV<Hamlet> div,
      RouterClusterMetrics metrics) throws YarnException, IOException, InterruptedException {
    FederationStateStoreFacade facade = FederationStateStoreFacade.getInstance();
    Map<SubClusterId, SubClusterInfo> subClustersInfo = facade.getSubClusters(true);

    // Sort the SubClusters.
    List<SubClusterInfo> subclusters = new ArrayList<>();
    subclusters.addAll(subClustersInfo.values());
    Comparator<? super SubClusterInfo> cmp = Comparator.comparing(o -> o.getSubClusterId());
    Collections.sort(subclusters, cmp);
    Client client = RouterWebServiceUtil.createJerseyClient(this.router.getConfig());

    Hamlet.TBODY<Hamlet.TABLE<Hamlet.DIV<Hamlet>>> fsMetricsScheduleTr =
        div.h3("Federation Scheduler Metrics").
        table("#schedulermetricsoverview").
        thead().$class("ui-widget-header").
        tr().
        th().$class("ui-state-default").__("SubCluster").__().
        th().$class("ui-state-default").__("Scheduler Type").__().
        th().$class("ui-state-default").__("Scheduling Resource Type").__().
        th().$class("ui-state-default").__("Minimum Allocation").__().
        th().$class("ui-state-default").__("Maximum Allocation").__().
        th().$class("ui-state-default")
        .__("Maximum Cluster Application Priority").__().
        __().
        __().
        tbody().$class("ui-widget-content");

    // Traverse all SubClusters to get cluster information.
    for (SubClusterInfo subcluster : subclusters) {
      // Call the RM interface to obtain schedule information
      String webAppAddress =  WebAppUtils.getHttpSchemePrefix(this.router.getConfig()) +
          subcluster.getRMWebServiceAddress();
      SchedulerTypeInfo typeInfo = RouterWebServiceUtil
          .genericForward(webAppAddress, null, SchedulerTypeInfo.class, HTTPMethods.GET,
          RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.SCHEDULER, null, null,
          this.router.getConfig(), client);
      RouterSchedulerMetrics rsMetrics = new RouterSchedulerMetrics(subcluster, metrics, typeInfo);

      // Basic information
      fsMetricsScheduleTr.tr().
          td(rsMetrics.getSubCluster()).
          td(rsMetrics.getSchedulerType()).
          td(rsMetrics.getSchedulingResourceType()).
          td(rsMetrics.getMinimumAllocation()).
          td(rsMetrics.getMaximumAllocation()).
          td(rsMetrics.getApplicationPriority())
          .__();
    }

    fsMetricsScheduleTr.__().__();
  }
}
