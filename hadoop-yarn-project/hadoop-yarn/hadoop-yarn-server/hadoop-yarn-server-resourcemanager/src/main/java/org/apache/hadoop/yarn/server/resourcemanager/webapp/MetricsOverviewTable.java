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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.UserMetricsInfo;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

/**
 * Provides an table with an overview of many cluster wide metrics and if
 * per user metrics are enabled it will show an overview of what the
 * current user is using on the cluster.
 */
public class MetricsOverviewTable extends HtmlBlock {
  private static final long BYTES_IN_MB = 1024 * 1024;

  private final RMContext rmContext;
  private final ResourceManager rm;

  @Inject
  MetricsOverviewTable(RMContext context, ResourceManager rm, ViewContext ctx) {
    super(ctx);
    this.rmContext = context;
    this.rm = rm;
  }


  @Override
  protected void render(Block html) {
    //Yes this is a hack, but there is no other way to insert
    //CSS in the correct spot
    html.style(".metrics {margin-bottom:5px}"); 
    
    ClusterMetricsInfo clusterMetrics = 
        new ClusterMetricsInfo(this.rm, this.rmContext);
    
    DIV<Hamlet> div = html.div().$class("metrics");
    
    div.h3("Cluster Metrics").
    table("#metricsoverview").
    thead().$class("ui-widget-header").
      tr().
        th().$class("ui-state-default")._("Apps Submitted")._().
        th().$class("ui-state-default")._("Apps Pending")._().
        th().$class("ui-state-default")._("Apps Running")._().
        th().$class("ui-state-default")._("Apps Completed")._().
        th().$class("ui-state-default")._("Containers Running")._().
        th().$class("ui-state-default")._("Memory Used")._().
        th().$class("ui-state-default")._("Memory Total")._().
        th().$class("ui-state-default")._("Memory Reserved")._().
        th().$class("ui-state-default")._("VCores Used")._().
        th().$class("ui-state-default")._("VCores Total")._().
        th().$class("ui-state-default")._("VCores Reserved")._().
        th().$class("ui-state-default")._("Active Nodes")._().
        th().$class("ui-state-default")._("Decommissioned Nodes")._().
        th().$class("ui-state-default")._("Lost Nodes")._().
        th().$class("ui-state-default")._("Unhealthy Nodes")._().
        th().$class("ui-state-default")._("Rebooted Nodes")._().
      _().
    _().
    tbody().$class("ui-widget-content").
      tr().
        td(String.valueOf(clusterMetrics.getAppsSubmitted())).
        td(String.valueOf(clusterMetrics.getAppsPending())).
        td(String.valueOf(clusterMetrics.getAppsRunning())).
        td(
            String.valueOf(
                clusterMetrics.getAppsCompleted() + 
                clusterMetrics.getAppsFailed() + clusterMetrics.getAppsKilled()
                )
            ).
        td(String.valueOf(clusterMetrics.getContainersAllocated())).
        td(StringUtils.byteDesc(clusterMetrics.getAllocatedMB() * BYTES_IN_MB)).
        td(StringUtils.byteDesc(clusterMetrics.getTotalMB() * BYTES_IN_MB)).
        td(StringUtils.byteDesc(clusterMetrics.getReservedMB() * BYTES_IN_MB)).
        td(String.valueOf(clusterMetrics.getAllocatedVirtualCores())).
        td(String.valueOf(clusterMetrics.getTotalVirtualCores())).
        td(String.valueOf(clusterMetrics.getReservedVirtualCores())).
        td().a(url("nodes"),String.valueOf(clusterMetrics.getActiveNodes()))._().
        td().a(url("nodes/decommissioned"),String.valueOf(clusterMetrics.getDecommissionedNodes()))._().
        td().a(url("nodes/lost"),String.valueOf(clusterMetrics.getLostNodes()))._().
        td().a(url("nodes/unhealthy"),String.valueOf(clusterMetrics.getUnhealthyNodes()))._().
        td().a(url("nodes/rebooted"),String.valueOf(clusterMetrics.getRebootedNodes()))._().
      _().
    _()._();
    
    String user = request().getRemoteUser();
    if (user != null) {
      UserMetricsInfo userMetrics = new UserMetricsInfo(this.rm, this.rmContext, user);
      if (userMetrics.metricsAvailable()) {
        div.h3("User Metrics for " + user).
        table("#usermetricsoverview").
        thead().$class("ui-widget-header").
          tr().
            th().$class("ui-state-default")._("Apps Submitted")._().
            th().$class("ui-state-default")._("Apps Pending")._().
            th().$class("ui-state-default")._("Apps Running")._().
            th().$class("ui-state-default")._("Apps Completed")._().
            th().$class("ui-state-default")._("Containers Running")._().
            th().$class("ui-state-default")._("Containers Pending")._().
            th().$class("ui-state-default")._("Containers Reserved")._().
            th().$class("ui-state-default")._("Memory Used")._().
            th().$class("ui-state-default")._("Memory Pending")._().
            th().$class("ui-state-default")._("Memory Reserved")._().
            th().$class("ui-state-default")._("VCores Used")._().
            th().$class("ui-state-default")._("VCores Pending")._().
            th().$class("ui-state-default")._("VCores Reserved")._().
          _().
        _().
        tbody().$class("ui-widget-content").
          tr().
            td(String.valueOf(userMetrics.getAppsSubmitted())).
            td(String.valueOf(userMetrics.getAppsPending())).
            td(String.valueOf(userMetrics.getAppsRunning())).
            td(
                String.valueOf(
                    (userMetrics.getAppsCompleted() + 
                     userMetrics.getAppsFailed() + userMetrics.getAppsKilled())
                    )
              ).
            td(String.valueOf(userMetrics.getRunningContainers())).
            td(String.valueOf(userMetrics.getPendingContainers())).
            td(String.valueOf(userMetrics.getReservedContainers())).
            td(StringUtils.byteDesc(userMetrics.getAllocatedMB() * BYTES_IN_MB)).
            td(StringUtils.byteDesc(userMetrics.getPendingMB() * BYTES_IN_MB)).
            td(StringUtils.byteDesc(userMetrics.getReservedMB() * BYTES_IN_MB)).
            td(String.valueOf(userMetrics.getAllocatedVirtualCores())).
            td(String.valueOf(userMetrics.getPendingVirtualCores())).
            td(String.valueOf(userMetrics.getReservedVirtualCores())).
          _().
        _()._();
        
      }
    }

    div._();
  }
}
