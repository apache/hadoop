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
import org.apache.hadoop.yarn.api.records.ResourceTypeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.UserMetricsInfo;

import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

import java.util.Arrays;

/**
 * Provides an table with an overview of many cluster wide metrics and if
 * per user metrics are enabled it will show an overview of what the
 * current user is using on the cluster.
 */
public class MetricsOverviewTable extends HtmlBlock {
  private static final long BYTES_IN_MB = 1024 * 1024;

  private final ResourceManager rm;

  @Inject
  MetricsOverviewTable(ResourceManager rm, ViewContext ctx) {
    super(ctx);
    this.rm = rm;
  }


  @Override
  protected void render(Block html) {
    //Yes this is a hack, but there is no other way to insert
    //CSS in the correct spot
    html.style(".metrics {margin-bottom:5px}"); 
    
    ClusterMetricsInfo clusterMetrics = new ClusterMetricsInfo(this.rm);
    
    DIV<Hamlet> div = html.div().$class("metrics");
    
    div.h3("Cluster Metrics").
    table("#metricsoverview").
    thead().$class("ui-widget-header").
      tr().
        th().$class("ui-state-default").__("Apps Submitted").__().
        th().$class("ui-state-default").__("Apps Pending").__().
        th().$class("ui-state-default").__("Apps Running").__().
        th().$class("ui-state-default").__("Apps Completed").__().
        th().$class("ui-state-default").__("Containers Running").__().
        th().$class("ui-state-default").__("Memory Used").__().
        th().$class("ui-state-default").__("Memory Total").__().
        th().$class("ui-state-default").__("Memory Reserved").__().
        th().$class("ui-state-default").__("VCores Used").__().
        th().$class("ui-state-default").__("VCores Total").__().
        th().$class("ui-state-default").__("VCores Reserved").__().
        __().
        __().
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
        __().
        __().__();

    div.h3("Cluster Nodes Metrics").
    table("#nodemetricsoverview").
    thead().$class("ui-widget-header").
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
    tbody().$class("ui-widget-content").
      tr().
        td().a(url("nodes"), String.valueOf(clusterMetrics.getActiveNodes())).__().
        td().a(url("nodes/decommissioning"), String.valueOf(clusterMetrics.getDecommissioningNodes())).__().
        td().a(url("nodes/decommissioned"), String.valueOf(clusterMetrics.getDecommissionedNodes())).__().
        td().a(url("nodes/lost"), String.valueOf(clusterMetrics.getLostNodes())).__().
        td().a(url("nodes/unhealthy"), String.valueOf(clusterMetrics.getUnhealthyNodes())).__().
        td().a(url("nodes/rebooted"), String.valueOf(clusterMetrics.getRebootedNodes())).__().
        td().a(url("nodes/shutdown"), String.valueOf(clusterMetrics.getShutdownNodes())).__().
        __().
        __().__();

    String user = request().getRemoteUser();
    if (user != null) {
      UserMetricsInfo userMetrics = new UserMetricsInfo(this.rm, user);
      if (userMetrics.metricsAvailable()) {
        div.h3("User Metrics for " + user).
        table("#usermetricsoverview").
        thead().$class("ui-widget-header").
          tr().
            th().$class("ui-state-default").__("Apps Submitted").__().
            th().$class("ui-state-default").__("Apps Pending").__().
            th().$class("ui-state-default").__("Apps Running").__().
            th().$class("ui-state-default").__("Apps Completed").__().
            th().$class("ui-state-default").__("Containers Running").__().
            th().$class("ui-state-default").__("Containers Pending").__().
            th().$class("ui-state-default").__("Containers Reserved").__().
            th().$class("ui-state-default").__("Memory Used").__().
            th().$class("ui-state-default").__("Memory Pending").__().
            th().$class("ui-state-default").__("Memory Reserved").__().
            th().$class("ui-state-default").__("VCores Used").__().
            th().$class("ui-state-default").__("VCores Pending").__().
            th().$class("ui-state-default").__("VCores Reserved").__().
            __().
            __().
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
            __().
            __().__();
        
      }
    }

    SchedulerInfo schedulerInfo = new SchedulerInfo(this.rm);
    
    div.h3("Scheduler Metrics").
    table("#schedulermetricsoverview").
    thead().$class("ui-widget-header").
      tr().
        th().$class("ui-state-default").__("Scheduler Type").__().
        th().$class("ui-state-default").__("Scheduling Resource Type").__().
        th().$class("ui-state-default").__("Minimum Allocation").__().
        th().$class("ui-state-default").__("Maximum Allocation").__().
        th().$class("ui-state-default")
            .__("Maximum Cluster Application Priority").__().
        __().
        __().
    tbody().$class("ui-widget-content").
      tr().
        td(String.valueOf(schedulerInfo.getSchedulerType())).
        td(String.valueOf(Arrays.toString(ResourceUtils.getResourcesTypeInfo()
            .toArray(new ResourceTypeInfo[0])))).
        td(schedulerInfo.getMinAllocation().toString()).
        td(schedulerInfo.getMaxAllocation().toString()).
        td(String.valueOf(schedulerInfo.getMaxClusterLevelAppPriority())).
        __().
        __().__();

    div.__();
  }
}
