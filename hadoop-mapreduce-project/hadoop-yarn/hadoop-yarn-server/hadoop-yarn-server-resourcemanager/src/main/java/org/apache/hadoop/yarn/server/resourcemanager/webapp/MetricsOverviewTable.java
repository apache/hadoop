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

import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
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
  private static final long BYTES_IN_GB = 1024 * 1024 * 1024;
  
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
    
    ResourceScheduler rs = rm.getResourceScheduler();
    QueueMetrics metrics = rs.getRootQueueMetrics();
    ClusterMetrics clusterMetrics = ClusterMetrics.getMetrics();
    
    int appsSubmitted = metrics.getAppsSubmitted();
    int reservedGB = metrics.getReservedGB();
    int availableGB = metrics.getAvailableGB();
    int allocatedGB = metrics.getAllocatedGB();
    int containersAllocated = metrics.getAllocatedContainers();
    int totalGB = availableGB + reservedGB + allocatedGB;

    int totalNodes = clusterMetrics.getNumNMs();
    int lostNodes = clusterMetrics.getNumLostNMs();
    int unhealthyNodes = clusterMetrics.getUnhealthyNMs();
    int decommissionedNodes = clusterMetrics.getNumDecommisionedNMs();
    int rebootedNodes = clusterMetrics.getNumRebootedNMs();

    
    DIV<Hamlet> div = html.div().$class("metrics");
    
    div.table("#metricsoverview").
    thead().$class("ui-widget-header").
      tr().
        th().$class("ui-state-default")._("Apps Submitted")._().
        th().$class("ui-state-default")._("Containers Running")._().
        th().$class("ui-state-default")._("Memory Used")._().
        th().$class("ui-state-default")._("Memopry Total")._().
        th().$class("ui-state-default")._("Memory Reserved")._().
        th().$class("ui-state-default")._("Total Nodes")._().
        th().$class("ui-state-default")._("Decommissioned Nodes")._().
        th().$class("ui-state-default")._("Lost Nodes")._().
        th().$class("ui-state-default")._("Unhealthy Nodes")._().
        th().$class("ui-state-default")._("Rebooted Nodes")._().
      _().
    _().
    tbody().$class("ui-widget-content").
      tr().
        td(String.valueOf(appsSubmitted)).
        td(String.valueOf(containersAllocated)).
        td(StringUtils.byteDesc(allocatedGB * BYTES_IN_GB)).
        td(StringUtils.byteDesc(totalGB * BYTES_IN_GB)).
        td(StringUtils.byteDesc(reservedGB * BYTES_IN_GB)).
        td().a(url("nodes"),String.valueOf(totalNodes))._(). 
        td().a(url("nodes/decommissioned"),String.valueOf(decommissionedNodes))._(). 
        td().a(url("nodes/lost"),String.valueOf(lostNodes))._().
        td().a(url("nodes/unhealthy"),String.valueOf(unhealthyNodes))._().
        td().a(url("nodes/rebooted"),String.valueOf(rebootedNodes))._().
      _().
    _()._();
    
    String user = request().getRemoteUser();
    if (user != null) {
      QueueMetrics userMetrics = metrics.getUserMetrics(user);
      if(userMetrics != null) {
        int myAppsSubmitted = userMetrics.getAppsSubmitted();
        int myRunningContainers = userMetrics.getAllocatedContainers();
        int myPendingContainers = userMetrics.getPendingContainers();
        int myReservedContainers = userMetrics.getReservedContainers();
        int myReservedGB = userMetrics.getReservedGB();
        int myPendingGB = userMetrics.getPendingGB();
        int myAllocatedGB = userMetrics.getAllocatedGB();
        div.table("#usermetricsoverview").
        thead().$class("ui-widget-header").
          tr().
            th().$class("ui-state-default")._("Apps Submitted ("+user+")")._().
            th().$class("ui-state-default")._("Containers Running ("+user+")")._().
            th().$class("ui-state-default")._("Containers Pending ("+user+")")._().
            th().$class("ui-state-default")._("Containers Reserved ("+user+")")._().
            th().$class("ui-state-default")._("Memory Used ("+user+")")._().
            th().$class("ui-state-default")._("Memory Pending ("+user+")")._().
            th().$class("ui-state-default")._("Memory Reserved ("+user+")")._().
          _().
        _().
        tbody().$class("ui-widget-content").
          tr().
            td(String.valueOf(myAppsSubmitted)).
            td(String.valueOf(myRunningContainers)).
            td(String.valueOf(myPendingContainers)).
            td(String.valueOf(myReservedContainers)).
            td(StringUtils.byteDesc(myAllocatedGB * BYTES_IN_GB)).
            td(StringUtils.byteDesc(myPendingGB * BYTES_IN_GB)).
            td(StringUtils.byteDesc(myReservedGB * BYTES_IN_GB)).
          _().
        _()._();
      }
    }

    div._();
  }
}
