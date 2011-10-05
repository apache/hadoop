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

import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebApp.NODE_STATE;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

class NodesPage extends RmView {

  static class NodesBlock extends HtmlBlock {
    private static final long BYTES_IN_MB = 1024 * 1024;
    final RMContext rmContext;
    final ResourceManager rm;

    @Inject
    NodesBlock(RMContext context, ResourceManager rm, ViewContext ctx) {
      super(ctx);
      this.rmContext = context;
      this.rm = rm;
    }

    @Override
    protected void render(Block html) {
      html._(MetricsOverviewTable.class);
      
      ResourceScheduler sched = rm.getResourceScheduler();
      String type = $(NODE_STATE);
      TBODY<TABLE<Hamlet>> tbody = html.table("#nodes").
          thead().
          tr().
          th(".rack", "Rack").
          th(".state", "Node State").
          th(".nodeaddress", "Node Address").
          th(".nodehttpaddress", "Node HTTP Address").
          th(".healthStatus", "Health-status").
          th(".lastHealthUpdate", "Last health-update").
          th(".healthReport", "Health-report").
          th(".containers", "Containers").
          th(".mem", "Mem Used").
          th(".mem", "Mem Avail").
          _()._().
          tbody();
      RMNodeState stateFilter = null;
      if(type != null && !type.isEmpty()) {
        stateFilter = RMNodeState.valueOf(type.toUpperCase());
      }
      for (RMNode ni : this.rmContext.getRMNodes().values()) {
        if(stateFilter != null) {
          RMNodeState state = ni.getState();
          if(!stateFilter.equals(state)) {
            continue;
          }
        }
        NodeId id = ni.getNodeID();
        SchedulerNodeReport report = sched.getNodeReport(id);
        int numContainers = 0;
        int usedMemory = 0;
        int availableMemory = 0;
        if(report != null) {
          numContainers = report.getNumContainers();
          usedMemory = report.getUsedResource().getMemory();
          availableMemory = report.getAvailableResource().getMemory();
        }

        NodeHealthStatus health = ni.getNodeHealthStatus();
        tbody.tr().
            td(ni.getRackName()).
            td(String.valueOf(ni.getState())).
            td(String.valueOf(ni.getNodeID().toString())).
            td().a("http://" + ni.getHttpAddress(), ni.getHttpAddress())._().
            td(health.getIsNodeHealthy() ? "Healthy" : "Unhealthy").
            td(Times.format(health.getLastHealthReportTime())).
            td(String.valueOf(health.getHealthReport())).
            td(String.valueOf(numContainers)).
            td().br().$title(String.valueOf(usedMemory))._().
              _(StringUtils.byteDesc(usedMemory * BYTES_IN_MB))._().
            td().br().$title(String.valueOf(usedMemory))._().
              _(StringUtils.byteDesc(availableMemory * BYTES_IN_MB))._().
            _();
      }
      tbody._()._();
    }
  }

  @Override protected void preHead(Page.HTML<_> html) {
    commonPreHead(html);
    String type = $(NODE_STATE);
    String title = "Nodes of the cluster";
    if(type != null && !type.isEmpty()) {
      title = title+" ("+type+")";
    }
    setTitle(title);
    set(DATATABLES_ID, "nodes");
    set(initID(DATATABLES, "nodes"), nodesTableInit());
    setTableStyles(html, "nodes", ".healthStatus {width:10em}",
                   ".healthReport {width:10em}");
  }

  @Override protected Class<? extends SubView> content() {
    return NodesBlock.class;
  }

  private String nodesTableInit() {
    StringBuilder b = tableInit().append(",aoColumnDefs:[");
    b.append("{'bSearchable':false, 'aTargets': [7]} ,");
    b.append("{'sType':'title-numeric', 'bSearchable':false, " +
    		"'aTargets': [ 8, 9] }]}");
    return b.toString();
  }
}
