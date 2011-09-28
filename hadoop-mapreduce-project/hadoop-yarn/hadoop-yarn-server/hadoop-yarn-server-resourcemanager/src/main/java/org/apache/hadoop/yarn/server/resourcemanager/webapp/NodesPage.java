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

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

import org.apache.hadoop.yarn.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

class NodesPage extends RmView {

  static class NodesBlock extends HtmlBlock {
    final RMContext rmContext;

    @Inject
    NodesBlock(RMContext context, ViewContext ctx) {
      super(ctx);
      this.rmContext = context;
    }

    @Override
    protected void render(Block html) {
      TBODY<TABLE<Hamlet>> tbody = html.table("#nodes").
          thead().
          tr().
          th(".rack", "Rack").
          th(".nodeaddress", "Node Address").
          th(".nodehttpaddress", "Node HTTP Address").
          th(".healthStatus", "Health-status").
          th(".lastHealthUpdate", "Last health-update").
          th(".healthReport", "Health-report").
          th(".containers", "Containers").
//          th(".mem", "Mem Used (MB)").
//          th(".mem", "Mem Avail (MB)").
          _()._().
          tbody();
      for (RMNode ni : this.rmContext.getRMNodes().values()) {
        NodeHealthStatus health = ni.getNodeHealthStatus();
        tbody.tr().
            td(ni.getRackName()).
            td(String.valueOf(ni.getNodeID().toString())).
            td().a("http://" + ni.getHttpAddress(), ni.getHttpAddress())._().
            td(health.getIsNodeHealthy() ? "Healthy" : "Unhealthy").
            td(Times.format(health.getLastHealthReportTime())).
            td(String.valueOf(health.getHealthReport())).
            // TODO: acm: refactor2 FIXME
            //td(String.valueOf(ni.getNumContainers())).
            // TODO: FIXME Vinodkv
//            td(String.valueOf(ni.getUsedResource().getMemory())).
//            td(String.valueOf(ni.getAvailableResource().getMemory())).
            td("n/a")._();
      }
      tbody._()._();
    }
  }

  @Override protected void preHead(Page.HTML<_> html) {
    commonPreHead(html);
    setTitle("Nodes of the cluster");
    set(DATATABLES_ID, "nodes");
    set(initID(DATATABLES, "nodes"), nodesTableInit());
    setTableStyles(html, "nodes", ".healthStatus {width:10em}",
                   ".healthReport {width:10em}");
  }

  @Override protected Class<? extends SubView> content() {
    return NodesBlock.class;
  }

  private String nodesTableInit() {
    return tableInit().
        // rack, nodeid, host, healthStatus, health update ts, health report,
        // containers, memused, memavail
        append(", aoColumns:[null, null, null, null, null, null, ").
        append("{sType:'title-numeric', bSearchable:false}]}").
        toString();
  }
}
