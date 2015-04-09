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

import static org.apache.hadoop.yarn.webapp.YarnWebParams.NODE_STATE;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.NODE_LABEL;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

import java.util.Collection;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TR;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

class NodesPage extends RmView {

  static class NodesBlock extends HtmlBlock {
    final ResourceManager rm;
    private static final long BYTES_IN_MB = 1024 * 1024;

    @Inject
    NodesBlock(ResourceManager rm, ViewContext ctx) {
      super(ctx);
      this.rm = rm;
    }

    @Override
    protected void render(Block html) {
      html._(MetricsOverviewTable.class);

      ResourceScheduler sched = rm.getResourceScheduler();
      String type = $(NODE_STATE);
      String labelFilter = $(NODE_LABEL, CommonNodeLabelsManager.ANY).trim();
      TBODY<TABLE<Hamlet>> tbody =
          html.table("#nodes").thead().tr()
              .th(".nodelabels", "Node Labels")
              .th(".rack", "Rack")
              .th(".state", "Node State")
              .th(".nodeaddress", "Node Address")
              .th(".nodehttpaddress", "Node HTTP Address")
              .th(".lastHealthUpdate", "Last health-update")
              .th(".healthReport", "Health-report")
              .th(".containers", "Containers")
              .th(".mem", "Mem Used")
              .th(".mem", "Mem Avail")
              .th(".vcores", "VCores Used")
              .th(".vcores", "VCores Avail")
              .th(".nodeManagerVersion", "Version")._()._().tbody();
      NodeState stateFilter = null;
      if (type != null && !type.isEmpty()) {
        stateFilter = NodeState.valueOf(StringUtils.toUpperCase(type));
      }
      Collection<RMNode> rmNodes = this.rm.getRMContext().getRMNodes().values();
      boolean isInactive = false;
      if (stateFilter != null) {
        switch (stateFilter) {
        case DECOMMISSIONED:
        case LOST:
        case REBOOTED:
          rmNodes = this.rm.getRMContext().getInactiveRMNodes().values();
          isInactive = true;
          break;
        default:
          LOG.debug("Unexpected state filter for inactive RM node");
        }
      }
      for (RMNode ni : rmNodes) {
        if (stateFilter != null) {
          NodeState state = ni.getState();
          if (!stateFilter.equals(state)) {
            continue;
          }
        } else {
          // No filter. User is asking for all nodes. Make sure you skip the
          // unhealthy nodes.
          if (ni.getState() == NodeState.UNHEALTHY) {
            continue;
          }
        }
        // Besides state, we need to filter label as well.
        if (!labelFilter.equals(RMNodeLabelsManager.ANY)) {
          if (labelFilter.isEmpty()) {
            // Empty label filter means only shows nodes without label
            if (!ni.getNodeLabels().isEmpty()) {
              continue;
            }
          } else if (!ni.getNodeLabels().contains(labelFilter)) {
            // Only nodes have given label can show on web page.
            continue;
          }
        }
        NodeInfo info = new NodeInfo(ni, sched);
        int usedMemory = (int) info.getUsedMemory();
        int availableMemory = (int) info.getAvailableMemory();
        TR<TBODY<TABLE<Hamlet>>> row =
            tbody.tr().td(StringUtils.join(",", info.getNodeLabels()))
                .td(info.getRack()).td(info.getState()).td(info.getNodeId());
        if (isInactive) {
          row.td()._("N/A")._();
        } else {
          String httpAddress = info.getNodeHTTPAddress();
          row.td().a("//" + httpAddress, httpAddress)._();
        }
        row.td().br().$title(String.valueOf(info.getLastHealthUpdate()))._()
            ._(Times.format(info.getLastHealthUpdate()))._()
            .td(info.getHealthReport())
            .td(String.valueOf(info.getNumContainers())).td().br()
            .$title(String.valueOf(usedMemory))._()
            ._(StringUtils.byteDesc(usedMemory * BYTES_IN_MB))._().td().br()
            .$title(String.valueOf(availableMemory))._()
            ._(StringUtils.byteDesc(availableMemory * BYTES_IN_MB))._()
            .td(String.valueOf(info.getUsedVirtualCores()))
            .td(String.valueOf(info.getAvailableVirtualCores()))
            .td(ni.getNodeManagerVersion())._();
      }
      tbody._()._();
    }
  }

  @Override
  protected void preHead(Page.HTML<_> html) {
    commonPreHead(html);
    String type = $(NODE_STATE);
    String title = "Nodes of the cluster";
    if (type != null && !type.isEmpty()) {
      title = title + " (" + type + ")";
    }
    setTitle(title);
    set(DATATABLES_ID, "nodes");
    set(initID(DATATABLES, "nodes"), nodesTableInit());
    setTableStyles(html, "nodes", ".healthStatus {width:10em}",
        ".healthReport {width:10em}");
  }

  @Override
  protected Class<? extends SubView> content() {
    return NodesBlock.class;
  }

  private String nodesTableInit() {
    StringBuilder b = tableInit().append(", aoColumnDefs: [");
    b.append("{'bSearchable': false, 'aTargets': [ 7 ]}");
    b.append(", {'sType': 'title-numeric', 'bSearchable': false, "
        + "'aTargets': [ 8, 9 ] }");
    b.append(", {'sType': 'title-numeric', 'aTargets': [ 5 ]}");
    b.append("]}");
    return b.toString();
  }
}
