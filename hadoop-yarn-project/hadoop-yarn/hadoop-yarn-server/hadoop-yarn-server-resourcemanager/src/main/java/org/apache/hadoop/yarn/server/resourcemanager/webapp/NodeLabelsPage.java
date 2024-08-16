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

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;

import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.nodelabels.RMNodeLabel;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TR;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.commons.text.StringEscapeUtils;

import com.google.inject.Inject;

public class NodeLabelsPage extends RmView {
  static class NodeLabelsBlock extends HtmlBlock {
    final ResourceManager rm;

    @Inject
    NodeLabelsBlock(ResourceManager rm, ViewContext ctx) {
      super(ctx);
      this.rm = rm;
    }

    @Override
    protected void render(Block html) {
      TBODY<TABLE<Hamlet>> tbody = html.table("#nodelabels").
          thead().
          tr().
          th(".name", "Label Name").
          th(".type", "Label Type").
          th(".numOfActiveNMs", "Num Of Active NMs").
          th(".totalResource", "Total Resource").
          __().__().
          tbody();

      // List to store JSON data for DataTables
      StringBuilder nodeLabelsTableData = new StringBuilder("[\n");

      // Fetch node labels data
      RMNodeLabelsManager nlm = rm.getRMContext().getNodeLabelManager();
      for (RMNodeLabel info : nlm.pullRMNodeLabelsInfo()) {
        String labelName = info.getLabelName().isEmpty() ? NodeLabel.DEFAULT_NODE_LABEL_PARTITION : info.getLabelName();
        String labelType = info.getIsExclusive() ? "Exclusive Partition" : "Non Exclusive Partition";
        int nActiveNMs = info.getNumActiveNMs();
        String totalResource = info.getResource().toFormattedString();

        // Append data to JSON string
        nodeLabelsTableData
                .append("[\"")
                .append(StringEscapeUtils.escapeEcmaScript(StringEscapeUtils.escapeHtml4(labelName)))
                .append("\",\"")
                .append(StringEscapeUtils.escapeEcmaScript(StringEscapeUtils.escapeHtml4(labelType)))
                .append("\",\"")
                .append(nActiveNMs)
                .append("\",\"")
                .append(StringEscapeUtils.escapeEcmaScript(StringEscapeUtils.escapeHtml4(totalResource)))
                .append("\"],\n");

        // Generate HTML table rows
        TR<TBODY<TABLE<Hamlet>>> row = tbody.tr().td(labelName).td(labelType);

        if (nActiveNMs > 0) {
          row = row.td().a(url("nodes", "?" + YarnWebParams.NODE_LABEL + "=" + labelName), String.valueOf(nActiveNMs)).__();
        } else {
          row = row.td(String.valueOf(nActiveNMs));
        }

        row.td(totalResource).__();
      }

      if (nodeLabelsTableData.charAt(nodeLabelsTableData.length() - 2) == ',') {
        nodeLabelsTableData.delete(nodeLabelsTableData.length() - 2, nodeLabelsTableData.length() - 1);
      }
      nodeLabelsTableData.append("]");

      // Include DataTables initialization script with the JSON data
      html.script().$type("text/javascript")
              .__("nodeLabelsTableData=" + nodeLabelsTableData + "\nopts.data = {data: nodeLabelsTableData}" +
                      "\nnodeLabelsDataTable = DataTableHelper('#nodelabels', opts, false);").__();

      tbody.__().__();
    }
  }

  @Override protected void preHead(Page.HTML<__> html) {
    commonPreHead(html);
    String title = "Node labels of the cluster";
    setTitle(title);
    set(DATATABLES_ID, "nodelabels");
    setTableStyles(html, "nodelabels", ".healthStatus {width:10em}",
                   ".healthReport {width:10em}");
  }

  @Override protected Class<? extends SubView> content() {
    return NodeLabelsBlock.class;
  }
}
