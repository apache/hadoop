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

import org.apache.hadoop.yarn.nodelabels.NodeLabel;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TR;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

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
          th(".numOfActiveNMs", "Num Of Active NMs").
          th(".totalResource", "Total Resource").
          _()._().
          tbody();
  
      RMNodeLabelsManager nlm = rm.getRMContext().getNodeLabelManager();
      for (NodeLabel info : nlm.pullRMNodeLabelsInfo()) {
        TR<TBODY<TABLE<Hamlet>>> row =
            tbody.tr().td(
                info.getLabelName().isEmpty() ? "<NO_LABEL>" : info
                    .getLabelName());
        int nActiveNMs = info.getNumActiveNMs();
        if (nActiveNMs > 0) {
          row = row.td()
          .a(url("nodes",
              "?" + YarnWebParams.NODE_LABEL + "=" + info.getLabelName()),
              String.valueOf(nActiveNMs))
           ._();
        } else {
          row = row.td(String.valueOf(nActiveNMs));
        }
        row.td(info.getResource().toString())._();
      }
      tbody._()._();
    }
  }

  @Override protected void preHead(Page.HTML<_> html) {
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
