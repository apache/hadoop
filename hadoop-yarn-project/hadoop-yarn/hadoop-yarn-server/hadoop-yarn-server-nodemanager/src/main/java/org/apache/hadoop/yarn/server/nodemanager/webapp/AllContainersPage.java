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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

import java.util.Map.Entry;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.BODY;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.commons.text.StringEscapeUtils;

import com.google.inject.Inject;

public class AllContainersPage extends NMView {

  @Override protected void preHead(Page.HTML<__> html) {
    commonPreHead(html);
    setTitle("All containers running on this node");
    set(DATATABLES_ID, "containers");
    set(initID(DATATABLES, "containers"), containersTableInit());
    setTableStyles(html, "containers");
  }

  private String containersTableInit() {
    return tableInit().
        // containerid, executiontype, containerid, log-url
        append(", aoColumns:[").append(getContainersIdColumnDefs())
        .append(", null, null, {bSearchable:false}]} ").toString();
  }

  private String getContainersIdColumnDefs() {
    StringBuilder sb = new StringBuilder();
    return sb.append("{'sType':'natural', 'aTargets': [0]")
        .append(", 'mRender': parseHadoopID }").toString();
  }
  @Override
  protected Class<? extends SubView> content() {
    return AllContainersBlock.class;
  }

  public static class AllContainersBlock extends HtmlBlock implements
      YarnWebParams {

    private final Context nmContext;

    @Inject
    public AllContainersBlock(Context nmContext) {
      this.nmContext = nmContext;
    }

    @Override
    protected void render(Block html) {
      TBODY<TABLE<Hamlet>> tbody = html.table("#containers").
              thead().
              tr().
              th(".containerId", "ContainerId").
              th(".executionType", "ExecutionType").
              th(".containerState", "ContainerState").
              th(".logs", "Logs").
              __().__()
            .tbody();

      StringBuilder containersTableData = new StringBuilder("[\n");
      boolean first = true;

      for (Entry<ContainerId, Container> entry : this.nmContext
          .getContainers().entrySet()) {
        ContainerInfo info = new ContainerInfo(this.nmContext, entry.getValue());

        String containerId = info.getId();
        String executionType = info.getExecutionType();
        String containerState = info.getState();
        String logLink = info.getShortLogLink();

        if (!first) {
          containersTableData.append(",\n");
        }
        first = false;

        containersTableData.append("[\"<a href='")
                .append(url("container", containerId))
                .append("'>")
                .append(StringEscapeUtils.escapeEcmaScript(
                        StringEscapeUtils.escapeHtml4(containerId)))
                .append("</a>\",\"")
                .append(StringEscapeUtils.escapeEcmaScript(
                        StringEscapeUtils.escapeHtml4(executionType)))
                .append("\",\"")
                .append(StringEscapeUtils.escapeEcmaScript(
                        StringEscapeUtils.escapeHtml4(containerState)))
                .append("\",\"<a href='")
                .append(url(logLink))
                .append("'>logs</a>\"]");
      }

      if (containersTableData.charAt(containersTableData.length() - 2) == ',') {
        containersTableData.delete(containersTableData.length() - 2,
                containersTableData.length() - 1);
      }
      containersTableData.append("]");
      html.script().$type("text/javascript")
              .__("containersTableData=" + containersTableData +
                      "\nopts.data = {data: containersTableData}" +
                      "\ncontainersDataTable = DataTableHelper('#containers', opts, false);")
              .__();

      tbody.__().__();
    }

  }
}
