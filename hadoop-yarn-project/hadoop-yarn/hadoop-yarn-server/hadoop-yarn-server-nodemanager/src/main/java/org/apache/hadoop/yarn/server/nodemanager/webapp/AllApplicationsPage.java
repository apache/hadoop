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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TR;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.commons.text.StringEscapeUtils;

import com.google.inject.Inject;

public class AllApplicationsPage extends NMView {

  @Override protected void preHead(Page.HTML<__> html) {
    commonPreHead(html);
    setTitle("Applications running on this node");
    set(DATATABLES_ID, "applications");
    set(initID(DATATABLES, "applications"), appsTableInit());
    setTableStyles(html, "applications");
  }

  private String appsTableInit() {
    return tableInit().
        // Sort by id upon page load
        append(", aaSorting: [[0, 'asc']]").
        // applicationid, applicationstate
        append(", aoColumns:[").append(getApplicationsIdColumnDefs())
        .append(", null]} ").toString();
  }

  private String getApplicationsIdColumnDefs() {
    StringBuilder sb = new StringBuilder();
    return sb.append("{'sType':'natural', 'aTargets': [0]")
        .append(", 'mRender': parseHadoopID }").toString();
  }

  @Override
  protected Class<? extends SubView> content() {
    return AllApplicationsBlock.class;
  }

  public static class AllApplicationsBlock extends HtmlBlock implements
      YarnWebParams {

    private final Context nmContext;

    @Inject
    public AllApplicationsBlock(Context nmContext) {
      this.nmContext = nmContext;
    }

    @Override
    protected void render(Block html) {

      TBODY<TABLE<Hamlet>> tbody =
        html.table("#applications").
              thead().
                tr().
                th(".appId", "ApplicationId").
                th(".appState", "ApplicationState").
                __().__().
                tbody();

            StringBuilder applicationsTableData = new StringBuilder("[");
            boolean first = true;

      for (Entry<ApplicationId, Application> entry : this.nmContext
          .getApplications().entrySet()) {
        AppInfo info = new AppInfo(entry.getValue());

                String appId = info.getId();
                String appState = info.getState();

                if (!first) {
                    applicationsTableData.append(",");
                }
                first = false;

                applicationsTableData
                        .append("[\"<a href='")
                        .append(url("application", appId))
                        .append("'>")
                        .append(StringEscapeUtils.escapeEcmaScript(
                                StringEscapeUtils.escapeHtml4(appId)))
                        .append("</a>\",\"")
                        .append(StringEscapeUtils.escapeEcmaScript(
                          StringEscapeUtils.escapeHtml4(appState)))
                    .append("\"]");

            TR<TBODY<TABLE<Hamlet>>> row = tbody.tr();
            row = row.td().a(url("application", appId), appId).__();
            row.td(appState).__();
            }
            applicationsTableData.append("]");
            html.script().$type("text/javascript")
            .__("applicationsTableData=" + applicationsTableData +
        "\nopts.data = {data: applicationsTableData}" +
        "\napplicationsDataTable = DataTableHelper('#applications', opts, false);").__();

      tbody.__().__();
    }
  }
}
