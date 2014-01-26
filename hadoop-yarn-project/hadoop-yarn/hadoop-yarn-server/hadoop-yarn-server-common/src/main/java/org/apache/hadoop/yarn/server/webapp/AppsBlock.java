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

package org.apache.hadoop.yarn.server.webapp;

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_STATE;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR_VALUE;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.server.api.ApplicationContext;
import org.apache.hadoop.yarn.server.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

public class AppsBlock extends HtmlBlock {

  protected ApplicationContext appContext;

  @Inject
  AppsBlock(ApplicationContext appContext, ViewContext ctx) {
    super(ctx);
    this.appContext = appContext;
  }

  @Override
  public void render(Block html) {
    setTitle("Applications");

    TBODY<TABLE<Hamlet>> tbody =
        html.table("#apps").thead().tr().th(".id", "ID").th(".user", "User")
          .th(".name", "Name").th(".type", "Application Type")
          .th(".queue", "Queue").th(".starttime", "StartTime")
          .th(".finishtime", "FinishTime").th(".state", "State")
          .th(".finalstatus", "FinalStatus").th(".progress", "Progress")
          .th(".ui", "Tracking UI")._()._().tbody();
    Collection<YarnApplicationState> reqAppStates = null;
    String reqStateString = $(APP_STATE);
    if (reqStateString != null && !reqStateString.isEmpty()) {
      String[] appStateStrings = reqStateString.split(",");
      reqAppStates = new HashSet<YarnApplicationState>(appStateStrings.length);
      for (String stateString : appStateStrings) {
        reqAppStates.add(YarnApplicationState.valueOf(stateString));
      }
    }

    Collection<ApplicationReport> appReports;
    try {
      appReports = appContext.getAllApplications().values();
    } catch (IOException e) {
      String message = "Failed to read the applications.";
      LOG.error(message, e);
      html.p()._(message)._();
      return;
    }
    StringBuilder appsTableData = new StringBuilder("[\n");
    for (ApplicationReport appReport : appReports) {
      if (reqAppStates != null
          && !reqAppStates.contains(appReport.getYarnApplicationState())) {
        continue;
      }
      AppInfo app = new AppInfo(appReport);
      String percent = String.format("%.1f", app.getProgress());
      // AppID numerical value parsed by parseHadoopID in yarn.dt.plugins.js
      appsTableData
        .append("[\"<a href='")
        .append(url("app", app.getAppId()))
        .append("'>")
        .append(app.getAppId())
        .append("</a>\",\"")
        .append(
          StringEscapeUtils.escapeJavaScript(StringEscapeUtils.escapeHtml(app
            .getUser())))
        .append("\",\"")
        .append(
          StringEscapeUtils.escapeJavaScript(StringEscapeUtils.escapeHtml(app
            .getName())))
        .append("\",\"")
        .append(
          StringEscapeUtils.escapeJavaScript(StringEscapeUtils.escapeHtml(app
            .getType())))
        .append("\",\"")
        .append(
          StringEscapeUtils.escapeJavaScript(StringEscapeUtils.escapeHtml(app
            .getQueue()))).append("\",\"").append(app.getStartedTime())
        .append("\",\"").append(app.getFinishedTime())
        .append("\",\"")
        .append(app.getAppState())
        .append("\",\"")
        .append(app.getFinalAppStatus())
        .append("\",\"")
        // Progress bar
        .append("<br title='").append(percent).append("'> <div class='")
        .append(C_PROGRESSBAR).append("' title='").append(join(percent, '%'))
        .append("'> ").append("<div class='").append(C_PROGRESSBAR_VALUE)
        .append("' style='").append(join("width:", percent, '%'))
        .append("'> </div> </div>").append("\",\"<a href='");

      String trackingURL =
          app.getTrackingUrl() == null ? "#" : app.getTrackingUrl();

      appsTableData.append(trackingURL).append("'>").append("History")
        .append("</a>\"],\n");

    }
    if (appsTableData.charAt(appsTableData.length() - 2) == ',') {
      appsTableData.delete(appsTableData.length() - 2,
        appsTableData.length() - 1);
    }
    appsTableData.append("]");
    html.script().$type("text/javascript")
      ._("var appsTableData=" + appsTableData)._();

    tbody._()._();
  }
}
