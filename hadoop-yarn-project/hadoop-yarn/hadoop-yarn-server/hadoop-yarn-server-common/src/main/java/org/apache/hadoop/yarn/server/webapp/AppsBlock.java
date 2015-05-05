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
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.EnumSet;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

public class AppsBlock extends HtmlBlock {

  private static final Log LOG = LogFactory.getLog(AppsBlock.class);
  protected ApplicationBaseProtocol appBaseProt;
  protected EnumSet<YarnApplicationState> reqAppStates;
  protected UserGroupInformation callerUGI;
  protected Collection<ApplicationReport> appReports;

  @Inject
  protected AppsBlock(ApplicationBaseProtocol appBaseProt, ViewContext ctx) {
    super(ctx);
    this.appBaseProt = appBaseProt;
  }

  protected void fetchData() throws YarnException, IOException,
      InterruptedException {
    reqAppStates = EnumSet.noneOf(YarnApplicationState.class);
    String reqStateString = $(APP_STATE);
    if (reqStateString != null && !reqStateString.isEmpty()) {
      String[] appStateStrings = reqStateString.split(",");
      for (String stateString : appStateStrings) {
        reqAppStates.add(YarnApplicationState.valueOf(stateString.trim()));
      }
    }

    callerUGI = getCallerUGI();
    final GetApplicationsRequest request =
        GetApplicationsRequest.newInstance(reqAppStates);
    if (callerUGI == null) {
      appReports = appBaseProt.getApplications(request).getApplicationList();
    } else {
      appReports =
          callerUGI
            .doAs(new PrivilegedExceptionAction<Collection<ApplicationReport>>() {
              @Override
              public Collection<ApplicationReport> run() throws Exception {
                return appBaseProt.getApplications(request)
                  .getApplicationList();
              }
            });
    }
  }

  @Override
  public void render(Block html) {
    setTitle("Applications");

    try {
      fetchData();
    }
    catch( Exception e) {
      String message = "Failed to read the applications.";
      LOG.error(message, e);
      html.p()._(message)._();
      return;
    }
    renderData(html);
  }

  protected void renderData(Block html) {
    TBODY<TABLE<Hamlet>> tbody =
        html.table("#apps").thead().tr().th(".id", "ID").th(".user", "User")
          .th(".name", "Name").th(".type", "Application Type")
          .th(".queue", "Queue").th(".starttime", "StartTime")
          .th(".finishtime", "FinishTime").th(".state", "State")
          .th(".finalstatus", "FinalStatus").th(".progress", "Progress")
          .th(".ui", "Tracking UI")._()._().tbody();

    StringBuilder appsTableData = new StringBuilder("[\n");
    for (ApplicationReport appReport : appReports) {
      // TODO: remove the following condition. It is still here because
      // the history side implementation of ApplicationBaseProtocol
      // hasn't filtering capability (YARN-1819).
      if (!reqAppStates.isEmpty()
          && !reqAppStates.contains(appReport.getYarnApplicationState())) {
        continue;
      }
      AppInfo app = new AppInfo(appReport);
      String percent = StringUtils.format("%.1f", app.getProgress());
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
        .append(app.getAppState() == null ? UNAVAILABLE : app.getAppState())
        .append("\",\"")
        .append(app.getFinalAppStatus())
        .append("\",\"")
        // Progress bar
        .append("<br title='").append(percent).append("'> <div class='")
        .append(C_PROGRESSBAR).append("' title='").append(join(percent, '%'))
        .append("'> ").append("<div class='").append(C_PROGRESSBAR_VALUE)
        .append("' style='").append(join("width:", percent, '%'))
        .append("'> </div> </div>").append("\",\"<a ");

      String trackingURL =
          app.getTrackingUrl() == null
              || app.getTrackingUrl().equals(UNAVAILABLE) ? null : app
            .getTrackingUrl();

      String trackingUI =
          app.getTrackingUrl() == null || app.getTrackingUrl().equals(UNAVAILABLE)
              ? "Unassigned"
              : app.getAppState() == YarnApplicationState.FINISHED
                  || app.getAppState() == YarnApplicationState.FAILED
                  || app.getAppState() == YarnApplicationState.KILLED
                  ? "History" : "ApplicationMaster";
      appsTableData.append(trackingURL == null ? "#" : "href='" + trackingURL)
        .append("'>").append(trackingUI).append("</a>\"],\n");

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
