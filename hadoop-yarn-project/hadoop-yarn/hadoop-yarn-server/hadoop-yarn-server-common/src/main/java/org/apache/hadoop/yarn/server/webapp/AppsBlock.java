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
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_START_TIME_BEGIN;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_START_TIME_END;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APPS_NUM;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR_VALUE;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.math.LongRange;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppsBlock extends HtmlBlock {

  private static final Logger LOG = LoggerFactory.getLogger(AppsBlock.class);
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
    String appsNumStr = $(APPS_NUM);
    if (appsNumStr != null && !appsNumStr.isEmpty()) {
      long appsNum = Long.parseLong(appsNumStr);
      request.setLimit(appsNum);
    }

    String appStartedTimeBegainStr = $(APP_START_TIME_BEGIN);
    long appStartedTimeBegain = 0;
    if (appStartedTimeBegainStr != null && !appStartedTimeBegainStr.isEmpty()) {
      appStartedTimeBegain = Long.parseLong(appStartedTimeBegainStr);
      if (appStartedTimeBegain < 0) {
        throw new BadRequestException(
          "app.started-time.begin must be greater than 0");
      }
    }
    String appStartedTimeEndStr = $(APP_START_TIME_END);
    long appStartedTimeEnd = Long.MAX_VALUE;
    if (appStartedTimeEndStr != null && !appStartedTimeEndStr.isEmpty()) {
      appStartedTimeEnd = Long.parseLong(appStartedTimeEndStr);
      if (appStartedTimeEnd < 0) {
        throw new BadRequestException(
          "app.started-time.end must be greater than 0");
      }
    }
    if (appStartedTimeBegain > appStartedTimeEnd) {
      throw new BadRequestException(
        "app.started-time.end must be greater than app.started-time.begin");
    }
    request.setStartRange(
        new LongRange(appStartedTimeBegain, appStartedTimeEnd));

    if (callerUGI == null) {
      appReports = getApplicationReport(request);
    } else {
      appReports =
          callerUGI
            .doAs(new PrivilegedExceptionAction<Collection<ApplicationReport>>() {
              @Override
              public Collection<ApplicationReport> run() throws Exception {
                return getApplicationReport(request);
              }
            });
    }
  }

  protected List<ApplicationReport> getApplicationReport(
      final GetApplicationsRequest request) throws YarnException, IOException {
    return appBaseProt.getApplications(request).getApplicationList();
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
      html.p().__(message).__();
      return;
    }
    renderData(html);
  }

  protected void renderData(Block html) {
    TBODY<TABLE<Hamlet>> tbody =
        html.table("#apps").thead().tr().th(".id", "ID").th(".user", "User")
          .th(".name", "Name").th(".type", "Application Type")
          .th(".queue", "Queue").th(".priority", "Application Priority")
          .th(".starttime", "StartTime")
          .th(".launchtime", "LaunchTime")
          .th(".finishtime", "FinishTime")
          .th(".state", "State").th(".finalstatus", "FinalStatus")
          .th(".progress", "Progress").th(".ui", "Tracking UI").__().__().tbody();

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
            .getQueue()))).append("\",\"").append(String
                .valueOf(app.getPriority()))
        .append("\",\"").append(app.getStartedTime())
        .append("\",\"").append(app.getLaunchTime())
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
      .__("var appsTableData=" + appsTableData).__();

    tbody.__().__();
  }
}
