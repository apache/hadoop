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
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APPLICATION_ID;

import java.io.IOException;
import java.util.Collection;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.server.api.ApplicationContext;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

public class AppBlock extends HtmlBlock {

  protected ApplicationContext appContext;

  @Inject
  AppBlock(ApplicationContext appContext, ViewContext ctx) {
    super(ctx);
    this.appContext = appContext;
  }

  @Override
  protected void render(Block html) {
    String aid = $(APPLICATION_ID);
    if (aid.isEmpty()) {
      puts("Bad request: requires Application ID");
      return;
    }

    ApplicationId appID = null;
    try {
      appID = Apps.toAppID(aid);
    } catch (Exception e) {
      puts("Invalid Application ID: " + aid);
      return;
    }

    ApplicationReport appReport;
    try {
      appReport = appContext.getApplication(appID);
    } catch (IOException e) {
      String message = "Failed to read the application " + appID + ".";
      LOG.error(message, e);
      html.p()._(message)._();
      return;
    }
    if (appReport == null) {
      puts("Application not found: " + aid);
      return;
    }
    AppInfo app = new AppInfo(appReport);

    setTitle(join("Application ", aid));

    info("Application Overview")
      ._("User:", app.getUser())
      ._("Name:", app.getName())
      ._("Application Type:", app.getType())
      ._("State:", app.getAppState())
      ._("FinalStatus:", app.getFinalAppStatus())
      ._("Started:", Times.format(app.getStartedTime()))
      ._(
        "Elapsed:",
        StringUtils.formatTime(Times.elapsed(app.getStartedTime(),
          app.getFinishedTime())))
      ._("Tracking URL:",
        app.getTrackingUrl() == null ? "#" : root_url(app.getTrackingUrl()),
        "History")._("Diagnostics:", app.getDiagnosticsInfo());

    html._(InfoBlock.class);

    Collection<ApplicationAttemptReport> attempts;
    try {
      attempts = appContext.getApplicationAttempts(appID).values();
    } catch (IOException e) {
      String message =
          "Failed to read the attempts of the application " + appID + ".";
      LOG.error(message, e);
      html.p()._(message)._();
      return;
    }

    // Application Attempt Table
    TBODY<TABLE<Hamlet>> tbody =
        html.table("#attempts").thead().tr().th(".id", "Attempt ID")
          .th(".started", "Started").th(".node", "Node").th(".logs", "Logs")
          ._()._().tbody();

    StringBuilder attemptsTableData = new StringBuilder("[\n");
    for (ApplicationAttemptReport appAttemptReport : attempts) {
      AppAttemptInfo appAttempt = new AppAttemptInfo(appAttemptReport);
      ContainerReport containerReport;
      try {
        containerReport =
            appContext.getAMContainer(appAttemptReport
              .getApplicationAttemptId());
      } catch (IOException e) {
        String message =
            "Failed to read the AM container of the application attempt "
                + appAttemptReport.getApplicationAttemptId() + ".";
        LOG.error(message, e);
        html.p()._(message)._();
        return;
      }
      long startTime = Long.MAX_VALUE;
      String logsLink = null;
      if (containerReport != null) {
        ContainerInfo container = new ContainerInfo(containerReport);
        startTime = container.getStartedTime();
        logsLink = containerReport.getLogUrl();
      }
      String nodeLink = null;
      if (appAttempt.getHost() != null && appAttempt.getRpcPort() >= 0
          && appAttempt.getRpcPort() < 65536) {
        nodeLink = appAttempt.getHost() + ":" + appAttempt.getRpcPort();
      }
      // AppAttemptID numerical value parsed by parseHadoopID in
      // yarn.dt.plugins.js
      attemptsTableData
        .append("[\"<a href='")
        .append(url("appattempt", appAttempt.getAppAttemptId()))
        .append("'>")
        .append(appAttempt.getAppAttemptId())
        .append("</a>\",\"")
        .append(startTime)
        .append("\",\"<a href='")
        .append(
          nodeLink == null ? "#" : url("//", nodeLink))
        .append("'>")
        .append(
          nodeLink == null ? "N/A" : StringEscapeUtils
            .escapeJavaScript(StringEscapeUtils.escapeHtml(nodeLink)))
        .append("</a>\",\"<a href='")
        .append(logsLink == null ? "#" : logsLink).append("'>")
        .append(logsLink == null ? "N/A" : "Logs").append("</a>\"],\n");
    }
    if (attemptsTableData.charAt(attemptsTableData.length() - 2) == ',') {
      attemptsTableData.delete(attemptsTableData.length() - 2,
        attemptsTableData.length() - 1);
    }
    attemptsTableData.append("]");
    html.script().$type("text/javascript")
      ._("var attemptsTableData=" + attemptsTableData)._();

    tbody._()._();
  }
}
