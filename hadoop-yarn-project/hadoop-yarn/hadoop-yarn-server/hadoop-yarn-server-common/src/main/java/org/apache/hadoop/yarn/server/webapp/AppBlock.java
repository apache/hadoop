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
import static org.apache.hadoop.yarn.webapp.YarnWebParams.WEB_UI_TYPE;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ContainerNotFoundException;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

public class AppBlock extends HtmlBlock {

  private static final Log LOG = LogFactory.getLog(AppBlock.class);
  protected ApplicationBaseProtocol appBaseProt;
  protected Configuration conf;
  protected ApplicationId appID = null;

  @Inject
  protected AppBlock(ApplicationBaseProtocol appBaseProt, ViewContext ctx,
      Configuration conf) {
    super(ctx);
    this.appBaseProt = appBaseProt;
    this.conf = conf;
  }

  @Override
  protected void render(Block html) {
    String webUiType = $(WEB_UI_TYPE);
    String aid = $(APPLICATION_ID);
    if (aid.isEmpty()) {
      puts("Bad request: requires Application ID");
      return;
    }

    try {
      appID = Apps.toAppID(aid);
    } catch (Exception e) {
      puts("Invalid Application ID: " + aid);
      return;
    }

    UserGroupInformation callerUGI = getCallerUGI();
    ApplicationReport appReport;
    try {
      final GetApplicationReportRequest request =
          GetApplicationReportRequest.newInstance(appID);
      if (callerUGI == null) {
        appReport =
            appBaseProt.getApplicationReport(request).getApplicationReport();
      } else {
        appReport = callerUGI.doAs(
            new PrivilegedExceptionAction<ApplicationReport> () {
          @Override
          public ApplicationReport run() throws Exception {
            return appBaseProt.getApplicationReport(request)
                .getApplicationReport();
          }
        });
      }
    } catch (Exception e) {
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

    if (webUiType != null
        && webUiType.equals(YarnWebParams.RM_WEB_UI)
        && conf.getBoolean(YarnConfiguration.RM_WEBAPP_UI_ACTIONS_ENABLED,
          YarnConfiguration.DEFAULT_RM_WEBAPP_UI_ACTIONS_ENABLED)) {
      // Application Kill
      html.div()
        .button()
          .$onclick("confirmAction()").b("Kill Application")._()
          ._();

      StringBuilder script = new StringBuilder();
      script.append("function confirmAction() {")
          .append(" b = confirm(\"Are you sure?\");")
          .append(" if (b == true) {")
          .append(" $.ajax({")
          .append(" type: 'PUT',")
          .append(" url: '/ws/v1/cluster/apps/").append(aid).append("/state',")
          .append(" contentType: 'application/json',")
          .append(" data: '{\"state\":\"KILLED\"}',")
          .append(" dataType: 'json'")
          .append(" }).done(function(data){")
          .append(" setTimeout(function(){")
          .append(" location.href = '/cluster/app/").append(aid).append("';")
          .append(" }, 1000);")
          .append(" }).fail(function(data){")
          .append(" console.log(data);")
          .append(" });")
          .append(" }")
          .append("}");

      html.script().$type("text/javascript")._(script.toString())._();
    }

    info("Application Overview")
      ._("User:", app.getUser())
      ._("Name:", app.getName())
      ._("Application Type:", app.getType())
      ._("Application Tags:",
        app.getApplicationTags() == null ? "" : app.getApplicationTags())
      ._(
        "YarnApplicationState:",
        app.getAppState() == null ? UNAVAILABLE : clarifyAppState(app
          .getAppState()))
      ._("FinalStatus Reported by AM:",
        clairfyAppFinalStatus(app.getFinalAppStatus()))
      ._("Started:", Times.format(app.getStartedTime()))
      ._(
        "Elapsed:",
        StringUtils.formatTime(Times.elapsed(app.getStartedTime(),
          app.getFinishedTime())))
      ._(
        "Tracking URL:",
        app.getTrackingUrl() == null
            || app.getTrackingUrl().equals(UNAVAILABLE) ? null : root_url(app
          .getTrackingUrl()),
        app.getTrackingUrl() == null
            || app.getTrackingUrl().equals(UNAVAILABLE) ? "Unassigned" : app
          .getAppState() == YarnApplicationState.FINISHED
            || app.getAppState() == YarnApplicationState.FAILED
            || app.getAppState() == YarnApplicationState.KILLED ? "History"
            : "ApplicationMaster")
      ._("Diagnostics:",
        app.getDiagnosticsInfo() == null ? "" : app.getDiagnosticsInfo());

    Collection<ApplicationAttemptReport> attempts;
    try {
      final GetApplicationAttemptsRequest request =
          GetApplicationAttemptsRequest.newInstance(appID);
      if (callerUGI == null) {
        attempts = appBaseProt.getApplicationAttempts(request)
            .getApplicationAttemptList();
      } else {
        attempts = callerUGI.doAs(
            new PrivilegedExceptionAction<Collection<ApplicationAttemptReport>> () {
          @Override
          public Collection<ApplicationAttemptReport> run() throws Exception {
            return appBaseProt.getApplicationAttempts(request)
                .getApplicationAttemptList();
          }
        });
      }
    } catch (Exception e) {
      String message =
          "Failed to read the attempts of the application " + appID + ".";
      LOG.error(message, e);
      html.p()._(message)._();
      return;
    }

    createApplicationMetricsTable(html);

    html._(InfoBlock.class);

    generateApplicationTable(html, callerUGI, attempts);

  }

  protected void generateApplicationTable(Block html,
      UserGroupInformation callerUGI,
      Collection<ApplicationAttemptReport> attempts) {
    // Application Attempt Table
    TBODY<TABLE<Hamlet>> tbody =
        html.table("#attempts").thead().tr().th(".id", "Attempt ID")
          .th(".started", "Started").th(".node", "Node").th(".logs", "Logs")
          ._()._().tbody();

    StringBuilder attemptsTableData = new StringBuilder("[\n");
    for (final ApplicationAttemptReport appAttemptReport : attempts) {
      AppAttemptInfo appAttempt = new AppAttemptInfo(appAttemptReport);
      ContainerReport containerReport;
      try {
        final GetContainerReportRequest request =
                GetContainerReportRequest.newInstance(
                      appAttemptReport.getAMContainerId());
        if (callerUGI == null) {
          containerReport =
              appBaseProt.getContainerReport(request).getContainerReport();
        } else {
          containerReport = callerUGI.doAs(
              new PrivilegedExceptionAction<ContainerReport>() {
            @Override
            public ContainerReport run() throws Exception {
              ContainerReport report = null;
              if (request.getContainerId() != null) {
                  try {
                    report = appBaseProt.getContainerReport(request)
                        .getContainerReport();
                  } catch (ContainerNotFoundException ex) {
                    LOG.warn(ex.getMessage());
                  }
              }
              return report;
            }
          });
        }
      } catch (Exception e) {
        String message =
            "Failed to read the AM container of the application attempt "
                + appAttemptReport.getApplicationAttemptId() + ".";
        LOG.error(message, e);
        html.p()._(message)._();
        return;
      }
      long startTime = 0L;
      String logsLink = null;
      String nodeLink = null;
      if (containerReport != null) {
        ContainerInfo container = new ContainerInfo(containerReport);
        startTime = container.getStartedTime();
        logsLink = containerReport.getLogUrl();
        nodeLink = containerReport.getNodeHttpAddress();
      }
      attemptsTableData
        .append("[\"<a href='")
        .append(url("appattempt", appAttempt.getAppAttemptId()))
        .append("'>")
        .append(appAttempt.getAppAttemptId())
        .append("</a>\",\"")
        .append(startTime)
        .append("\",\"<a ")
        .append(nodeLink == null ? "#" : "href='" + nodeLink)
        .append("'>")
        .append(nodeLink == null ? "N/A" : StringEscapeUtils
            .escapeJavaScript(StringEscapeUtils.escapeHtml(nodeLink)))
        .append("</a>\",\"<a ")
        .append(logsLink == null ? "#" : "href='" + logsLink).append("'>")
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

  private String clarifyAppState(YarnApplicationState state) {
    String ret = state.toString();
    switch (state) {
    case NEW:
      return ret + ": waiting for application to be initialized";
    case NEW_SAVING:
      return ret + ": waiting for application to be persisted in state-store.";
    case SUBMITTED:
      return ret + ": waiting for application to be accepted by scheduler.";
    case ACCEPTED:
      return ret + ": waiting for AM container to be allocated, launched and"
          + " register with RM.";
    case RUNNING:
      return ret + ": AM has registered with RM and started running.";
    default:
      return ret;
    }
  }

  private String clairfyAppFinalStatus(FinalApplicationStatus status) {
    if (status == FinalApplicationStatus.UNDEFINED) {
      return "Application has not completed yet.";
    }
    return status.toString();
  }

  // The preemption metrics only need to be shown in RM WebUI
  protected void createApplicationMetricsTable(Block html) {

  }
}
