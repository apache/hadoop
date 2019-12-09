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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.http.RestCsrfPreventionFilter;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ContainerNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.ResponseInfo;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppBlock extends HtmlBlock {

  private static final Logger LOG = LoggerFactory.getLogger(AppBlock.class);
  protected ApplicationBaseProtocol appBaseProt;
  protected Configuration conf;
  protected ApplicationId appID = null;
  private boolean unsecuredUI = true;


  @Inject
  protected AppBlock(ApplicationBaseProtocol appBaseProt, ViewContext ctx,
      Configuration conf) {
    super(ctx);
    this.appBaseProt = appBaseProt;
    this.conf = conf;
    // check if UI is unsecured.
    String httpAuth = conf.get(CommonConfigurationKeys.HADOOP_HTTP_AUTHENTICATION_TYPE);
    this.unsecuredUI = (httpAuth != null) && httpAuth.equals("simple");
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
        appReport = getApplicationReport(request);
      } else {
        appReport = callerUGI.doAs(
            new PrivilegedExceptionAction<ApplicationReport> () {
          @Override
          public ApplicationReport run() throws Exception {
            return getApplicationReport(request);
          }
        });
      }
    } catch (Exception e) {
      String message = "Failed to read the application " + appID + ".";
      LOG.error(message, e);
      html.p().__(message).__();
      return;
    }

    if (appReport == null) {
      puts("Application not found: " + aid);
      return;
    }

    AppInfo app = new AppInfo(appReport);

    setTitle(join("Application ", aid));

    //Validate if able to read application attempts
    // which should also validate if kill is allowed for the user based on ACLs

    Collection<ApplicationAttemptReport> attempts;
    try {
      final GetApplicationAttemptsRequest request =
          GetApplicationAttemptsRequest.newInstance(appID);
      if (callerUGI == null) {
        attempts = getApplicationAttemptsReport(request);
      } else {
        attempts = callerUGI.doAs(
          new PrivilegedExceptionAction<Collection<
              ApplicationAttemptReport>>() {
            @Override
            public Collection<ApplicationAttemptReport> run()
                throws Exception {
              return getApplicationAttemptsReport(request);
            }
          });
      }
    } catch (Exception e) {
      String message =
          "Failed to read the attempts of the application " + appID + ".";
      LOG.error(message, e);
      html.p().__(message).__();
      return;
    }


    // YARN-6890. for secured cluster allow anonymous UI access, application kill
    // shouldn't be there.
    boolean unsecuredUIForSecuredCluster = UserGroupInformation.isSecurityEnabled()
        && this.unsecuredUI;

    if (webUiType != null
        && webUiType.equals(YarnWebParams.RM_WEB_UI)
        && conf.getBoolean(YarnConfiguration.RM_WEBAPP_UI_ACTIONS_ENABLED,
          YarnConfiguration.DEFAULT_RM_WEBAPP_UI_ACTIONS_ENABLED)
            && !unsecuredUIForSecuredCluster
            && !isAppInFinalState(app)) {
      // Application Kill
      html.div()
        .button()
          .$onclick("confirmAction()").b("Kill Application").__()
          .__();

      StringBuilder script = new StringBuilder();
      script.append("function confirmAction() {")
          .append(" b = confirm(\"Are you sure?\");")
          .append(" if (b == true) {")
          .append(" $.ajax({")
          .append(" type: 'PUT',")
          .append(" url: '/ws/v1/cluster/apps/").append(aid).append("/state',")
          .append(" contentType: 'application/json',")
          .append(getCSRFHeaderString(conf))
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

      html.script().$type("text/javascript").__(script.toString()).__();
    }

    String schedulerPath = WebAppUtils.getResolvedRMWebAppURLWithScheme(conf) +
        "/cluster/scheduler?openQueues=" + app.getQueue();

    generateOverviewTable(app, schedulerPath, webUiType, appReport);

    createApplicationMetricsTable(html);

    html.__(InfoBlock.class);

    generateApplicationTable(html, callerUGI, attempts);

  }

  /**
   * Generate overview table for app web page.
   * @param app app info.
   * @param schedulerPath schedule path.
   * @param webUiType web ui type.
   * @param appReport app report.
   */
  private void generateOverviewTable(AppInfo app, String schedulerPath,
      String webUiType, ApplicationReport appReport) {
    ResponseInfo overviewTable = info("Application Overview")
        .__("User:", schedulerPath, app.getUser())
        .__("Name:", app.getName())
        .__("Application Type:", app.getType())
        .__("Application Tags:",
            app.getApplicationTags() == null ? "" : app.getApplicationTags())
        .__("Application Priority:", clarifyAppPriority(app.getPriority()))
        .__(
            "YarnApplicationState:",
            app.getAppState() == null ? UNAVAILABLE : clarifyAppState(app
                .getAppState()))
        .__("Queue:", schedulerPath, app.getQueue())
        .__("FinalStatus Reported by AM:",
            clairfyAppFinalStatus(app.getFinalAppStatus()))
        .__("Started:", Times.format(app.getStartedTime()))
        .__("Launched:", Times.format(app.getLaunchTime()))
        .__("Finished:", Times.format(app.getFinishedTime()))
        .__("Elapsed:", StringUtils.formatTime(app.getElapsedTime()))
        .__(
            "Tracking URL:",
            app.getTrackingUrl() == null
                || app.getTrackingUrl().equals(UNAVAILABLE) ? null : root_url(app
                .getTrackingUrl()),
            app.getTrackingUrl() == null
                || app.getTrackingUrl().equals(UNAVAILABLE) ? "Unassigned" : app
                .getAppState() == YarnApplicationState.FINISHED
                || app.getAppState() == YarnApplicationState.FAILED
                || app.getAppState() == YarnApplicationState.KILLED ? "History"
                : "ApplicationMaster");
    if (webUiType != null
        && webUiType.equals(YarnWebParams.RM_WEB_UI)) {
      LogAggregationStatus status = getLogAggregationStatus();
      if (status == null) {
        overviewTable.__("Log Aggregation Status:", "N/A");
      } else if (status == LogAggregationStatus.DISABLED
          || status == LogAggregationStatus.NOT_START
          || status == LogAggregationStatus.SUCCEEDED) {
        overviewTable.__("Log Aggregation Status:", status.name());
      } else {
        overviewTable.__("Log Aggregation Status:",
            root_url("logaggregationstatus", app.getAppId()), status.name());
      }
      long timeout = appReport.getApplicationTimeouts()
          .get(ApplicationTimeoutType.LIFETIME).getRemainingTime();
      if (timeout < 0) {
        overviewTable.__("Application Timeout (Remaining Time):", "Unlimited");
      } else {
        overviewTable.__("Application Timeout (Remaining Time):",
            String.format("%d seconds", timeout));
      }
    }
    overviewTable.__("Diagnostics:",
        app.getDiagnosticsInfo() == null ? "" : app.getDiagnosticsInfo());
    overviewTable.__("Unmanaged Application:", app.isUnmanagedApp());
    overviewTable.__("Application Node Label expression:",
        app.getAppNodeLabelExpression() == null ? "<Not set>"
            : app.getAppNodeLabelExpression());
    overviewTable.__("AM container Node Label expression:",
        app.getAmNodeLabelExpression() == null ? "<Not set>"
            : app.getAmNodeLabelExpression());
  }

  protected void generateApplicationTable(Block html,
      UserGroupInformation callerUGI,
      Collection<ApplicationAttemptReport> attempts) {
    // Application Attempt Table
    TBODY<TABLE<Hamlet>> tbody =
        html.table("#attempts").thead().tr().th(".id", "Attempt ID")
          .th(".started", "Started").th(".node", "Node").th(".logs", "Logs")
          .__().__().tbody();

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
              getContainerReport(request);
        } else {
          containerReport = callerUGI.doAs(
              new PrivilegedExceptionAction<ContainerReport>() {
            @Override
            public ContainerReport run() throws Exception {
              ContainerReport report = null;
              if (request.getContainerId() != null) {
                  try {
                    report = getContainerReport(request);
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
        html.p().__(message).__();
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
            .escapeEcmaScript(StringEscapeUtils.escapeHtml4(nodeLink)))
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
      .__("var attemptsTableData=" + attemptsTableData).__();

    tbody.__().__();
  }

  protected ContainerReport getContainerReport(
      final GetContainerReportRequest request)
      throws YarnException, IOException {
    return appBaseProt.getContainerReport(request).getContainerReport();
  }

  protected List<ApplicationAttemptReport> getApplicationAttemptsReport(
      final GetApplicationAttemptsRequest request)
      throws YarnException, IOException {
    return appBaseProt.getApplicationAttempts(request)
        .getApplicationAttemptList();
  }

  protected ApplicationReport getApplicationReport(
      final GetApplicationReportRequest request)
      throws YarnException, IOException {
    return appBaseProt.getApplicationReport(request).getApplicationReport();
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

  private String clarifyAppPriority(int priority) {
    return priority + " (Higher Integer value indicates higher priority)";
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

  // This will be overrided in RMAppBlock
  protected LogAggregationStatus getLogAggregationStatus() {
    return null;
  }

  public static String getCSRFHeaderString(Configuration conf) {
    String ret = "";
    if (conf.getBoolean(YarnConfiguration.RM_CSRF_ENABLED, false)) {
      ret = " headers : { '";
      Map<String, String> filterParams = RestCsrfPreventionFilter
          .getFilterParams(conf, YarnConfiguration.RM_CSRF_PREFIX);
      if (filterParams
          .containsKey(RestCsrfPreventionFilter.CUSTOM_HEADER_PARAM)) {
        ret += filterParams.get(RestCsrfPreventionFilter.CUSTOM_HEADER_PARAM);
      } else {
        ret += RestCsrfPreventionFilter.HEADER_DEFAULT;
      }
      ret += "' : 'null' },";
    }
    return ret;
  }

  private boolean isAppInFinalState(AppInfo app) {
    return app.getAppState() == YarnApplicationState.FINISHED
        || app.getAppState() == YarnApplicationState.FAILED
        || app.getAppState() == YarnApplicationState.KILLED;
  }
}
