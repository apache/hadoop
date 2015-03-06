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
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._EVEN;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._INFO_WRAP;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._ODD;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._TH;

import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.List;

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
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ContainerNotFoundException;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

public class AppBlock extends HtmlBlock {

  private static final Log LOG = LogFactory.getLog(AppBlock.class);
  protected ApplicationBaseProtocol appBaseProt;
  protected Configuration conf;

  @Inject
  AppBlock(ApplicationBaseProtocol appBaseProt, ViewContext ctx, Configuration conf) {
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

    ApplicationId appID = null;
    try {
      appID = Apps.toAppID(aid);
    } catch (Exception e) {
      puts("Invalid Application ID: " + aid);
      return;
    }

    UserGroupInformation callerUGI = getCallerUGI();
    ApplicationReport appReport = null;
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
      ._("YarnApplicationState:",
        app.getAppState() == null ? UNAVAILABLE : clarifyAppState(app
          .getAppState()))
      ._("FinalStatus Reported by AM:",
        clairfyAppFinalStatus(app.getFinalAppStatus()))
      ._("Started:", Times.format(app.getStartedTime()))
      ._(
        "Elapsed:",
        StringUtils.formatTime(Times.elapsed(app.getStartedTime(),
          app.getFinishedTime())))
      ._("Tracking URL:",
        app.getTrackingUrl() == null || app.getTrackingUrl() == UNAVAILABLE
            ? null : root_url(app.getTrackingUrl()),
        app.getTrackingUrl() == null || app.getTrackingUrl() == UNAVAILABLE
            ? "Unassigned" : app.getAppState() == YarnApplicationState.FINISHED
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

    //TODO:YARN-3284
    //The preemption metrics will be exposed from ApplicationReport
    // and ApplicationAttemptReport
    ApplicationResourceUsageReport usageReport =
        appReport.getApplicationResourceUsageReport();
    DIV<Hamlet> pdiv = html.
        _(InfoBlock.class).
        div(_INFO_WRAP);
    info("Application Overview").clear();
    info("Application Metrics")
        ._("Total Resource Preempted:",
          Resources.none()) // TODO: YARN-3284
        ._("Total Number of Non-AM Containers Preempted:",
          String.valueOf(0)) // TODO: YARN-3284
        ._("Total Number of AM Containers Preempted:",
          String.valueOf(0)) // TODO: YARN-3284
        ._("Resource Preempted from Current Attempt:",
          Resources.none()) // TODO: YARN-3284
        ._("Number of Non-AM Containers Preempted from Current Attempt:",
          0) // TODO: YARN-3284
        ._("Aggregate Resource Allocation:",
          String.format("%d MB-seconds, %d vcore-seconds", usageReport == null
            ? 0 : usageReport.getMemorySeconds(), usageReport == null ? 0
            : usageReport.getVcoreSeconds()));
    pdiv._();

    html._(InfoBlock.class);

    // Application Attempt Table
    TBODY<TABLE<Hamlet>> tbody =
        html.table("#attempts").thead().tr().th(".id", "Attempt ID")
          .th(".started", "Started").th(".node", "Node").th(".logs", "Logs")
          ._()._().tbody();

    StringBuilder attemptsTableData = new StringBuilder("[\n");
    for (final ApplicationAttemptReport appAttemptReport : attempts) {
      AppAttemptInfo appAttempt = new AppAttemptInfo(appAttemptReport);
      ContainerReport containerReport = null;
      try {
        // AM container is always the first container of the attempt
        final GetContainerReportRequest request =
            GetContainerReportRequest.newInstance(ContainerId.newContainerId(
              appAttemptReport.getApplicationAttemptId(), 1));
        if (callerUGI == null) {
          containerReport =
              appBaseProt.getContainerReport(request).getContainerReport();
        } else {
          containerReport = callerUGI.doAs(
              new PrivilegedExceptionAction<ContainerReport> () {
            @Override
            public ContainerReport run() throws Exception {
              ContainerReport report = null;
              try {
                report = appBaseProt.getContainerReport(request)
                    .getContainerReport();
              } catch (ContainerNotFoundException ex) {
                LOG.warn(ex.getMessage());
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
        .append("#") // TODO: replace with node http address (YARN-1884)
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

    createContainerLocalityTable(html); //TODO:YARN-3284
    createResourceRequestsTable(html, null); //TODO:YARN-3284
  }

  //TODO: YARN-3284
  //The containerLocality metrics will be exposed from AttemptReport
  private void createContainerLocalityTable(Block html) {
    int totalAllocatedContainers = 0; //TODO: YARN-3284
    int[][] localityStatistics = new int[0][0];//TODO:YARN-3284
    DIV<Hamlet> div = html.div(_INFO_WRAP);
    TABLE<DIV<Hamlet>> table =
        div.h3(
          "Total Allocated Containers: "
              + totalAllocatedContainers).h3("Each table cell"
            + " represents the number of NodeLocal/RackLocal/OffSwitch containers"
            + " satisfied by NodeLocal/RackLocal/OffSwitch resource requests.").table(
          "#containerLocality");
    table.
      tr().
        th(_TH, "").
        th(_TH, "Node Local Request").
        th(_TH, "Rack Local Request").
        th(_TH, "Off Switch Request").
      _();

    String[] containersType =
        { "Num Node Local Containers (satisfied by)", "Num Rack Local Containers (satisfied by)",
            "Num Off Switch Containers (satisfied by)" };
    boolean odd = false;
    for (int i = 0; i < localityStatistics.length; i++) {
      table.tr((odd = !odd) ? _ODD : _EVEN).td(containersType[i])
        .td(String.valueOf(localityStatistics[i][0]))
        .td(i == 0 ? "" : String.valueOf(localityStatistics[i][1]))
        .td(i <= 1 ? "" : String.valueOf(localityStatistics[i][2]))._();
    }
    table._();
    div._();
  }

  //TODO:YARN-3284
  //The resource requests metrics will be exposed from attemptReport
  private void createResourceRequestsTable(Block html, List<ResourceRequest> resouceRequests) {
    TBODY<TABLE<Hamlet>> tbody =
        html.table("#ResourceRequests").thead().tr()
          .th(".priority", "Priority")
          .th(".resourceName", "ResourceName")
          .th(".totalResource", "Capability")
          .th(".numContainers", "NumContainers")
          .th(".relaxLocality", "RelaxLocality")
          .th(".nodeLabelExpression", "NodeLabelExpression")._()._().tbody();

    Resource totalResource = Resource.newInstance(0, 0);
    if (resouceRequests != null) {
      for (ResourceRequest request : resouceRequests) {
        if (request.getNumContainers() == 0) {
          continue;
        }

        tbody.tr()
          .td(String.valueOf(request.getPriority()))
          .td(request.getResourceName())
          .td(String.valueOf(request.getCapability()))
          .td(String.valueOf(request.getNumContainers()))
          .td(String.valueOf(request.getRelaxLocality()))
          .td(request.getNodeLabelExpression() == null ? "N/A" : request
              .getNodeLabelExpression())._();
        if (request.getResourceName().equals(ResourceRequest.ANY)) {
          Resources.addTo(totalResource,
            Resources.multiply(request.getCapability(),
              request.getNumContainers()));
        }
      }
    }
    html.div().$class("totalResourceRequests")
      .h3("Total Outstanding Resource Requests: " + totalResource)._();
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
}
