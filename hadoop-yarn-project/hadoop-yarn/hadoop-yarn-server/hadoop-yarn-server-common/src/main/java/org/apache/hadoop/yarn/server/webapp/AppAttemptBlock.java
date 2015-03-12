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
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APPLICATION_ATTEMPT_ID;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.WEB_UI_TYPE;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._EVEN;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._INFO_WRAP;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._ODD;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._TH;

import java.security.PrivilegedExceptionAction;
import java.util.Collection;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

public class AppAttemptBlock extends HtmlBlock {

  private static final Log LOG = LogFactory.getLog(AppAttemptBlock.class);
  protected ApplicationBaseProtocol appBaseProt;

  @Inject
  public AppAttemptBlock(ApplicationBaseProtocol appBaseProt, ViewContext ctx) {
    super(ctx);
    this.appBaseProt = appBaseProt;
  }

  @Override
  protected void render(Block html) {
    String webUiType = $(WEB_UI_TYPE);
    String attemptid = $(APPLICATION_ATTEMPT_ID);
    if (attemptid.isEmpty()) {
      puts("Bad request: requires application attempt ID");
      return;
    }

    ApplicationAttemptId appAttemptId = null;
    try {
      appAttemptId = ConverterUtils.toApplicationAttemptId(attemptid);
    } catch (IllegalArgumentException e) {
      puts("Invalid application attempt ID: " + attemptid);
      return;
    }

    UserGroupInformation callerUGI = getCallerUGI();
    ApplicationAttemptReport appAttemptReport = null;
    try {
      final GetApplicationAttemptReportRequest request =
          GetApplicationAttemptReportRequest.newInstance(appAttemptId);
      if (callerUGI == null) {
        appAttemptReport =
            appBaseProt.getApplicationAttemptReport(request)
              .getApplicationAttemptReport();
      } else {
        appAttemptReport = callerUGI.doAs(
            new PrivilegedExceptionAction<ApplicationAttemptReport> () {
          @Override
          public ApplicationAttemptReport run() throws Exception {
            return appBaseProt.getApplicationAttemptReport(request)
                .getApplicationAttemptReport();
          }
        });
      }
    } catch (Exception e) {
      String message =
          "Failed to read the application attempt " + appAttemptId + ".";
      LOG.error(message, e);
      html.p()._(message)._();
      return;
    }

    if (appAttemptReport == null) {
      puts("Application Attempt not found: " + attemptid);
      return;
    }

    boolean exceptionWhenGetContainerReports = false;
    Collection<ContainerReport> containers = null;
    try {
      final GetContainersRequest request =
          GetContainersRequest.newInstance(appAttemptId);
      if (callerUGI == null) {
        containers = appBaseProt.getContainers(request).getContainerList();
      } else {
        containers = callerUGI.doAs(
            new PrivilegedExceptionAction<Collection<ContainerReport>> () {
          @Override
          public Collection<ContainerReport> run() throws Exception {
            return  appBaseProt.getContainers(request).getContainerList();
          }
        });
      }
    } catch (RuntimeException e) {
      // have this block to suppress the findbugs warning
      exceptionWhenGetContainerReports = true;
    } catch (Exception e) {
      exceptionWhenGetContainerReports = true;
    }

    AppAttemptInfo appAttempt = new AppAttemptInfo(appAttemptReport);

    setTitle(join("Application Attempt ", attemptid));

    String node = "N/A";
    if (appAttempt.getHost() != null && appAttempt.getRpcPort() >= 0
        && appAttempt.getRpcPort() < 65536) {
      node = appAttempt.getHost() + ":" + appAttempt.getRpcPort();
    }
    info("Application Attempt Overview")
      ._(
        "Application Attempt State:",
        appAttempt.getAppAttemptState() == null ? UNAVAILABLE : appAttempt
          .getAppAttemptState())
      ._(
        "AM Container:",
        appAttempt.getAmContainerId() == null || containers == null
            || !hasAMContainer(appAttemptReport.getAMContainerId(), containers)
            ? null : root_url("container", appAttempt.getAmContainerId()),
        String.valueOf(appAttempt.getAmContainerId()))
      ._("Node:", node)
      ._(
        "Tracking URL:",
        appAttempt.getTrackingUrl() == null
            || appAttempt.getTrackingUrl() == UNAVAILABLE ? null
            : root_url(appAttempt.getTrackingUrl()),
        appAttempt.getTrackingUrl() == null
            || appAttempt.getTrackingUrl() == UNAVAILABLE
            ? "Unassigned"
            : appAttempt.getAppAttemptState() == YarnApplicationAttemptState.FINISHED
                || appAttempt.getAppAttemptState() == YarnApplicationAttemptState.FAILED
                || appAttempt.getAppAttemptState() == YarnApplicationAttemptState.KILLED
                ? "History" : "ApplicationMaster")
      ._("Diagnostics Info:", appAttempt.getDiagnosticsInfo() == null ?
          "" : appAttempt.getDiagnosticsInfo());

    html._(InfoBlock.class);

    if (exceptionWhenGetContainerReports) {
      html
        .p()
        ._(
          "Sorry, Failed to get containers for application attempt" + attemptid
              + ".")._();
      return;
    }

    // Container Table
    TBODY<TABLE<Hamlet>> tbody =
        html.table("#containers").thead().tr().th(".id", "Container ID")
          .th(".node", "Node").th(".exitstatus", "Container Exit Status")
          .th(".logs", "Logs")._()._().tbody();

    StringBuilder containersTableData = new StringBuilder("[\n");
    for (ContainerReport containerReport : containers) {
      ContainerInfo container = new ContainerInfo(containerReport);
      // ConatinerID numerical value parsed by parseHadoopID in
      // yarn.dt.plugins.js
      containersTableData
        .append("[\"<a href='")
        .append(url("container", container.getContainerId()))
        .append("'>")
        .append(container.getContainerId())
        .append("</a>\",\"<a ")
        .append(
          container.getNodeHttpAddress() == null ? "#" : "href='"
              + container.getNodeHttpAddress())
        .append("'>")
        .append(container.getNodeHttpAddress() == null ? "N/A" :
            StringEscapeUtils.escapeJavaScript(StringEscapeUtils
                .escapeHtml(container.getNodeHttpAddress())))
        .append("</a>\",\"")
        .append(container.getContainerExitStatus()).append("\",\"<a href='")
        .append(container.getLogUrl() == null ?
            "#" : container.getLogUrl()).append("'>")
        .append(container.getLogUrl() == null ?
            "N/A" : "Logs").append("</a>\"],\n");
    }
    if (containersTableData.charAt(containersTableData.length() - 2) == ',') {
      containersTableData.delete(containersTableData.length() - 2,
        containersTableData.length() - 1);
    }
    containersTableData.append("]");
    html.script().$type("text/javascript")
      ._("var containersTableData=" + containersTableData)._();

    tbody._()._();

    if (webUiType.equals(YarnWebParams.RM_WEB_UI)) {
      createContainerLocalityTable(html); // TODO:YARN-3284
    }
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

  private boolean hasAMContainer(ContainerId containerId,
      Collection<ContainerReport> containers) {
    for (ContainerReport container : containers) {
      if (containerId.equals(container.getContainerId())) {
        return true;
      }
    }
    return false;
  }
}
