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
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;
import com.google.inject.Inject;

public class AppAttemptBlock extends HtmlBlock {

  private static final Log LOG = LogFactory.getLog(AppAttemptBlock.class);
  protected ApplicationBaseProtocol appBaseProt;
  protected ApplicationAttemptId appAttemptId = null;

  @Inject
  public AppAttemptBlock(ApplicationBaseProtocol appBaseProt, ViewContext ctx) {
    super(ctx);
    this.appBaseProt = appBaseProt;
  }

  @Override
  protected void render(Block html) {
    String attemptid = $(APPLICATION_ATTEMPT_ID);
    if (attemptid.isEmpty()) {
      puts("Bad request: requires application attempt ID");
      return;
    }

    try {
      appAttemptId = ApplicationAttemptId.fromString(attemptid);
    } catch (IllegalArgumentException e) {
      puts("Invalid application attempt ID: " + attemptid);
      return;
    }

    UserGroupInformation callerUGI = getCallerUGI();
    ApplicationAttemptReport appAttemptReport;
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
    generateOverview(appAttemptReport, containers, appAttempt, node);

    if (exceptionWhenGetContainerReports) {
      html
        .p()
        ._(
          "Sorry, Failed to get containers for application attempt" + attemptid
              + ".")._();
      return;
    }

    createAttemptHeadRoomTable(html);
    html._(InfoBlock.class);

    createTablesForAttemptMetrics(html);

    // Container Table
    TBODY<TABLE<Hamlet>> tbody =
        html.table("#containers").thead().tr().th(".id", "Container ID")
          .th(".node", "Node").th(".exitstatus", "Container Exit Status")
          .th(".logs", "Logs")._()._().tbody();

    StringBuilder containersTableData = new StringBuilder("[\n");
    for (ContainerReport containerReport : containers) {
      ContainerInfo container = new ContainerInfo(containerReport);
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
  }

  protected void generateOverview(ApplicationAttemptReport appAttemptReport,
      Collection<ContainerReport> containers, AppAttemptInfo appAttempt,
      String node) {
    String amContainerId = appAttempt.getAmContainerId();
    info("Application Attempt Overview")
      ._(
        "Application Attempt State:",
        appAttempt.getAppAttemptState() == null ? UNAVAILABLE : appAttempt
          .getAppAttemptState())
      ._("AM Container:",
          amContainerId == null
              || containers == null
              || !hasAMContainer(appAttemptReport.getAMContainerId(),
                  containers) ? null : root_url("container", amContainerId),
          amContainerId == null ? "N/A" : amContainerId)
      ._("Node:", node)
      ._(
        "Tracking URL:",
        appAttempt.getTrackingUrl() == null
            || appAttempt.getTrackingUrl().equals(UNAVAILABLE) ? null
            : root_url(appAttempt.getTrackingUrl()),
        appAttempt.getTrackingUrl() == null
            || appAttempt.getTrackingUrl().equals(UNAVAILABLE)
            ? "Unassigned"
            : appAttempt.getAppAttemptState() == YarnApplicationAttemptState.FINISHED
                || appAttempt.getAppAttemptState() == YarnApplicationAttemptState.FAILED
                || appAttempt.getAppAttemptState() == YarnApplicationAttemptState.KILLED
                ? "History" : "ApplicationMaster")
      ._(
        "Diagnostics Info:",
        appAttempt.getDiagnosticsInfo() == null ? "" : appAttempt
          .getDiagnosticsInfo());
  }

  protected boolean hasAMContainer(ContainerId containerId,
      Collection<ContainerReport> containers) {
    for (ContainerReport container : containers) {
      if (containerId.equals(container.getContainerId())) {
        return true;
      }
    }
    return false;
  }

  protected void createAttemptHeadRoomTable(Block html) {
    
  }

  protected void createTablesForAttemptMetrics(Block html) {

  }
}
