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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI._INFO_WRAP;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.AppBlock;
import org.apache.hadoop.yarn.util.StringHelper;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

public class RMAppBlock extends AppBlock{

  private final ResourceManager rm;
  private final Configuration conf;


  @Inject
  RMAppBlock(ViewContext ctx, Configuration conf, ResourceManager rm) {
    super(null, ctx, conf);
    this.conf = conf;
    this.rm = rm;
  }

  @Override
  protected void render(Block html) {
    super.render(html);
  }

  @Override
  protected void createApplicationMetricsTable(Block html){
    RMApp rmApp = this.rm.getRMContext().getRMApps().get(appID);
    RMAppMetrics appMetrics = rmApp == null ? null : rmApp.getRMAppMetrics();
    // Get attempt metrics and fields, it is possible currentAttempt of RMApp is
    // null. In that case, we will assume resource preempted and number of Non
    // AM container preempted on that attempt is 0
    RMAppAttemptMetrics attemptMetrics;
    if (rmApp == null || null == rmApp.getCurrentAppAttempt()) {
      attemptMetrics = null;
    } else {
      attemptMetrics = rmApp.getCurrentAppAttempt().getRMAppAttemptMetrics();
    }
    Resource attemptResourcePreempted =
        attemptMetrics == null ? Resources.none() : attemptMetrics
          .getResourcePreempted();
    int attemptNumNonAMContainerPreempted =
        attemptMetrics == null ? 0 : attemptMetrics
          .getNumNonAMContainersPreempted();
    DIV<Hamlet> pdiv = html.
        __(InfoBlock.class).
        div(_INFO_WRAP);
    info("Application Overview").clear();
    info("Application Metrics")
        .__("Total Resource Preempted:",
          appMetrics == null ? "N/A" : appMetrics.getResourcePreempted())
        .__("Total Number of Non-AM Containers Preempted:",
          appMetrics == null ? "N/A"
              : appMetrics.getNumNonAMContainersPreempted())
        .__("Total Number of AM Containers Preempted:",
          appMetrics == null ? "N/A"
              : appMetrics.getNumAMContainersPreempted())
        .__("Resource Preempted from Current Attempt:",
          attemptResourcePreempted)
        .__("Number of Non-AM Containers Preempted from Current Attempt:",
          attemptNumNonAMContainerPreempted)
        .__("Aggregate Resource Allocation:", appMetrics == null ? "N/A" :
            StringHelper
                .getResourceSecondsString(appMetrics.getResourceSecondsMap()))
        .__("Aggregate Preempted Resource Allocation:",
            appMetrics == null ? "N/A" : StringHelper.getResourceSecondsString(
                appMetrics.getPreemptedResourceSecondsMap()));

    pdiv.__();
  }

  @Override
  protected void generateApplicationTable(Block html,
      UserGroupInformation callerUGI,
      Collection<ApplicationAttemptReport> attempts) {
    // Application Attempt Table
    Hamlet.TBODY<Hamlet.TABLE<Hamlet>> tbody =
        html.table("#attempts").thead().tr().th(".id", "Attempt ID")
            .th(".started", "Started").th(".node", "Node").th(".logs", "Logs")
            .th(".appBlacklistednodes", "Nodes blacklisted by the application",
                "Nodes blacklisted by the app")
            .th(".rmBlacklistednodes", "Nodes blacklisted by the RM for the"
                + " app", "Nodes blacklisted by the system").__().__().tbody();

    RMApp rmApp = this.rm.getRMContext().getRMApps().get(this.appID);
    if (rmApp == null) {
      return;
    }
    StringBuilder attemptsTableData = new StringBuilder("[\n");
    for (final ApplicationAttemptReport appAttemptReport : attempts) {
      RMAppAttempt rmAppAttempt =
          rmApp.getRMAppAttempt(appAttemptReport.getApplicationAttemptId());
      if (rmAppAttempt == null) {
        continue;
      }
      AppAttemptInfo attemptInfo =
          new AppAttemptInfo(this.rm, rmAppAttempt, rmApp.getUser(),
              WebAppUtils.getHttpSchemePrefix(conf));
      Set<String> nodes = rmAppAttempt.getBlacklistedNodes();
      // nodes which are blacklisted by the application
      String appBlacklistedNodesCount = String.valueOf(nodes.size());
      // nodes which are blacklisted by the RM for AM launches
      String rmBlacklistedNodesCount =
          String.valueOf(rmAppAttempt.getAMBlacklistManager()
            .getBlacklistUpdates().getBlacklistAdditions().size());
      String nodeLink = attemptInfo.getNodeHttpAddress();
      if (nodeLink != null) {
        nodeLink = WebAppUtils.getHttpSchemePrefix(conf) + nodeLink;
      }
      String logsLink = attemptInfo.getLogsLink();
      attemptsTableData
          .append("[\"<a href='")
          .append(url("appattempt", rmAppAttempt.getAppAttemptId().toString()))
          .append("'>")
          .append(String.valueOf(rmAppAttempt.getAppAttemptId()))
          .append("</a>\",\"")
          .append(attemptInfo.getStartTime())
          .append("\",\"<a ")
          .append(nodeLink == null ? "#" : "href='" + nodeLink)
          .append("'>")
          .append(nodeLink == null ? "N/A" : StringEscapeUtils
              .escapeEcmaScript(StringEscapeUtils.escapeHtml4(nodeLink)))
          .append("</a>\",\"<a ")
          .append(logsLink == null ? "#" : "href='" + logsLink).append("'>")
          .append(logsLink == null ? "N/A" : "Logs").append("</a>\",")
          .append("\"").append(appBlacklistedNodesCount).append("\",")
          .append("\"").append(rmBlacklistedNodesCount).append("\"],\n");
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

  @Override
  protected LogAggregationStatus getLogAggregationStatus() {
    RMApp rmApp = this.rm.getRMContext().getRMApps().get(appID);
    if (rmApp == null) {
      return null;
    }
    return rmApp.getLogAggregationStatusForAppReport();
  }

  @Override
  protected ContainerReport getContainerReport(
      final GetContainerReportRequest request)
      throws YarnException, IOException {
    return rm.getClientRMService().getContainerReport(request)
        .getContainerReport();
  }

  @Override
  protected List<ApplicationAttemptReport> getApplicationAttemptsReport(
      final GetApplicationAttemptsRequest request)
      throws YarnException, IOException {
    return rm.getClientRMService().getApplicationAttempts(request)
        .getApplicationAttemptList();
  }

  @Override
  protected ApplicationReport getApplicationReport(
      final GetApplicationReportRequest request)
      throws YarnException, IOException {
    return rm.getClientRMService().getApplicationReport(request)
        .getApplicationReport();
  }

}
