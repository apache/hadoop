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

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR_VALUE;

import java.util.Set;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.webapp.AppsBlock;
import org.apache.hadoop.yarn.server.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.webapp.View;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;

import com.google.inject.Inject;

public class RMAppsBlock extends AppsBlock {

  private ResourceManager rm;

  @Inject
  RMAppsBlock(ResourceManager rm, ApplicationBaseProtocol appBaseProt,
      View.ViewContext ctx) {
    super(appBaseProt, ctx);
    this.rm = rm;
  }

  @Override
  protected void renderData(Block html) {
    TBODY<TABLE<Hamlet>> tbody =
        html.table("#apps").thead().tr().th(".id", "ID").th(".user", "User")
          .th(".name", "Name").th(".type", "Application Type")
          .th(".queue", "Queue").th(".priority", "Application Priority")
          .th(".starttime", "StartTime")
          .th(".finishtime", "FinishTime").th(".state", "State")
          .th(".finalstatus", "FinalStatus")
          .th(".runningcontainer", "Running Containers")
          .th(".allocatedCpu", "Allocated CPU VCores")
          .th(".allocatedMemory", "Allocated Memory MB")
          .th(".queuePercentage", "% of Queue")
          .th(".clusterPercentage", "% of Cluster")
          .th(".progress", "Progress")
          .th(".ui", "Tracking UI")
          .th(".blacklisted", "Blacklisted Nodes")._()
          ._().tbody();

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
      ApplicationAttemptId appAttemptId = ApplicationAttemptId.fromString(
          app.getCurrentAppAttemptId());
      String queuePercent = "N/A";
      String clusterPercent = "N/A";
      if(appReport.getApplicationResourceUsageReport() != null) {
        queuePercent = String.format("%.1f",
            appReport.getApplicationResourceUsageReport()
                .getQueueUsagePercentage());
        clusterPercent = String.format("%.1f",
            appReport.getApplicationResourceUsageReport().getClusterUsagePercentage());
      }

      String blacklistedNodesCount = "N/A";
      RMAppAttempt appAttempt =
          rm.getRMContext().getRMApps().get(appAttemptId.getApplicationId())
              .getAppAttempts().get(appAttemptId);
      Set<String> nodes =
          null == appAttempt ? null : appAttempt.getBlacklistedNodes();
      if (nodes != null) {
        blacklistedNodesCount = String.valueOf(nodes.size());
      }
      String percent = StringUtils.format("%.1f", app.getProgress());
      appsTableData
        .append("[\"<a href='")
        .append(url("app", app.getAppId()))
        .append("'>")
        .append(app.getAppId())
        .append("</a>\",\"")
        .append(
          StringEscapeUtils.escapeJavaScript(
              StringEscapeUtils.escapeHtml(app.getUser())))
        .append("\",\"")
        .append(
          StringEscapeUtils.escapeJavaScript(
              StringEscapeUtils.escapeHtml(app.getName())))
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
        .append("\",\"").append(app.getFinishedTime())
        .append("\",\"")
        .append(app.getAppState() == null ? UNAVAILABLE : app.getAppState())
        .append("\",\"")
        .append(app.getFinalAppStatus())
        .append("\",\"")
        .append(app.getRunningContainers() == -1 ? "N/A" : String
            .valueOf(app.getRunningContainers()))
        .append("\",\"")
        .append(app.getAllocatedCpuVcores() == -1 ? "N/A" : String
            .valueOf(app.getAllocatedCpuVcores()))
        .append("\",\"")
        .append(app.getAllocatedMemoryMB() == -1 ? "N/A" :
            String.valueOf(app.getAllocatedMemoryMB()))
        .append("\",\"")
        .append(queuePercent)
        .append("\",\"")
        .append(clusterPercent)
        .append("\",\"")
        // Progress bar
          .append("<br title='").append(percent).append("'> <div class='")
        .append(C_PROGRESSBAR).append("' title='").append(join(percent, '%'))
        .append("'> ").append("<div class='").append(C_PROGRESSBAR_VALUE)
        .append("' style='").append(join("width:", percent, '%'))
        .append("'> </div> </div>").append("\",\"<a ");

      String trackingURL =
          app.getTrackingUrl() == null
              || app.getTrackingUrl().equals(UNAVAILABLE)
              || app.getAppState() == YarnApplicationState.NEW ? null : app
              .getTrackingUrl();

      String trackingUI =
          app.getTrackingUrl() == null
              || app.getTrackingUrl().equals(UNAVAILABLE)
              || app.getAppState() == YarnApplicationState.NEW ? "Unassigned"
              : app.getAppState() == YarnApplicationState.FINISHED
              || app.getAppState() == YarnApplicationState.FAILED
              || app.getAppState() == YarnApplicationState.KILLED ? "History"
              : "ApplicationMaster";
      appsTableData.append(trackingURL == null ? "#" : "href='" + trackingURL)
        .append("'>").append(trackingUI).append("</a>\",").append("\"")
        .append(blacklistedNodesCount).append("\"],\n");

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
