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

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.webapp.AppsBlock;
import org.apache.hadoop.yarn.server.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.webapp.View;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.THEAD;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TR;

import com.google.inject.Inject;

public class RMAppsBlock extends AppsBlock {

  private ResourceManager rm;

  /** Columns for the Apps RM page. */
  static final ColumnHeader[] COLUMNS = {
      new ColumnHeader(".id", "ID"),
      new ColumnHeader(".user", "User"),
      new ColumnHeader(".name", "Name"),
      new ColumnHeader(".type", "Application Type"),
      new ColumnHeader(".apptag", "Application Tags"),
      new ColumnHeader(".queue", "Queue"),
      new ColumnHeader(".priority", "Application Priority"),
      new ColumnHeader(".starttime", "StartTime"),
      new ColumnHeader(".IDlaunchtime", "LaunchTime"),
      new ColumnHeader(".finishtime", "FinishTime"),
      new ColumnHeader(".state", "State"),
      new ColumnHeader(".finalstatus", "FinalStatus"),
      new ColumnHeader(".runningcontainer", "Running Containers"),
      new ColumnHeader(".allocatedCpu", "Allocated CPU VCores"),
      new ColumnHeader(".allocatedMemory", "Allocated Memory MB"),
      new ColumnHeader(".allocatedGpu", "Allocated GPUs"),
      new ColumnHeader(".reservedCpu", "Reserved CPU VCores"),
      new ColumnHeader(".reservedMemory", "Reserved Memory MB"),
      new ColumnHeader(".reservedGpu", "Reserved GPUs"),
      new ColumnHeader(".queuePercentage", "% of Queue"),
      new ColumnHeader(".clusterPercentage", "% of Cluster"),
      new ColumnHeader(".progress", "Progress"),
      new ColumnHeader(".ui", "Tracking UI"),
      new ColumnHeader(".blacklisted", "Blacklisted Nodes"),
  };

  @Inject
  RMAppsBlock(ResourceManager rm, View.ViewContext ctx) {
    super(null, ctx);
    this.rm = rm;
  }

  @Override
  protected void renderData(Block html) {

    TR<THEAD<TABLE<Hamlet>>> tr = html.table("#apps").thead().tr();
    for (ColumnHeader col : COLUMNS) {
      tr = tr.th(col.getSelector(), col.getCData());
    }
    TBODY<TABLE<Hamlet>> tbody = tr.__().__().tbody();

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
      RMApp rmApp = rm.getRMContext().getRMApps()
          .get(appAttemptId.getApplicationId());
      boolean isAppInCompletedState = false;
      if (rmApp != null) {
        RMAppAttempt appAttempt = rmApp.getRMAppAttempt(appAttemptId);
        Set<String> nodes =
            null == appAttempt ? null : appAttempt.getBlacklistedNodes();
        if (nodes != null) {
          blacklistedNodesCount = String.valueOf(nodes.size());
        }
        isAppInCompletedState = rmApp.isAppInCompletedStates();
      }
      String percent = StringUtils.format("%.1f", app.getProgress());
      appsTableData
        .append("[\"<a href='")
        .append(url("app", app.getAppId()))
        .append("'>")
        .append(app.getAppId())
        .append("</a>\",\"")
        .append(
          StringEscapeUtils.escapeEcmaScript(
              StringEscapeUtils.escapeHtml4(app.getUser())))
        .append("\",\"")
        .append(
          StringEscapeUtils.escapeEcmaScript(
              StringEscapeUtils.escapeHtml4(app.getName())))
        .append("\",\"")
        .append(
          StringEscapeUtils.escapeEcmaScript(StringEscapeUtils.escapeHtml4(app
            .getType())))
        .append("\",\"")
        .append(
          StringEscapeUtils.escapeEcmaScript(StringEscapeUtils.escapeHtml4(
            app.getApplicationTags() == null ? "" : app.getApplicationTags())))
        .append("\",\"")
        .append(
          StringEscapeUtils.escapeEcmaScript(StringEscapeUtils.escapeHtml4(app
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
        .append(app.getRunningContainers() == -1 ? "N/A" : String
            .valueOf(app.getRunningContainers()))
        .append("\",\"")
        .append(app.getAllocatedCpuVcores() == -1 ? "N/A" : String
            .valueOf(app.getAllocatedCpuVcores()))
        .append("\",\"")
        .append(app.getAllocatedMemoryMB() == -1 ? "N/A" :
            String.valueOf(app.getAllocatedMemoryMB()))
        .append("\",\"")
        .append((isAppInCompletedState && app.getAllocatedGpus() <= 0)
            ? UNAVAILABLE : String.valueOf(app.getAllocatedGpus()))
        .append("\",\"")
        .append(app.getReservedCpuVcores() == -1 ? "N/A" : String
            .valueOf(app.getReservedCpuVcores()))
        .append("\",\"")
        .append(app.getReservedMemoryMB() == -1 ? "N/A" :
            String.valueOf(app.getReservedMemoryMB()))
        .append("\",\"")
        .append((isAppInCompletedState && app.getReservedGpus() <= 0)
            ? UNAVAILABLE : String.valueOf(app.getReservedGpus()))
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
              : Apps.isApplicationFinalState(app.getAppState()) ?
              "History" : "ApplicationMaster";
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
      .__("var appsTableData=" + appsTableData).__();

    tbody.__().__();
  }

  @Override
  protected List<ApplicationReport> getApplicationReport(
      final GetApplicationsRequest request) throws YarnException, IOException {
    return rm.getClientRMService().getApplications(request)
        .getApplicationList();
  }
}
