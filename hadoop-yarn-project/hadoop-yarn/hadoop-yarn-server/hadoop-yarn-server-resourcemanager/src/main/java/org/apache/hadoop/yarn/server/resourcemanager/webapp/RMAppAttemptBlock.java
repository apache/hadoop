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

import static org.apache.hadoop.yarn.webapp.view.JQueryUI._EVEN;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._INFO_WRAP;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._ODD;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._TH;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceRequestInfo;
import org.apache.hadoop.yarn.server.webapp.AppAttemptBlock;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

public class RMAppAttemptBlock extends AppAttemptBlock{

  private final ResourceManager rm;
  protected Configuration conf;

  @Inject
  RMAppAttemptBlock(ViewContext ctx, ResourceManager rm, Configuration conf) {
    super(null, ctx);
    this.rm = rm;
    this.conf = conf;
  }

  private void createResourceRequestsTable(Block html) {
    AppInfo app =
        new AppInfo(rm, rm.getRMContext().getRMApps()
          .get(this.appAttemptId.getApplicationId()), true,
          WebAppUtils.getHttpSchemePrefix(conf));

    List<ResourceRequestInfo> resourceRequests = app.getResourceRequests();
    if (resourceRequests == null || resourceRequests.isEmpty()) {
      return;
    }

    DIV<Hamlet> div = html.div(_INFO_WRAP);
    // Requests Table
    TBODY<TABLE<DIV<Hamlet>>> tbody = div
        .h3("Total Outstanding Resource Requests: "
            + getTotalResource(resourceRequests))
        .table("#resourceRequests").thead().tr().th(".priority", "Priority")
        .th(".resource", "ResourceName").th(".capacity", "Capability")
        .th(".containers", "NumContainers")
        .th(".relaxlocality", "RelaxLocality")
        .th(".labelexpression", "NodeLabelExpression").__().__().tbody();

    StringBuilder resourceRequestTableData = new StringBuilder("[\n");
    for (ResourceRequestInfo resourceRequest  : resourceRequests) {
      if (resourceRequest.getNumContainers() == 0) {
        continue;
      }
      resourceRequestTableData.append("[\"")
          .append(String.valueOf(resourceRequest.getPriority())).append("\",\"")
          .append(resourceRequest.getResourceName()).append("\",\"")
          .append(StringEscapeUtils.escapeJavaScript(StringEscapeUtils
              .escapeHtml(String.valueOf(resourceRequest.getCapability()))))
          .append("\",\"")
          .append(String.valueOf(resourceRequest.getNumContainers()))
          .append("\",\"")
          .append(String.valueOf(resourceRequest.getRelaxLocality()))
          .append("\",\"")
          .append(resourceRequest.getNodeLabelExpression() == null ? "N/A"
              : resourceRequest.getNodeLabelExpression())
          .append("\"],\n");
    }
    if (resourceRequestTableData
        .charAt(resourceRequestTableData.length() - 2) == ',') {
      resourceRequestTableData.delete(resourceRequestTableData.length() - 2,
          resourceRequestTableData.length() - 1);
    }
    resourceRequestTableData.append("]");
    html.script().$type("text/javascript")
        .__("var resourceRequestsTableData=" + resourceRequestTableData).__();
    tbody.__().__();
    div.__();
  }

  private Resource getTotalResource(List<ResourceRequestInfo> requests) {
    Resource totalResource = Resource.newInstance(0, 0);
    if (requests == null) {
      return totalResource;
    }
    for (ResourceRequestInfo request : requests) {
      if (request.getNumContainers() == 0) {
        continue;
      }
      if (request.getResourceName().equals(ResourceRequest.ANY)) {
        Resources.addTo(
            totalResource,
            Resources.multiply(request.getCapability().getResource(),
            request.getNumContainers()));
      }
    }
    return totalResource;
  }

  private void createContainerLocalityTable(Block html) {
    RMAppAttemptMetrics attemptMetrics = null;
    RMAppAttempt attempt = getRMAppAttempt();
    if (attempt != null) {
      attemptMetrics = attempt.getRMAppAttemptMetrics();
    }
    
    if (attemptMetrics == null) {
      return;
    }

    DIV<Hamlet> div = html.div(_INFO_WRAP);
    TABLE<DIV<Hamlet>> table =
        div.h3(
          "Total Allocated Containers: "
              + attemptMetrics.getTotalAllocatedContainers()).h3("Each table cell"
            + " represents the number of NodeLocal/RackLocal/OffSwitch containers"
            + " satisfied by NodeLocal/RackLocal/OffSwitch resource requests.").table(
          "#containerLocality");
    table.
      tr().
        th(_TH, "").
        th(_TH, "Node Local Request").
        th(_TH, "Rack Local Request").
        th(_TH, "Off Switch Request").
        __();

    String[] containersType =
        { "Num Node Local Containers (satisfied by)", "Num Rack Local Containers (satisfied by)",
            "Num Off Switch Containers (satisfied by)" };
    boolean odd = false;
    for (int i = 0; i < attemptMetrics.getLocalityStatistics().length; i++) {
      table.tr((odd = !odd) ? _ODD : _EVEN).td(containersType[i])
        .td(String.valueOf(attemptMetrics.getLocalityStatistics()[i][0]))
        .td(i == 0 ? "" : String.valueOf(attemptMetrics.getLocalityStatistics()[i][1]))
        .td(i <= 1 ? "" : String.valueOf(attemptMetrics.getLocalityStatistics()[i][2])).__();
    }
    table.__();
    div.__();
  }

  private boolean isApplicationInFinalState(YarnApplicationAttemptState state) {
    return state == YarnApplicationAttemptState.FINISHED
        || state == YarnApplicationAttemptState.FAILED
        || state == YarnApplicationAttemptState.KILLED;
  }

  @Override
  protected void createAttemptHeadRoomTable(Block html) {
    RMAppAttempt attempt = getRMAppAttempt();
    if (attempt != null) {
      if (!isApplicationInFinalState(YarnApplicationAttemptState
          .valueOf(attempt.getAppAttemptState().toString()))) {
        RMAppAttemptMetrics metrics = attempt.getRMAppAttemptMetrics();
        DIV<Hamlet> pdiv = html.__(InfoBlock.class).div(_INFO_WRAP);
        info("Application Attempt Overview").clear();
        info("Application Attempt Metrics").__(
          "Application Attempt Headroom : ", metrics == null ? "N/A" :
            metrics.getApplicationAttemptHeadroom());
        pdiv.__();
      }
    }
  }

  private RMAppAttempt getRMAppAttempt() {
    ApplicationId appId = this.appAttemptId.getApplicationId();
    RMAppAttempt attempt = null;
    RMApp rmApp = rm.getRMContext().getRMApps().get(appId);
    if (rmApp != null) { 
      attempt = rmApp.getAppAttempts().get(appAttemptId);
    }
    return attempt;
  }

  protected void generateOverview(ApplicationAttemptReport appAttemptReport,
      Collection<ContainerReport> containers, AppAttemptInfo appAttempt,
      String node) {

    RMAppAttempt rmAppAttempt = getRMAppAttempt();
    // nodes which are blacklisted by the application
    String appBlacklistedNodes =
        getNodeString(rmAppAttempt.getBlacklistedNodes());
    // nodes which are blacklisted by the RM for AM launches
    String rmBlackListedNodes =
        getNodeString(rmAppAttempt.getAMBlacklistManager()
          .getBlacklistUpdates().getBlacklistAdditions());

    info("Application Attempt Overview")
      .__(
        "Application Attempt State:",
        appAttempt.getAppAttemptState() == null ? UNAVAILABLE : appAttempt
          .getAppAttemptState())
        .__("Started:", Times.format(appAttempt.getStartedTime()))
        .__("Elapsed:",
            org.apache.hadoop.util.StringUtils.formatTime(Times.elapsed(
                appAttempt.getStartedTime(), appAttempt.getFinishedTime())))
      .__(
        "AM Container:",
        appAttempt.getAmContainerId() == null || containers == null
            || !hasAMContainer(appAttemptReport.getAMContainerId(), containers)
            ? null : root_url("container", appAttempt.getAmContainerId()),
        appAttempt.getAmContainerId() == null ? "N/A" :
          String.valueOf(appAttempt.getAmContainerId()))
      .__("Node:", node)
      .__(
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
      .__(
        "Diagnostics Info:",
        appAttempt.getDiagnosticsInfo() == null ? "" : appAttempt
          .getDiagnosticsInfo())
      .__("Nodes blacklisted by the application:", appBlacklistedNodes)
      .__("Nodes blacklisted by the system:", rmBlackListedNodes);
  }

  private String getNodeString(Collection<String> nodes) {
    String concatinatedString = "-";
    if (null != nodes && !nodes.isEmpty()) {
      concatinatedString = StringUtils.join(nodes, ", ");
    }
    return concatinatedString;
  }

  @Override
  protected void createTablesForAttemptMetrics(Block html) {
    createContainerLocalityTable(html);
    createResourceRequestsTable(html);
  }

  @Override
  protected List<ContainerReport> getContainers(
      final GetContainersRequest request) throws YarnException, IOException {
    return rm.getClientRMService().getContainers(request).getContainerList();
  }

  @Override
  protected ApplicationAttemptReport getApplicationAttemptReport(
      final GetApplicationAttemptReportRequest request)
      throws YarnException, IOException {
    return rm.getClientRMService().getApplicationAttemptReport(request)
        .getApplicationAttemptReport();
  }
}
