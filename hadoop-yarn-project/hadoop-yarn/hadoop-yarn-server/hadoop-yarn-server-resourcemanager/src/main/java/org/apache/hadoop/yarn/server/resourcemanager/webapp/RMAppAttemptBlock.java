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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.webapp.AppAttemptBlock;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;
import com.google.inject.Inject;
import java.util.List;

import java.util.Collection;
import java.util.Set;

public class RMAppAttemptBlock extends AppAttemptBlock{

  private final ResourceManager rm;
  protected Configuration conf;

  @Inject
  RMAppAttemptBlock(ViewContext ctx, ResourceManager rm, Configuration conf) {
    super(rm.getClientRMService(), ctx);
    this.rm = rm;
    this.conf = conf;
  }

  private void createResourceRequestsTable(Block html) {
    AppInfo app =
        new AppInfo(rm, rm.getRMContext().getRMApps()
          .get(this.appAttemptId.getApplicationId()), true,
          WebAppUtils.getHttpSchemePrefix(conf));

    List<ResourceRequest> resourceRequests = app.getResourceRequests();
    if (resourceRequests == null || resourceRequests.isEmpty()) {
      return;
    }

    DIV<Hamlet> div = html.div(_INFO_WRAP);
    TABLE<DIV<Hamlet>> table =
        div.h3("Total Outstanding Resource Requests: "
          + getTotalResource(resourceRequests)).table(
              "#ResourceRequests");

    table.tr().
      th(_TH, "Priority").
      th(_TH, "ResourceName").
      th(_TH, "Capability").
      th(_TH, "NumContainers").
      th(_TH, "RelaxLocality").
      th(_TH, "NodeLabelExpression").
    _();

    boolean odd = false;
    for (ResourceRequest request : resourceRequests) {
      if (request.getNumContainers() == 0) {
        continue;
      }
      table.tr((odd = !odd) ? _ODD : _EVEN)
        .td(String.valueOf(request.getPriority()))
        .td(request.getResourceName())
        .td(String.valueOf(request.getCapability()))
        .td(String.valueOf(request.getNumContainers()))
        .td(String.valueOf(request.getRelaxLocality()))
        .td(request.getNodeLabelExpression() == null ? "N/A" : request
            .getNodeLabelExpression())._();
    }
    table._();
    div._();
  }

  private Resource getTotalResource(List<ResourceRequest> requests) {
    Resource totalResource = Resource.newInstance(0, 0);
    if (requests == null) {
      return totalResource;
    }
    for (ResourceRequest request : requests) {
      if (request.getNumContainers() == 0) {
        continue;
      }
      if (request.getResourceName().equals(ResourceRequest.ANY)) {
        Resources.addTo(
          totalResource,
          Resources.multiply(request.getCapability(),
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
      _();

    String[] containersType =
        { "Num Node Local Containers (satisfied by)", "Num Rack Local Containers (satisfied by)",
            "Num Off Switch Containers (satisfied by)" };
    boolean odd = false;
    for (int i = 0; i < attemptMetrics.getLocalityStatistics().length; i++) {
      table.tr((odd = !odd) ? _ODD : _EVEN).td(containersType[i])
        .td(String.valueOf(attemptMetrics.getLocalityStatistics()[i][0]))
        .td(i == 0 ? "" : String.valueOf(attemptMetrics.getLocalityStatistics()[i][1]))
        .td(i <= 1 ? "" : String.valueOf(attemptMetrics.getLocalityStatistics()[i][2]))._();
    }
    table._();
    div._();
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
        DIV<Hamlet> pdiv = html._(InfoBlock.class).div(_INFO_WRAP);
        info("Application Attempt Overview").clear();
        info("Application Attempt Metrics")._(
          "Application Attempt Headroom : ", metrics == null ? "N/A" :
            metrics.getApplicationAttemptHeadroom());
        pdiv._();
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

  @Override
  protected void createTablesForAttemptMetrics(Block html) {
    createContainerLocalityTable(html);
    createResourceRequestsTable(html);
  }

  protected void generateOverview(ApplicationAttemptReport appAttemptReport,
      Collection<ContainerReport> containers, AppAttemptInfo appAttempt,
      String node) {

    String blacklistedNodes = "-";
    Set<String> nodes =
        getBlacklistedNodes(rm, getRMAppAttempt().getAppAttemptId());
    if (nodes != null) {
      if (!nodes.isEmpty()) {
        blacklistedNodes = StringUtils.join(nodes, ", ");
      }
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
          .getDiagnosticsInfo())._("Blacklisted Nodes:", blacklistedNodes);
  }

  public static Set<String> getBlacklistedNodes(ResourceManager rm,
      ApplicationAttemptId appid) {
    if (rm.getResourceScheduler() instanceof AbstractYarnScheduler) {
      AbstractYarnScheduler ayScheduler =
          (AbstractYarnScheduler) rm.getResourceScheduler();
      SchedulerApplicationAttempt attempt =
          ayScheduler.getApplicationAttempt(appid);
      if (attempt != null) {
        return attempt.getBlacklistedNodes();
      }
    }
    return null;
  }
}
