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
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APPLICATION_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._EVEN;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._INFO_WRAP;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._ODD;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._TH;

import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

public class AppBlock extends HtmlBlock {

  private final Configuration conf;
  private final ResourceManager rm;

  @Inject
  AppBlock(ResourceManager rm, ViewContext ctx, Configuration conf) {
    super(ctx);
    this.conf = conf;
    this.rm = rm;
  }

  @Override
  protected void render(Block html) {
    String aid = $(APPLICATION_ID);
    if (aid.isEmpty()) {
      puts("Bad request: requires application ID");
      return;
    }

    ApplicationId appID = null;
    try {
      appID = Apps.toAppID(aid);
    } catch (Exception e) {
      puts("Invalid Application ID: " + aid);
      return;
    }

    RMContext context = this.rm.getRMContext();
    RMApp rmApp = context.getRMApps().get(appID);
    if (rmApp == null) {
      puts("Application not found: "+ aid);
      return;
    }
    AppInfo app =
        new AppInfo(rm, rmApp, true, WebAppUtils.getHttpSchemePrefix(conf));

    // Check for the authorization.
    String remoteUser = request().getRemoteUser();
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }
    if (callerUGI != null
        && !(this.rm.getApplicationACLsManager().checkAccess(callerUGI,
            ApplicationAccessType.VIEW_APP, app.getUser(), appID) || this.rm
            .getQueueACLsManager().checkAccess(callerUGI,
                QueueACL.ADMINISTER_QUEUE, app.getQueue()))) {
      puts("You (User " + remoteUser
          + ") are not authorized to view application " + appID);
      return;
    }

    setTitle(join("Application ", aid));

    RMAppMetrics appMerics = rmApp.getRMAppMetrics();
    
    // Get attempt metrics and fields, it is possible currentAttempt of RMApp is
    // null. In that case, we will assume resource preempted and number of Non
    // AM container preempted on that attempt is 0
    RMAppAttemptMetrics attemptMetrics;
    if (null == rmApp.getCurrentAppAttempt()) {
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
    
    info("Application Overview")
        ._("User:", app.getUser())
        ._("Name:", app.getName())
        ._("Application Type:", app.getApplicationType())
        ._("Application Tags:", app.getApplicationTags())
        ._("YarnApplicationState:", clarifyAppState(app.getState()))
        ._("FinalStatus Reported by AM:",
          clairfyAppFinalStatus(app.getFinalStatus()))
        ._("Started:", Times.format(app.getStartTime()))
        ._("Elapsed:",
            StringUtils.formatTime(Times.elapsed(app.getStartTime(),
                app.getFinishTime())))
        ._("Tracking URL:",
            !app.isTrackingUrlReady() ? "#" : app.getTrackingUrlPretty(),
            app.getTrackingUI())
        ._("Diagnostics:", app.getNote());

    DIV<Hamlet> pdiv = html.
        _(InfoBlock.class).
        div(_INFO_WRAP);
    info("Application Overview").clear();
    info("Application Metrics")
        ._("Total Resource Preempted:",
          appMerics.getResourcePreempted())
        ._("Total Number of Non-AM Containers Preempted:",
          String.valueOf(appMerics.getNumNonAMContainersPreempted()))
        ._("Total Number of AM Containers Preempted:",
          String.valueOf(appMerics.getNumAMContainersPreempted()))
        ._("Resource Preempted from Current Attempt:",
          attemptResourcePreempted)
        ._("Number of Non-AM Containers Preempted from Current Attempt:",
          attemptNumNonAMContainerPreempted)
        ._("Aggregate Resource Allocation:",
          String.format("%d MB-seconds, %d vcore-seconds", 
              appMerics.getMemorySeconds(), appMerics.getVcoreSeconds()));
    pdiv._();

    Collection<RMAppAttempt> attempts = rmApp.getAppAttempts().values();
    String amString =
        attempts.size() == 1 ? "ApplicationMaster" : "ApplicationMasters";

    DIV<Hamlet> div = html.
        _(InfoBlock.class).
        div(_INFO_WRAP);
    // MRAppMasters Table
    TABLE<DIV<Hamlet>> table = div.table("#app");
    table.
      tr().
        th(amString).
      _().
      tr().
        th(_TH, "Attempt Number").
        th(_TH, "Start Time").
        th(_TH, "Node").
        th(_TH, "Logs").
      _();

    boolean odd = false;
    for (RMAppAttempt attempt : attempts) {
      AppAttemptInfo attemptInfo = new AppAttemptInfo(attempt, app.getUser());
      table.tr((odd = !odd) ? _ODD : _EVEN).
        td(String.valueOf(attemptInfo.getAttemptId())).
        td(Times.format(attemptInfo.getStartTime())).
        td().a(".nodelink", url("//",
            attemptInfo.getNodeHttpAddress()),
            attemptInfo.getNodeHttpAddress())._().
        td().a(".logslink", url(attemptInfo.getLogsLink()), "logs")._().
      _();
    }

    table._();
    div._();

    createContainerLocalityTable(html, attemptMetrics);
    createResourceRequestsTable(html, app);
  }

  private void createContainerLocalityTable(Block html,
      RMAppAttemptMetrics attemptMetrics) {
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

  private void createResourceRequestsTable(Block html, AppInfo app) {
    TBODY<TABLE<Hamlet>> tbody =
        html.table("#ResourceRequests").thead().tr()
          .th(".priority", "Priority")
          .th(".resourceName", "Resource Name")
          .th(".totalResource", "Capability")
          .th(".numContainers", "Num Containers")
          .th(".relaxLocality", "Relax Locality")
          .th(".nodeLabelExpression", "Node Label Expression")._()._().tbody();

    Resource totalResource = Resource.newInstance(0, 0);
    if (app.getResourceRequests() != null) {
      for (ResourceRequest request : app.getResourceRequests()) {
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
