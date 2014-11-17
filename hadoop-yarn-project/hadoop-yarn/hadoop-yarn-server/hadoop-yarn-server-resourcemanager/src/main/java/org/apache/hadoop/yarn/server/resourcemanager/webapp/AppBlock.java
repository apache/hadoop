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
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.security.QueueACLsManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

public class AppBlock extends HtmlBlock {

  private ApplicationACLsManager aclsManager;
  private QueueACLsManager queueACLsManager;
  private final Configuration conf;

  @Inject
  AppBlock(ResourceManager rm, ViewContext ctx,
      ApplicationACLsManager aclsManager, QueueACLsManager queueACLsManager,
      Configuration conf) {
    super(ctx);
    this.aclsManager = aclsManager;
    this.queueACLsManager = queueACLsManager;
    this.conf = conf;
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

    RMContext context = getInstance(RMContext.class);
    RMApp rmApp = context.getRMApps().get(appID);
    if (rmApp == null) {
      puts("Application not found: "+ aid);
      return;
    }
    AppInfo app = new AppInfo(rmApp, true, WebAppUtils.getHttpSchemePrefix(conf));

    // Check for the authorization.
    String remoteUser = request().getRemoteUser();
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }
    if (callerUGI != null
        && !(this.aclsManager.checkAccess(callerUGI,
                ApplicationAccessType.VIEW_APP, app.getUser(), appID) ||
             this.queueACLsManager.checkAccess(callerUGI,
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
        ._("State:", app.getState())
        ._("FinalStatus:", app.getFinalStatus())
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
  }
}
