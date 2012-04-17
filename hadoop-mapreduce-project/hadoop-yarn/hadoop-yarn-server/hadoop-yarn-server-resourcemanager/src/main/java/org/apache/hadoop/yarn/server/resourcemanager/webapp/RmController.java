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

import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebApp.QUEUE_NAME;
import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APPLICATION_ID;

import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.StringHelper;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.Controller;
import org.apache.hadoop.yarn.webapp.ResponseInfo;
import org.apache.hadoop.yarn.webapp.YarnWebParams;

import com.google.inject.Inject;

// Do NOT rename/refactor this to RMView as it will wreak havoc
// on Mac OS HFS as its case-insensitive!
public class RmController extends Controller {

  private ApplicationACLsManager aclsManager;

  @Inject
  RmController(RequestContext ctx, ApplicationACLsManager aclsManager) {
    super(ctx);
    this.aclsManager = aclsManager;
  }

  @Override public void index() {
    setTitle("Applications");
  }

  public void about() {
    setTitle("About the Cluster");
    render(AboutPage.class);
  }

  public void app() {
    String aid = $(APPLICATION_ID);
    if (aid.isEmpty()) {
      setStatus(HttpServletResponse.SC_BAD_REQUEST);
      setTitle("Bad request: requires application ID");
      return;
    }
    ApplicationId appID = Apps.toAppID(aid);
    RMContext context = getInstance(RMContext.class);
    RMApp rmApp = context.getRMApps().get(appID);
    if (rmApp == null) {
      // TODO: handle redirect to jobhistory server
      setStatus(HttpServletResponse.SC_NOT_FOUND);
      setTitle("Application not found: "+ aid);
      return;
    }
    AppInfo app = new AppInfo(rmApp, true);

    // Check for the authorization.
    String remoteUser = request().getRemoteUser();
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }
    if (callerUGI != null
        && !this.aclsManager.checkAccess(callerUGI,
            ApplicationAccessType.VIEW_APP, app.getUser(), appID)) {
      setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      setTitle("Unauthorized request for viewing application " + appID);
      renderText("You (User " + remoteUser
          + ") are not authorized to view the logs for application " + appID);
      return;
    }

    setTitle(join("Application ", aid));

    ResponseInfo info = info("Application Overview").
      _("User:", app.getUser()).
      _("Name:", app.getName()).
      _("State:", app.getState()).
      _("FinalStatus:", app.getFinalStatus()).
      _("Started:", Times.format(app.getStartTime())).
      _("Elapsed:", StringUtils.formatTime(
        Times.elapsed(app.getStartTime(), app.getFinishTime()))).
      _("Tracking URL:", !app.isTrackingUrlReady() ?
        "#" : app.getTrackingUrlPretty(), app.getTrackingUI()).
      _("Diagnostics:", app.getNote());
    if (app.amContainerLogsExist()) {
      info._("AM container logs:", app.getAMContainerLogs(), app.getAMContainerLogs());
    } else {
      info._("AM container logs:", "");
    }
    render(AppPage.class);
  }

  public void nodes() {
    render(NodesPage.class);
  }

  public void scheduler() {
    // limit applications to those in states relevant to scheduling
    set(YarnWebParams.APP_STATE, StringHelper.cjoin(
        RMAppState.NEW.toString(),
        RMAppState.SUBMITTED.toString(),
        RMAppState.ACCEPTED.toString(),
        RMAppState.RUNNING.toString()));

    ResourceManager rm = getInstance(ResourceManager.class);
    ResourceScheduler rs = rm.getResourceScheduler();
    if (rs == null || rs instanceof CapacityScheduler) {
      setTitle("Capacity Scheduler");
      render(CapacitySchedulerPage.class);
      return;
    }
    setTitle("Default Scheduler");
    render(DefaultSchedulerPage.class);
  }

  public void queue() {
    setTitle(join("Queue ", get(QUEUE_NAME, "unknown")));
  }

  public void submit() {
    setTitle("Application Submission Not Allowed");
  }

  public void json() {
    renderJSON(AppsList.class);
  }
}
