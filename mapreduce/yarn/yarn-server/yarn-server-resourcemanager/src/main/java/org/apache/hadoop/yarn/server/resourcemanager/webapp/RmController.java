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

import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebApp.APP_ID;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebApp.QUEUE_NAME;
import static org.apache.hadoop.yarn.util.StringHelper.join;

import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.ApplicationsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.Controller;
import org.apache.hadoop.yarn.webapp.ResponseInfo;

import com.google.inject.Inject;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.AppContext;

// Do NOT rename/refactor this to RMView as it will wreak havoc
// on Mac OS HFS as its case-insensitive!
public class RmController extends Controller {
  @Inject RmController(RequestContext ctx) { super(ctx); }

  @Override public void index() {
    setTitle("Applications");
  }

  public void info() {
    setTitle("About the Cluster");
    long ts = ResourceManager.clusterTimeStamp;
    ResourceManager rm = getInstance(ResourceManager.class);
    info("Cluster overview").
      _("Cluster ID:", ts).
      _("ResourceManager state:", rm.getServiceState()).
      _("ResourceManager started on:", Times.format(ts)).
      _("ResourceManager version:", YarnVersionInfo.getBuildVersion()).
      _("Hadoop version:", VersionInfo.getBuildVersion());
    render(InfoPage.class);
  }

  public void app() {
    String aid = $(APP_ID);
    if (aid.isEmpty()) {
      setStatus(response().SC_BAD_REQUEST);
      setTitle("Bad request: requires application ID");
      return;
    }
    ApplicationId appID = Apps.toAppID(aid);
    ApplicationsManager asm = getInstance(ApplicationsManager.class);
    AppContext app = asm.getAppContext(appID);
    if (app == null) {
      // TODO: handle redirect to jobhistory server
      setStatus(response().SC_NOT_FOUND);
      setTitle("Application not found: "+ aid);
      return;
    }
    setTitle(join("Application ", aid));
    String trackingUrl = app.getMaster().getTrackingUrl();
    String ui = trackingUrl == null ? "UNASSIGNED" :
        (app.getFinishTime() == 0 ? "ApplicationMaster" : "JobHistory");

    ResponseInfo info = info("Application Overview").
      _("User:", app.getUser()).
      _("Name:", app.getName()).
      _("State:", app.getMaster().getState()).
      _("Started:", Times.format(app.getStartTime())).
      _("Elapsed:", StringUtils.formatTime(
        Times.elapsed(app.getStartTime(), app.getFinishTime()))).
      _("Tracking URL:", trackingUrl == null ? "#" :
        join("http://", trackingUrl), ui).
      _("Diagnostics:", app.getMaster().getDiagnostics());
    Container masterContainer = app.getMasterContainer();
    if (masterContainer != null) {
      String url = join("http://", masterContainer.getNodeHttpAddress(),
          "/yarn", "/containerlogs/",
          ConverterUtils.toString(masterContainer.getId()));
      info._("AM container logs:", url, url);
    } else {
      info._("AM container logs:", "AM not yet registered with RM");
    }
    render(InfoPage.class);
  }

  public void nodes() {
    render(NodesPage.class);
  }

  public void scheduler() {
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
