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
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_STATE;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR_VALUE;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerInfo;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

/**
 * Shows application information specific to the fair
 * scheduler as part of the fair scheduler page.
 */
public class FairSchedulerAppsBlock extends HtmlBlock {
  final ConcurrentMap<ApplicationId, RMApp> apps;
  final FairSchedulerInfo fsinfo;
  final Configuration conf;
  final ResourceManager rm;
  @Inject
  public FairSchedulerAppsBlock(ResourceManager rm, ViewContext ctx,
      Configuration conf) {
    super(ctx);
    FairScheduler scheduler = (FairScheduler) rm.getResourceScheduler();
    fsinfo = new FairSchedulerInfo(scheduler);
    apps = new ConcurrentHashMap<ApplicationId, RMApp>();
    for (Map.Entry<ApplicationId, RMApp> entry : rm.getRMContext().getRMApps()
        .entrySet()) {
      if (!(RMAppState.NEW.equals(entry.getValue().getState())
          || RMAppState.NEW_SAVING.equals(entry.getValue().getState())
          || RMAppState.SUBMITTED.equals(entry.getValue().getState()))) {
        apps.put(entry.getKey(), entry.getValue());
      }
    }
    this.conf = conf;
    this.rm = rm;
  }
  
  @Override public void render(Block html) {
    TBODY<TABLE<Hamlet>> tbody = html.
      table("#apps").
        thead().
          tr().
            th(".id", "ID").
            th(".user", "User").
            th(".name", "Name").
            th(".type", "Application Type").
            th(".queue", "Queue").
            th(".fairshare", "Fair Share").
            th(".starttime", "StartTime").
            th(".finishtime", "FinishTime").
            th(".state", "State").
            th(".finalstatus", "FinalStatus").
            th(".runningcontainer", "Running Containers").
            th(".allocatedCpu", "Allocated CPU VCores").
            th(".allocatedMemory", "Allocated Memory MB").
            th(".progress", "Progress").
            th(".ui", "Tracking UI")._()._().
        tbody();
    Collection<YarnApplicationState> reqAppStates = null;
    String reqStateString = $(APP_STATE);
    if (reqStateString != null && !reqStateString.isEmpty()) {
      String[] appStateStrings = reqStateString.split(",");
      reqAppStates = new HashSet<YarnApplicationState>(appStateStrings.length);
      for(String stateString : appStateStrings) {
        reqAppStates.add(YarnApplicationState.valueOf(stateString));
      }
    }
    StringBuilder appsTableData = new StringBuilder("[\n");
    for (RMApp app : apps.values()) {
      if (reqAppStates != null && !reqAppStates.contains(app.createApplicationState())) {
        continue;
      }
      AppInfo appInfo = new AppInfo(rm, app, true, WebAppUtils.getHttpSchemePrefix(conf));
      String percent = StringUtils.format("%.1f", appInfo.getProgress());
      ApplicationAttemptId attemptId = app.getCurrentAppAttempt().getAppAttemptId();
      long fairShare = fsinfo.getAppFairShare(attemptId);
      if (fairShare == FairSchedulerInfo.INVALID_FAIR_SHARE) {
        // FairScheduler#applications don't have the entry. Skip it.
        continue;
      }
      appsTableData.append("[\"<a href='")
      .append(url("app", appInfo.getAppId())).append("'>")
      .append(appInfo.getAppId()).append("</a>\",\"")
      .append(StringEscapeUtils.escapeJavaScript(StringEscapeUtils.escapeHtml(
        appInfo.getUser()))).append("\",\"")
      .append(StringEscapeUtils.escapeJavaScript(StringEscapeUtils.escapeHtml(
        appInfo.getName()))).append("\",\"")
      .append(StringEscapeUtils.escapeJavaScript(StringEscapeUtils.escapeHtml(
        appInfo.getApplicationType()))).append("\",\"")
      .append(StringEscapeUtils.escapeJavaScript(StringEscapeUtils.escapeHtml(
        appInfo.getQueue()))).append("\",\"")
      .append(fairShare).append("\",\"")
      .append(appInfo.getStartTime()).append("\",\"")
      .append(appInfo.getFinishTime()).append("\",\"")
      .append(appInfo.getState()).append("\",\"")
      .append(appInfo.getFinalStatus()).append("\",\"")
      .append(appInfo.getRunningContainers() == -1 ? "N/A" : String
         .valueOf(appInfo.getRunningContainers())).append("\",\"")
      .append(appInfo.getAllocatedVCores() == -1 ? "N/A" : String
        .valueOf(appInfo.getAllocatedVCores())).append("\",\"")
      .append(appInfo.getAllocatedMB() == -1 ? "N/A" : String
        .valueOf(appInfo.getAllocatedMB())).append("\",\"")
      // Progress bar
      .append("<br title='").append(percent)
      .append("'> <div class='").append(C_PROGRESSBAR).append("' title='")
      .append(join(percent, '%')).append("'> ").append("<div class='")
      .append(C_PROGRESSBAR_VALUE).append("' style='")
      .append(join("width:", percent, '%')).append("'> </div> </div>")
      .append("\",\"<a href='");

      String trackingURL =
        !appInfo.isTrackingUrlReady()? "#" : appInfo.getTrackingUrlPretty();
      
      appsTableData.append(trackingURL).append("'>")
      .append(appInfo.getTrackingUI()).append("</a>\"],\n");

    }
    if(appsTableData.charAt(appsTableData.length() - 2) == ',') {
      appsTableData.delete(appsTableData.length()-2, appsTableData.length()-1);
    }
    appsTableData.append("]");
    html.script().$type("text/javascript").
    _("var appsTableData=" + appsTableData)._();

    tbody._()._();
  }
}
