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

import static org.apache.commons.lang.StringEscapeUtils.escapeHtml;
import static org.apache.commons.lang.StringEscapeUtils.escapeJavaScript;
import static org.apache.hadoop.yarn.webapp.view.Jsons._SEP;
import static org.apache.hadoop.yarn.webapp.view.Jsons.appendLink;
import static org.apache.hadoop.yarn.webapp.view.Jsons.appendProgressBar;
import static org.apache.hadoop.yarn.webapp.view.Jsons.appendSortable;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.Controller.RequestContext;
import org.apache.hadoop.yarn.webapp.ToJSON;
import org.apache.hadoop.yarn.webapp.view.JQueryUI.Render;

import com.google.inject.Inject;
import com.google.inject.servlet.RequestScoped;

// So we only need to do asm.getApplications once in a request
@RequestScoped
class AppsList implements ToJSON {
  final RequestContext rc;
  final ConcurrentMap<ApplicationId, RMApp> apps;
  Render rendering;

  @Inject AppsList(RequestContext ctx, RMContext rmContext) {
    rc = ctx;
    apps = rmContext.getRMApps();
  }

  void toDataTableArrays(Collection<RMAppState> requiredAppStates, PrintWriter out) {
    out.append('[');
    boolean first = true;
    for (RMApp app : apps.values()) {
      if (requiredAppStates != null &&
          !requiredAppStates.contains(app.getState())) {
        continue;
      }
      AppInfo appInfo = new AppInfo(app, true);
      String startTime = Times.format(appInfo.getStartTime());
      String finishTime = Times.format(appInfo.getFinishTime());
      if (first) {
        first = false;
      } else {
        out.append(",\n");
      }
      out.append("[\"");
      appendSortable(out, appInfo.getAppIdNum());
      appendLink(out, appInfo.getAppId(), rc.prefix(), "app",
          appInfo.getAppId()).append(_SEP).
          append(escapeHtml(appInfo.getUser())).append(_SEP).
          append(escapeJavaScript(escapeHtml(appInfo.getName()))).append(_SEP).
          append(escapeHtml(appInfo.getQueue())).append(_SEP);
      appendSortable(out, appInfo.getStartTime()).
          append(startTime).append(_SEP);
      appendSortable(out, appInfo.getFinishTime()).
          append(finishTime).append(_SEP).
          append(appInfo.getState()).append(_SEP).
          append(appInfo.getFinalStatus()).append(_SEP);
      appendProgressBar(out, appInfo.getProgress()).append(_SEP);
      appendLink(out, appInfo.getTrackingUI(), rc.prefix(),
          !appInfo.isTrackingUrlReady() ?
            "#" : appInfo.getTrackingUrlPretty()).
          append("\"]");
    }
    out.append(']');
  }

  @Override
  public void toJSON(PrintWriter out) {
    out.print("{\"aaData\":");
    toDataTableArrays(null, out);
    out.print("}\n");
  }
}
