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

package org.apache.hadoop.yarn.server.router.webapp;

import static org.apache.commons.text.StringEscapeUtils.escapeHtml4;
import static org.apache.commons.text.StringEscapeUtils.escapeEcmaScript;
import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR_VALUE;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

/**
 * Applications block for the Router Web UI.
 */
public class AppsBlock extends HtmlBlock {
  private final Router router;

  @Inject
  AppsBlock(Router router, ViewContext ctx) {
    super(ctx);
    this.router = router;
  }

  @Override
  protected void render(Block html) {
    // Get the applications from the Resource Managers
    Configuration conf = this.router.getConfig();
    String webAppAddress = WebAppUtils.getRouterWebAppURLWithScheme(conf);
    AppsInfo apps = RouterWebServiceUtil.genericForward(webAppAddress, null,
        AppsInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS, null, null);

    setTitle("Applications");

    TBODY<TABLE<Hamlet>> tbody = html.table("#apps").thead()
        .tr()
        .th(".id", "ID")
        .th(".user", "User")
        .th(".name", "Name")
        .th(".type", "Application Type")
        .th(".queue", "Queue")
        .th(".priority", "Application Priority")
        .th(".starttime", "StartTime")
        .th(".finishtime", "FinishTime")
        .th(".state", "State")
        .th(".finalstatus", "FinalStatus")
        .th(".progress", "Progress")
        .th(".ui", "Tracking UI")
        .__().__().tbody();

    // Render the applications
    StringBuilder appsTableData = new StringBuilder("[\n");
    for (AppInfo app : apps.getApps()) {
      try {

        String percent = String.format("%.1f", app.getProgress() * 100.0F);
        String trackingURL =
            app.getTrackingUrl() == null ? "#" : app.getTrackingUrl();
        // AppID numerical value parsed by parseHadoopID in yarn.dt.plugins.js
        appsTableData.append("[\"")
            .append("<a href='").append(trackingURL).append("'>")
            .append(app.getAppId()).append("</a>\",\"")
            .append(escape(app.getUser())).append("\",\"")
            .append(escape(app.getName())).append("\",\"")
            .append(escape(app.getApplicationType())).append("\",\"")
            .append(escape(app.getQueue())).append("\",\"")
            .append(String.valueOf(app.getPriority())).append("\",\"")
            .append(app.getStartTime()).append("\",\"")
            .append(app.getFinishTime()).append("\",\"")
            .append(app.getState()).append("\",\"")
            .append(app.getFinalStatus()).append("\",\"")
            // Progress bar
            .append("<br title='").append(percent).append("'> <div class='")
            .append(C_PROGRESSBAR).append("' title='")
            .append(join(percent, '%')).append("'> ").append("<div class='")
            .append(C_PROGRESSBAR_VALUE).append("' style='")
            .append(join("width:", percent, '%')).append("'> </div> </div>")
            // History link
            .append("\",\"<a href='").append(trackingURL).append("'>")
            .append("History").append("</a>");
        appsTableData.append("\"],\n");

      } catch (Exception e) {
        LOG.info(
            "Cannot add application {}: {}", app.getAppId(), e.getMessage());
      }
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

  private static String escape(String str) {
    return escapeEcmaScript(escapeHtml4(str));
  }
}
