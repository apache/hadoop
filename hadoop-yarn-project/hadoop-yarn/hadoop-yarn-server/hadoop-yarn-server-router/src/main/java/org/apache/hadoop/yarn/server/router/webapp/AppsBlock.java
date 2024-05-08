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
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_SC;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_STATE;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR_VALUE;

import com.sun.jersey.api.client.Client;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import com.google.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Applications block for the Router Web UI.
 */
public class AppsBlock extends RouterBlock {

  private final Router router;
  private final Configuration conf;

  @Inject
  AppsBlock(Router router, ViewContext ctx) {
    super(router, ctx);
    this.router = router;
    this.conf = this.router.getConfig();
  }

  @Override
  protected void render(Block html) {

    boolean isEnabled = isYarnFederationEnabled();

    // Get subClusterName
    String subClusterName = $(APP_SC);
    String reqState = $(APP_STATE);

    // We will try to get the subClusterName.
    // If the subClusterName is not empty,
    // it means that we need to get the Node list of a subCluster.
    AppsInfo appsInfo = null;
    if (subClusterName != null && !subClusterName.isEmpty()) {
      initSubClusterMetricsOverviewTable(html, subClusterName);
      appsInfo = getSubClusterAppsInfo(subClusterName, reqState);
    } else {
      // Metrics Overview Table
      html.__(MetricsOverviewTable.class);
      appsInfo = getYarnFederationAppsInfo(isEnabled);
    }

    initYarnFederationAppsOfCluster(appsInfo, html);
  }

  private static String escape(String str) {
    return escapeEcmaScript(escapeHtml4(str));
  }

  private AppsInfo getYarnFederationAppsInfo(boolean isEnabled) {
    String webAddress = null;
    if (isEnabled) {
      webAddress = WebAppUtils.getRouterWebAppURLWithScheme(this.conf);
    } else {
      webAddress = WebAppUtils.getRMWebAppURLWithScheme(this.conf);
    }
    return getSubClusterAppsInfoByWebAddress(webAddress, StringUtils.EMPTY);
  }

  private AppsInfo getSubClusterAppsInfo(String subCluster, String states) {
    try {
      SubClusterId subClusterId = SubClusterId.newInstance(subCluster);
      FederationStateStoreFacade facade = FederationStateStoreFacade.getInstance(this.conf);
      SubClusterInfo subClusterInfo = facade.getSubCluster(subClusterId);

      if (subClusterInfo != null) {
        // Prepare webAddress
        String webAddress = subClusterInfo.getRMWebServiceAddress();
        String herfWebAppAddress;
        if (webAddress != null && !webAddress.isEmpty()) {
          herfWebAppAddress = WebAppUtils.getHttpSchemePrefix(conf) + webAddress;
          return getSubClusterAppsInfoByWebAddress(herfWebAppAddress, states);
        }
      }
    } catch (Exception e) {
      LOG.error("get AppsInfo From SubCluster = {} error.", subCluster, e);
    }
    return null;
  }

  private AppsInfo getSubClusterAppsInfoByWebAddress(String webAddress, String states) {
    Client client = RouterWebServiceUtil.createJerseyClient(conf);
    Map<String, String[]> queryParams = new HashMap<>();
    if (StringUtils.isNotBlank(states)) {
      queryParams.put("states", new String[]{states});
    }
    AppsInfo apps = RouterWebServiceUtil
        .genericForward(webAddress, null, AppsInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS, null, queryParams, conf,
        client);
    client.destroy();
    return apps;
  }

  private void initYarnFederationAppsOfCluster(AppsInfo appsInfo, Block html) {

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

    if (appsInfo != null && CollectionUtils.isNotEmpty(appsInfo.getApps())) {

      List<String> appInfoList =
          appsInfo.getApps().stream().map(this::parseAppInfoData).collect(Collectors.toList());

      if (CollectionUtils.isNotEmpty(appInfoList)) {
        String formattedAppInfo = StringUtils.join(appInfoList, ",");
        appsTableData.append(formattedAppInfo);
      }
    }

    appsTableData.append("]");
    html.script().$type("text/javascript")
        .__("var appsTableData=" + appsTableData).__();

    tbody.__().__();
  }

  private String parseAppInfoData(AppInfo app) {
    StringBuilder appsDataBuilder = new StringBuilder();
    try {
      String percent = String.format("%.1f", app.getProgress() * 100.0F);
      String trackingURL = app.getTrackingUrl() == null ? "#" : app.getTrackingUrl();

      // AppID numerical value parsed by parseHadoopID in yarn.dt.plugins.js
      appsDataBuilder.append("[\"")
          .append("<a href='").append(trackingURL).append("'>")
          .append(app.getAppId()).append("</a>\",\"")
          .append(escape(app.getUser())).append("\",\"")
          .append(escape(app.getName())).append("\",\"")
          .append(escape(app.getApplicationType())).append("\",\"")
          .append(escape(app.getQueue())).append("\",\"")
          .append(app.getPriority()).append("\",\"")
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
      appsDataBuilder.append("\"]\n");

    } catch (Exception e) {
      LOG.warn("Cannot add application {}: {}", app.getAppId(), e.getMessage());
    }
    return appsDataBuilder.toString();
  }
}
