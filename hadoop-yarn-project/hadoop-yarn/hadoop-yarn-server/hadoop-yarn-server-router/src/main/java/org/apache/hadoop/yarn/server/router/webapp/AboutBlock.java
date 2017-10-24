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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

/**
 * About block for the Router Web UI.
 */
public class AboutBlock extends HtmlBlock {

  private static final long BYTES_IN_MB = 1024 * 1024;

  private final Router router;

  @Inject
  AboutBlock(Router router, ViewContext ctx) {
    super(ctx);
    this.router = router;
  }

  @Override
  protected void render(Block html) {
    Configuration conf = this.router.getConfig();
    String webAppAddress = WebAppUtils.getRouterWebAppURLWithScheme(conf);

    ClusterMetricsInfo metrics = RouterWebServiceUtil.genericForward(
        webAppAddress, null, ClusterMetricsInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.METRICS, null, null);
    boolean isEnabled = conf.getBoolean(
        YarnConfiguration.FEDERATION_ENABLED,
        YarnConfiguration.DEFAULT_FEDERATION_ENABLED);
    info("Cluster Status").
        _("Federation Enabled", isEnabled).
        _("Applications Submitted", "N/A").
        _("Applications Pending", "N/A").
        _("Applications Running", "N/A").
        _("Applications Failed", "N/A").
        _("Applications Killed", "N/A").
        _("Applications Completed", "N/A").
        _("Containers Allocated", metrics.getContainersAllocated()).
        _("Containers Reserved", metrics.getReservedContainers()).
        _("Containers Pending", metrics.getPendingContainers()).
        _("Available Memory",
            StringUtils.byteDesc(metrics.getAvailableMB() * BYTES_IN_MB)).
        _("Allocated Memory",
            StringUtils.byteDesc(metrics.getAllocatedMB() * BYTES_IN_MB)).
        _("Reserved Memory",
            StringUtils.byteDesc(metrics.getReservedMB() * BYTES_IN_MB)).
        _("Total Memory",
            StringUtils.byteDesc(metrics.getTotalMB() * BYTES_IN_MB)).
        _("Available VirtualCores", metrics.getAvailableVirtualCores()).
        _("Allocated VirtualCores", metrics.getAllocatedVirtualCores()).
        _("Reserved VirtualCores", metrics.getReservedVirtualCores()).
        _("Total VirtualCores", metrics.getTotalVirtualCores()).
        _("Active Nodes", metrics.getActiveNodes()).
        _("Lost Nodes", metrics.getLostNodes()).
        _("Available Nodes", metrics.getDecommissionedNodes()).
        _("Unhealthy Nodes", metrics.getUnhealthyNodes()).
        _("Rebooted Nodes", metrics.getRebootedNodes()).
        _("Total Nodes", metrics.getTotalNodes());

    html._(InfoBlock.class);
  }
}