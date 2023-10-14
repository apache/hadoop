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

import com.sun.jersey.api.client.Client;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodesInfo;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TR;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import com.google.inject.Inject;

import java.util.Date;

import static org.apache.hadoop.yarn.webapp.YarnWebParams.NODE_SC;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.NODE_LABEL;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.NODE_STATE;

/**
 * Nodes block for the Router Web UI.
 */
public class NodesBlock extends RouterBlock {

  private final Router router;

  @Inject
  NodesBlock(Router router, ViewContext ctx) {
    super(router, ctx);
    this.router = router;
  }

  @Override
  protected void render(Block html) {

    boolean isEnabled = isYarnFederationEnabled();

    // Get subClusterName
    String subClusterName = $(NODE_SC);
    String state = $(NODE_STATE);
    String nodeLabel = $(NODE_LABEL);

    // We will try to get the subClusterName.
    // If the subClusterName is not empty,
    // it means that we need to get the Node list of a subCluster.
    NodesInfo nodesInfo;
    if (subClusterName != null && !subClusterName.isEmpty() &&
        !ROUTER.equalsIgnoreCase(subClusterName)) {
      initSubClusterMetricsOverviewTable(html, subClusterName);
      nodesInfo = getSubClusterNodesInfo(subClusterName);
    } else {
      // Metrics Overview Table
      html.__(MetricsOverviewTable.class);
      nodesInfo = getYarnFederationNodesInfo(isEnabled);
    }

    // Initialize NodeInfo List
    initYarnFederationNodesOfCluster(nodesInfo, html, state, nodeLabel);
  }

  private NodesInfo getYarnFederationNodesInfo(boolean isEnabled) {
    Configuration config = this.router.getConfig();
    String webAddress;
    if (isEnabled) {
      webAddress = WebAppUtils.getRouterWebAppURLWithScheme(this.router.getConfig());
    } else {
      webAddress = WebAppUtils.getRMWebAppURLWithScheme(config);
    }
    return getSubClusterNodesInfoByWebAddress(webAddress);
  }

  private NodesInfo getSubClusterNodesInfo(String subCluster) {
    try {
      SubClusterId subClusterId = SubClusterId.newInstance(subCluster);
      FederationStateStoreFacade facade =
          FederationStateStoreFacade.getInstance(this.router.getConfig());
      SubClusterInfo subClusterInfo = facade.getSubCluster(subClusterId);

      if (subClusterInfo != null) {
        // Prepare webAddress
        String webAddress = subClusterInfo.getRMWebServiceAddress();
        String herfWebAppAddress;
        if (webAddress != null && !webAddress.isEmpty()) {
          herfWebAppAddress =
              WebAppUtils.getHttpSchemePrefix(this.router.getConfig()) + webAddress;
          return getSubClusterNodesInfoByWebAddress(herfWebAppAddress);
        }
      }
    } catch (Exception e) {
      LOG.error("get NodesInfo From SubCluster = {} error.", subCluster, e);
    }
    return null;
  }

  private NodesInfo getSubClusterNodesInfoByWebAddress(String webAddress) {
    Configuration conf = this.router.getConfig();
    Client client = RouterWebServiceUtil.createJerseyClient(conf);
    NodesInfo nodes = RouterWebServiceUtil
        .genericForward(webAddress, null, NodesInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.NODES, null, null, conf,
        client);
    client.destroy();
    return nodes;
  }

  private void initYarnFederationNodesOfCluster(NodesInfo nodesInfo, Block html,
      String filterState, String filterLabel) {
    TBODY<TABLE<Hamlet>> tbody = html.table("#nodes").thead().tr()
        .th(".nodelabels", "Node Labels")
        .th(".rack", "Rack")
        .th(".state", "Node State")
        .th(".nodeaddress", "Node Address")
        .th(".nodehttpaddress", "Node HTTP Address")
        .th(".lastHealthUpdate", "Last health-update")
        .th(".healthReport", "Health-report")
        .th(".containers", "Containers")
        .th(".mem", "Mem Used")
        .th(".mem", "Mem Avail")
        .th(".vcores", "VCores Used")
        .th(".vcores", "VCores Avail")
        .th(".nodeManagerVersion", "Version")
        .__().__().tbody();

    if (nodesInfo != null && CollectionUtils.isNotEmpty(nodesInfo.getNodes())) {
      for (NodeInfo info : nodesInfo.getNodes()) {
        if (filterState != null && !filterState.isEmpty() && !filterState.equals(info.getState())) {
          continue;
        }

        // Besides state, we need to filter label as well.
        if (!filterLabel.equals(RMNodeLabelsManager.ANY)) {
          if (filterLabel.isEmpty()) {
            // Empty label filter means only shows nodes without label
            if (!info.getNodeLabels().isEmpty()) {
              continue;
            }
          } else if (!info.getNodeLabels().contains(filterLabel)) {
            // Only nodes have given label can show on web page.
            continue;
          }
        }

        int usedMemory = (int) info.getUsedMemory();
        int availableMemory = (int) info.getAvailableMemory();
        TR<TBODY<TABLE<Hamlet>>> row = tbody.tr();
        row.td().__(StringUtils.join(",", info.getNodeLabels())).__();
        row.td().__(info.getRack()).__();
        row.td().__(info.getState()).__();
        row.td().__(info.getNodeId()).__();
        boolean isInactive = false;
        if (isInactive) {
          row.td().__(UNAVAILABLE).__();
        } else {
          String httpAddress = info.getNodeHTTPAddress();
          String herfWebAppAddress = "";
          if (httpAddress != null && !httpAddress.isEmpty()) {
            herfWebAppAddress =
                WebAppUtils.getHttpSchemePrefix(this.router.getConfig()) + httpAddress;
          }
          row.td().a(herfWebAppAddress, httpAddress).__();
        }

        row.td().br().$title(String.valueOf(info.getLastHealthUpdate())).__()
            .__(new Date(info.getLastHealthUpdate())).__()
            .td(info.getHealthReport())
            .td(String.valueOf(info.getNumContainers())).td().br()
            .$title(String.valueOf(usedMemory)).__()
            .__(StringUtils.byteDesc(usedMemory * BYTES_IN_MB)).__().td().br()
            .$title(String.valueOf(availableMemory)).__()
            .__(StringUtils.byteDesc(availableMemory * BYTES_IN_MB)).__()
            .td(String.valueOf(info.getUsedVirtualCores()))
            .td(String.valueOf(info.getAvailableVirtualCores()))
            .td(info.getVersion()).__();
      }
    }

    tbody.__().__();
  }
}
