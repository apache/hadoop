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
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodesInfo;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TR;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import com.google.inject.Inject;

import java.util.List;

import static org.apache.hadoop.yarn.webapp.YarnWebParams.NODE_SC;

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

    // If Yarn Federation is not enabled, the user needs to be prompted.
    initUserHelpInformationDiv(html, isEnabled);

    // Metrics Overview Table
    html.__(MetricsOverviewTable.class);

    // Get subClusterName
    String subClusterName = $(NODE_SC);

    NodesInfo nodesInfo = null;

    if (subClusterName != null && !subClusterName.isEmpty()) {
      // If subClusterName is not empty, we need to get NodeList of subCluster
      nodesInfo = getSubClusterNodesInfo(subClusterName);
    } else {
      nodesInfo = getYarnFederationNodesInfo(isEnabled);
    }

    if (nodesInfo != null && CollectionUtils.isNotEmpty(nodesInfo.getNodes())) {
      initYarnFederationNodesOfCluster(nodesInfo.getNodes(), html);
    }
  }

  private NodesInfo getYarnFederationNodesInfo(boolean isEnabled) {
    if (isEnabled) {
      String webAddress = WebAppUtils.getRouterWebAppURLWithScheme(this.router.getConfig());
      return getSubClusterNodesInfoByWebAddress(webAddress);
    }
    return null;
  }

  private NodesInfo getSubClusterNodesInfo(String subCluster) {
    try {
      SubClusterId subClusterId = SubClusterId.newInstance(subCluster);
      FederationStateStoreFacade facade = FederationStateStoreFacade.getInstance();
      SubClusterInfo subClusterInfo = facade.getSubCluster(subClusterId);

      if (subClusterInfo != null) {
        // Prepare webAddress
        String webAddress = subClusterInfo.getRMWebServiceAddress();
        String herfWebAppAddress = "";
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
    return nodes;
  }

  private void initYarnFederationNodesOfCluster(List<NodeInfo> nodeInfos, Block html) {
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

    if (nodeInfos != null && !nodeInfos.isEmpty()) {
      for (NodeInfo info : nodeInfos) {
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
            .__(Times.format(info.getLastHealthUpdate())).__()
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
