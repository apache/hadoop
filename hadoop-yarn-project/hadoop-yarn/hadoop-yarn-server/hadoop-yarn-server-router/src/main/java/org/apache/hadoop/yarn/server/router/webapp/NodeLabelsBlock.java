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

import com.google.inject.Inject;
import com.sun.jersey.api.client.Client;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.PartitionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import static org.apache.hadoop.yarn.webapp.YarnWebParams.NODE_SC;

/**
 * Navigation block for the Router Web UI.
 */
public class NodeLabelsBlock extends RouterBlock {

  private Router router;

  @Inject
  public NodeLabelsBlock(Router router, ViewContext ctx) {
    super(router, ctx);
    this.router = router;
  }

  @Override
  protected void render(Block html) {
    boolean isEnabled = isYarnFederationEnabled();

    // Get subClusterName
    String subClusterName = $(NODE_SC);

    NodeLabelsInfo nodeLabelsInfo = null;
    if (StringUtils.isNotEmpty(subClusterName)) {
      nodeLabelsInfo = getSubClusterNodeLabelsInfo(subClusterName);
    } else {
      nodeLabelsInfo = getYarnFederationNodeLabelsInfo(isEnabled);
    }

    initYarnFederationNodeLabelsOfCluster(nodeLabelsInfo, html);
  }

  /**
   * Get NodeLabels Info based on SubCluster.
   * @return NodeLabelsInfo.
   */
  private NodeLabelsInfo getSubClusterNodeLabelsInfo(String subCluster) {
    try {
      SubClusterId subClusterId = SubClusterId.newInstance(subCluster);
      FederationStateStoreFacade facade =
          FederationStateStoreFacade.getInstance(router.getConfig());
      SubClusterInfo subClusterInfo = facade.getSubCluster(subClusterId);

      if (subClusterInfo != null) {
        // Prepare webAddress
        String webAddress = subClusterInfo.getRMWebServiceAddress();
        String herfWebAppAddress = "";
        if (webAddress != null && !webAddress.isEmpty()) {
          herfWebAppAddress =
              WebAppUtils.getHttpSchemePrefix(this.router.getConfig()) + webAddress;
          return getSubClusterNodeLabelsByWebAddress(herfWebAppAddress);
        }
      }
    } catch (Exception e) {
      LOG.error("get NodeLabelsInfo From SubCluster = {} error.", subCluster, e);
    }
    return null;
  }

  /**
   * We will obtain the NodeLabel information of multiple sub-clusters.
   *
   * If Federation mode is enabled, get the NodeLabels of multiple sub-clusters,
   * otherwise get the NodeLabels of the local cluster.
   *
   * @param isEnabled Whether to enable Federation mode,
   * true, Federation mode; false, Non-Federation mode.
   *
   * @return NodeLabelsInfo.
   */
  private NodeLabelsInfo getYarnFederationNodeLabelsInfo(boolean isEnabled) {
    Configuration config = this.router.getConfig();
    String webAddress;
    if (isEnabled) {
      webAddress = WebAppUtils.getRouterWebAppURLWithScheme(config);
    } else {
      webAddress = WebAppUtils.getRMWebAppURLWithScheme(config);
    }
    return getSubClusterNodeLabelsByWebAddress(webAddress);
  }

  /**
   * Get NodeLabels based on WebAddress.
   *
   * @param webAddress RM WebAddress.
   * @return NodeLabelsInfo.
   */
  private NodeLabelsInfo getSubClusterNodeLabelsByWebAddress(String webAddress) {
    Configuration conf = this.router.getConfig();
    Client client = RouterWebServiceUtil.createJerseyClient(conf);
    NodeLabelsInfo nodes = RouterWebServiceUtil
        .genericForward(webAddress, null, NodeLabelsInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.GET_RM_NODE_LABELS, null, null, conf,
        client);
    client.destroy();
    return nodes;
  }

  /**
   * Initialize the Router page based on NodeLabels.
   *
   * @param nodeLabelsInfo NodeLabelsInfo.
   * @param html html Block.
   */
  private void initYarnFederationNodeLabelsOfCluster(NodeLabelsInfo nodeLabelsInfo, Block html) {

    Hamlet.TBODY<Hamlet.TABLE<Hamlet>> tbody = html.table("#nodelabels").
        thead().
        tr().
        th(".name", "Label Name").
        th(".type", "Label Type").
        th(".numOfActiveNMs", "Num Of Active NMs").
        th(".totalResource", "Total Resource").
        __().__().
        tbody();

    if (nodeLabelsInfo != null) {
      for (NodeLabelInfo info : nodeLabelsInfo.getNodeLabelsInfo()) {
        Hamlet.TR<Hamlet.TBODY<Hamlet.TABLE<Hamlet>>> row =
            tbody.tr().td(info.getName().isEmpty() ?
            NodeLabel.DEFAULT_NODE_LABEL_PARTITION : info.getName());
        String type = (info.getExclusivity()) ? "Exclusive Partition" : "Non Exclusive Partition";
        row = row.td(type);
        int nActiveNMs = info.getActiveNMs();
        if (nActiveNMs > 0) {
          row = row.td().a(url("nodes",
              "?" + YarnWebParams.NODE_LABEL + "=" + info.getName()), String.valueOf(nActiveNMs))
              .__();
        } else {
          row = row.td(String.valueOf(nActiveNMs));
        }

        row = row.td(String.valueOf(nActiveNMs));
        PartitionInfo partitionInfo = info.getPartitionInfo();
        ResourceInfo available = partitionInfo.getResourceAvailable();
        row.td(available.toFormattedString()).__();
      }
    }

    tbody.__().__();
  }
}
