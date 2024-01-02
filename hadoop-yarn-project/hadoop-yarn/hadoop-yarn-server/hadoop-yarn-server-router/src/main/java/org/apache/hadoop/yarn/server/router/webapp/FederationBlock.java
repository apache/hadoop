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

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Date;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import com.google.inject.Inject;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.api.json.JSONJAXBContext;
import com.sun.jersey.api.json.JSONUnmarshaller;

class FederationBlock extends RouterBlock {

  private final Router router;

  @Inject
  FederationBlock(ViewContext ctx, Router router) {
    super(router, ctx);
    this.router = router;
  }

  @Override
  public void render(Block html) {

    boolean isEnabled = isYarnFederationEnabled();

    // init Html Page Federation
    initHtmlPageFederation(html, isEnabled);
  }

  /**
   * Parse the capability and obtain the metric information of the cluster.
   *
   * @param capability metric json obtained from RM.
   * @return ClusterMetricsInfo Object
   */
  protected ClusterMetricsInfo getClusterMetricsInfo(String capability) {
    try {
      if (capability != null && !capability.isEmpty()) {
        JSONJAXBContext jc = new JSONJAXBContext(
            JSONConfiguration.mapped().rootUnwrapping(false).build(), ClusterMetricsInfo.class);
        JSONUnmarshaller unmarShaller = jc.createJSONUnmarshaller();
        StringReader stringReader = new StringReader(capability);
        ClusterMetricsInfo clusterMetrics =
            unmarShaller.unmarshalFromJSON(stringReader, ClusterMetricsInfo.class);
        return clusterMetrics;
      }
    } catch (Exception e) {
      LOG.error("Cannot parse SubCluster info", e);
    }
    return null;
  }

  /**
   * Initialize the subCluster details JavaScript of the Federation page.
   *
   * This part of the js script will control to display or hide the detailed information
   * of the subCluster when the user clicks on the subClusterId.
   *
   * We will obtain the specific information of a SubCluster,
   * including the information of Applications, Resources, and Nodes.
   *
   * @param html html object
   * @param subClusterDetailMap subCluster Detail Map
   */
  private void initFederationSubClusterDetailTableJs(Block html,
      List<Map<String, String>> subClusterDetailMap) {
    Gson gson = new Gson();
    html.script().$type("text/javascript").
        __(" var scTableData = " + gson.toJson(subClusterDetailMap) + "; ")
        .__();
    html.script(root_url("static/federation/federation.js"));
  }

  /**
   * Initialize the Html page.
   *
   * @param html html object
   */
  private void initHtmlPageFederation(Block html, boolean isEnabled) {
    List<Map<String, String>> lists = new ArrayList<>();

    // Table header
    TBODY<TABLE<Hamlet>> tbody =
        html.table("#rms").$class("cell-border").$style("width:100%").thead().tr()
        .th(".id", "SubCluster")
        .th(".state", "State")
        .th(".lastStartTime", "LastStartTime")
        .th(".lastHeartBeat", "LastHeartBeat")
        .th(".resources", "Resources")
        .th(".nodes", "Nodes")
        .__().__().tbody();

    try {
      if (isEnabled) {
        initSubClusterPage(tbody, lists);
      } else {
        initLocalClusterPage(tbody, lists);
      }
    } catch (Exception e) {
      LOG.error("Cannot render Router Federation.", e);
    }

    // Init FederationBlockTableJs
    initFederationSubClusterDetailTableJs(html, lists);

    // Tips
    tbody.__().__().div().p().$style("color:red")
        .__("*The application counts are local per subcluster").__().__();
  }

  /**
   * Initialize the Federation page of the local-cluster.
   *
   * @param tbody HTML tbody.
   * @param lists subCluster page data list.
   */
  private void initLocalClusterPage(TBODY<TABLE<Hamlet>> tbody, List<Map<String, String>> lists) {
    Configuration config = this.router.getConfig();
    SubClusterInfo localCluster = getSubClusterInfoByLocalCluster(config);
    if (localCluster != null) {
      try {
        initSubClusterPageItem(tbody, localCluster, lists);
      } catch (Exception e) {
        LOG.error("init LocalCluster = {} page data error.", localCluster, e);
      }
    }
  }

  /**
   * Initialize the Federation page of the sub-cluster.
   *
   * @param tbody HTML tbody.
   * @param lists subCluster page data list.
   */
  private void initSubClusterPage(TBODY<TABLE<Hamlet>> tbody, List<Map<String, String>> lists) {
    // Sort the SubClusters
    List<SubClusterInfo> subClusters = getSubClusterInfoList();

    // Iterate through the sub-clusters and display data for each sub-cluster.
    // If a sub-cluster cannot display data, skip it.
    for (SubClusterInfo subCluster : subClusters) {
      try {
        initSubClusterPageItem(tbody, subCluster, lists);
      } catch (Exception e) {
        LOG.error("init subCluster = {} page data error.", subCluster, e);
      }
    }
  }

  /**
   * We will initialize the specific SubCluster's data within this method.
   *
   * @param tbody HTML TBody.
   * @param subClusterInfo Sub-cluster information.
   * @param lists Used to record data that needs to be displayed in JS.
   */
  private void initSubClusterPageItem(TBODY<TABLE<Hamlet>> tbody,
      SubClusterInfo subClusterInfo, List<Map<String, String>> lists) {

    Map<String, String> subClusterMap = new HashMap<>();

    // Prepare subCluster
    SubClusterId subClusterId = subClusterInfo.getSubClusterId();
    String subClusterIdText = subClusterId.getId();

    // Prepare WebAppAddress
    String webAppAddress = subClusterInfo.getRMWebServiceAddress();
    String herfWebAppAddress = "";
    if (webAppAddress != null && !webAppAddress.isEmpty()) {
      herfWebAppAddress =
          WebAppUtils.getHttpSchemePrefix(this.router.getConfig()) + webAppAddress;
    }

    // Prepare Capability
    String capability = subClusterInfo.getCapability();
    ClusterMetricsInfo subClusterMetricsInfo = getClusterMetricsInfo(capability);

    if (subClusterMetricsInfo == null) {
      return;
    }

    // Prepare LastStartTime & LastHeartBeat
    Date lastStartTime = new Date(subClusterInfo.getLastStartTime());
    Date lastHeartBeat = new Date(subClusterInfo.getLastHeartBeat());

    // Prepare Resource
    long totalMB = subClusterMetricsInfo.getTotalMB();
    String totalMBDesc = StringUtils.byteDesc(totalMB * BYTES_IN_MB);
    long totalVirtualCores = subClusterMetricsInfo.getTotalVirtualCores();
    String resources = String.format("<memory:%s, vCores:%s>", totalMBDesc, totalVirtualCores);

    // Prepare Node
    long totalNodes = subClusterMetricsInfo.getTotalNodes();
    long activeNodes = subClusterMetricsInfo.getActiveNodes();
    String nodes = String.format("<totalNodes:%s, activeNodes:%s>", totalNodes, activeNodes);

    // Prepare HTML Table
    String stateStyle = "color:#dc3545;font-weight:bolder";
    SubClusterState state = subClusterInfo.getState();
    if (SubClusterState.SC_RUNNING == state) {
      stateStyle = "color:#28a745;font-weight:bolder";
    }

    tbody.tr().$id(subClusterIdText)
        .td().$class("details-control").a(herfWebAppAddress, subClusterIdText).__()
        .td().$style(stateStyle).__(state.name()).__()
        .td().__(lastStartTime).__()
        .td().__(lastHeartBeat).__()
        .td(resources)
        .td(nodes)
        .__();

    // Formatted memory information
    long allocatedMB = subClusterMetricsInfo.getAllocatedMB();
    String allocatedMBDesc = StringUtils.byteDesc(allocatedMB * BYTES_IN_MB);
    long availableMB = subClusterMetricsInfo.getAvailableMB();
    String availableMBDesc = StringUtils.byteDesc(availableMB * BYTES_IN_MB);
    long pendingMB = subClusterMetricsInfo.getPendingMB();
    String pendingMBDesc = StringUtils.byteDesc(pendingMB * BYTES_IN_MB);
    long reservedMB = subClusterMetricsInfo.getReservedMB();
    String reservedMBDesc = StringUtils.byteDesc(reservedMB * BYTES_IN_MB);

    subClusterMap.put("totalmemory", totalMBDesc);
    subClusterMap.put("allocatedmemory", allocatedMBDesc);
    subClusterMap.put("availablememory", availableMBDesc);
    subClusterMap.put("pendingmemory", pendingMBDesc);
    subClusterMap.put("reservedmemory", reservedMBDesc);
    subClusterMap.put("subcluster", subClusterId.getId());
    subClusterMap.put("capability", capability);
    lists.add(subClusterMap);
  }

}
