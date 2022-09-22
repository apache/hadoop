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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.api.json.JSONJAXBContext;
import com.sun.jersey.api.json.JSONUnmarshaller;

class FederationBlock extends HtmlBlock {

  private final Router router;

  @Inject
  FederationBlock(ViewContext ctx, Router router) {
    super(ctx);
    this.router = router;
  }

  @Override
  public void render(Block html) {

    Configuration conf = this.router.getConfig();
    boolean isEnabled = conf.getBoolean(YarnConfiguration.FEDERATION_ENABLED,
        YarnConfiguration.DEFAULT_FEDERATION_ENABLED);

    if (isEnabled) {

      List<Map<String, String>> lists = new ArrayList<>();

      // Table header
      TBODY<TABLE<Hamlet>> tbody =
          html.table("#rms").$class("display").$style("width:100%").thead().tr()
          .th(".id", "SubCluster")
          .th(".state", "State")
          .th(".lastStartTime", "LastStartTime")
          .th(".lastHeartBeat", "LastHeartBeat")
          .th(".resource", "Resource")
          .th(".nodes", "Nodes")
          .__().__().tbody();

      try {
        // Binding to the FederationStateStore
        FederationStateStoreFacade facade = FederationStateStoreFacade.getInstance();

        Map<SubClusterId, SubClusterInfo> subClustersInfo =
            facade.getSubClusters(true);

        // Sort the SubClusters
        List<SubClusterInfo> subclusters = new ArrayList<>();
        subclusters.addAll(subClustersInfo.values());
        Comparator<? super SubClusterInfo> cmp = Comparator.comparing(o -> o.getSubClusterId());
        Collections.sort(subclusters, cmp);

        for (SubClusterInfo subcluster : subclusters) {

          Map<String, String> subclusterMap = new HashMap<>();

          // Prepare subCluster
          SubClusterId subClusterId = subcluster.getSubClusterId();
          String anchorText = "";
          if (subClusterId != null) {
            anchorText = subClusterId.getId();
          }

          // Prepare WebAppAddress
          String webAppAddress = subcluster.getRMWebServiceAddress();
          String herfWebAppAddress = "";
          if (webAppAddress != null && !webAppAddress.isEmpty()) {
            herfWebAppAddress = "//" + webAppAddress;
          }

          // Prepare Capability
          String capability = subcluster.getCapability();
          ClusterMetricsInfo subClusterInfo = getClusterMetricsInfo(capability);

          // Prepare LastStartTime & LastHeartBeat
          String lastStartTime =
              DateFormatUtils.format(subcluster.getLastStartTime(), DATE_PATTERN);
          String lastHeartBeat =
              DateFormatUtils.format(subcluster.getLastHeartBeat(), DATE_PATTERN);

          // Prepare Resource
          long totalMB = subClusterInfo.getTotalMB();
          String totalMBDesc = StringUtils.byteDesc(totalMB * BYTES_IN_MB);
          long totalVirtualCores = subClusterInfo.getTotalVirtualCores();
          String resources = String.format("<Memory:%s, VCore:%s>", totalMBDesc, totalVirtualCores);

          // Prepare Node
          long totalNodes = subClusterInfo.getTotalNodes();
          long activeNodes = subClusterInfo.getActiveNodes();
          String nodes = String.format("<Total Nodes:%s, Active Nodes:%s>",
              totalNodes, activeNodes);

          // Prepare HTML Table
          tbody.tr().$id(subClusterId.toString())
              .td().$class("details-control").a(herfWebAppAddress, anchorText).__()
              .td(subcluster.getState().name())
              .td(lastStartTime)
              .td(lastHeartBeat)
              .td(resources)
              .td(nodes)
          .__();

          subclusterMap.put("subcluster", subClusterId.getId());
          subclusterMap.put("capability", capability);
          lists.add(subclusterMap);
        }
      } catch (Exception e) {
        LOG.error("Cannot render Router Federation.", e);
      }

      // Init FederationBlockTableJs
      initFederationBlockTableJs(html, lists);

      // Tips
      tbody.__().__().div().p().$style("color:red")
           .__("*The application counts are local per subcluster").__().__();
    } else {
      // When Federation is not enabled, user information needs to be prompted
      Hamlet.DIV<Hamlet> div = html.div("#div_id");
      div.p().$style("color:red").__("Federation is not Enabled.").__()
         .p().__()
         .p().__("We can refer to the following documents to configure Yarn Federation. ").__()
         .p().$style("color:blue").__()
          .a("https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/Federation.html",
          "Hadoop: YARN Federation").
         __();
    }
  }

  private static ClusterMetricsInfo getClusterMetricsInfo(String capability) {
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

  private static void initFederationBlockTableJs(Block html, List<Map<String, String>> jsonMap) {
    Gson gson =new Gson();
    html.script().$type("text/javascript").
         __("$(document).ready(function() { " +
          " var appsTableData = " + gson.toJson(jsonMap) + "; " +
          " var table = $('#rms').DataTable(); " +
          " $('#rms tbody').on('click', 'td.details-control', function () { " +
          " var tr = $(this).closest('tr');  " +
          " var row = table.row(tr); " +
          " if (row.child.isShown()) {  " +
          "  row.child.hide(); " +
          "  tr.removeClass('shown'); " +
          " } else { " +
          "  console.log(row.id());\n " +
          "  var capabilityArr = appsTableData.filter(item=>(item.subcluster === row.id())); " +
          "  var capabilityObj = JSON.parse(capabilityArr[0].capability).clusterMetrics; " +
          "  row.child(" +
          "     '<table>" +
          "          <tr>" +
          "              <td>" +
          "                  <h3>Application Metrics</h3>" +
          "                  appsSubmitted : '+capabilityObj.appsSubmitted+' </p>" +
          "                  appsCompleted : '+capabilityObj.appsCompleted+' </p>" +
          "                  appsPending   : '+capabilityObj.appsPending+' </p>" +
          "                  appsRunning   : '+capabilityObj.appsRunning+' </p>" +
          "                  appsFailed    : '+capabilityObj.appsFailed+' </p> " +
          "                  appsKilled    : '+capabilityObj.appsKilled+' </p>" +
          "              </td>" +
          "              <td>" +
          "                 <h3>Resource Metrics</h3>" +
          "                 <h4>Memory</h4>" +
          "                    totalMB     :  '+capabilityObj.totalMB+' </p>" +
          "                 reservedMB     :  '+capabilityObj.reservedMB+' </p>" +
          "                 availableMB    :  '+capabilityObj.availableMB+' </p>" +
          "                 allocatedMB    :  '+capabilityObj.allocatedMB+' </p>" +
          "                 pendingMB      :  '+capabilityObj.pendingMB+' </p>" +
          "                 <h4>VirtualCores</h4>" +
          "                 totalVirtualCores  :  '+capabilityObj.totalVirtualCores+' </p>" +
          "                 reservedVirtualCores  :  '+capabilityObj.reservedVirtualCores+' </p>" +
          "                 availableVirtualCores :  '+capabilityObj.availableVirtualCores+' </p>" +
          "                 allocatedVirtualCores :  '+capabilityObj.allocatedVirtualCores+' </p>" +
          "                 pendingVirtualCores   :  '+capabilityObj.pendingVirtualCores+' </p>" +
          "                 <h4>Containers</h4>" +
          "                 containersAllocated   :  '+capabilityObj.containersAllocated+' </p>" +
          "                 containersReserved    :  '+capabilityObj.containersReserved+' </p>" +
          "                 containersPending     :  '+capabilityObj.containersPending+' </p>" +
          "             </td>" +
          "             <td>" +
          "                <h3>Node Metrics</h3>" +
          "                totalNodes :  '+capabilityObj.totalNodes+' </p>" +
          "                lostNodes  :  '+capabilityObj.lostNodes+' </p>" +
          "                unhealthyNodes : '+capabilityObj.unhealthyNodes+' </p>" +
          "                decommissioningNodes : '+capabilityObj.decommissioningNodes+' </p>" +
          "                decommissionedNodes :  '+capabilityObj.decommissionedNodes+' </p>" +
          "                rebootedNodes : '+capabilityObj.rebootedNodes+' </p>" +
          "                activeNodes : '+capabilityObj.activeNodes+' </p>" +
          "                shutdownNodes : '+capabilityObj.shutdownNodes+' " +
          "             </td>" +
          "          </tr>" +
          "     </table>').show(); "+
          "   tr.addClass('shown'); " +
          " } " +
          " }); });").__();
  }
}
