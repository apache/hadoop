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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
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

  private static final long BYTES_IN_MB = 1024 * 1024;

  private final Router router;

  @Inject
  FederationBlock(ViewContext ctx, Router router) {
    super(ctx);
    this.router = router;
  }

  @Override
  public void render(Block html) {
    Configuration conf = this.router.getConfig();
    boolean isEnabled = conf.getBoolean(
        YarnConfiguration.FEDERATION_ENABLED,
        YarnConfiguration.DEFAULT_FEDERATION_ENABLED);
    if (isEnabled) {
      setTitle("Federation");

      // Table header
      TBODY<TABLE<Hamlet>> tbody = html.table("#rms").thead().tr()
          .th(".id", "SubCluster")
          .th(".submittedA", "Applications Submitted*")
          .th(".pendingA", "Applications Pending*")
          .th(".runningA", "Applications Running*")
          .th(".failedA", "Applications Failed*")
          .th(".killedA", "Applications Killed*")
          .th(".completedA", "Applications Completed*")
          .th(".contAllocated", "Containers Allocated")
          .th(".contReserved", "Containers Reserved")
          .th(".contPending", "Containers Pending")
          .th(".availableM", "Available Memory")
          .th(".allocatedM", "Allocated Memory")
          .th(".reservedM", "Reserved Memory")
          .th(".totalM", "Total Memory")
          .th(".availableVC", "Available VirtualCores")
          .th(".allocatedVC", "Allocated VirtualCores")
          .th(".reservedVC", "Reserved VirtualCores")
          .th(".totalVC", "Total VirtualCores")
          .th(".activeN", "Active Nodes")
          .th(".lostN", "Lost Nodes")
          .th(".availableN", "Available Nodes")
          .th(".unhealtyN", "Unhealthy Nodes")
          .th(".rebootedN", "Rebooted Nodes")
          .th(".totalN", "Total Nodes")
          .__().__().tbody();

      try {
        // Binding to the FederationStateStore
        FederationStateStoreFacade facade =
            FederationStateStoreFacade.getInstance();
        Map<SubClusterId, SubClusterInfo> subClustersInfo =
            facade.getSubClusters(true);

        // Sort the SubClusters
        List<SubClusterInfo> subclusters = new ArrayList<>();
        subclusters.addAll(subClustersInfo.values());
        Comparator<? super SubClusterInfo> cmp =
            new Comparator<SubClusterInfo>() {
              @Override
              public int compare(SubClusterInfo o1, SubClusterInfo o2) {
                return o1.getSubClusterId().compareTo(o2.getSubClusterId());
              }
            };
        Collections.sort(subclusters, cmp);

        for (SubClusterInfo subcluster : subclusters) {
          SubClusterId subClusterId = subcluster.getSubClusterId();
          String webAppAddress = subcluster.getRMWebServiceAddress();
          String capability = subcluster.getCapability();
          ClusterMetricsInfo subClusterInfo = getClusterMetricsInfo(capability);

          // Building row per SubCluster
          tbody.tr().td().a("//" + webAppAddress, subClusterId.toString()).__()
              .td(Integer.toString(subClusterInfo.getAppsSubmitted()))
              .td(Integer.toString(subClusterInfo.getAppsPending()))
              .td(Integer.toString(subClusterInfo.getAppsRunning()))
              .td(Integer.toString(subClusterInfo.getAppsFailed()))
              .td(Integer.toString(subClusterInfo.getAppsKilled()))
              .td(Integer.toString(subClusterInfo.getAppsCompleted()))
              .td(Integer.toString(subClusterInfo.getContainersAllocated()))
              .td(Integer.toString(subClusterInfo.getReservedContainers()))
              .td(Integer.toString(subClusterInfo.getPendingContainers()))
              .td(StringUtils.byteDesc(
                  subClusterInfo.getAvailableMB() * BYTES_IN_MB))
              .td(StringUtils.byteDesc(
                  subClusterInfo.getAllocatedMB() * BYTES_IN_MB))
              .td(StringUtils.byteDesc(
                  subClusterInfo.getReservedMB() * BYTES_IN_MB))
              .td(StringUtils.byteDesc(
                  subClusterInfo.getTotalMB() * BYTES_IN_MB))
              .td(Long.toString(subClusterInfo.getAvailableVirtualCores()))
              .td(Long.toString(subClusterInfo.getAllocatedVirtualCores()))
              .td(Long.toString(subClusterInfo.getReservedVirtualCores()))
              .td(Long.toString(subClusterInfo.getTotalVirtualCores()))
              .td(Integer.toString(subClusterInfo.getActiveNodes()))
              .td(Integer.toString(subClusterInfo.getLostNodes()))
              .td(Integer.toString(subClusterInfo.getDecommissionedNodes()))
              .td(Integer.toString(subClusterInfo.getUnhealthyNodes()))
              .td(Integer.toString(subClusterInfo.getRebootedNodes()))
              .td(Integer.toString(subClusterInfo.getTotalNodes())).__();
        }
      } catch (YarnException e) {
        LOG.error("Cannot render ResourceManager", e);
      }

      tbody.__().__().div()
          .p().__("*The application counts are local per subcluster").__().__();
    } else {
      setTitle("Federation is not Enabled!");
    }
  }

  private static ClusterMetricsInfo getClusterMetricsInfo(String capability) {
    ClusterMetricsInfo clusterMetrics = null;
    try {
      JSONJAXBContext jc = new JSONJAXBContext(
          JSONConfiguration.mapped().rootUnwrapping(false).build(),
          ClusterMetricsInfo.class);
      JSONUnmarshaller unmarshaller = jc.createJSONUnmarshaller();
      clusterMetrics = unmarshaller.unmarshalFromJSON(
          new StringReader(capability), ClusterMetricsInfo.class);
    } catch (Exception e) {
      LOG.error("Cannot parse SubCluster info", e);
    }
    return clusterMetrics;
  }
}
