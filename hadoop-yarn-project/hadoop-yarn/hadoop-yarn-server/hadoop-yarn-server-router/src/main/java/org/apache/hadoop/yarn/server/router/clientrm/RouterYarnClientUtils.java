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
package org.apache.hadoop.yarn.server.router.clientrm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.server.uam.UnmanagedApplicationManager;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Util class for Router Yarn client API calls.
 */
public final class RouterYarnClientUtils {

  private final static String PARTIAL_REPORT = "Partial Report ";

  private RouterYarnClientUtils() {

  }

  public static GetClusterMetricsResponse merge(
      Collection<GetClusterMetricsResponse> responses) {
    YarnClusterMetrics tmp = YarnClusterMetrics.newInstance(0);
    for (GetClusterMetricsResponse response : responses) {
      YarnClusterMetrics metrics = response.getClusterMetrics();
      tmp.setNumNodeManagers(
          tmp.getNumNodeManagers() + metrics.getNumNodeManagers());
      tmp.setNumActiveNodeManagers(
          tmp.getNumActiveNodeManagers() + metrics.getNumActiveNodeManagers());
      tmp.setNumDecommissionedNodeManagers(
          tmp.getNumDecommissionedNodeManagers() + metrics
              .getNumDecommissionedNodeManagers());
      tmp.setNumLostNodeManagers(
          tmp.getNumLostNodeManagers() + metrics.getNumLostNodeManagers());
      tmp.setNumRebootedNodeManagers(tmp.getNumRebootedNodeManagers() + metrics
          .getNumRebootedNodeManagers());
      tmp.setNumUnhealthyNodeManagers(
          tmp.getNumUnhealthyNodeManagers() + metrics
              .getNumUnhealthyNodeManagers());
    }
    return GetClusterMetricsResponse.newInstance(tmp);
  }

  /**
   * Merges a list of ApplicationReports grouping by ApplicationId. Our current policy is
   * to merge the application reports from the reachable SubClusters.
   * @param responses a list of ApplicationResponse to merge
   * @param returnPartialResult if the merge ApplicationReports should contain partial
   * result or not
   * @return the merged ApplicationsResponse
   */
  public static GetApplicationsResponse mergeApplications(
          Collection<GetApplicationsResponse> responses, boolean returnPartialResult){
    Map<ApplicationId, ApplicationReport> federationAM = new HashMap<>();
    Map<ApplicationId, ApplicationReport> federationUAMSum = new HashMap<>();

    for(GetApplicationsResponse appResponse : responses){
      for(ApplicationReport appReport : appResponse.getApplicationList()){
        ApplicationId appId = appReport.getApplicationId();
        // Check if this ApplicationReport is an AM
        if (appReport.getHost() != null) {
          // Insert in the list of AM
          federationAM.put(appId, appReport);
          // Check if there are any UAM found before
          if (federationUAMSum.containsKey(appId)) {
            // Merge the current AM with the found UAM
            mergeAMWithUAM(appReport, federationUAMSum.get(appId));
            // Remove the sum of the UAMs
            federationUAMSum.remove(appId);
          }
          // This ApplicationReport is an UAM
        } else {
          if (federationAM.containsKey(appId)) {
            // Merge the current UAM with its own AM
            mergeAMWithUAM(federationAM.get(appId), appReport);
          } else if (federationUAMSum.containsKey(appId)) {
            // Merge the current UAM with its own UAM and update the list of UAM
            federationUAMSum.put(appId,
                    mergeUAMWithUAM(federationUAMSum.get(appId), appReport));
          } else {
            // Insert in the list of UAM
            federationUAMSum.put(appId, appReport);
          }
        }
      }
    }
    
    // Check the remaining UAMs are depending or not from federation
    for (ApplicationReport appReport : federationUAMSum.values()) {
      if (returnPartialResult || appReport.getName() != null
              && !(appReport.getName().startsWith(UnmanagedApplicationManager.APP_NAME)
              || appReport.getName().startsWith(PARTIAL_REPORT))) {
        federationAM.put(appReport.getApplicationId(), appReport);
      }
    }

    List<ApplicationReport> appList = new ArrayList<>(federationAM.values());
    return GetApplicationsResponse.newInstance(appList);
  }

  private static ApplicationReport mergeUAMWithUAM(ApplicationReport uam1, ApplicationReport uam2){
    uam1.setName(PARTIAL_REPORT + uam1.getApplicationId());
    mergeAMWithUAM(uam1, uam1);
    mergeAMWithUAM(uam1, uam2);
    return uam1;
  }

  private static void mergeAMWithUAM(ApplicationReport am, ApplicationReport uam){
    ApplicationResourceUsageReport resourceUsageReport = am.getApplicationResourceUsageReport();
    resourceUsageReport.setNumUsedContainers(
            resourceUsageReport.getNumUsedContainers() + uam.getApplicationResourceUsageReport().getNumUsedContainers());
    resourceUsageReport.setNumReservedContainers(
            resourceUsageReport.getNumReservedContainers() + uam.getApplicationResourceUsageReport().getNumReservedContainers());
    resourceUsageReport.setUsedResources(Resources.add(resourceUsageReport.getUsedResources(),
            uam.getApplicationResourceUsageReport().getUsedResources()));
    resourceUsageReport.setReservedResources(Resources.add(resourceUsageReport.getReservedResources(),
            uam.getApplicationResourceUsageReport().getReservedResources()));
    resourceUsageReport.setNeededResources(Resources.add(resourceUsageReport.getNeededResources(),
            uam.getApplicationResourceUsageReport().getNeededResources()));
    resourceUsageReport.setMemorySeconds(
            resourceUsageReport.getMemorySeconds() + uam.getApplicationResourceUsageReport().getMemorySeconds());
    resourceUsageReport.setVcoreSeconds(
            resourceUsageReport.getVcoreSeconds() + uam.getApplicationResourceUsageReport().getVcoreSeconds());
    resourceUsageReport.setQueueUsagePercentage(
            resourceUsageReport.getQueueUsagePercentage() + uam.getApplicationResourceUsageReport().getQueueUsagePercentage());
    resourceUsageReport.setClusterUsagePercentage(
            resourceUsageReport.getClusterUsagePercentage() + uam.getApplicationResourceUsageReport().getClusterUsagePercentage());
    am.getApplicationTags().addAll(uam.getApplicationTags());
  }
}
