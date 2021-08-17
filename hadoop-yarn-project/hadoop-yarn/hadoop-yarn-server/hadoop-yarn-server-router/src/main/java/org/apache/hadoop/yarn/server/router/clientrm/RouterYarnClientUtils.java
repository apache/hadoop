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

import java.util.Collection;
import java.util.HashMap;
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
   * Merges a list of ApplicationReports grouping by ApplicationId.
   * Our current policy is to merge the application reports from the reachable
   * SubClusters.
   * @param responses a list of ApplicationResponse to merge
   * @param returnPartialResult if the merge ApplicationReports should contain
   * partial result or not
   * @return the merged ApplicationsResponse
   */
  public static GetApplicationsResponse mergeApplications(
      Collection<GetApplicationsResponse> responses,
      boolean returnPartialResult){
    Map<ApplicationId, ApplicationReport> federationAM = new HashMap<>();
    Map<ApplicationId, ApplicationReport> federationUAMSum = new HashMap<>();

    for (GetApplicationsResponse appResponse : responses){
      for (ApplicationReport appReport : appResponse.getApplicationList()){
        ApplicationId appId = appReport.getApplicationId();
        // Check if this ApplicationReport is an AM
        if (!appReport.isUnmanagedApp()) {
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
        } else if (federationAM.containsKey(appId)) {
          // Merge the current UAM with its own AM
          mergeAMWithUAM(federationAM.get(appId), appReport);
        } else if (federationUAMSum.containsKey(appId)) {
          // Merge the current UAM with its own UAM and update the list of UAM
          ApplicationReport mergedUAMReport =
              mergeUAMWithUAM(federationUAMSum.get(appId), appReport);
          federationUAMSum.put(appId, mergedUAMReport);
        } else {
          // Insert in the list of UAM
          federationUAMSum.put(appId, appReport);
        }
      }
    }
    // Check the remaining UAMs are depending or not from federation
    for (ApplicationReport appReport : federationUAMSum.values()) {
      if (mergeUamToReport(appReport.getName(), returnPartialResult)) {
        federationAM.put(appReport.getApplicationId(), appReport);
      }
    }

    return GetApplicationsResponse.newInstance(federationAM.values());
  }

  private static ApplicationReport mergeUAMWithUAM(ApplicationReport uam1,
      ApplicationReport uam2){
    uam1.setName(PARTIAL_REPORT + uam1.getApplicationId());
    mergeAMWithUAM(uam1, uam2);
    return uam1;
  }

  private static void mergeAMWithUAM(ApplicationReport am,
      ApplicationReport uam){
    ApplicationResourceUsageReport amResourceReport =
        am.getApplicationResourceUsageReport();

    ApplicationResourceUsageReport uamResourceReport =
        uam.getApplicationResourceUsageReport();

    amResourceReport.setNumUsedContainers(
        amResourceReport.getNumUsedContainers() +
            uamResourceReport.getNumUsedContainers());

    amResourceReport.setNumReservedContainers(
        amResourceReport.getNumReservedContainers() +
            uamResourceReport.getNumReservedContainers());

    amResourceReport.setUsedResources(Resources.add(
        amResourceReport.getUsedResources(),
        uamResourceReport.getUsedResources()));

    amResourceReport.setReservedResources(Resources.add(
        amResourceReport.getReservedResources(),
        uamResourceReport.getReservedResources()));

    amResourceReport.setNeededResources(Resources.add(
        amResourceReport.getNeededResources(),
        uamResourceReport.getNeededResources()));

    amResourceReport.setMemorySeconds(
        amResourceReport.getMemorySeconds() +
            uamResourceReport.getMemorySeconds());

    amResourceReport.setVcoreSeconds(
        amResourceReport.getVcoreSeconds() +
            uamResourceReport.getVcoreSeconds());

    amResourceReport.setQueueUsagePercentage(
        amResourceReport.getQueueUsagePercentage() +
            uamResourceReport.getQueueUsagePercentage());

    amResourceReport.setClusterUsagePercentage(
        amResourceReport.getClusterUsagePercentage() +
            uamResourceReport.getClusterUsagePercentage());

    am.setApplicationResourceUsageReport(amResourceReport);
  }

  /**
   * Returns whether or not to add an unmanaged application to the report.
   * @param appName Application Name
   * @param returnPartialResult if the merge ApplicationReports should contain
   * partial result or not
   */
  private static boolean mergeUamToReport(String appName,
      boolean returnPartialResult){
    if (returnPartialResult) {
      return true;
    }
    if (appName == null) {
      return false;
    }
    return !(appName.startsWith(UnmanagedApplicationManager.APP_NAME) ||
        appName.startsWith(PARTIAL_REPORT));
  }
}
