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
import java.util.List;

import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.server.uam.UnmanagedApplicationManager;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for RouterYarnClientUtils.
 */
public class TestRouterYarnClientUtils {

  private final static String PARTIAL_REPORT = "Partial Report ";

  @Test
  public void testClusterMetricsMerge() {
    ArrayList<GetClusterMetricsResponse> responses = new ArrayList<>();
    responses.add(getClusterMetricsResponse(1));
    responses.add(getClusterMetricsResponse(2));
    GetClusterMetricsResponse result = RouterYarnClientUtils.merge(responses);
    YarnClusterMetrics resultMetrics = result.getClusterMetrics();
    Assert.assertEquals(3, resultMetrics.getNumNodeManagers());
    Assert.assertEquals(3, resultMetrics.getNumActiveNodeManagers());
    Assert.assertEquals(3, resultMetrics.getNumDecommissionedNodeManagers());
    Assert.assertEquals(3, resultMetrics.getNumLostNodeManagers());
    Assert.assertEquals(3, resultMetrics.getNumRebootedNodeManagers());
    Assert.assertEquals(3, resultMetrics.getNumUnhealthyNodeManagers());
  }

  public GetClusterMetricsResponse getClusterMetricsResponse(int value) {
    YarnClusterMetrics metrics = YarnClusterMetrics.newInstance(value);
    metrics.setNumUnhealthyNodeManagers(value);
    metrics.setNumRebootedNodeManagers(value);
    metrics.setNumLostNodeManagers(value);
    metrics.setNumDecommissionedNodeManagers(value);
    metrics.setNumActiveNodeManagers(value);
    metrics.setNumNodeManagers(value);
    return GetClusterMetricsResponse.newInstance(metrics);
  }

  /**
   * This test validates the correctness of
   * RouterYarnClientUtils#mergeApplications.
   */
  @Test
  public void testMergeApplications() {
    ArrayList<GetApplicationsResponse> responses = new ArrayList<>();
    responses.add(getApplicationsResponse(1, false));
    responses.add(getApplicationsResponse(2, false));
    GetApplicationsResponse result = RouterYarnClientUtils.
        mergeApplications(responses, false);
    Assert.assertNotNull(result);
    Assert.assertEquals(2, result.getApplicationList().size());

    String appName1 = result.getApplicationList().get(0).getName();
    String appName2 = result.getApplicationList().get(1).getName();

    // Check that no Unmanaged applications are added to the result
    Assert.assertEquals(false,
        appName1.contains(UnmanagedApplicationManager.APP_NAME));
    Assert.assertEquals(false,
        appName2.contains(UnmanagedApplicationManager.APP_NAME));
  }

  /**
   * This test validates the correctness of
   * RouterYarnClientUtils#mergeApplications.
   */
  @Test
  public void testMergeUnmanagedApplications() {
    ArrayList<GetApplicationsResponse> responses = new ArrayList<>();
    responses.add(getApplicationsResponse(1, true));

    // Check response if partial results are enabled
    GetApplicationsResponse result = RouterYarnClientUtils.
        mergeApplications(responses, true);
    Assert.assertNotNull(result);
    Assert.assertEquals(1, result.getApplicationList().size());
    ApplicationReport appReport = result.getApplicationList().iterator().next();
    String appName = appReport.getName();
    Assert.assertTrue(appName.startsWith(PARTIAL_REPORT));

    // Check ApplicationResourceUsageReport merge
    ApplicationResourceUsageReport resourceUsageReport =
        appReport.getApplicationResourceUsageReport();

    Assert.assertEquals(2, resourceUsageReport.getNumUsedContainers());
    Assert.assertEquals(4, resourceUsageReport.getNumReservedContainers());

    // Check response if partial results are disabled
    result = RouterYarnClientUtils.
        mergeApplications(responses, false);
    Assert.assertNotNull(result);
    Assert.assertTrue(result.getApplicationList().isEmpty());
  }

  /**
   * This generates a GetApplicationsResponse with 2 applications with
   * same ApplicationId.
   * @param value Used as Id in ApplicationId
   * @param uamOnly If set to true, only unmanaged applications are added in
   *                response, else one managed and one unmanaged applications
   *                are added with same ApplicationId.
   * @return GetApplicationsResponse
   */
  private GetApplicationsResponse getApplicationsResponse(int value,
      boolean uamOnly) {
    String appName = uamOnly? UnmanagedApplicationManager.APP_NAME: "appname";
    List<ApplicationReport> applications = new ArrayList<>();

    // Create first application report. This is a managed app by default.
    // If uamOnly is true, this becomes unmanaged application.
    ApplicationId appId = ApplicationId.newInstance(1234, value);
    Resource resource = Resource.newInstance(1024, 1);
    ApplicationResourceUsageReport appResourceUsageReport =
        ApplicationResourceUsageReport.newInstance(
            1, 2, resource, resource,
            resource, null, 0.1f,
            0.1f, null);

    ApplicationReport appReport = ApplicationReport.newInstance(
        appId, ApplicationAttemptId.newInstance(appId, 1),
        "user", "queue", appName, "host",
        124, null, YarnApplicationState.RUNNING,
        "diagnostics", "url", 0, 0,
        0, FinalApplicationStatus.SUCCEEDED, appResourceUsageReport, "N/A",
        0.53789f, "YARN", null, null, uamOnly, null, null, null);

    // Create second application report. This is always unmanaged application.
    ApplicationId appId2 = ApplicationId.newInstance(1234, value);
    ApplicationReport appReport2 = ApplicationReport.newInstance(
        appId2, ApplicationAttemptId.newInstance(appId, 1),
        "user", "queue", UnmanagedApplicationManager.APP_NAME, "host",
        124, null, YarnApplicationState.RUNNING,
        "diagnostics", "url", 0, 0,
        0, FinalApplicationStatus.SUCCEEDED, appResourceUsageReport, "N/A",
        0.53789f, "YARN", null, null, true, null, null, null);

    applications.add(appReport);
    applications.add(appReport2);

    return GetApplicationsResponse.newInstance(applications);
  }
}
