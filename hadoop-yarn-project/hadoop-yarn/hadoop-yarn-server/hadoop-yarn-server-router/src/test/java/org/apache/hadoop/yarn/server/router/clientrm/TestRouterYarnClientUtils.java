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
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for RouterYarnClientUtils.
 */
public class TestRouterYarnClientUtils {

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
    responses.add(getApplicationsResponse(1));
    responses.add(getApplicationsResponse(2));
    GetApplicationsResponse result = RouterYarnClientUtils.
            mergeApplications(responses, false);
    Assert.assertNotNull(result);
    Assert.assertEquals(2, result.getApplicationList().size());
  }

  private GetApplicationsResponse getApplicationsResponse(int value) {
    List<ApplicationReport> applications = new ArrayList<>();

    //Add managed application to list
    ApplicationId applicationId = ApplicationId.newInstance(1234,
            value);
    Resource resource = Resource.newInstance(1024, 1);
    ApplicationResourceUsageReport appResourceUsageReport =
            ApplicationResourceUsageReport.newInstance(
                    1, 2, resource, resource,
                    resource, null, 0.1f,
                    0.1f, null);

    ApplicationReport newApplicationReport = ApplicationReport.newInstance(
            applicationId, ApplicationAttemptId.newInstance(applicationId,
                    1),
            "user", "queue", "appname", "host",
            124, null, YarnApplicationState.RUNNING,
            "diagnostics", "url", 0, 0,
            0, FinalApplicationStatus.SUCCEEDED,
            appResourceUsageReport,
            "N/A", 0.53789f, "YARN",
            null);

    //Add unmanaged application to list
    ApplicationId applicationId2 = ApplicationId.newInstance(1234,
            value);
    ApplicationReport newApplicationReport2 = ApplicationReport.newInstance(
            applicationId2, ApplicationAttemptId.newInstance(applicationId,
                    1),
            "user", "queue", "appname", null, 124,
            null, YarnApplicationState.RUNNING,
            "diagnostics", "url", 0, 0,
            0, FinalApplicationStatus.SUCCEEDED,
            appResourceUsageReport, "N/A", 0.53789f,
            "YARN", null);

    applications.add(newApplicationReport);
    applications.add(newApplicationReport2);

    return GetApplicationsResponse.newInstance(applications);
  }
}
