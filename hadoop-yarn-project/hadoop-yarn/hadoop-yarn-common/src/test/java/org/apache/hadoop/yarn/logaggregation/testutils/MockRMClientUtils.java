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

package org.apache.hadoop.yarn.logaggregation.testutils;

import java.util.List;

import org.apache.hadoop.test.MockitoUtil;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockRMClientUtils {
  public static ApplicationClientProtocol createMockRMClient(
          List<ApplicationId> finishedApplications,
          List<ApplicationId> runningApplications) throws Exception {
    final ApplicationClientProtocol mockProtocol =
        MockitoUtil.mockProtocol(ApplicationClientProtocol.class);
    if (finishedApplications != null && !finishedApplications.isEmpty()) {
      for (ApplicationId appId : finishedApplications) {
        GetApplicationReportRequest request = GetApplicationReportRequest.newInstance(appId);
        GetApplicationReportResponse response = createApplicationReportWithFinishedApplication();
        when(mockProtocol.getApplicationReport(request)).thenReturn(response);
      }
    }
    if (runningApplications != null && !runningApplications.isEmpty()) {
      for (ApplicationId appId : runningApplications) {
        GetApplicationReportRequest request = GetApplicationReportRequest.newInstance(appId);
        GetApplicationReportResponse response = createApplicationReportWithRunningApplication();
        when(mockProtocol.getApplicationReport(request)).thenReturn(response);
      }
    }
    return mockProtocol;
  }

  public static GetApplicationReportResponse createApplicationReportWithRunningApplication() {
    ApplicationReport report = mock(ApplicationReport.class);
    when(report.getYarnApplicationState()).thenReturn(
            YarnApplicationState.RUNNING);
    GetApplicationReportResponse response =
            mock(GetApplicationReportResponse.class);
    when(response.getApplicationReport()).thenReturn(report);
    return response;
  }

  public static GetApplicationReportResponse createApplicationReportWithFinishedApplication() {
    ApplicationReport report = mock(ApplicationReport.class);
    when(report.getYarnApplicationState()).thenReturn(YarnApplicationState.FINISHED);
    GetApplicationReportResponse response = mock(GetApplicationReportResponse.class);
    when(response.getApplicationReport()).thenReturn(report);
    return response;
  }
}
