package org.apache.hadoop.yarn.logaggregation.testutils;

import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockRMClientUtils {
  public static ApplicationClientProtocol createMockRMClient(
          List<ApplicationId> finishedApplications,
          List<ApplicationId> runningApplications) throws Exception {
    final ApplicationClientProtocol mockProtocol = mock(ApplicationClientProtocol.class);
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
