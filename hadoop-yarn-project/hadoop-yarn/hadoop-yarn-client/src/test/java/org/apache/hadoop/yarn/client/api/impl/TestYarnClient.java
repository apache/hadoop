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

package org.apache.hadoop.yarn.client.api.impl;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Test;

public class TestYarnClient {

  @Test
  public void test() {
    // More to come later.
  }

  @Test
  public void testClientStop() {
    Configuration conf = new Configuration();
    ResourceManager rm = new ResourceManager();
    rm.init(conf);
    rm.start();

    YarnClient client = YarnClient.createYarnClient();
    client.init(conf);
    client.start();
    client.stop();
  }

  @Test (timeout = 30000)
  public void testSubmitApplication() {
    Configuration conf = new Configuration();
    conf.setLong(YarnConfiguration.YARN_CLIENT_APP_SUBMISSION_POLL_INTERVAL_MS,
        100); // speed up tests
    final YarnClient client = new MockYarnClient();
    client.init(conf);
    client.start();

    YarnApplicationState[] exitStates = new YarnApplicationState[]
        {
          YarnApplicationState.SUBMITTED,
          YarnApplicationState.ACCEPTED,
          YarnApplicationState.RUNNING,
          YarnApplicationState.FINISHED,
          YarnApplicationState.FAILED,
          YarnApplicationState.KILLED
        };
    for (int i = 0; i < exitStates.length; ++i) {
      ApplicationSubmissionContext context =
          mock(ApplicationSubmissionContext.class);
      ApplicationId applicationId = ApplicationId.newInstance(
          System.currentTimeMillis(), i);
      when(context.getApplicationId()).thenReturn(applicationId);
      ((MockYarnClient) client).setYarnApplicationState(exitStates[i]);
      try {
        client.submitApplication(context);
      } catch (YarnException e) {
        Assert.fail("Exception is not expected.");
      } catch (IOException e) {
        Assert.fail("Exception is not expected.");
      }
      verify(((MockYarnClient) client).mockReport,times(4 * i + 4))
          .getYarnApplicationState();
    }

    client.stop();
  }

  @Test(timeout = 30000)
  public void testApplicationType() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    MockRM rm = new MockRM();
    rm.start();
    RMApp app = rm.submitApp(2000);
    RMApp app1 =
        rm.submitApp(200, "name", "user",
          new HashMap<ApplicationAccessType, String>(), false, "default", -1,
          null, "MAPREDUCE");
    Assert.assertEquals("YARN", app.getApplicationType());
    Assert.assertEquals("MAPREDUCE", app1.getApplicationType());
    rm.stop();
  }

  @Test(timeout = 30000)
  public void testApplicationTypeLimit() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    MockRM rm = new MockRM();
    rm.start();
    RMApp app1 =
        rm.submitApp(200, "name", "user",
          new HashMap<ApplicationAccessType, String>(), false, "default", -1,
          null, "MAPREDUCE-LENGTH-IS-20");
    Assert.assertEquals("MAPREDUCE-LENGTH-IS-", app1.getApplicationType());
    rm.stop();
  }

  @Test (timeout = 10000)
  public void testGetApplications() throws YarnException, IOException {
    Configuration conf = new Configuration();
    final YarnClient client = new MockYarnClient();
    client.init(conf);
    client.start();

    List<ApplicationReport> expectedReports = ((MockYarnClient)client).getReports();

    Set<String> appTypes = new HashSet<String>();
    appTypes.add("YARN");
    appTypes.add("NON-YARN");

    List<ApplicationReport> reports = client.getApplications(appTypes);
    Assert.assertEquals(reports.size(), 2);
    Assert
        .assertTrue((reports.get(0).getApplicationType().equals("YARN") && reports
            .get(1).getApplicationType().equals("NON-YARN"))
            || (reports.get(1).getApplicationType().equals("YARN") && reports
                .get(0).getApplicationType().equals("NON-YARN")));
    for(ApplicationReport report : reports) {
      Assert.assertTrue(expectedReports.contains(report));
    }

    reports = client.getApplications();
    Assert.assertEquals(reports, expectedReports);

    client.stop();
  }

  private static class MockYarnClient extends YarnClientImpl {
    private ApplicationReport mockReport;
    private List<ApplicationReport> reports;

    public MockYarnClient() {
      super();
      reports = createAppReports();
    }

    @Override
    public void start() {
      rmClient = mock(ApplicationClientProtocol.class);
      GetApplicationReportResponse mockResponse =
          mock(GetApplicationReportResponse.class);
      mockReport = mock(ApplicationReport.class);
      try{
        when(rmClient.getApplicationReport(any(
            GetApplicationReportRequest.class))).thenReturn(mockResponse);
      } catch (YarnException e) {
        Assert.fail("Exception is not expected.");
      } catch (IOException e) {
        Assert.fail("Exception is not expected.");
      }
      when(mockResponse.getApplicationReport()).thenReturn(mockReport);
    }

    @Override
    public List<ApplicationReport> getApplications(
        Set<String> applicationTypes) throws YarnException, IOException {
      GetApplicationsRequest request =
          applicationTypes == null ? GetApplicationsRequest.newInstance()
              : GetApplicationsRequest.newInstance(applicationTypes);
      when(rmClient.getApplications(request))
          .thenReturn(
              getApplicationReports(reports,
                  request));
      GetApplicationsResponse response = rmClient.getApplications(request);
      return response.getApplicationList();
    }

    @Override
    public void stop() {
    }

    public void setYarnApplicationState(YarnApplicationState state) {
      when(mockReport.getYarnApplicationState()).thenReturn(
          YarnApplicationState.NEW, YarnApplicationState.NEW_SAVING,
          YarnApplicationState.NEW_SAVING, state);
    }

    public List<ApplicationReport> getReports() {
      return this.reports;
    }

    private List<ApplicationReport> createAppReports() {
      ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
      ApplicationReport newApplicationReport = ApplicationReport.newInstance(
          applicationId, ApplicationAttemptId.newInstance(applicationId, 1),
          "user", "queue", "appname", "host", 124, null,
          YarnApplicationState.FINISHED, "diagnostics", "url", 0, 0,
          FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.53789f, "YARN");
      List<ApplicationReport> applicationReports =
          new ArrayList<ApplicationReport>();
      applicationReports.add(newApplicationReport);

      ApplicationId applicationId2 = ApplicationId.newInstance(1234, 6);
      ApplicationReport newApplicationReport2 = ApplicationReport.newInstance(
          applicationId2, ApplicationAttemptId.newInstance(applicationId2, 2),
          "user2", "queue2", "appname2", "host2", 125, null,
          YarnApplicationState.FINISHED, "diagnostics2", "url2", 2, 2,
          FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.63789f, "NON-YARN");
      applicationReports.add(newApplicationReport2);

      ApplicationId applicationId3 = ApplicationId.newInstance(1234, 7);
      ApplicationReport newApplicationReport3 = ApplicationReport.newInstance(
          applicationId3, ApplicationAttemptId.newInstance(applicationId3, 3),
          "user3", "queue3", "appname3", "host3", 126, null,
          YarnApplicationState.FINISHED, "diagnostics3", "url3", 3, 3,
          FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.73789f, "MAPREDUCE");
      applicationReports.add(newApplicationReport3);
      return applicationReports;
    }

    private GetApplicationsResponse getApplicationReports(
        List<ApplicationReport> applicationReports,
        GetApplicationsRequest request) {

      List<ApplicationReport> appReports = new ArrayList<ApplicationReport>();
      Set<String> appTypes = request.getApplicationTypes();
      boolean bypassFilter = appTypes.isEmpty();

      for (ApplicationReport appReport : applicationReports) {
        if (!(bypassFilter || appTypes.contains(
            appReport.getApplicationType()))) {
          continue;
        }
        appReports.add(appReport);
      }
      GetApplicationsResponse response =
          GetApplicationsResponse.newInstance(appReports);
      return response;
    }
  }

}
