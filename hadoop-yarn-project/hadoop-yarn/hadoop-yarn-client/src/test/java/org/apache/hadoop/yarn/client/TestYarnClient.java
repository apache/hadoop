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

package org.apache.hadoop.yarn.client;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.util.Records;
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

    YarnClient client = new YarnClientImpl();
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
      ApplicationId applicationId = Records.newRecord(ApplicationId.class);
      applicationId.setClusterTimestamp(System.currentTimeMillis());
      applicationId.setId(i);
      when(context.getApplicationId()).thenReturn(applicationId);
      ((MockYarnClient) client).setYarnApplicationState(exitStates[i]);
      try {
        client.submitApplication(context);
      } catch (YarnRemoteException e) {
        Assert.fail("Exception is not expected.");
      } catch (IOException e) {
        Assert.fail("Exception is not expected.");
      }
      verify(((MockYarnClient) client).mockReport,times(4 * i + 4))
          .getYarnApplicationState();
    }

    client.stop();
  }

  private static class MockYarnClient extends YarnClientImpl {
    private ApplicationReport mockReport;

    public MockYarnClient() {
      super();
    }

    @Override
    public void start() {
      rmClient = mock(ClientRMProtocol.class);
      GetApplicationReportResponse mockResponse =
          mock(GetApplicationReportResponse.class);
      mockReport = mock(ApplicationReport.class);
      try{
        when(rmClient.getApplicationReport(any(
            GetApplicationReportRequest.class))).thenReturn(mockResponse);
      } catch (YarnRemoteException e) {
        Assert.fail("Exception is not expected.");
      } catch (IOException e) {
        Assert.fail("Exception is not expected.");
      }
      when(mockResponse.getApplicationReport()).thenReturn(mockReport);
    }

    @Override
    public void stop() {
    }

    public void setYarnApplicationState(YarnApplicationState state) {
      when(mockReport.getYarnApplicationState()).thenReturn(
          YarnApplicationState.NEW, YarnApplicationState.NEW_SAVING,
          YarnApplicationState.NEW_SAVING, state);
    }
  }

}
