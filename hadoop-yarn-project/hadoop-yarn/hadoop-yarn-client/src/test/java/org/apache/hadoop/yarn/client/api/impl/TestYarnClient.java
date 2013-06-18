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
import java.util.HashMap;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
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
  
  private static class MockYarnClient extends YarnClientImpl {
    private ApplicationReport mockReport;

    public MockYarnClient() {
      super();
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
    public void stop() {
    }

    public void setYarnApplicationState(YarnApplicationState state) {
      when(mockReport.getYarnApplicationState()).thenReturn(
          YarnApplicationState.NEW, YarnApplicationState.NEW_SAVING,
          YarnApplicationState.NEW_SAVING, state);
    }
  }

}
