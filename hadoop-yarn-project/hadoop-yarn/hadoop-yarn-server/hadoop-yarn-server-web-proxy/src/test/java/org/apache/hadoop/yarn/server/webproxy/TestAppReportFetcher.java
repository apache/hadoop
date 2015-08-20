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

package org.apache.hadoop.yarn.server.webproxy;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationHistoryProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestAppReportFetcher {

  static ApplicationHistoryProtocol historyManager;
  static Configuration conf = new Configuration();
  private static ApplicationClientProtocol appManager;
  private static AppReportFetcher fetcher;
  private final String appNotFoundExceptionMsg = "APP NOT FOUND";

  @After
  public void cleanUp() {
    historyManager = null;
    appManager = null;
    fetcher = null;
  }

  public void testHelper(boolean isAHSEnabled)
      throws YarnException, IOException {
    conf.setBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED,
        isAHSEnabled);
    appManager = Mockito.mock(ApplicationClientProtocol.class);
    Mockito.when(appManager
        .getApplicationReport(Mockito.any(GetApplicationReportRequest.class)))
        .thenThrow(new ApplicationNotFoundException(appNotFoundExceptionMsg));
    fetcher = new AppReportFetcherForTest(conf, appManager);
    ApplicationId appId = ApplicationId.newInstance(0,0);
    fetcher.getApplicationReport(appId);
  }

  @Test
  public void testFetchReportAHSEnabled() throws YarnException, IOException {
    testHelper(true);
    Mockito.verify(historyManager, Mockito.times(1))
    .getApplicationReport(Mockito.any(GetApplicationReportRequest.class));
    Mockito.verify(appManager, Mockito.times(1))
    .getApplicationReport(Mockito.any(GetApplicationReportRequest.class));
  }

  @Test
  public void testFetchReportAHSDisabled() throws YarnException, IOException {
    try {
      testHelper(false);
    } catch (ApplicationNotFoundException e) {
      Assert.assertTrue(e.getMessage() == appNotFoundExceptionMsg);
      /* RM will not know of the app and Application History Service is disabled
       * So we will not try to get the report from AHS and RM will throw
       * ApplicationNotFoundException
       */
    }
    Mockito.verify(appManager, Mockito.times(1))
    .getApplicationReport(Mockito.any(GetApplicationReportRequest.class));
    if (historyManager != null) {
      Assert.fail("HistoryManager should be null as AHS is disabled");
    }
  }

  static class AppReportFetcherForTest extends AppReportFetcher {

    public AppReportFetcherForTest(Configuration conf,
        ApplicationClientProtocol acp) {
      super(conf, acp);
    }

    @Override
    protected ApplicationHistoryProtocol getAHSProxy(Configuration conf)
        throws IOException
    {
      GetApplicationReportResponse resp = Mockito.
          mock(GetApplicationReportResponse.class);
      historyManager = Mockito.mock(ApplicationHistoryProtocol.class);
      try {
        Mockito.when(historyManager.getApplicationReport(Mockito
            .any(GetApplicationReportRequest.class))).thenReturn(resp);
      } catch (YarnException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      return historyManager;
    }
  }
}
