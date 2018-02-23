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
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationHistoryProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.AHSProxy;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

/**
 * This class abstracts away how ApplicationReports are fetched.
 */
public class AppReportFetcher {
  enum AppReportSource { RM, AHS }
  private final Configuration conf;
  private final ApplicationClientProtocol applicationsManager;
  private final ApplicationHistoryProtocol historyManager;
  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private boolean isAHSEnabled;

  /**
   * Create a new Connection to the RM/Application History Server
   * to fetch Application reports.
   * @param conf the conf to use to know where the RM is.
   */
  public AppReportFetcher(Configuration conf) {
    if (conf.getBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED,
        YarnConfiguration.DEFAULT_APPLICATION_HISTORY_ENABLED)) {
      isAHSEnabled = true;
    }
    this.conf = conf;
    try {
      applicationsManager = ClientRMProxy.createRMProxy(conf,
          ApplicationClientProtocol.class);
      if (isAHSEnabled) {
        historyManager = getAHSProxy(conf);
      } else {
        this.historyManager = null;
      }
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
  }
  
  /**
   * Create a direct connection to RM instead of a remote connection when
   * the proxy is running as part of the RM. Also create a remote connection to
   * Application History Server if it is enabled.
   * @param conf the configuration to use
   * @param applicationsManager what to use to get the RM reports.
   */
  public AppReportFetcher(Configuration conf, ApplicationClientProtocol applicationsManager) {
    if (conf.getBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED,
        YarnConfiguration.DEFAULT_APPLICATION_HISTORY_ENABLED)) {
      isAHSEnabled = true;
    }
    this.conf = conf;
    this.applicationsManager = applicationsManager;
    if (isAHSEnabled) {
      try {
        historyManager = getAHSProxy(conf);
      } catch (IOException e) {
        throw new YarnRuntimeException(e);
      }
    } else {
      this.historyManager = null;
    }
  }

  protected ApplicationHistoryProtocol getAHSProxy(Configuration configuration)
      throws IOException {
    return AHSProxy.createAHSProxy(configuration,
      ApplicationHistoryProtocol.class,
      configuration.getSocketAddr(YarnConfiguration.TIMELINE_SERVICE_ADDRESS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ADDRESS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_PORT));
  }

  /**
   * Get an application report for the specified application id from the RM and
   * fall back to the Application History Server if not found in RM.
   * @param appId id of the application to get.
   * @return the ApplicationReport for the appId.
   * @throws YarnException on any error.
   * @throws IOException
   */
  public FetchedAppReport getApplicationReport(ApplicationId appId)
  throws YarnException, IOException {
    GetApplicationReportRequest request = recordFactory
        .newRecordInstance(GetApplicationReportRequest.class);
    request.setApplicationId(appId);

    ApplicationReport appReport;
    FetchedAppReport fetchedAppReport;
    try {
      appReport = applicationsManager.
          getApplicationReport(request).getApplicationReport();
      fetchedAppReport = new FetchedAppReport(appReport, AppReportSource.RM);
    } catch (ApplicationNotFoundException e) {
      if (!isAHSEnabled) {
        // Just throw it as usual if historyService is not enabled.
        throw e;
      }
      //Fetch the application report from AHS
      appReport = historyManager.
          getApplicationReport(request).getApplicationReport();
      fetchedAppReport = new FetchedAppReport(appReport, AppReportSource.AHS);
    }
    return fetchedAppReport;
  }

  public void stop() {
    if (this.applicationsManager != null) {
      RPC.stopProxy(this.applicationsManager);
    }
    if (this.historyManager != null) {
      RPC.stopProxy(this.historyManager);
    }
  }

  /*
   * This class creates a bundle of the application report and the source from
   * where the the report was fetched. This allows the WebAppProxyServlet
   * to make decisions for the application report based on the source.
   */
  static class FetchedAppReport {
    private ApplicationReport appReport;
    private AppReportSource appReportSource;

    public FetchedAppReport(ApplicationReport appReport,
        AppReportSource appReportSource) {
      this.appReport = appReport;
      this.appReportSource = appReportSource;
    }

    public AppReportSource getAppReportSource() {
      return this.appReportSource;
    }

    public ApplicationReport getApplicationReport() {
      return this.appReport;
    }
  }
}
