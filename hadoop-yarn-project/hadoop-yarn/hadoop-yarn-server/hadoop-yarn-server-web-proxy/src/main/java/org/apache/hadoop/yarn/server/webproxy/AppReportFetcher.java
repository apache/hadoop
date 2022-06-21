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
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationHistoryProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.AHSProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.StringHelper;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

/**
 * This class abstracts away how ApplicationReports are fetched.
 */
public abstract class AppReportFetcher {

  protected enum AppReportSource {RM, AHS}

  private final Configuration conf;
  private ApplicationHistoryProtocol historyManager;
  private String ahsAppPageUrlBase;
  private final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);
  private boolean isAHSEnabled;

  public AppReportFetcher(Configuration conf) {
    this.conf = conf;
    if (conf.getBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED,
        YarnConfiguration.DEFAULT_APPLICATION_HISTORY_ENABLED)) {
      this.isAHSEnabled = true;
      this.ahsAppPageUrlBase =
          StringHelper.pjoin(WebAppUtils.getHttpSchemePrefix(conf)
                  + WebAppUtils.getAHSWebAppURLWithoutScheme(conf),
              "applicationhistory", "app");
    }
    try {
      if (this.isAHSEnabled) {
        this.historyManager = getAHSProxy(conf);
      } else {
        this.historyManager = null;
      }
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
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
  public abstract FetchedAppReport getApplicationReport(ApplicationId appId)
      throws YarnException, IOException;

  public abstract String getRmAppPageUrlBase(ApplicationId appId)
      throws IOException, YarnException;

  public String getAhsAppPageUrlBase() {
    return this.ahsAppPageUrlBase;
  }

  public abstract void stop();

  protected Configuration getConf() {
    return this.conf;
  }

  protected ApplicationHistoryProtocol getHistoryManager() {
    return this.historyManager;
  }

  protected RecordFactory getRecordFactory() {
    return this.recordFactory;
  }

  protected boolean isAHSEnabled() {
    return this.isAHSEnabled;
  }

  @VisibleForTesting
  public void setHistoryManager(
      ApplicationHistoryProtocol historyManager) {
    this.historyManager = historyManager;
  }

  /*
   * This class creates a bundle of the application report and the source from
   * where the the report was fetched. This allows the WebAppProxyServlet
   * to make decisions for the application report based on the source.
   */
  protected static class FetchedAppReport {
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
