/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.webproxy;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.StringHelper;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

public class DefaultAppReportFetcher extends AppReportFetcher {

  private final ApplicationClientProtocol applicationsManager;
  private String rmAppPageUrlBase;

  /**
   * Create a new Connection to the RM/Application History Server
   * to fetch Application reports.
   *
   * @param conf the conf to use to know where the RM is.
   */
  public DefaultAppReportFetcher(Configuration conf) {
    super(conf);
    this.rmAppPageUrlBase =
        StringHelper.pjoin(WebAppUtils.getResolvedRMWebAppURLWithScheme(conf), "cluster", "app");
    try {
      this.applicationsManager = ClientRMProxy.createRMProxy(conf, ApplicationClientProtocol.class);
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
  }

  /**
   * Create a direct connection to RM instead of a remote connection when
   * the proxy is running as part of the RM. Also create a remote connection to
   * Application History Server if it is enabled.
   *
   * @param conf                the configuration to use
   * @param applicationsManager what to use to get the RM reports.
   */
  public DefaultAppReportFetcher(Configuration conf,
      ApplicationClientProtocol applicationsManager) {
    super(conf);
    this.rmAppPageUrlBase =
        StringHelper.pjoin(WebAppUtils.getResolvedRMWebAppURLWithScheme(conf), "cluster", "app");
    this.applicationsManager = applicationsManager;
  }

  /**
   * Get an application report for the specified application id from the RM and
   * fall back to the Application History Server if not found in RM.
   *
   * @param appId id of the application to get.
   * @return the ApplicationReport for the appId.
   * @throws YarnException on any error.
   * @throws IOException   connection exception.
   */
  @Override
  public FetchedAppReport getApplicationReport(ApplicationId appId)
      throws YarnException, IOException {
    return super.getApplicationReport(applicationsManager, appId);
  }

  public String getRmAppPageUrlBase(ApplicationId appId) throws YarnException, IOException {
    return this.rmAppPageUrlBase;
  }

  public void stop() {
    super.stop();
    if (this.applicationsManager != null) {
      RPC.stopProxy(this.applicationsManager);
    }
  }
}
