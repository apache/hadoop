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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

/**
 * This class abstracts away how ApplicationReports are fetched.
 */
public class AppReportFetcher {
  private static final Log LOG = LogFactory.getLog(AppReportFetcher.class);
  private final Configuration conf;
  private final ApplicationClientProtocol applicationsManager;
  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  
  /**
   * Create a new Connection to the RM to fetch Application reports.
   * @param conf the conf to use to know where the RM is.
   */
  public AppReportFetcher(Configuration conf) {
    this.conf = conf;
    try {
      applicationsManager = ClientRMProxy.createRMProxy(conf,
          ApplicationClientProtocol.class);
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
  }
  
  /**
   * Just call directly into the applicationsManager given instead of creating
   * a remote connection to it.  This is mostly for when the Proxy is running
   * as part of the RM already.
   * @param conf the configuration to use
   * @param applicationsManager what to use to get the RM reports.
   */
  public AppReportFetcher(Configuration conf, ApplicationClientProtocol applicationsManager) {
    this.conf = conf;
    this.applicationsManager = applicationsManager;
  }
  
  /**
   * Get a report for the specified app.
   * @param appId the id of the application to get. 
   * @return the ApplicationReport for that app.
   * @throws YarnException on any error.
   * @throws IOException
   */
  public ApplicationReport getApplicationReport(ApplicationId appId)
  throws YarnException, IOException {
    GetApplicationReportRequest request = recordFactory
        .newRecordInstance(GetApplicationReportRequest.class);
    request.setApplicationId(appId);
    
    GetApplicationReportResponse response = applicationsManager
        .getApplicationReport(request);
    return response.getApplicationReport();
  }

  public void stop() {
    if (this.applicationsManager != null) {
      RPC.stopProxy(this.applicationsManager);
    }
  }
}
