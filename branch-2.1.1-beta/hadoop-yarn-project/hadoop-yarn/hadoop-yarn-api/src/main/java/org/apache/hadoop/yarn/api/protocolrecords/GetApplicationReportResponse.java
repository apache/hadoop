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

package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>The response sent by the <code>ResourceManager</code> to a client
 * requesting an application report.</p>
 * 
 * <p>The response includes an {@link ApplicationReport} which has details such 
 * as user, queue, name, host on which the <code>ApplicationMaster</code> is 
 * running, RPC port, tracking URL, diagnostics, start time etc.</p>
 * 
 * @see ApplicationClientProtocol#getApplicationReport(GetApplicationReportRequest)
 */
@Public
@Stable
public abstract class GetApplicationReportResponse {

  @Private
  @Unstable
  public static GetApplicationReportResponse newInstance(
      ApplicationReport ApplicationReport) {
    GetApplicationReportResponse response =
        Records.newRecord(GetApplicationReportResponse.class);
    response.setApplicationReport(ApplicationReport);
    return response;
  }

  /**
   * Get the <code>ApplicationReport</code> for the application.
   * @return <code>ApplicationReport</code> for the application
   */
  @Public
  @Stable
  public abstract ApplicationReport getApplicationReport();
  
  @Private
  @Unstable
  public abstract void setApplicationReport(ApplicationReport ApplicationReport);
}
