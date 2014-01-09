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

package org.apache.hadoop.yarn.api;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * <p>The protocol between clients and the <code>ApplicationHistoryService</code>
 * to get information on completed applications etc.</p> 
 */
public interface ApplicationHistoryProtocol {

  /**
   * <p>The interface used by clients to get a report of an Application from
   * the <code>HistoryServer</code>.</p>
   * 
   * <p>The client, via {@link GetApplicationReportRequest} provides the
   * {@link ApplicationId} of the application.</p>
   *
   * <p> In secure mode,the <code>HistoryServer</code> verifies access to the
   * application etc. before accepting the request.</p> 
   * 
   * <p>The <code>HistoryServer</code> responds with a 
   * {@link GetApplicationReportResponse} which includes the 
   * {@link ApplicationReport} for the application.</p>
   * 
   * <p>If the user does not have <code>VIEW_APP</code> access then the
   * following fields in the report will be set to stubbed values:
   * <ul>
   *   <li>host - set to "N/A"</li>
   *   <li>RPC port - set to -1</li>
   *   <li>client token - set to "N/A"</li>
   *   <li>diagnostics - set to "N/A"</li>
   *   <li>tracking URL - set to "N/A"</li>
   *   <li>original tracking URL - set to "N/A"</li>
   *   <li>resource usage report - all values are -1</li>
   * </ul></p>
   *
   * @param request request for an application report
   * @return application report 
   * @throws YarnException
   */
  public GetApplicationReportResponse getApplicationReport(
      GetApplicationReportRequest request) throws YarnException;

  /**
   * <p>The interface used by clients to get a report of all Applications
   * in the cluster from the <code>HistoryServer</code>.</p>
   * 
   * <p>The <code>HistoryServer</code> responds with a 
   * {@link GetApplicationsRequest} which includes the 
   * {@link ApplicationReport} for all the applications.</p>
   * 
   * <p>If the user does not have <code>VIEW_APP</code> access for an
   * application then the corresponding report will be filtered as
   * described in {@link #getApplicationReport(GetApplicationReportRequest)}.
   * </p>
   *
   * @param request request for report on all running applications
   * @return report on all running applications
   * @throws YarnException
   */
  public GetApplicationsResponse getApplications(
      GetApplicationsRequest request) throws YarnException;
  
  /**
   * <p>The interface used by clients to get delegation token, enabling the 
   * containers to be able to talk to the service using those tokens.
   * 
   *  <p> The <code>HistoryServer</code> responds with the delegation token
   *  {@link Token} that can be used by the client to speak to this
   *  service.
   * @param request request to get a delegation token for the client.
   * @return delegation token that can be used to talk to this service
   * @throws YarnException
   */
  public GetDelegationTokenResponse getDelegationToken(
      GetDelegationTokenRequest request) throws YarnException;
  
  /**
   * Renew an existing delegation token.
   * 
   * @param request the delegation token to be renewed.
   * @return the new expiry time for the delegation token.
   * @throws YarnException
   */
  @Private
  public RenewDelegationTokenResponse renewDelegationToken(
      RenewDelegationTokenRequest request) throws YarnException;

  /**
   * Cancel an existing delegation token.
   * 
   * @param request the delegation token to be cancelled.
   * @return an empty response.
   * @throws YarnException
   */
  @Private
  public CancelDelegationTokenResponse cancelDelegationToken(
      CancelDelegationTokenRequest request) throws YarnException;
}
