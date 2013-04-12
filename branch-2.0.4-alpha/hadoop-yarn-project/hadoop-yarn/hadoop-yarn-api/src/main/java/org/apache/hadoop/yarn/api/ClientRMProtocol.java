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
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.DelegationToken;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;

/**
 * <p>The protocol between clients and the <code>ResourceManager</code>
 * to submit/abort jobs and to get information on applications, cluster metrics,
 * nodes, queues and ACLs.</p> 
 */
@Public
@Stable
public interface ClientRMProtocol {
  /**
   * <p>The interface used by clients to obtain a new {@link ApplicationId} for 
   * submitting new applications.</p>
   * 
   * <p>The <code>ResourceManager</code> responds with a new, monotonically
   * increasing, {@link ApplicationId} which is used by the client to submit
   * a new application.</p>
   *
   * <p>The <code>ResourceManager</code> also responds with details such 
   * as minimum and maximum resource capabilities in the cluster as specified in
   * {@link GetNewApplicationResponse}.</p>
   *
   * @param request request to get a new <code>ApplicationId</code>
   * @return response containing the new <code>ApplicationId</code> to be used
   * to submit an application
   * @throws YarnRemoteException
   * @see #submitApplication(SubmitApplicationRequest)
   */
  public GetNewApplicationResponse getNewApplication(
      GetNewApplicationRequest request)
  throws YarnRemoteException;
  
  /**
   * <p>The interface used by clients to submit a new application to the
   * <code>ResourceManager.</code></p>
   * 
   * <p>The client is required to provide details such as queue, 
   * {@link Resource} required to run the <code>ApplicationMaster</code>, 
   * the equivalent of {@link ContainerLaunchContext} for launching
   * the <code>ApplicationMaster</code> etc. via the 
   * {@link SubmitApplicationRequest}.</p>
   * 
   * <p>Currently the <code>ResourceManager</code> sends an immediate (empty) 
   * {@link SubmitApplicationResponse} on accepting the submission and throws 
   * an exception if it rejects the submission.</p>
   * 
   * <p> In secure mode,the <code>ResourceManager</code> verifies access to
   * queues etc. before accepting the application submission.</p>
   * 
   * @param request request to submit a new application
   * @return (empty) response on accepting the submission
   * @throws YarnRemoteException
   * @see #getNewApplication(GetNewApplicationRequest)
   */
  public SubmitApplicationResponse submitApplication(
      SubmitApplicationRequest request) 
  throws YarnRemoteException;
  
  /**
   * <p>The interface used by clients to request the 
   * <code>ResourceManager</code> to abort submitted application.</p>
   * 
   * <p>The client, via {@link KillApplicationRequest} provides the
   * {@link ApplicationId} of the application to be aborted.</p>
   * 
   * <p> In secure mode,the <code>ResourceManager</code> verifies access to the
   * application, queue etc. before terminating the application.</p> 
   * 
   * <p>Currently, the <code>ResourceManager</code> returns an empty response
   * on success and throws an exception on rejecting the request.</p>
   * 
   * @param request request to abort a submited application
   * @return <code>ResourceManager</code> returns an empty response
   *         on success and throws an exception on rejecting the request
   * @throws YarnRemoteException
   * @see #getQueueUserAcls(GetQueueUserAclsInfoRequest) 
   */
  public KillApplicationResponse forceKillApplication(
      KillApplicationRequest request) 
  throws YarnRemoteException;

  /**
   * <p>The interface used by clients to get a report of an Application from
   * the <code>ResourceManager</code>.</p>
   * 
   * <p>The client, via {@link GetApplicationReportRequest} provides the
   * {@link ApplicationId} of the application.</p>
   *
   * <p> In secure mode,the <code>ResourceManager</code> verifies access to the
   * application, queue etc. before accepting the request.</p> 
   * 
   * <p>The <code>ResourceManager</code> responds with a 
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
   * @throws YarnRemoteException
   */
  public GetApplicationReportResponse getApplicationReport(
      GetApplicationReportRequest request) 
  throws YarnRemoteException;
  
  /**
   * <p>The interface used by clients to get metrics about the cluster from
   * the <code>ResourceManager</code>.</p>
   * 
   * <p>The <code>ResourceManager</code> responds with a
   * {@link GetClusterMetricsResponse} which includes the 
   * {@link YarnClusterMetrics} with details such as number of current
   * nodes in the cluster.</p>
   * 
   * @param request request for cluster metrics
   * @return cluster metrics
   * @throws YarnRemoteException
   */
  public GetClusterMetricsResponse getClusterMetrics(
      GetClusterMetricsRequest request) 
  throws YarnRemoteException;
  
  /**
   * <p>The interface used by clients to get a report of all Applications
   * in the cluster from the <code>ResourceManager</code>.</p>
   * 
   * <p>The <code>ResourceManager</code> responds with a 
   * {@link GetAllApplicationsResponse} which includes the 
   * {@link ApplicationReport} for all the applications.</p>
   * 
   * <p>If the user does not have <code>VIEW_APP</code> access for an
   * application then the corresponding report will be filtered as
   * described in {@link #getApplicationReport(GetApplicationReportRequest)}.
   * </p>
   *
   * @param request request for report on all running applications
   * @return report on all running applications
   * @throws YarnRemoteException
   */
  public GetAllApplicationsResponse getAllApplications(
      GetAllApplicationsRequest request) 
  throws YarnRemoteException;
  
  /**
   * <p>The interface used by clients to get a report of all nodes
   * in the cluster from the <code>ResourceManager</code>.</p>
   * 
   * <p>The <code>ResourceManager</code> responds with a 
   * {@link GetClusterNodesResponse} which includes the 
   * {@link NodeReport} for all the nodes in the cluster.</p>
   * 
   * @param request request for report on all nodes
   * @return report on all nodes
   * @throws YarnRemoteException
   */
  public GetClusterNodesResponse getClusterNodes(
      GetClusterNodesRequest request) 
  throws YarnRemoteException;
  
  /**
   * <p>The interface used by clients to get information about <em>queues</em>
   * from the <code>ResourceManager</code>.</p>
   * 
   * <p>The client, via {@link GetQueueInfoRequest}, can ask for details such
   * as used/total resources, child queues, running applications etc.</p>
   *
   * <p> In secure mode,the <code>ResourceManager</code> verifies access before
   * providing the information.</p> 
   * 
   * @param request request to get queue information
   * @return queue information
   * @throws YarnRemoteException
   */
  public GetQueueInfoResponse getQueueInfo(
      GetQueueInfoRequest request) 
  throws YarnRemoteException;
  
  /**
   * <p>The interface used by clients to get information about <em>queue 
   * acls</em> for <em>current user</em> from the <code>ResourceManager</code>.
   * </p>
   * 
   * <p>The <code>ResourceManager</code> responds with queue acls for all
   * existing queues.</p>
   * 
   * @param request request to get queue acls for <em>current user</em>
   * @return queue acls for <em>current user</em>
   * @throws YarnRemoteException
   */
  public GetQueueUserAclsInfoResponse getQueueUserAcls(
      GetQueueUserAclsInfoRequest request) 
  throws YarnRemoteException;
  
  /**
   * <p>The interface used by clients to get delegation token, enabling the 
   * containers to be able to talk to the service using those tokens.
   * 
   *  <p> The <code>ResourceManager</code> responds with the delegation token
   *  {@link DelegationToken} that can be used by the client to speak to this
   *  service.
   * @param request request to get a delegation token for the client.
   * @return delegation token that can be used to talk to this service
   * @throws YarnRemoteException
   */
  public GetDelegationTokenResponse getDelegationToken(
      GetDelegationTokenRequest request) 
  throws YarnRemoteException;
  
  /**
   * Renew an existing delegation token.
   * 
   * @param request the delegation token to be renewed.
   * @return the new expiry time for the delegation token.
   * @throws YarnRemoteException
   */
  @Private
  public RenewDelegationTokenResponse renewDelegationToken(
      RenewDelegationTokenRequest request) throws YarnRemoteException;

  /**
   * Cancel an existing delegation token.
   * 
   * @param request the delegation token to be cancelled.
   * @return an empty response.
   * @throws YarnRemoteException
   */
  @Private
  public CancelDelegationTokenResponse cancelDelegationToken(
      CancelDelegationTokenRequest request) throws YarnRemoteException;
}
