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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
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
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;

/**
 * <p>The protocol between clients and the <code>ResourceManager</code>
 * to submit/abort jobs and to get information on applications, cluster metrics,
 * nodes, queues and ACLs.</p> 
 */
@Public
@Stable
public interface ApplicationClientProtocol {
  /**
   * <p>The interface used by clients to obtain a new {@link ApplicationId} for 
   * submitting new applications.</p>
   * 
   * <p>The <code>ResourceManager</code> responds with a new, monotonically
   * increasing, {@link ApplicationId} which is used by the client to submit
   * a new application.</p>
   *
   * <p>The <code>ResourceManager</code> also responds with details such 
   * as maximum resource capabilities in the cluster as specified in
   * {@link GetNewApplicationResponse}.</p>
   *
   * @param request request to get a new <code>ApplicationId</code>
   * @return response containing the new <code>ApplicationId</code> to be used
   * to submit an application
   * @throws YarnException
   * @throws IOException
   * @see #submitApplication(SubmitApplicationRequest)
   */
  @Public
  @Stable
  @Idempotent
  public GetNewApplicationResponse getNewApplication(
      GetNewApplicationRequest request)
  throws YarnException, IOException;
  
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
   * an exception if it rejects the submission. However, this call needs to be
   * followed by {@link #getApplicationReport(GetApplicationReportRequest)}
   * to make sure that the application gets properly submitted - obtaining a
   * {@link SubmitApplicationResponse} from ResourceManager doesn't guarantee
   * that RM 'remembers' this application beyond failover or restart. If RM
   * failover or RM restart happens before ResourceManager saves the
   * application's state successfully, the subsequent
   * {@link #getApplicationReport(GetApplicationReportRequest)} will throw
   * a {@link ApplicationNotFoundException}. The Clients need to re-submit
   * the application with the same {@link ApplicationSubmissionContext} when
   * it encounters the {@link ApplicationNotFoundException} on the
   * {@link #getApplicationReport(GetApplicationReportRequest)} call.</p>
   * 
   * <p>During the submission process, it checks whether the application
   * already exists. If the application exists, it will simply return
   * SubmitApplicationResponse</p>
   *
   * <p> In secure mode,the <code>ResourceManager</code> verifies access to
   * queues etc. before accepting the application submission.</p>
   * 
   * @param request request to submit a new application
   * @return (empty) response on accepting the submission
   * @throws YarnException
   * @throws IOException
   * @throws InvalidResourceRequestException
   *           The exception is thrown when a {@link ResourceRequest} is out of
   *           the range of the configured lower and upper resource boundaries.
   * @see #getNewApplication(GetNewApplicationRequest)
   */
  @Public
  @Stable
  @Idempotent
  public SubmitApplicationResponse submitApplication(
      SubmitApplicationRequest request) 
  throws YarnException, IOException;
  
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
   * @param request request to abort a submitted application
   * @return <code>ResourceManager</code> returns an empty response
   *         on success and throws an exception on rejecting the request
   * @throws YarnException
   * @throws IOException
   * @see #getQueueUserAcls(GetQueueUserAclsInfoRequest) 
   */
  @Public
  @Stable
  @Idempotent
  public KillApplicationResponse forceKillApplication(
      KillApplicationRequest request) 
  throws YarnException, IOException;

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
   * @throws YarnException
   * @throws IOException
   */
  @Public
  @Stable
  @Idempotent
  public GetApplicationReportResponse getApplicationReport(
      GetApplicationReportRequest request) 
  throws YarnException, IOException;
  
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
   * @throws YarnException
   * @throws IOException
   */
  @Public
  @Stable
  @Idempotent
  public GetClusterMetricsResponse getClusterMetrics(
      GetClusterMetricsRequest request) 
  throws YarnException, IOException;
  
  /**
   * <p>The interface used by clients to get a report of Applications
   * matching the filters defined by {@link GetApplicationsRequest}
   * in the cluster from the <code>ResourceManager</code>.</p>
   * 
   * <p>The <code>ResourceManager</code> responds with a 
   * {@link GetApplicationsResponse} which includes the
   * {@link ApplicationReport} for the applications.</p>
   * 
   * <p>If the user does not have <code>VIEW_APP</code> access for an
   * application then the corresponding report will be filtered as
   * described in {@link #getApplicationReport(GetApplicationReportRequest)}.
   * </p>
   *
   * @param request request for report on applications
   * @return report on applications matching the given application types
   *           defined in the request
   * @throws YarnException
   * @throws IOException
   * @see GetApplicationsRequest
   */
  @Public
  @Stable
  @Idempotent
  public GetApplicationsResponse getApplications(
      GetApplicationsRequest request)
  throws YarnException, IOException;
  
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
   * @throws YarnException
   * @throws IOException
   */
  @Public
  @Stable
  @Idempotent
  public GetClusterNodesResponse getClusterNodes(
      GetClusterNodesRequest request) 
  throws YarnException, IOException;
  
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
   * @throws YarnException
   * @throws IOException
   */
  @Public
  @Stable
  @Idempotent
  public GetQueueInfoResponse getQueueInfo(
      GetQueueInfoRequest request) 
  throws YarnException, IOException;
  
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
   * @throws YarnException
   * @throws IOException
   */
  @Public
  @Stable
 @Idempotent
  public GetQueueUserAclsInfoResponse getQueueUserAcls(
      GetQueueUserAclsInfoRequest request) 
  throws YarnException, IOException;
  
  /**
   * <p>The interface used by clients to get delegation token, enabling the 
   * containers to be able to talk to the service using those tokens.
   * 
   *  <p> The <code>ResourceManager</code> responds with the delegation
   *  {@link Token} that can be used by the client to speak to this
   *  service.
   * @param request request to get a delegation token for the client.
   * @return delegation token that can be used to talk to this service
   * @throws YarnException
   * @throws IOException
   */
  @Public
  @Stable
  @Idempotent
  public GetDelegationTokenResponse getDelegationToken(
      GetDelegationTokenRequest request) 
  throws YarnException, IOException;
  
  /**
   * Renew an existing delegation {@link Token}.
   * 
   * @param request the delegation token to be renewed.
   * @return the new expiry time for the delegation token.
   * @throws YarnException
   * @throws IOException
   */
  @Private
  @Unstable
  @Idempotent
  public RenewDelegationTokenResponse renewDelegationToken(
      RenewDelegationTokenRequest request) throws YarnException,
      IOException;

  /**
   * Cancel an existing delegation {@link Token}.
   * 
   * @param request the delegation token to be cancelled.
   * @return an empty response.
   * @throws YarnException
   * @throws IOException
   */
  @Private
  @Unstable
  @Idempotent
  public CancelDelegationTokenResponse cancelDelegationToken(
      CancelDelegationTokenRequest request) throws YarnException,
      IOException;
  
  /**
   * Move an application to a new queue.
   * 
   * @param request the application ID and the target queue
   * @return an empty response
   * @throws YarnException
   * @throws IOException
   */
  @Public
  @Unstable
  @Idempotent
  public MoveApplicationAcrossQueuesResponse moveApplicationAcrossQueues(
      MoveApplicationAcrossQueuesRequest request) throws YarnException, IOException;

  /**
   * <p>
   * The interface used by clients to get a report of an Application Attempt
   * from the <code>ResourceManager</code> 
   * </p>
   * 
   * <p>
   * The client, via {@link GetApplicationAttemptReportRequest} provides the
   * {@link ApplicationAttemptId} of the application attempt.
   * </p>
   * 
   * <p>
   * In secure mode,the <code>ResourceManager</code> verifies access to
   * the method before accepting the request.
   * </p>
   * 
   * <p>
   * The <code>ResourceManager</code> responds with a
   * {@link GetApplicationAttemptReportResponse} which includes the
   * {@link ApplicationAttemptReport} for the application attempt.
   * </p>
   * 
   * <p>
   * If the user does not have <code>VIEW_APP</code> access then the following
   * fields in the report will be set to stubbed values:
   * <ul>
   * <li>host</li>
   * <li>RPC port</li>
   * <li>client token</li>
   * <li>diagnostics - set to "N/A"</li>
   * <li>tracking URL</li>
   * </ul>
   * </p>
   * 
   * @param request
   *          request for an application attempt report
   * @return application attempt report
   * @throws YarnException
   * @throws IOException
   */
  @Public
  @Unstable
  @Idempotent
  public GetApplicationAttemptReportResponse getApplicationAttemptReport(
      GetApplicationAttemptReportRequest request) throws YarnException,
      IOException;

  /**
   * <p>
   * The interface used by clients to get a report of all Application attempts
   * in the cluster from the <code>ResourceManager</code>
   * </p>
   * 
   * <p>
   * The <code>ResourceManager</code> responds with a
   * {@link GetApplicationAttemptsRequest} which includes the
   * {@link ApplicationAttemptReport} for all the applications attempts of a
   * specified application attempt.
   * </p>
   * 
   * <p>
   * If the user does not have <code>VIEW_APP</code> access for an application
   * then the corresponding report will be filtered as described in
   * {@link #getApplicationAttemptReport(GetApplicationAttemptReportRequest)}.
   * </p>
   * 
   * @param request
   *          request for reports on all application attempts of an application
   * @return reports on all application attempts of an application
   * @throws YarnException
   * @throws IOException
   */
  @Public
  @Unstable
  @Idempotent
  public GetApplicationAttemptsResponse getApplicationAttempts(
      GetApplicationAttemptsRequest request) throws YarnException, IOException;

  /**
   * <p>
   * The interface used by clients to get a report of an Container from the
   * <code>ResourceManager</code>
   * </p>
   * 
   * <p>
   * The client, via {@link GetContainerReportRequest} provides the
   * {@link ContainerId} of the container.
   * </p>
   * 
   * <p>
   * In secure mode,the <code>ResourceManager</code> verifies access to the
   * method before accepting the request.
   * </p>
   * 
   * <p>
   * The <code>ResourceManager</code> responds with a
   * {@link GetContainerReportResponse} which includes the
   * {@link ContainerReport} for the container.
   * </p>
   * 
   * @param request
   *          request for a container report
   * @return container report
   * @throws YarnException
   * @throws IOException
   */
  @Public
  @Unstable
  @Idempotent
  public GetContainerReportResponse getContainerReport(
      GetContainerReportRequest request) throws YarnException, IOException;

  /**
   * <p>
   * The interface used by clients to get a report of Containers for an
   * application attempt from the <code>ResourceManager</code>
   * </p>
   * 
   * <p>
   * The client, via {@link GetContainersRequest} provides the
   * {@link ApplicationAttemptId} of the application attempt.
   * </p>
   * 
   * <p>
   * In secure mode,the <code>ResourceManager</code> verifies access to the
   * method before accepting the request.
   * </p>
   * 
   * <p>
   * The <code>ResourceManager</code> responds with a
   * {@link GetContainersResponse} which includes a list of
   * {@link ContainerReport} for all the containers of a specific application
   * attempt.
   * </p>
   * 
   * @param request
   *          request for a list of container reports of an application attempt.
   * @return reports on all containers of an application attempt
   * @throws YarnException
   * @throws IOException
   */
  @Public
  @Unstable
  @Idempotent
  public GetContainersResponse getContainers(GetContainersRequest request)
      throws YarnException, IOException;

}
