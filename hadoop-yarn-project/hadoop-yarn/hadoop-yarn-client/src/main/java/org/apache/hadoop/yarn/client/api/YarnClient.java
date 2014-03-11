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

package org.apache.hadoop.yarn.client.api;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.exceptions.ApplicationIdNotProvidedException;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;

@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class YarnClient extends AbstractService {

  /**
   * Create a new instance of YarnClient.
   */
  @Public
  public static YarnClient createYarnClient() {
    YarnClient client = new YarnClientImpl();
    return client;
  }

  @Private
  protected YarnClient(String name) {
    super(name);
  }

  /**
   * <p>
   * Obtain a {@link YarnClientApplication} for a new application,
   * which in turn contains the {@link ApplicationSubmissionContext} and
   * {@link org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse}
   * objects.
   * </p>
   *
   * @return {@link YarnClientApplication} built for a new application
   * @throws YarnException
   * @throws IOException
   */
  public abstract YarnClientApplication createApplication()
      throws YarnException, IOException;

  /**
   * <p>
   * Submit a new application to <code>YARN.</code> It is a blocking call - it
   * will not return {@link ApplicationId} until the submitted application is
   * submitted successfully and accepted by the ResourceManager.
   * </p>
   * 
   * <p>
   * Users should provide an {@link ApplicationId} as part of the parameter
   * {@link ApplicationSubmissionContext} when submitting a new application,
   * otherwise it will throw the {@link ApplicationIdNotProvidedException}.
   * </p>
   *
   * <p>This internally calls {@link ApplicationClientProtocol#submitApplication
   * (SubmitApplicationRequest)}, and after that, it internally invokes
   * {@link ApplicationClientProtocol#getApplicationReport
   * (GetApplicationReportRequest)} and waits till it can make sure that the
   * application gets properly submitted. If RM fails over or RM restart
   * happens before ResourceManager saves the application's state,
   * {@link ApplicationClientProtocol
   * #getApplicationReport(GetApplicationReportRequest)} will throw
   * the {@link ApplicationNotFoundException}. This API automatically resubmits
   * the application with the same {@link ApplicationSubmissionContext} when it
   * catches the {@link ApplicationNotFoundException}</p>
   *
   * @param appContext
   *          {@link ApplicationSubmissionContext} containing all the details
   *          needed to submit a new application
   * @return {@link ApplicationId} of the accepted application
   * @throws YarnException
   * @throws IOException
   * @see #createApplication()
   */
  public abstract ApplicationId submitApplication(
      ApplicationSubmissionContext appContext) throws YarnException,
      IOException;

  /**
   * <p>
   * Kill an application identified by given ID.
   * </p>
   * 
   * @param applicationId
   *          {@link ApplicationId} of the application that needs to be killed
   * @throws YarnException
   *           in case of errors or if YARN rejects the request due to
   *           access-control restrictions.
   * @throws IOException
   * @see #getQueueAclsInfo()
   */
  public abstract void killApplication(ApplicationId applicationId) throws YarnException,
      IOException;

  /**
   * <p>
   * Get a report of the given Application.
   * </p>
   * 
   * <p>
   * In secure mode, <code>YARN</code> verifies access to the application, queue
   * etc. before accepting the request.
   * </p>
   * 
   * <p>
   * If the user does not have <code>VIEW_APP</code> access then the following
   * fields in the report will be set to stubbed values:
   * <ul>
   * <li>host - set to "N/A"</li>
   * <li>RPC port - set to -1</li>
   * <li>client token - set to "N/A"</li>
   * <li>diagnostics - set to "N/A"</li>
   * <li>tracking URL - set to "N/A"</li>
   * <li>original tracking URL - set to "N/A"</li>
   * <li>resource usage report - all values are -1</li>
   * </ul>
   * </p>
   * 
   * @param appId
   *          {@link ApplicationId} of the application that needs a report
   * @return application report
   * @throws YarnException
   * @throws IOException
   */
  public abstract ApplicationReport getApplicationReport(ApplicationId appId)
      throws YarnException, IOException;

  /**
   * Get the AMRM token of the application.
   * <p/>
   * The AMRM token is required for AM to RM scheduling operations. For 
   * managed Application Masters Yarn takes care of injecting it. For unmanaged
   * Applications Masters, the token must be obtained via this method and set
   * in the {@link org.apache.hadoop.security.UserGroupInformation} of the
   * current user.
   * <p/>
   * The AMRM token will be returned only if all the following conditions are
   * met:
   * <li>
   *   <ul>the requester is the owner of the ApplicationMaster</ul>
   *   <ul>the application master is an unmanaged ApplicationMaster</ul>
   *   <ul>the application master is in ACCEPTED state</ul>
   * </li>
   * Else this method returns NULL.
   *
   * @param appId {@link ApplicationId} of the application to get the AMRM token
   * @return the AMRM token if available
   * @throws YarnException
   * @throws IOException
   */
  public abstract org.apache.hadoop.security.token.Token<AMRMTokenIdentifier>
      getAMRMToken(ApplicationId appId) throws YarnException, IOException;

  /**
   * <p>
   * Get a report (ApplicationReport) of all Applications in the cluster.
   * </p>
   *
   * <p>
   * If the user does not have <code>VIEW_APP</code> access for an application
   * then the corresponding report will be filtered as described in
   * {@link #getApplicationReport(ApplicationId)}.
   * </p>
   *
   * @return a list of reports of all running applications
   * @throws YarnException
   * @throws IOException
   */
  public abstract List<ApplicationReport> getApplications()
      throws YarnException, IOException;

  /**
   * <p>
   * Get a report (ApplicationReport) of Applications
   * matching the given application types in the cluster.
   * </p>
   *
   * <p>
   * If the user does not have <code>VIEW_APP</code> access for an application
   * then the corresponding report will be filtered as described in
   * {@link #getApplicationReport(ApplicationId)}.
   * </p>
   *
   * @param applicationTypes
   * @return a list of reports of applications
   * @throws YarnException
   * @throws IOException
   */
  public abstract List<ApplicationReport> getApplications(
      Set<String> applicationTypes) throws YarnException, IOException;

  /**
   * <p>
   * Get a report (ApplicationReport) of Applications matching the given
   * application states in the cluster.
   * </p>
   *
   * <p>
   * If the user does not have <code>VIEW_APP</code> access for an application
   * then the corresponding report will be filtered as described in
   * {@link #getApplicationReport(ApplicationId)}.
   * </p>
   *
   * @param applicationStates
   * @return a list of reports of applications
   * @throws YarnException
   * @throws IOException
   */
  public abstract List<ApplicationReport>
      getApplications(EnumSet<YarnApplicationState> applicationStates)
          throws YarnException, IOException;

  /**
   * <p>
   * Get a report (ApplicationReport) of Applications matching the given
   * application types and application states in the cluster.
   * </p>
   *
   * <p>
   * If the user does not have <code>VIEW_APP</code> access for an application
   * then the corresponding report will be filtered as described in
   * {@link #getApplicationReport(ApplicationId)}.
   * </p>
   *
   * @param applicationTypes
   * @param applicationStates
   * @return a list of reports of applications
   * @throws YarnException
   * @throws IOException
   */
  public abstract List<ApplicationReport> getApplications(
      Set<String> applicationTypes,
      EnumSet<YarnApplicationState> applicationStates) throws YarnException,
      IOException;

  /**
   * <p>
   * Get metrics ({@link YarnClusterMetrics}) about the cluster.
   * </p>
   * 
   * @return cluster metrics
   * @throws YarnException
   * @throws IOException
   */
  public abstract YarnClusterMetrics getYarnClusterMetrics() throws YarnException,
      IOException;

  /**
   * <p>
   * Get a report of nodes ({@link NodeReport}) in the cluster.
   * </p>
   * 
   * @param states The {@link NodeState}s to filter on. If no filter states are
   *          given, nodes in all states will be returned.
   * @return A list of node reports
   * @throws YarnException
   * @throws IOException
   */
  public abstract List<NodeReport> getNodeReports(NodeState... states)
      throws YarnException, IOException;

  /**
   * <p>
   * Get a delegation token so as to be able to talk to YARN using those tokens.
   * 
   * @param renewer
   *          Address of the renewer who can renew these tokens when needed by
   *          securely talking to YARN.
   * @return a delegation token ({@link Token}) that can be used to
   *         talk to YARN
   * @throws YarnException
   * @throws IOException
   */
  public abstract Token getRMDelegationToken(Text renewer)
      throws YarnException, IOException;

  /**
   * <p>
   * Get information ({@link QueueInfo}) about a given <em>queue</em>.
   * </p>
   * 
   * @param queueName
   *          Name of the queue whose information is needed
   * @return queue information
   * @throws YarnException
   *           in case of errors or if YARN rejects the request due to
   *           access-control restrictions.
   * @throws IOException
   */
  public abstract QueueInfo getQueueInfo(String queueName) throws YarnException,
      IOException;

  /**
   * <p>
   * Get information ({@link QueueInfo}) about all queues, recursively if there
   * is a hierarchy
   * </p>
   * 
   * @return a list of queue-information for all queues
   * @throws YarnException
   * @throws IOException
   */
  public abstract List<QueueInfo> getAllQueues() throws YarnException, IOException;

  /**
   * <p>
   * Get information ({@link QueueInfo}) about top level queues.
   * </p>
   * 
   * @return a list of queue-information for all the top-level queues
   * @throws YarnException
   * @throws IOException
   */
  public abstract List<QueueInfo> getRootQueueInfos() throws YarnException, IOException;

  /**
   * <p>
   * Get information ({@link QueueInfo}) about all the immediate children queues
   * of the given queue
   * </p>
   * 
   * @param parent
   *          Name of the queue whose child-queues' information is needed
   * @return a list of queue-information for all queues who are direct children
   *         of the given parent queue.
   * @throws YarnException
   * @throws IOException
   */
  public abstract List<QueueInfo> getChildQueueInfos(String parent) throws YarnException,
      IOException;

  /**
   * <p>
   * Get information about <em>acls</em> for <em>current user</em> on all the
   * existing queues.
   * </p>
   * 
   * @return a list of queue acls ({@link QueueUserACLInfo}) for
   *         <em>current user</em>
   * @throws YarnException
   * @throws IOException
   */
  public abstract List<QueueUserACLInfo> getQueueAclsInfo() throws YarnException,
      IOException;
  
  /**
   * <p>
   * Get a report of the given ApplicationAttempt.
   * </p>
   * 
   * <p>
   * In secure mode, <code>YARN</code> verifies access to the application, queue
   * etc. before accepting the request.
   * </p>
   * 
   * @param applicationAttemptId
   *          {@link ApplicationAttemptId} of the application attempt that needs
   *          a report
   * @return application attempt report
   * @throws YarnException
   * @throws {@link ApplicationAttemptNotFoundException} if application attempt
   *         not found
   * @throws IOException
   */
  public abstract ApplicationAttemptReport getApplicationAttemptReport(
      ApplicationAttemptId applicationAttemptId) throws YarnException, IOException;

  /**
   * <p>
   * Get a report of all (ApplicationAttempts) of Application in the cluster.
   * </p>
   * 
   * @param applicationId
   * @return a list of reports for all application attempts for specified
   *         application.
   * @throws YarnException
   * @throws IOException
   */
  public abstract List<ApplicationAttemptReport> getApplicationAttempts(
      ApplicationId applicationId) throws YarnException, IOException;

  /**
   * <p>
   * Get a report of the given Container.
   * </p>
   * 
   * <p>
   * In secure mode, <code>YARN</code> verifies access to the application, queue
   * etc. before accepting the request.
   * </p>
   * 
   * @param containerId
   *          {@link ContainerId} of the container that needs a report
   * @return container report
   * @throws YarnException
   * @throws {@link ContainerNotFoundException} if container not found.
   * @throws IOException
   */
  public abstract ContainerReport getContainerReport(ContainerId containerId)
      throws YarnException, IOException;

  /**
   * <p>
   * Get a report of all (Containers) of ApplicationAttempt in the cluster.
   * </p>
   * 
   * @param applicationAttemptId
   * @return a list of reports of all containers for specified application
   *         attempts
   * @throws YarnException
   * @throws IOException
   */
  public abstract List<ContainerReport> getContainers(
      ApplicationAttemptId applicationAttemptId) throws YarnException,
      IOException;
  
  /**
   * <p>
   * Attempts to move the given application to the given queue.
   * </p>
   * 
   * @param appId
   *    Application to move.
   * @param queue
   *    Queue to place it in to.
   * @throws YarnException
   * @throws IOException
   */
  public abstract void moveApplicationAcrossQueues(ApplicationId appId,
      String queue) throws YarnException, IOException;
}
