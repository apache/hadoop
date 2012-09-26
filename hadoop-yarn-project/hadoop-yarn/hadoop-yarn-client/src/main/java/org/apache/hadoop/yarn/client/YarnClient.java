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

package org.apache.hadoop.yarn.client;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.DelegationToken;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.service.Service;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface YarnClient extends Service {

  /**
   * <p>
   * Obtain a new {@link ApplicationId} for submitting new applications.
   * </p>
   * 
   * <p>
   * Returns a response which contains {@link ApplicationId} that can be used to
   * submit a new application. See
   * {@link #submitApplication(ApplicationSubmissionContext)}.
   * </p>
   * 
   * <p>
   * See {@link GetNewApplicationResponse} for other information that is
   * returned.
   * </p>
   * 
   * @return response containing the new <code>ApplicationId</code> to be used
   *         to submit an application
   * @throws YarnRemoteException
   */
  GetNewApplicationResponse getNewApplication() throws YarnRemoteException;

  /**
   * <p>
   * Submit a new application to <code>YARN.</code>
   * </p>
   * 
   * @param appContext
   *          {@link ApplicationSubmissionContext} containing all the details
   *          needed to submit a new application
   * @return {@link ApplicationId} of the accepted application
   * @throws YarnRemoteException
   * @see #getNewApplication()
   */
  ApplicationId submitApplication(ApplicationSubmissionContext appContext)
      throws YarnRemoteException;

  /**
   * <p>
   * Kill an application identified by given ID.
   * </p>
   * 
   * @param applicationId
   *          {@link ApplicationId} of the application that needs to be killed
   * @throws YarnRemoteException
   *           in case of errors or if YARN rejects the request due to
   *           access-control restrictions.
   * @see #getQueueAclsInfo()
   */
  void killApplication(ApplicationId applicationId) throws YarnRemoteException;

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
   * @throws YarnRemoteException
   */
  ApplicationReport getApplicationReport(ApplicationId appId)
      throws YarnRemoteException;

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
   * @throws YarnRemoteException
   */
  List<ApplicationReport> getApplicationList() throws YarnRemoteException;

  /**
   * <p>
   * Get metrics ({@link YarnClusterMetrics}) about the cluster.
   * </p>
   * 
   * @return cluster metrics
   * @throws YarnRemoteException
   */
  YarnClusterMetrics getYarnClusterMetrics() throws YarnRemoteException;

  /**
   * <p>
   * Get a report of all nodes ({@link NodeReport}) in the cluster.
   * </p>
   * 
   * @return A list of report of all nodes
   * @throws YarnRemoteException
   */
  List<NodeReport> getNodeReports() throws YarnRemoteException;

  /**
   * <p>
   * Get a delegation token so as to be able to talk to YARN using those tokens.
   * 
   * @param renewer
   *          Address of the renewer who can renew these tokens when needed by
   *          securely talking to YARN.
   * @return a delegation token ({@link DelegationToken}) that can be used to
   *         talk to YARN
   * @throws YarnRemoteException
   */
  DelegationToken getRMDelegationToken(Text renewer) throws YarnRemoteException;

  /**
   * <p>
   * Get information ({@link QueueInfo}) about a given <em>queue</em>.
   * </p>
   * 
   * @param queueName
   *          Name of the queue whose information is needed
   * @return queue information
   * @throws YarnRemoteException
   *           in case of errors or if YARN rejects the request due to
   *           access-control restrictions.
   */
  QueueInfo getQueueInfo(String queueName) throws YarnRemoteException;

  /**
   * <p>
   * Get information ({@link QueueInfo}) about all queues, recursively if there
   * is a hierarchy
   * </p>
   * 
   * @return a list of queue-information for all queues
   * @throws YarnRemoteException
   */
  List<QueueInfo> getAllQueues() throws YarnRemoteException;

  /**
   * <p>
   * Get information ({@link QueueInfo}) about top level queues.
   * </p>
   * 
   * @return a list of queue-information for all the top-level queues
   * @throws YarnRemoteException
   */
  List<QueueInfo> getRootQueueInfos() throws YarnRemoteException;

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
   * @throws YarnRemoteException
   */
  List<QueueInfo> getChildQueueInfos(String parent) throws YarnRemoteException;

  /**
   * <p>
   * Get information about <em>acls</em> for <em>current user</em> on all the
   * existing queues.
   * </p>
   * 
   * @return a list of queue acls ({@link QueueUserACLInfo}) for
   *         <em>current user</em>
   * @throws YarnRemoteException
   */
  List<QueueUserACLInfo> getQueueAclsInfo() throws YarnRemoteException;
}
