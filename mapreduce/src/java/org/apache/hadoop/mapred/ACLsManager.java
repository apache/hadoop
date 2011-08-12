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
package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.AuditLogger.Constants;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;

/**
 * Manages MapReduce cluster administrators and access checks for
 * job level operations and queue level operations.
 * Uses JobACLsManager for access checks of job level operations and
 * QueueManager for queue operations.
 */
@InterfaceAudience.Private
public class ACLsManager {

  static Log LOG = LogFactory.getLog(ACLsManager.class);
  // MROwner(user who started this mapreduce cluster)'s ugi
  private final UserGroupInformation mrOwner;
  // mapreduce cluster administrators
  private final AccessControlList adminAcl;
  
  private final JobACLsManager jobACLsManager;
  private final QueueManager queueManager;
  
  private final boolean aclsEnabled;

  public ACLsManager(Configuration conf, JobACLsManager jobACLsManager,
      QueueManager queueManager) throws IOException {

    mrOwner = UserGroupInformation.getCurrentUser();
    adminAcl = new AccessControlList(conf.get(MRConfig.MR_ADMINS, " "));
    adminAcl.addUser(mrOwner.getShortUserName());
    
    String deprecatedSuperGroup = conf.get(MRConfig.MR_SUPERGROUP);
    if (deprecatedSuperGroup != null) {
      LOG.warn(MRConfig.MR_SUPERGROUP + " is deprecated. Use " 
          + MRConfig.MR_ADMINS + " instead");
      adminAcl.addGroup(deprecatedSuperGroup);
    }
    aclsEnabled = conf.getBoolean(MRConfig.MR_ACLS_ENABLED, false);

    this.jobACLsManager = jobACLsManager;
    this.queueManager = queueManager;
  }

  public UserGroupInformation getMROwner() {
    return mrOwner;
  }

  AccessControlList getAdminsAcl() {
    return adminAcl;
  }

  public JobACLsManager getJobACLsManager() {
    return jobACLsManager;
  }

  /**
   * Is the calling user an admin for the mapreduce cluster ?
   * i.e. either cluster owner or cluster administrator
   * @return true, if user is an admin
   */
  public boolean isMRAdmin(UserGroupInformation callerUGI) {
    if (adminAcl.isUserAllowed(callerUGI)) {
      return true;
    }
    return false;
  }

  /**
   * Check the ACLs for a user doing the passed operation.
   * <ul>
   * <li>If ACLs are disabled, allow all users.</li>
   * <li>Otherwise, if the operation is not a job operation(for eg.
   *  submit-job-to-queue), then allow only (a) clusterOwner(who started the
   *  cluster), (b) cluster administrators and (c) members of
   *  queue-submit-job-acl for the queue.</li>
   * <li>If the operation is a job operation, then allow only (a) jobOwner,
   * (b) clusterOwner(who started the cluster), (c) cluster administrators,
   * (d) members of queue admins acl for the queue and (e) members of job
   * acl for the job operation</li>
   * </ul>
   * 
   * @param job   the job on which operation is requested
   * @param callerUGI  the user who is requesting the operation
   * @param operation  the operation for which authorization is needed
   * @throws AccessControlException
   */
   public void checkAccess(JobInProgress job, UserGroupInformation callerUGI,
       Operation operation) throws AccessControlException {

    String queue = job.getProfile().getQueueName();
    String jobId = job.getJobID().toString();
    JobStatus jobStatus = job.getStatus();
    String jobOwner = jobStatus.getUsername();
    AccessControlList jobAcl =
        jobStatus.getJobACLs().get(operation.jobACLNeeded);

    checkAccess(jobId, callerUGI, queue, operation, jobOwner, jobAcl);
  }

  /**
   * Check the ACLs for a user doing the passed job operation.
   * <ul>
   * <li>If ACLs are disabled, allow all users.</li>
   * <li>Otherwise, allow only (a) jobOwner,
   * (b) clusterOwner(who started the cluster), (c) cluster administrators,
   * (d) members of job acl for the jobOperation</li>
   * </ul>
   * 
   * @param jobStatus  the status of the job
   * @param callerUGI  the user who is trying to perform the operation
   * @param queue      the job queue name
   * @param operation  the operation for which authorization is needed
   */
  void checkAccess(JobStatus jobStatus, UserGroupInformation callerUGI,
      String queue, Operation operation) throws AccessControlException {

    String jobId = jobStatus.getJobID().toString();
    String jobOwner = jobStatus.getUsername();
    AccessControlList jobAcl =
      jobStatus.getJobACLs().get(operation.jobACLNeeded);

    // If acls are enabled, check if callerUGI is jobOwner, queue admin,
    // cluster admin or part of job ACL
    checkAccess(jobId, callerUGI, queue, operation, jobOwner, jobAcl);
  }

  /**
   * Check the ACLs for a user doing the passed operation.
   * <ul>
   * <li>If ACLs are disabled, allow all users.</li>
   * <li>Otherwise, if the operation is not a job operation(for eg.
   *  submit-job-to-queue), then allow only (a) clusterOwner(who started the
   *  cluster), (b) cluster administrators and (c) members of
   *  queue-submit-job-acl for the queue.</li>
   * <li>If the operation is a job operation, then allow only (a) jobOwner,
   * (b) clusterOwner(who started the cluster), (c) cluster administrators,
   * (d) members of queue admins acl for the queue and (e) members of job
   * acl for the job operation</li>
   * </ul>
   * 
   * @param jobId      the job id
   * @param callerUGI  the user who is trying to perform the operation
   * @param queue      the job queue name
   * @param operation  the operation for which authorization is needed
   * @param jobOwner   the user who submitted(or is submitting) this job
   * @param jobAcl     could be job-view-acl or job-modify-acl depending on the
   *                   job operation.
   */
  void checkAccess(String jobId, UserGroupInformation callerUGI,
      String queue, Operation operation, String jobOwner,
      AccessControlList jobAcl) throws AccessControlException {

    String user = callerUGI.getShortUserName();
    String targetResource = jobId + " in queue " + queue;

    if (!aclsEnabled) {
      AuditLogger.logSuccess(user, operation.name(), targetResource);
      return;
    }

    // Allow mapreduce cluster admins to do any queue operation and
    // any job operation
    if (isMRAdmin(callerUGI)) {
      AuditLogger.logSuccess(user, operation.name(), targetResource);
      return;
    }

    if (operation == Operation.SUBMIT_JOB) {
      // This is strictly queue operation(not a job operation)
      if (!queueManager.hasAccess(queue, operation.qACLNeeded, callerUGI)) {
        AuditLogger.logFailure(user, operation.name(),
            queueManager.getQueueACL(queue, operation.qACLNeeded).toString(),
            targetResource, Constants.UNAUTHORIZED_USER);

        throw new AccessControlException("User "
            + callerUGI.getShortUserName() + " cannot perform "
            + "operation " + operation.name() + " on queue " + queue
            + ".\n Please run \"hadoop queue -showacls\" "
            + "command to find the queues you have access to .");
      } else {
        AuditLogger.logSuccess(user, operation.name(), targetResource);
        return;
      }
    }

    // Check if callerUGI is queueAdmin(in some cases only), jobOwner or
    // part of job-acl.

    // queueManager and queue are null only when called from
    // TaskTracker(i.e. from TaskLogServlet) for the operation VIEW_TASK_LOGS.
    // Caller of this method takes care of checking if callerUGI is a
    // queue administrator for that operation.
    if (operation == Operation.VIEW_TASK_LOGS) {
      if (jobACLsManager.checkAccess(callerUGI, operation.jobACLNeeded,
          jobOwner, jobAcl)) {
        AuditLogger.logSuccess(user, operation.name(), targetResource);
        return;
      }
    } else if (queueManager.hasAccess(queue, operation.qACLNeeded, callerUGI) ||
        jobACLsManager.checkAccess(callerUGI, operation.jobACLNeeded,
            jobOwner, jobAcl)) {
      AuditLogger.logSuccess(user, operation.name(), targetResource);
      return;
    }

    AuditLogger.logFailure(user, operation.name(), jobAcl.toString(),
        targetResource, Constants.UNAUTHORIZED_USER);

    throw new AccessControlException("User "
        + callerUGI.getShortUserName() + " cannot perform operation "
        + operation.name() + " on " + jobId + " that is in the queue "
        + queue);
  }

}
