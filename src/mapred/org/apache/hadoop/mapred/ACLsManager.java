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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.AuditLogger.Constants;
import org.apache.hadoop.mapred.QueueManager.QueueOperation;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;

/**
 * Manages MapReduce cluster administrators and access checks for
 * job level operations and queue level operations.
 * Uses JobACLsManager for access checks of job level operations and
 * QueueManager for queue operations.
 */
class ACLsManager {

  // MROwner(user who started this mapreduce cluster)'s ugi
  private final UserGroupInformation mrOwner;
  private final AccessControlList adminAcl;
  
  private final JobACLsManager jobACLsManager;
  private final QueueManager queueManager;
  
  private final boolean aclsEnabled;

  ACLsManager(Configuration conf, JobACLsManager jobACLsManager,
      QueueManager queueManager) throws IOException {

    if (UserGroupInformation.isLoginKeytabBased()) {
      mrOwner = UserGroupInformation.getLoginUser();
    } else {
      mrOwner = UserGroupInformation.getCurrentUser();
    }

    aclsEnabled = conf.getBoolean(JobConf.MR_ACLS_ENABLED, false);

    adminAcl = new AccessControlList(conf.get(JobConf.MR_ADMINS, " "));
    adminAcl.addUser(mrOwner.getShortUserName());

    this.jobACLsManager = jobACLsManager;

    this.queueManager = queueManager;
  }

  UserGroupInformation getMROwner() {
    return mrOwner;
  }

  AccessControlList getAdminsAcl() {
    return adminAcl;
  }

  JobACLsManager getJobACLsManager() {
    return jobACLsManager;
  }

  /**
   * Is the calling user an admin for the mapreduce cluster ?
   * i.e. either cluster owner or member of mapreduce.cluster.administrators
   * @return true, if user is an admin
   */
  boolean isMRAdmin(UserGroupInformation callerUGI) {
    if (adminAcl.isUserAllowed(callerUGI)) {
      return true;
    }
    return false;
  }

  /**
   * Check the ACLs for a user doing the passed queue-operation and the passed
   * job operation.
   * <ul>
   * <li>If ACLs are disabled, allow all users.</li>
   * <li>If the operation is not a job operation(for eg. submit-job-to-queue),
   *  then allow only (a) clusterOwner(who started the cluster), (b) cluster 
   *  administrators (c) members of queue admins acl for the queue.</li>
   * <li>If the operation is a job operation, then allow only (a) jobOwner,
   * (b) clusterOwner(who started the cluster), (c) cluster administrators,
   * (d) members of queue admins acl for the queue and (e) members of job
   * acl for the jobOperation</li>
   * </ul>
   * 
   * @param job
   * @param callerUGI
   * @param oper
   * @param jobOperation
   * @throws AccessControlException
   * @throws IOException
   */
  void checkAccess(JobInProgress job,
      UserGroupInformation callerUGI, QueueOperation qOperation,
      JobACL jobOperation, String operationName) throws AccessControlException {

    String queue = job.getProfile().getQueueName();
    String jobId = job.getJobID().toString();
    JobStatus jobStatus = job.getStatus();
    String jobOwner = jobStatus.getUsername();
    AccessControlList jobAcl = jobStatus.getJobACLs().get(jobOperation);

    checkAccess(jobId, callerUGI, queue, qOperation,
        jobOperation, jobOwner, jobAcl, operationName);
  }

  /**
   * Check the ACLs for a user doing the passed job operation.
   * <ul>
   * <li>If ACLs are disabled, allow all users.</li>
   * <li>Otherwise, allow only (a) jobOwner,
   * (b) clusterOwner(who started the cluster), (c) cluster administrators,
   * (d) members of job acl for the jobOperation</li>
   * </ul>
   */
  void checkAccess(JobStatus jobStatus, UserGroupInformation callerUGI,
      JobACL jobOperation, String operationName) throws AccessControlException {

    String jobId = jobStatus.getJobID().toString();
    String jobOwner = jobStatus.getUsername();
    AccessControlList jobAcl = jobStatus.getJobACLs().get(jobOperation);

    // If acls are enabled, check if jobOwner, cluster admin or part of job ACL
    checkAccess(jobId, callerUGI, jobOperation, jobOwner, jobAcl,
        operationName);
  }

  /**
   * Check the ACLs for a user doing the passed job operation.
   * <ul>
   * <li>If ACLs are disabled, allow all users.</li>
   * <li>Otherwise, allow only (a) jobOwner,
   * (b) clusterOwner(who started the cluster), (c) cluster administrators,
   * (d) members of job acl for the jobOperation</li>
   * </ul>
   */
  void checkAccess(String jobId, UserGroupInformation callerUGI,
      JobACL jobOperation, String jobOwner, AccessControlList jobAcl,
      String operationName)
      throws AccessControlException {
    // TODO: Queue admins are to be allowed to do the job view operation.
    checkAccess(jobId, callerUGI, null, null, jobOperation, jobOwner, jobAcl,
        operationName);
  }

  /**
   * Check the ACLs for a user doing the passed queue-operation and the passed
   * job operation.
   * <ul>
   * <li>If ACLs are disabled, allow all users.</li>
   * <li>If the operation is not a job operation(for eg. submit-job-to-queue),
   *  then allow only (a) clusterOwner(who started the cluster), (b)cluster 
   *  administrators and (c) members of queue admins acl for the queue.</li>
   * <li>If the operation is a job operation, then allow only (a) jobOwner,
   * (b) clusterOwner(who started the cluster), (c) cluster administrators,
   * (d) members of queue admins acl for the queue and (e) members of job
   * acl for the jobOperation</li>
   * </ul>
   * 
   * callerUGI user who is trying to perform the qOperation/jobOperation.
   * jobAcl could be job-view-acl or job-modify-acl depending on jobOperation.
   */
  void checkAccess(String jobId, UserGroupInformation callerUGI,
      String queue, QueueOperation qOperation,
      JobACL jobOperation, String jobOwner, AccessControlList jobAcl,
      String operationName)
      throws AccessControlException {
    if (!aclsEnabled) {
      return;
    }

    String user = callerUGI.getShortUserName();

    // Allow mapreduce cluster admins to do any queue operation and
    // any job operation
    if (isMRAdmin(callerUGI)) {
      if (qOperation == QueueOperation.SUBMIT_JOB) {
        AuditLogger.logSuccess(user, operationName, queue);
      } else {
        AuditLogger.logSuccess(user, operationName, jobId);
      }
      return;
    }

    if (qOperation == QueueOperation.SUBMIT_JOB) {
      // This is strictly queue operation(not a job operation) like
      // submit-job-to-queue.
      if (!queueManager.hasAccess(queue, qOperation, callerUGI)) {
        AuditLogger.logFailure(user, operationName, null, queue,
            Constants.UNAUTHORIZED_USER + ", job : " + jobId);

        throw new AccessControlException("User "
            + callerUGI.getShortUserName() + " cannot perform "
            + "operation " + operationName + " on queue " + queue
            + ".\n Please run \"hadoop queue -showacls\" "
            + "command to find the queues you have access to .");
      } else {
        AuditLogger.logSuccess(user, operationName, queue);
        return;
      }
    }

    if (jobOperation == JobACL.VIEW_JOB) {
      // check if jobOwner or part of acl-view-job
      if (jobACLsManager.checkAccess(callerUGI, jobOperation,
          jobOwner, jobAcl)) {
        AuditLogger.logSuccess(user, operationName, jobId.toString());
        return;
      }
      else {
        AuditLogger.logFailure(user, operationName, null,
            jobId.toString(), Constants.UNAUTHORIZED_USER);
        throw new AccessControlException("User "
            + callerUGI.getShortUserName() + " cannot perform operation "
            + operationName + " on " + jobId);
      }
    }

    if (jobOperation == JobACL.MODIFY_JOB) {
      // check if queueAdmin, jobOwner or part of acl-modify-job
      if (queueManager.hasAccess(queue, qOperation, callerUGI)) {
        AuditLogger.logSuccess(user, operationName, queue);
        return;
      } else if (jobACLsManager.checkAccess(callerUGI, jobOperation,
                 jobOwner, jobAcl)) {
        AuditLogger.logSuccess(user, operationName, jobId);
        return;
      }
      AuditLogger.logFailure(user, operationName, null,
          jobId.toString(), Constants.UNAUTHORIZED_USER + ", queue : "
          + queue);

      throw new AccessControlException("User "
          + callerUGI.getShortUserName() + " cannot perform operation "
          + operationName + " on " + jobId + " that is in the queue "
          + queue);
    }

    throw new AccessControlException("Unsupported queue operation "
        + qOperation + " on queue " + queue + ", job operation "
        + jobOperation + " on job " + jobId + " and the actual-operation "
        + operationName);
  }

}
