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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapred.AuditLogger;
import org.apache.hadoop.mapred.AuditLogger.Constants;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;

public abstract class JobACLsManager {

	  static final Log LOG = LogFactory.getLog(JobACLsManager.class);

	  protected abstract boolean isJobLevelAuthorizationEnabled();

	  protected abstract boolean isSuperUserOrSuperGroup(
	      UserGroupInformation callerUGI);

	  /**
	   * Construct the jobACLs from the configuration so that they can be kept in
	   * the memory. If authorization is disabled on the JT, nothing is constructed
	   * and an empty map is returned.
	   * 
	   * @return JobACL to AccessControlList map.
	   */
	  Map<JobACL, AccessControlList> constructJobACLs(JobConf conf) {
	    
	    Map<JobACL, AccessControlList> acls =
	      new HashMap<JobACL, AccessControlList>();

	    // Don't construct anything if authorization is disabled.
	    if (!isJobLevelAuthorizationEnabled()) {
	      return acls;
	    }

	    for (JobACL aclName : JobACL.values()) {
	      String aclConfigName = aclName.getAclName();
	      String aclConfigured = conf.get(aclConfigName);
	      if (aclConfigured == null) {
	        // If ACLs are not configured at all, we grant no access to anyone. So
	        // jobOwner and superuser/supergroup _only_ can do 'stuff'
	        aclConfigured = "";
	      }
	      acls.put(aclName, new AccessControlList(aclConfigured));
	    }
	    return acls;
	  }

	  /**
	   * If authorization is enabled, checks whether the user (in the callerUGI) is
	   * authorized to perform the operation specified by 'jobOperation' on the job.
	   * <ul>
	   * <li>The owner of the job can do any operation on the job</li>
	   * <li>The superuser/supergroup is always permitted to do operations on any
	   * job.</li>
	   * <li>For all other users/groups job-acls are checked</li>
	   * </ul>
	   * 
	   * @param jobStatus
	   * @param callerUGI
	   * @param jobOperation
	   */
	  void checkAccess(JobStatus jobStatus, UserGroupInformation callerUGI,
	      JobACL jobOperation) throws AccessControlException {

	    JobID jobId = jobStatus.getJobID();
	    String jobOwner = jobStatus.getUsername();
	    AccessControlList acl = jobStatus.getJobACLs().get(jobOperation);
	    checkAccess(jobId, callerUGI, jobOperation, jobOwner, acl);
	  }

	  /**
	   * If authorization is enabled, checks whether the user (in the callerUGI) is
	   * authorized to perform the operation specified by 'jobOperation' on the job.
	   * <ul>
	   * <li>The owner of the job can do any operation on the job</li>
	   * <li>The superuser/supergroup is always permitted to do operations on any
	   * job.</li>
	   * <li>For all other users/groups job-acls are checked</li>
	   * </ul>
	   * @param jobId
	   * @param callerUGI
	   * @param jobOperation
	   * @param jobOwner
	   * @param jobACL
	   * @throws AccessControlException
	   */
	  void checkAccess(JobID jobId, UserGroupInformation callerUGI,
	      JobACL jobOperation, String jobOwner, AccessControlList jobACL)
	      throws AccessControlException {

	    String user = callerUGI.getShortUserName();
	    if (!isJobLevelAuthorizationEnabled()) {
	      return;
	    }

	    // Allow superusers/supergroups
	    // Allow Job-owner as the job's owner is always part of all the ACLs
	    if (callerUGI.getShortUserName().equals(jobOwner)
	        || isSuperUserOrSuperGroup(callerUGI) 
	        || jobACL.isUserAllowed(callerUGI)) {
	      AuditLogger.logSuccess(user, jobOperation.name(),  jobId.toString());
	      return;
	    }

	    // log this event to the audit log
	    AuditLogger.logFailure(user, jobOperation.name(), jobACL.toString(), 
	                           jobId.toString(), Constants.UNAUTHORIZED_USER);
	    throw new AccessControlException(callerUGI
	        + " is not authorized for performing the operation "
	        + jobOperation.toString() + " on " + jobId + ". "
	        + jobOperation.toString()
	        + " Access control list configured for this job : "
	        + jobACL.toString());
	  }
	}
