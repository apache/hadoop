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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class JobACLsManager {

  static final Logger LOG = LoggerFactory.getLogger(JobACLsManager.class);
  Configuration conf;
  private final AccessControlList adminAcl;

  public JobACLsManager(Configuration conf) {
    adminAcl = new AccessControlList(conf.get(MRConfig.MR_ADMINS, " "));
    this.conf = conf;
  }

  public boolean areACLsEnabled() {
    return conf.getBoolean(MRConfig.MR_ACLS_ENABLED, false);
  }

  /**
   * Construct the jobACLs from the configuration so that they can be kept in
   * the memory. If authorization is disabled on the JT, nothing is constructed
   * and an empty map is returned.
   * 
   * @return JobACL to AccessControlList map.
   */
  public Map<JobACL, AccessControlList> constructJobACLs(Configuration conf) {

    Map<JobACL, AccessControlList> acls =
        new HashMap<JobACL, AccessControlList>();

    // Don't construct anything if authorization is disabled.
    if (!areACLsEnabled()) {
      return acls;
    }

    for (JobACL aclName : JobACL.values()) {
      String aclConfigName = aclName.getAclName();
      String aclConfigured = conf.get(aclConfigName);
      if (aclConfigured == null) {
        // If ACLs are not configured at all, we grant no access to anyone. So
        // jobOwner and cluster administrator _only_ can do 'stuff'
        aclConfigured = " ";
      }
      acls.put(aclName, new AccessControlList(aclConfigured));
    }
    return acls;
  }

  /**
    * Is the calling user an admin for the mapreduce cluster
    * i.e. member of mapreduce.cluster.administrators
    * @return true, if user is an admin
    */
   boolean isMRAdmin(UserGroupInformation callerUGI) {
     if (adminAcl.isUserAllowed(callerUGI)) {
       return true;
     }
     return false;
   }

  /**
   * If authorization is enabled, checks whether the user (in the callerUGI)
   * is authorized to perform the operation specified by 'jobOperation' on
   * the job by checking if the user is jobOwner or part of job ACL for the
   * specific job operation.
   * <ul>
   * <li>The owner of the job can do any operation on the job</li>
   * <li>For all other users/groups job-acls are checked</li>
   * </ul>
   * @param callerUGI
   * @param jobOperation
   * @param jobOwner
   * @param jobACL
   */
  public boolean checkAccess(UserGroupInformation callerUGI,
      JobACL jobOperation, String jobOwner, AccessControlList jobACL) {

    if (LOG.isDebugEnabled()) {
      LOG.debug("checkAccess job acls, jobOwner: " + jobOwner + " jobacl: "
          + jobOperation.toString() + " user: " + callerUGI.getShortUserName());
    }
    String user = callerUGI.getShortUserName();
    if (!areACLsEnabled()) {
      return true;
    }

    // Allow Job-owner for any operation on the job
    if (isMRAdmin(callerUGI)
        || user.equals(jobOwner)
        || jobACL.isUserAllowed(callerUGI)) {
      return true;
    }

    return false;
  }
}
