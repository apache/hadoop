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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.junit.Test;

/**
 * Test the job acls manager
 */
public class TestJobAclsManager {

  @Test
  public void testClusterAdmins() {
    Map<JobACL, AccessControlList> tmpJobACLs = new HashMap<JobACL, AccessControlList>();
    Configuration conf = new Configuration();
    String jobOwner = "testuser";
    conf.set(JobACL.VIEW_JOB.getAclName(), jobOwner);
    conf.set(JobACL.MODIFY_JOB.getAclName(), jobOwner);
    conf.setBoolean(MRConfig.MR_ACLS_ENABLED, true);
    String clusterAdmin = "testuser2";
    conf.set(MRConfig.MR_ADMINS, clusterAdmin);

    JobACLsManager aclsManager = new JobACLsManager(conf);
    tmpJobACLs = aclsManager.constructJobACLs(conf);
    final Map<JobACL, AccessControlList> jobACLs = tmpJobACLs;

    UserGroupInformation callerUGI = UserGroupInformation.createUserForTesting(
        clusterAdmin, new String[] {});

    // cluster admin should have access
    boolean val = aclsManager.checkAccess(callerUGI, JobACL.VIEW_JOB, jobOwner,
        jobACLs.get(JobACL.VIEW_JOB));
    assertTrue("cluster admin should have view access", val);
    val = aclsManager.checkAccess(callerUGI, JobACL.MODIFY_JOB, jobOwner,
        jobACLs.get(JobACL.MODIFY_JOB));
    assertTrue("cluster admin should have modify access", val);
  }

  @Test
  public void testClusterNoAdmins() {
    Map<JobACL, AccessControlList> tmpJobACLs = new HashMap<JobACL, AccessControlList>();
    Configuration conf = new Configuration();
    String jobOwner = "testuser";
    conf.set(JobACL.VIEW_JOB.getAclName(), "");
    conf.setBoolean(MRConfig.MR_ACLS_ENABLED, true);
    String noAdminUser = "testuser2";

    JobACLsManager aclsManager = new JobACLsManager(conf);
    tmpJobACLs = aclsManager.constructJobACLs(conf);
    final Map<JobACL, AccessControlList> jobACLs = tmpJobACLs;

    UserGroupInformation callerUGI = UserGroupInformation.createUserForTesting(
        noAdminUser, new String[] {});
    // random user should not have access
    boolean val = aclsManager.checkAccess(callerUGI, JobACL.VIEW_JOB, jobOwner,
        jobACLs.get(JobACL.VIEW_JOB));
    assertFalse("random user should not have view access", val);
    val = aclsManager.checkAccess(callerUGI, JobACL.MODIFY_JOB, jobOwner,
        jobACLs.get(JobACL.MODIFY_JOB));
    assertFalse("random user should not have modify access", val);

    callerUGI = UserGroupInformation.createUserForTesting(jobOwner,
        new String[] {});
    // Owner should have access
    val = aclsManager.checkAccess(callerUGI, JobACL.VIEW_JOB, jobOwner,
        jobACLs.get(JobACL.VIEW_JOB));
    assertTrue("owner should have view access", val);
    val = aclsManager.checkAccess(callerUGI, JobACL.MODIFY_JOB, jobOwner,
        jobACLs.get(JobACL.MODIFY_JOB));
    assertTrue("owner should have modify access", val);
  }

  @Test
  public void testAclsOff() {
    Map<JobACL, AccessControlList> tmpJobACLs = new HashMap<JobACL, AccessControlList>();
    Configuration conf = new Configuration();
    String jobOwner = "testuser";
    conf.set(JobACL.VIEW_JOB.getAclName(), jobOwner);
    conf.setBoolean(MRConfig.MR_ACLS_ENABLED, false);
    String noAdminUser = "testuser2";

    JobACLsManager aclsManager = new JobACLsManager(conf);
    tmpJobACLs = aclsManager.constructJobACLs(conf);
    final Map<JobACL, AccessControlList> jobACLs = tmpJobACLs;

    UserGroupInformation callerUGI = UserGroupInformation.createUserForTesting(
        noAdminUser, new String[] {});
    // acls off so anyone should have access
    boolean val = aclsManager.checkAccess(callerUGI, JobACL.VIEW_JOB, jobOwner,
        jobACLs.get(JobACL.VIEW_JOB));
    assertTrue("acls off so anyone should have access", val);
  }

  @Test
  public void testGroups() {
    Map<JobACL, AccessControlList> tmpJobACLs = new HashMap<JobACL, AccessControlList>();
    Configuration conf = new Configuration();
    String jobOwner = "testuser";
    conf.set(JobACL.VIEW_JOB.getAclName(), jobOwner);
    conf.setBoolean(MRConfig.MR_ACLS_ENABLED, true);
    String user = "testuser2";
    String adminGroup = "adminGroup";
    conf.set(MRConfig.MR_ADMINS, " " + adminGroup);

    JobACLsManager aclsManager = new JobACLsManager(conf);
    tmpJobACLs = aclsManager.constructJobACLs(conf);
    final Map<JobACL, AccessControlList> jobACLs = tmpJobACLs;

    UserGroupInformation callerUGI = UserGroupInformation.createUserForTesting(
     user, new String[] {adminGroup});
    // acls off so anyone should have access
    boolean val = aclsManager.checkAccess(callerUGI, JobACL.VIEW_JOB, jobOwner,
        jobACLs.get(JobACL.VIEW_JOB));
    assertTrue("user in admin group should have access", val);
  }
}
