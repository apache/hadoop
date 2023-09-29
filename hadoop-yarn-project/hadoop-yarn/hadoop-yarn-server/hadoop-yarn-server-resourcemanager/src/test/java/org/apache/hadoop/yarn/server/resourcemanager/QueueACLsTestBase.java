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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.junit.Assert;

import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Test;

public abstract class QueueACLsTestBase extends ACLsTestBase {

  protected static final String QUEUED = "D";
  protected static final String QUEUED1 = "D1";
  private static final String ALL_ACL = "*";
  private static final String NONE_ACL = " ";


  abstract public String getQueueD();

  abstract public String getQueueD1();

  abstract public void updateConfigWithDAndD1Queues(String rootAcl,
      String queueDAcl, String queueD1Acl) throws IOException;

  @After
  public void tearDown() {
    if (resourceManager != null) {
      resourceManager.stop();
    }
  }

  @Test
  public void testApplicationACLs() throws Exception {

    verifyKillAppSuccess(QUEUE_A_USER, QUEUE_A_USER, QUEUEA, true);
    verifyKillAppSuccess(QUEUE_A_USER, QUEUE_A_ADMIN, QUEUEA, true);
    verifyKillAppSuccess(QUEUE_A_USER, COMMON_USER, QUEUEA, true);
    verifyKillAppSuccess(QUEUE_A_USER, ROOT_ADMIN, QUEUEA, true);
    verifyKillAppFailure(QUEUE_A_USER, QUEUE_B_USER, QUEUEA, true);
    verifyKillAppFailure(QUEUE_A_USER, QUEUE_B_ADMIN, QUEUEA, true);

    verifyKillAppSuccess(QUEUE_B_USER, QUEUE_B_USER, QUEUEB, true);
    verifyKillAppSuccess(QUEUE_B_USER, QUEUE_B_ADMIN, QUEUEB, true);
    verifyKillAppSuccess(QUEUE_B_USER, COMMON_USER, QUEUEB, true);
    verifyKillAppSuccess(QUEUE_B_USER, ROOT_ADMIN, QUEUEB, true);

    verifyKillAppFailure(QUEUE_B_USER, QUEUE_A_USER, QUEUEB, true);
    verifyKillAppFailure(QUEUE_B_USER, QUEUE_A_ADMIN, QUEUEB, true);

    verifyKillAppSuccess(ROOT_ADMIN, ROOT_ADMIN, QUEUEA, false);
    verifyKillAppSuccess(ROOT_ADMIN, ROOT_ADMIN, QUEUEB, false);

    verifyGetClientAMToken(QUEUE_A_USER, ROOT_ADMIN, QUEUEA, true);

  }

  /**
   * Test for the case when the following submit application
   * and administer queue ACLs are defined:
   * root: (none)
   *    D: * (all)
   *      D1: * (all)
   * Expected result: the user will have access only to D and D1 queues.
   * @throws IOException
   */
  @Test
  public void testQueueAclRestrictedRootACL() throws IOException {
    updateConfigWithDAndD1Queues(NONE_ACL, ALL_ACL, ALL_ACL);
    checkAccess(false, true, true);
  }

  /**
   * Test for the case when the following submit application
   * and administer queue ACLs are defined:
   * root: (none)
   *    D:  (none)
   *      D1:  (none)
   * Expected result: the user will have to none of the queues.
   * @throws IOException
   */
  @Test
  public void testQueueAclNoAccess() throws IOException {
    updateConfigWithDAndD1Queues(NONE_ACL, NONE_ACL, NONE_ACL);
    checkAccess(false, false, false);
  }

  /**
   * Test for the case when the following submit application
   * and administer queue ACLs are defined:
   * root: (none)
   *    D: * (all)
   *      D1:  (none)
   * Expected result: access to D1 will be permitted by root.D,
   * so the user will be able to access queues D and D1.
   * @throws IOException
   */
  @Test
  public void testQueueAclRestrictedRootAndD1() throws IOException {
    updateConfigWithDAndD1Queues(NONE_ACL, ALL_ACL, NONE_ACL);
    checkAccess(false, true, true);
  }

  /**
   * Test for the case when the following submit application
   * and administer queue ACLs are defined:
   * root: (none)
   *    D:  (none)
   *      D1:  (all)
   * Expected result: only queue D1 can be accessed.
   * @throws IOException
   */
  @Test
  public void testQueueAclRestrictedRootAndD() throws IOException {
    updateConfigWithDAndD1Queues(NONE_ACL, NONE_ACL, ALL_ACL);
    checkAccess(false, false, true);
  }

  /**
   * Test for the case when the following submit application
   * and administer queue ACLs are defined:
   * root: * (all)
   *    D:  (none)
   *      D1: * (all)
   * Expected result: access to D will be permitted from the root queue,
   * so the user will be able to access queues root, D and D1.
   * @throws IOException
   */
  @Test
  public void testQueueAclRestrictedD() throws IOException {
    updateConfigWithDAndD1Queues(ALL_ACL, NONE_ACL, ALL_ACL);
    checkAccess(true, true, true);
  }

  /**
   * Test for the case when the following submit application
   * and administer queue ACLs are defined:
   * root: * (all)
   *    D: * (all)
   *      D1:  (none)
   * Expected result: access to D1 will be permitted from queue D,
   * so the user will be able to access queues root, D and D1.
   * @throws IOException
   */
  @Test
  public void testQueueAclRestrictedD1() throws IOException {
    updateConfigWithDAndD1Queues(ALL_ACL, ALL_ACL, NONE_ACL);
    checkAccess(true, true, true);
  }

  /**
   * Test for the case when no ACLs are defined, so the default values are used
   * Expected result: The default ACLs for the root queue is "*"(all) and for
   * the other queues are " " (none), so the user will have access to all the
   * queues because they will have permissions from the root.
   *
   * @throws IOException
   */
  @Test
  public void testQueueAclDefaultValues() throws IOException {
    updateConfigWithDAndD1Queues(null, null, null);
    checkAccess(true, true, true);
  }

  private void checkAccess(boolean rootAccess, boolean dAccess,
          boolean d1Access)throws IOException {
    checkAccess(rootAccess, "root");
    checkAccess(dAccess, getQueueD());
    checkAccess(d1Access, getQueueD1());
  }


  private void checkAccess(boolean access, String queueName)
      throws IOException {
    UserGroupInformation user = UserGroupInformation.getCurrentUser();

    String failureMsg = "Wrong %s access to %s queue";
    Assert.assertEquals(
        String.format(failureMsg, QueueACL.ADMINISTER_QUEUE, queueName),
        access, resourceManager.getResourceScheduler()
        .checkAccess(user, QueueACL.ADMINISTER_QUEUE, queueName));
    Assert.assertEquals(
        String.format(failureMsg, QueueACL.SUBMIT_APPLICATIONS, queueName),
        access, resourceManager.getResourceScheduler()
        .checkAccess(user, QueueACL.SUBMIT_APPLICATIONS, queueName));
  }

  private void verifyGetClientAMToken(String submitter, String queueAdmin,
      String queueName, boolean setupACLs) throws Exception {
    ApplicationId applicationId =
        submitAppAndGetAppId(submitter, queueName, setupACLs);
    final GetApplicationReportRequest appReportRequest =
        GetApplicationReportRequest.newInstance(applicationId);

    ApplicationClientProtocol submitterClient = getRMClientForUser(submitter);
    ApplicationClientProtocol adMinUserClient = getRMClientForUser(queueAdmin);

    GetApplicationReportResponse submitterGetReport =
        submitterClient.getApplicationReport(appReportRequest);
    GetApplicationReportResponse adMinUserGetReport =
        adMinUserClient.getApplicationReport(appReportRequest);

    Assert.assertEquals(submitterGetReport.getApplicationReport()
      .getClientToAMToken(), adMinUserGetReport.getApplicationReport()
      .getClientToAMToken());
  }

  private void verifyKillAppFailure(String submitter, String killer,
      String queueName, boolean setupACLs) throws Exception {

    ApplicationId applicationId =
        submitAppAndGetAppId(submitter, queueName, setupACLs);

    final KillApplicationRequest finishAppRequest =
        KillApplicationRequest.newInstance(applicationId);

    ApplicationClientProtocol killerClient = getRMClientForUser(killer);

    // Kill app as the killer
    try {
      killerClient.forceKillApplication(finishAppRequest);
      Assert.fail("App killing by the enemy should fail!!");
    } catch (YarnException e) {
      LOG.info("Got exception while killing app as the enemy", e);
      Assert.assertTrue(e.getMessage().contains(
        "User " + killer + " cannot perform operation MODIFY_APP on "
            + applicationId));
    }

    getRMClientForUser(submitter).forceKillApplication(finishAppRequest);
  }

  private void verifyKillAppSuccess(String submitter, String killer,
      String queueName, boolean setupACLs) throws Exception {
    ApplicationId applicationId =
        submitAppAndGetAppId(submitter, queueName, setupACLs);

    final KillApplicationRequest finishAppRequest =
        KillApplicationRequest.newInstance(applicationId);

    ApplicationClientProtocol ownerClient = getRMClientForUser(killer);

    // Kill app as killer
    ownerClient.forceKillApplication(finishAppRequest);
    resourceManager.waitForState(applicationId, RMAppState.KILLED);
  }

  private ApplicationId submitAppAndGetAppId(String submitter,
      String queueName, boolean setupACLs) throws Exception {

    GetNewApplicationRequest newAppRequest =
        GetNewApplicationRequest.newInstance();

    ApplicationClientProtocol submitterClient = getRMClientForUser(submitter);
    ApplicationId applicationId =
        submitterClient.getNewApplication(newAppRequest).getApplicationId();

    Resource resource = Resources.createResource(1024);
    Map<ApplicationAccessType, String> acls = createACLs(submitter, setupACLs);
    ContainerLaunchContext amContainerSpec =
        ContainerLaunchContext.newInstance(null, null, null, null, null, acls);

    ApplicationSubmissionContext appSubmissionContext =
        ApplicationSubmissionContext.newInstance(applicationId,
          "applicationName", queueName, null, amContainerSpec, false, true, 1,
          resource, "applicationType");
    appSubmissionContext.setApplicationId(applicationId);
    appSubmissionContext.setQueue(queueName);

    SubmitApplicationRequest submitRequest =
        SubmitApplicationRequest.newInstance(appSubmissionContext);
    submitterClient.submitApplication(submitRequest);
    resourceManager.waitForState(applicationId, RMAppState.ACCEPTED);
    return applicationId;
  }

  private Map<ApplicationAccessType, String> createACLs(String submitter,
      boolean setupACLs) {
    AccessControlList viewACL = new AccessControlList("");
    AccessControlList modifyACL = new AccessControlList("");
    if (setupACLs) {
      viewACL.addUser(submitter);
      viewACL.addUser(COMMON_USER);
      modifyACL.addUser(submitter);
      modifyACL.addUser(COMMON_USER);
    }
    Map<ApplicationAccessType, String> acls =
        new HashMap<ApplicationAccessType, String>();
    acls.put(ApplicationAccessType.VIEW_APP, viewACL.getAclString());
    acls.put(ApplicationAccessType.MODIFY_APP, modifyACL.getAclString());
    return acls;
  }
}
