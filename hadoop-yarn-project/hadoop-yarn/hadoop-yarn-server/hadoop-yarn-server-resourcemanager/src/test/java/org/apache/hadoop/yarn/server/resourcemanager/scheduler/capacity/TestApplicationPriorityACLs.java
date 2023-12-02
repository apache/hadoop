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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ACLsTestBase;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Test;


public class TestApplicationPriorityACLs extends ACLsTestBase {

  private final int defaultPriorityQueueA = 3;
  private final int defaultPriorityQueueB = 10;
  private final int maxPriorityQueueA = 5;
  private final int maxPriorityQueueB = 11;
  private final int clusterMaxPriority = 10;

  @Test
  public void testApplicationACLs() throws Exception {

    /*
     * Cluster Max-priority is 10. User 'queueA_user' has permission to submit
     * apps only at priority 5. Default priority for this user is 3.
     */

    // Case 1: App will be submitted with priority 5.
    verifyAppSubmitWithPrioritySuccess(QUEUE_A_USER, QUEUEA, 5);

    // Case 2: App will be rejected as submitted priority was 6.
    verifyAppSubmitWithPriorityFailure(QUEUE_A_USER, QUEUEA, 6);

    // Case 3: App will be submitted w/o priority, hence consider default 3.
    verifyAppSubmitWithPrioritySuccess(QUEUE_A_USER, QUEUEA, -1);

    // Case 4: App will be submitted with priority 11.
    verifyAppSubmitWithPrioritySuccess(QUEUE_B_USER, QUEUEB, 11);
  }

  private void verifyAppSubmitWithPrioritySuccess(String submitter,
      String queueName, int priority) throws Exception {
    Priority appPriority = null;
    if (priority > 0) {
      appPriority = Priority.newInstance(priority);
    } else {
      // RM will consider default priority for the submitted user. So update
      // priority to the default value to compare.
      priority = defaultPriorityQueueA;
    }

    ApplicationSubmissionContext submissionContext = prepareForAppSubmission(
        submitter, queueName, appPriority);
    submitAppToRMWithValidAcl(submitter, submissionContext);

    // Ideally get app report here and check the priority.
    verifyAppPriorityIsAccepted(submitter, submissionContext.getApplicationId(),
        priority);
  }

  private void verifyAppSubmitWithPriorityFailure(String submitter,
      String queueName, int priority) throws Exception {
    Priority appPriority = Priority.newInstance(priority);
    ApplicationSubmissionContext submissionContext = prepareForAppSubmission(
        submitter, queueName, appPriority);
    submitAppToRMWithInValidAcl(submitter, submissionContext);
  }

  private ApplicationSubmissionContext prepareForAppSubmission(String submitter,
      String queueName, Priority priority) throws Exception {

    GetNewApplicationRequest newAppRequest = GetNewApplicationRequest
        .newInstance();

    ApplicationClientProtocol submitterClient = getRMClientForUser(submitter);
    ApplicationId applicationId = submitterClient
        .getNewApplication(newAppRequest).getApplicationId();

    Resource resource = Resources.createResource(1024);

    ContainerLaunchContext amContainerSpec = ContainerLaunchContext
        .newInstance(null, null, null, null, null, null);
    ApplicationSubmissionContext appSubmissionContext = ApplicationSubmissionContext
        .newInstance(applicationId, "applicationName", queueName, null,
            amContainerSpec, false, true, 1, resource, "applicationType");
    appSubmissionContext.setApplicationId(applicationId);
    appSubmissionContext.setQueue(queueName);
    if (null != priority) {
      appSubmissionContext.setPriority(priority);
    }

    return appSubmissionContext;
  }

  private void submitAppToRMWithValidAcl(String submitter,
      ApplicationSubmissionContext appSubmissionContext)
      throws YarnException, IOException, InterruptedException {
    ApplicationClientProtocol submitterClient = getRMClientForUser(submitter);
    SubmitApplicationRequest submitRequest = SubmitApplicationRequest
        .newInstance(appSubmissionContext);
    submitterClient.submitApplication(submitRequest);
    resourceManager.waitForState(appSubmissionContext.getApplicationId(),
        RMAppState.ACCEPTED);
  }

  private void submitAppToRMWithInValidAcl(String submitter,
      ApplicationSubmissionContext appSubmissionContext)
      throws YarnException, IOException, InterruptedException {
    ApplicationClientProtocol submitterClient = getRMClientForUser(submitter);
    SubmitApplicationRequest submitRequest = SubmitApplicationRequest
        .newInstance(appSubmissionContext);
    try {
      submitterClient.submitApplication(submitRequest);
      Assert.fail();
    } catch (YarnException ex) {
      Assert.assertTrue(ex.getCause() instanceof RemoteException);
    }
  }

  private void verifyAppPriorityIsAccepted(String submitter,
      ApplicationId applicationId, int priority)
      throws IOException, InterruptedException {
    ApplicationClientProtocol submitterClient = getRMClientForUser(submitter);

    /**
     * If priority is greater than cluster max, RM will auto set to cluster max
     * Consider this scenario as a special case.
     */
    if (priority > clusterMaxPriority) {
      priority = clusterMaxPriority;
    }

    GetApplicationReportRequest request = GetApplicationReportRequest
        .newInstance(applicationId);
    try {
      GetApplicationReportResponse response = submitterClient
          .getApplicationReport(request);
      Assert.assertEquals(response.getApplicationReport().getPriority(),
          Priority.newInstance(priority));
    } catch (YarnException e) {
      Assert.fail("Application submission should not fail.");
    }
  }

  @Override
  protected Configuration createConfiguration() {
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[]{QUEUEA, QUEUEB, QUEUEC});

    csConf.setCapacity(CapacitySchedulerConfiguration.ROOT + "." + QUEUEA, 50f);
    csConf.setCapacity(CapacitySchedulerConfiguration.ROOT + "." + QUEUEB, 25f);
    csConf.setCapacity(CapacitySchedulerConfiguration.ROOT + "." + QUEUEC, 25f);

    String[] aclsForA = new String[2];
    aclsForA[0] = QUEUE_A_USER;
    aclsForA[1] = QUEUE_A_GROUP;
    csConf.setPriorityAcls(CapacitySchedulerConfiguration.ROOT + "." + QUEUEA,
        Priority.newInstance(maxPriorityQueueA),
        Priority.newInstance(defaultPriorityQueueA), aclsForA);

    String[] aclsForB = new String[2];
    aclsForB[0] = QUEUE_B_USER;
    aclsForB[1] = QUEUE_B_GROUP;
    csConf.setPriorityAcls(CapacitySchedulerConfiguration.ROOT + "." + QUEUEB,
        Priority.newInstance(maxPriorityQueueB),
        Priority.newInstance(defaultPriorityQueueB), aclsForB);

    csConf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    csConf.set(YarnConfiguration.RM_SCHEDULER,
        CapacityScheduler.class.getName());

    return csConf;
  }
}
