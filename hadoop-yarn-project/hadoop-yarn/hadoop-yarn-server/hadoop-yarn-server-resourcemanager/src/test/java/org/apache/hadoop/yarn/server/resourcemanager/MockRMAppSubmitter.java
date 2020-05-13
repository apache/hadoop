/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.util.Records;

/**
 * This class can submit an application to {@link MockRM}.
 */
public class MockRMAppSubmitter {

  public static RMApp submitWithMemory(long memory, MockRM mockRM)
      throws Exception {
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemorySize(memory);
    MockRMAppSubmissionData data = MockRMAppSubmissionData.Builder
        .createWithResource(resource, mockRM).build();
    return MockRMAppSubmitter.submit(mockRM, data);
  }

  public static RMApp submit(MockRM mockRM, MockRMAppSubmissionData data)
      throws Exception {
    ApplicationId appId =
        data.isAppIdProvided() ? data.getApplicationId() : null;
    ApplicationClientProtocol client = mockRM.getClientRMService();
    if (!data.isAppIdProvided()) {
      GetNewApplicationResponse resp = client.getNewApplication(Records
          .newRecord(GetNewApplicationRequest.class));
      appId = resp.getApplicationId();
    }
    SubmitApplicationRequest req = Records
        .newRecord(SubmitApplicationRequest.class);
    ApplicationSubmissionContext sub = Records
        .newRecord(ApplicationSubmissionContext.class);
    sub.setKeepContainersAcrossApplicationAttempts(data.isKeepContainers());
    sub.setApplicationId(appId);
    sub.setApplicationName(data.getName());
    sub.setMaxAppAttempts(data.getMaxAppAttempts());
    if (data.getApplicationTags() != null) {
      sub.setApplicationTags(data.getApplicationTags());
    }
    if (data.getApplicationTimeouts() != null
        && data.getApplicationTimeouts().size() > 0) {
      sub.setApplicationTimeouts(data.getApplicationTimeouts());
    }
    if (data.isUnmanaged()) {
      sub.setUnmanagedAM(true);
    }
    if (data.getQueue() != null) {
      sub.setQueue(data.getQueue());
    }
    if (data.getPriority() != null) {
      sub.setPriority(data.getPriority());
    }
    if (data.getAppNodeLabel() != null) {
      sub.setNodeLabelExpression(data.getAppNodeLabel());
    }
    sub.setApplicationType(data.getAppType());
    ContainerLaunchContext clc = Records
        .newRecord(ContainerLaunchContext.class);
    clc.setApplicationACLs(data.getAcls());
    if (data.getCredentials() != null
        && UserGroupInformation.isSecurityEnabled()) {
      DataOutputBuffer dob = new DataOutputBuffer();
      data.getCredentials().writeTokenStorageToStream(dob);
      ByteBuffer securityTokens =
          ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      clc.setTokens(securityTokens);
      clc.setTokensConf(data.getTokensConf());
    }
    sub.setAMContainerSpec(clc);
    sub.setAttemptFailuresValidityInterval(
        data.getAttemptFailuresValidityInterval());
    if (data.getLogAggregationContext() != null) {
      sub.setLogAggregationContext(data.getLogAggregationContext());
    }
    sub.setCancelTokensWhenComplete(data.isCancelTokensWhenComplete());

    Priority priority = data.getPriority();
    if (priority == null) {
      priority = Priority.newInstance(0);
    }

    List<ResourceRequest> amResourceRequests = data.getAmResourceRequests();
    if (amResourceRequests == null || amResourceRequests.isEmpty()) {
      ResourceRequest amResReq = ResourceRequest.newInstance(
          priority, ResourceRequest.ANY, data.getResource(), 1);
      amResourceRequests = Collections.singletonList(amResReq);
    }

    if (data.getAmLabel() != null && !data.getAmLabel().isEmpty()) {
      for (ResourceRequest amResourceRequest : amResourceRequests) {
        amResourceRequest.setNodeLabelExpression(data.getAmLabel().trim());
      }
    }
    sub.setAMContainerResourceRequests(amResourceRequests);

    req.setApplicationSubmissionContext(sub);
    UserGroupInformation fakeUser = UserGroupInformation
        .createUserForTesting(data.getUser(), new String[] { "someGroup" });
    PrivilegedExceptionAction<SubmitApplicationResponse> action =
        new SubmitApplicationResponsePrivilegedExceptionAction()
            .setClientReq(client, req);
    fakeUser.doAs(action);
    // make sure app is immediately available after submit
    if (data.isWaitForAccepted()) {
      mockRM.waitForState(appId, RMAppState.ACCEPTED);
    }
    RMApp rmApp = mockRM.getRMContext().getRMApps().get(appId);

    // unmanaged AM won't go to RMAppAttemptState.SCHEDULED.
    if (data.isWaitForAccepted() && !data.isUnmanaged()) {
      mockRM.waitForState(rmApp.getCurrentAppAttempt().getAppAttemptId(),
          RMAppAttemptState.SCHEDULED);
    }

    ((AbstractYarnScheduler)mockRM.getResourceScheduler()).update();

    return rmApp;
  }

  private static class SubmitApplicationResponsePrivilegedExceptionAction
      implements PrivilegedExceptionAction<SubmitApplicationResponse> {
    ApplicationClientProtocol client;
    SubmitApplicationRequest req;

    @Override
    public SubmitApplicationResponse run() throws IOException, YarnException {
      try {
        return client.submitApplication(req);
      } catch (YarnException | IOException e) {
        e.printStackTrace();
        throw  e;
      }
    }

    PrivilegedExceptionAction<SubmitApplicationResponse> setClientReq(
        ApplicationClientProtocol client, SubmitApplicationRequest req) {
      this.client = client;
      this.req = req;
      return this;
    }
  }
}
