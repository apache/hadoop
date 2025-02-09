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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestResourceMgrDelegate {

  /**
   * Tests that getRootQueues makes a request for the (recursive) child queues
   * @throws IOException
   */
  @Test
  public void testGetRootQueues() throws IOException, InterruptedException {
    final ApplicationClientProtocol applicationsManager = mock(ApplicationClientProtocol.class);
    GetQueueInfoResponse response = mock(GetQueueInfoResponse.class);
    org.apache.hadoop.yarn.api.records.QueueInfo queueInfo =
        mock(org.apache.hadoop.yarn.api.records.QueueInfo.class);
    when(response.getQueueInfo()).thenReturn(queueInfo);
    try {
      when(applicationsManager.getQueueInfo(any(
        GetQueueInfoRequest.class))).thenReturn(response);
    } catch (YarnException e) {
      throw new IOException(e);
    }

    ResourceMgrDelegate delegate = new ResourceMgrDelegate(
      new YarnConfiguration()) {
      @Override
      protected void serviceStart() throws Exception {
        assertTrue(this.client instanceof YarnClientImpl);
        ((YarnClientImpl) this.client).setRMClient(applicationsManager);
      }
    };
    delegate.getRootQueues();

    ArgumentCaptor<GetQueueInfoRequest> argument =
      ArgumentCaptor.forClass(GetQueueInfoRequest.class);
    try {
      verify(applicationsManager).getQueueInfo(
        argument.capture());
    } catch (YarnException e) {
      throw new IOException(e);
    }

    assertTrue(argument.getValue().getIncludeChildQueues(),
        "Children of root queue not requested");
    assertTrue(argument.getValue().getRecursive(),
        "Request wasn't to recurse through children");
  }

  @Test
  public void tesAllJobs() throws Exception {
    final ApplicationClientProtocol applicationsManager =
        mock(ApplicationClientProtocol.class);
    GetApplicationsResponse allApplicationsResponse = Records
        .newRecord(GetApplicationsResponse.class);
    List<ApplicationReport> applications = new ArrayList<ApplicationReport>();
    applications.add(getApplicationReport(YarnApplicationState.FINISHED,
        FinalApplicationStatus.FAILED));
    applications.add(getApplicationReport(YarnApplicationState.FINISHED,
        FinalApplicationStatus.SUCCEEDED));
    applications.add(getApplicationReport(YarnApplicationState.FINISHED,
        FinalApplicationStatus.KILLED));
    applications.add(getApplicationReport(YarnApplicationState.FAILED,
        FinalApplicationStatus.FAILED));
    allApplicationsResponse.setApplicationList(applications);
    when(applicationsManager.getApplications(any(GetApplicationsRequest.class)))
        .thenReturn(allApplicationsResponse);
    ResourceMgrDelegate resourceMgrDelegate = new ResourceMgrDelegate(
      new YarnConfiguration()) {
      @Override
      protected void serviceStart() throws Exception {
        assertTrue(this.client instanceof YarnClientImpl);
        ((YarnClientImpl) this.client).setRMClient(applicationsManager);
      }
    };
    JobStatus[] allJobs = resourceMgrDelegate.getAllJobs();

    assertEquals(State.FAILED, allJobs[0].getState());
    assertEquals(State.SUCCEEDED, allJobs[1].getState());
    assertEquals(State.KILLED, allJobs[2].getState());
    assertEquals(State.FAILED, allJobs[3].getState());
  }

  private ApplicationReport getApplicationReport(
      YarnApplicationState yarnApplicationState,
      FinalApplicationStatus finalApplicationStatus) {
    ApplicationReport appReport = mock(ApplicationReport.class);
    ApplicationResourceUsageReport appResources = mock(ApplicationResourceUsageReport.class);
    when(appReport.getApplicationId()).thenReturn(
        ApplicationId.newInstance(0, 0));
    when(appResources.getNeededResources()).thenReturn(
        Records.newRecord(Resource.class));
    when(appResources.getReservedResources()).thenReturn(
        Records.newRecord(Resource.class));
    when(appResources.getUsedResources()).thenReturn(
        Records.newRecord(Resource.class));
    when(appReport.getApplicationResourceUsageReport()).thenReturn(
        appResources);
    when(appReport.getYarnApplicationState()).thenReturn(
        yarnApplicationState);
    when(appReport.getFinalApplicationStatus()).thenReturn(
        finalApplicationStatus);

    return appReport;
  }
}
