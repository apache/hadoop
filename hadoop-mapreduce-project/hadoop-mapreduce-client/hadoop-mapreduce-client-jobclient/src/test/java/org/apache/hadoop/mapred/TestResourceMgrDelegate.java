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

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TestResourceMgrDelegate {

  /**
   * Tests that getRootQueues makes a request for the (recursive) child queues
   */
@Test
  public void testGetRootQueues() throws IOException, InterruptedException {
    ClientRMProtocol applicationsManager = Mockito.mock(ClientRMProtocol.class);
    GetQueueInfoResponse response = Mockito.mock(GetQueueInfoResponse.class);
    org.apache.hadoop.yarn.api.records.QueueInfo queueInfo =
      Mockito.mock(org.apache.hadoop.yarn.api.records.QueueInfo.class);
    Mockito.when(response.getQueueInfo()).thenReturn(queueInfo);
    Mockito.when(applicationsManager.getQueueInfo(Mockito.any(
      GetQueueInfoRequest.class))).thenReturn(response);

    ResourceMgrDelegate delegate = new ResourceMgrDelegate(
      new YarnConfiguration(), applicationsManager);
    delegate.getRootQueues();

    ArgumentCaptor<GetQueueInfoRequest> argument =
      ArgumentCaptor.forClass(GetQueueInfoRequest.class);
    Mockito.verify(delegate.applicationsManager).getQueueInfo(
      argument.capture());

    Assert.assertTrue("Children of root queue not requested",
      argument.getValue().getIncludeChildQueues());
    Assert.assertTrue("Request wasn't to recurse through children",
      argument.getValue().getRecursive());
  }

}
