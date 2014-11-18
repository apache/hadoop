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

package org.apache.hadoop.yarn.server.api.protocolrecords;

import java.util.Arrays;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RegisterNodeManagerRequestPBImpl;
import org.junit.Assert;
import org.junit.Test;

public class TestRegisterNodeManagerRequest {
  @Test
  public void testRegisterNodeManagerRequest() {
    RegisterNodeManagerRequest request =
        RegisterNodeManagerRequest.newInstance(
          NodeId.newInstance("host", 1234), 1234, Resource.newInstance(0, 0),
          "version", Arrays.asList(NMContainerStatus.newInstance(
            ContainerId.newContainerId(
              ApplicationAttemptId.newInstance(
                ApplicationId.newInstance(1234L, 1), 1), 1),
            ContainerState.RUNNING, Resource.newInstance(1024, 1), "good", -1,
            Priority.newInstance(0), 1234)), Arrays.asList(
            ApplicationId.newInstance(1234L, 1),
            ApplicationId.newInstance(1234L, 2)));

    // serialze to proto, and get request from proto
    RegisterNodeManagerRequest request1 =
        new RegisterNodeManagerRequestPBImpl(
            ((RegisterNodeManagerRequestPBImpl) request).getProto());

    // check values
    Assert.assertEquals(request1.getNMContainerStatuses().size(), request
        .getNMContainerStatuses().size());
    Assert.assertEquals(request1.getNMContainerStatuses().get(0).getContainerId(),
        request.getNMContainerStatuses().get(0).getContainerId());
    Assert.assertEquals(request1.getRunningApplications().size(), request
        .getRunningApplications().size());
    Assert.assertEquals(request1.getRunningApplications().get(0), request
        .getRunningApplications().get(0));
    Assert.assertEquals(request1.getRunningApplications().get(1), request
        .getRunningApplications().get(1));
  }
  
  @Test
  public void testRegisterNodeManagerRequestWithNullArrays() {
    RegisterNodeManagerRequest request =
        RegisterNodeManagerRequest.newInstance(NodeId.newInstance("host", 1234),
            1234, Resource.newInstance(0, 0), "version", null, null);

    // serialze to proto, and get request from proto
    RegisterNodeManagerRequest request1 =
        new RegisterNodeManagerRequestPBImpl(
            ((RegisterNodeManagerRequestPBImpl) request).getProto());

    // check values
    Assert.assertEquals(0, request1.getNMContainerStatuses().size());
    Assert.assertEquals(0, request1.getRunningApplications().size());
  }
}
