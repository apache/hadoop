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

package org.apache.hadoop.yarn.api;

import org.junit.Assert;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerResourceIncreaseRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerResourceIncreaseRequestPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerResourceIncreaseRequestProto;
import org.junit.Test;

public class TestContainerResourceIncreaseRequest {
  @Test
  public void ContainerResourceIncreaseRequest() {
    ContainerId containerId = ContainerId
        .newInstance(ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(1234, 3), 3), 7);
    Resource resource = Resource.newInstance(1023, 3);
    ContainerResourceIncreaseRequest context = ContainerResourceIncreaseRequest
        .newInstance(containerId, resource);

    // to proto and get it back
    ContainerResourceIncreaseRequestProto proto = 
        ((ContainerResourceIncreaseRequestPBImpl) context).getProto();
    ContainerResourceIncreaseRequest contextRecover = 
        new ContainerResourceIncreaseRequestPBImpl(proto);

    // check value
    Assert.assertEquals(contextRecover.getContainerId(), containerId);
    Assert.assertEquals(contextRecover.getCapability(), resource);
  }

  @Test
  public void testResourceChangeContextWithNullField() {
    ContainerResourceIncreaseRequest context = ContainerResourceIncreaseRequest
        .newInstance(null, null);

    // to proto and get it back
    ContainerResourceIncreaseRequestProto proto = 
        ((ContainerResourceIncreaseRequestPBImpl) context).getProto();
    ContainerResourceIncreaseRequest contextRecover = 
        new ContainerResourceIncreaseRequestPBImpl(proto);

    // check value
    Assert.assertNull(contextRecover.getContainerId());
    Assert.assertNull(contextRecover.getCapability());
  }
}
