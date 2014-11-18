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

import java.util.Arrays;

import org.junit.Assert;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerResourceIncrease;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerResourceIncreasePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerResourceIncreaseProto;
import org.junit.Test;

public class TestContainerResourceIncrease {
  @Test
  public void testResourceIncreaseContext() {
    byte[] identifier = new byte[] { 1, 2, 3, 4 };
    Token token = Token.newInstance(identifier, "", "".getBytes(), "");
    ContainerId containerId = ContainerId
        .newContainerId(ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(1234, 3), 3), 7);
    Resource resource = Resource.newInstance(1023, 3);
    ContainerResourceIncrease ctx = ContainerResourceIncrease.newInstance(
        containerId, resource, token);

    // get proto and recover to ctx
    ContainerResourceIncreaseProto proto = 
        ((ContainerResourceIncreasePBImpl) ctx).getProto();
    ctx = new ContainerResourceIncreasePBImpl(proto);

    // check values
    Assert.assertEquals(ctx.getCapability(), resource);
    Assert.assertEquals(ctx.getContainerId(), containerId);
    Assert.assertTrue(Arrays.equals(ctx.getContainerToken().getIdentifier()
        .array(), identifier));
  }
  
  @Test
  public void testResourceIncreaseContextWithNull() {
    ContainerResourceIncrease ctx = ContainerResourceIncrease.newInstance(null,
        null, null);
    
    // get proto and recover to ctx;
    ContainerResourceIncreaseProto proto = 
        ((ContainerResourceIncreasePBImpl) ctx).getProto();
    ctx = new ContainerResourceIncreasePBImpl(proto);

    // check values
    Assert.assertNull(ctx.getContainerToken());
    Assert.assertNull(ctx.getCapability());
    Assert.assertNull(ctx.getContainerId());
  }
}
