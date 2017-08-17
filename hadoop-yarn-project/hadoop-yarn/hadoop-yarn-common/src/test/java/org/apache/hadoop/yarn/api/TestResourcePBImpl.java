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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class to handle various proto related tests for resources.
 */
public class TestResourcePBImpl {
  @Test
  public void testEmptyResourcePBInit() throws Exception {
    Resource res = new ResourcePBImpl();
    // Assert to check it sets resource value and unit to default.
    Assert.assertEquals(0, res.getMemorySize());
    Assert.assertEquals(ResourceInformation.MEMORY_MB.getUnits(),
        res.getResourceInformation(ResourceInformation.MEMORY_MB.getName())
            .getUnits());
    Assert.assertEquals(ResourceInformation.VCORES.getUnits(),
        res.getResourceInformation(ResourceInformation.VCORES.getName())
            .getUnits());
  }

  @Test
  public void testResourcePBInitFromOldPB() throws Exception {
    YarnProtos.ResourceProto proto =
        YarnProtos.ResourceProto.newBuilder().setMemory(1024).setVirtualCores(3)
            .build();
    // Assert to check it sets resource value and unit to default.
    Resource res = new ResourcePBImpl(proto);
    Assert.assertEquals(1024, res.getMemorySize());
    Assert.assertEquals(3, res.getVirtualCores());
    Assert.assertEquals(ResourceInformation.MEMORY_MB.getUnits(),
        res.getResourceInformation(ResourceInformation.MEMORY_MB.getName())
            .getUnits());
    Assert.assertEquals(ResourceInformation.VCORES.getUnits(),
        res.getResourceInformation(ResourceInformation.VCORES.getName())
            .getUnits());
  }
}
