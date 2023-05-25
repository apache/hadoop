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

package org.apache.hadoop.mapreduce.v2.app.rm;

import org.apache.hadoop.yarn.api.records.Resource;
import org.junit.Assert;
import org.junit.Test;

import java.util.EnumSet;

import static org.apache.hadoop.yarn.proto.YarnServiceProtos.*;

public class TestResourceCalculatorUtils {
  @Test
  public void testComputeAvailableContainers() throws Exception {
    Resource clusterAvailableResources = Resource.newInstance(81920, 40);

    Resource nonZeroResource = Resource.newInstance(1024, 2);

    int expectedNumberOfContainersForMemory = 80;
    int expectedNumberOfContainersForCPU = 20;

    verifyDifferentResourceTypes(clusterAvailableResources, nonZeroResource,
        expectedNumberOfContainersForMemory,
        expectedNumberOfContainersForCPU);

    Resource zeroMemoryResource = Resource.newInstance(0,
        nonZeroResource.getVirtualCores());

    verifyDifferentResourceTypes(clusterAvailableResources, zeroMemoryResource,
        Integer.MAX_VALUE,
        expectedNumberOfContainersForCPU);

    Resource zeroCpuResource = Resource.newInstance(
        nonZeroResource.getMemorySize(), 0);

    verifyDifferentResourceTypes(clusterAvailableResources, zeroCpuResource,
        expectedNumberOfContainersForMemory,
        expectedNumberOfContainersForMemory);
  }

  private void verifyDifferentResourceTypes(Resource clusterAvailableResources,
      Resource nonZeroResource, int expectedNumberOfContainersForMemoryOnly,
      int expectedNumberOfContainersOverall) {

    Assert.assertEquals("Incorrect number of available containers for Memory",
        expectedNumberOfContainersForMemoryOnly,
        ResourceCalculatorUtils.computeAvailableContainers(
            clusterAvailableResources, nonZeroResource,
            EnumSet.of(SchedulerResourceTypes.MEMORY)));

    Assert.assertEquals("Incorrect number of available containers overall",
        expectedNumberOfContainersOverall,
        ResourceCalculatorUtils.computeAvailableContainers(
            clusterAvailableResources, nonZeroResource,
            EnumSet.of(SchedulerResourceTypes.CPU,
                SchedulerResourceTypes.MEMORY)));
  }
}
