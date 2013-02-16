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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceRequestPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.server.resourcemanager.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceCalculator;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.junit.Test;

public class TestSchedulerUtils {

  @Test
  public void testNormalizeRequest() {
    ResourceCalculator resourceCalculator = new DefaultResourceCalculator();
    
    final int minMemory = 1024;
    Resource minResource = Resources.createResource(minMemory, 0);
    
    ResourceRequest ask = new ResourceRequestPBImpl();

    // case negative memory
    ask.setCapability(Resources.createResource(-1024));
    Resource before = ask.getCapability();
    SchedulerUtils.normalizeRequest(ask, resourceCalculator, null, minResource);
    Resource after = ask.getCapability();
    assertEquals(minMemory, ask.getCapability().getMemory());
    assertTrue(before == after);

    // case zero memory
    ask.setCapability(Resources.createResource(0));
    before = ask.getCapability();
    SchedulerUtils.normalizeRequest(ask, resourceCalculator, null, minResource);
    after = ask.getCapability();
    assertEquals(minMemory, ask.getCapability().getMemory());
    assertTrue(before == after);

    // case memory is a multiple of minMemory
    ask.setCapability(Resources.createResource(2 * minMemory));
    before = ask.getCapability();
    SchedulerUtils.normalizeRequest(ask, resourceCalculator, null, minResource);
    after = ask.getCapability();
    assertEquals(2 * minMemory, ask.getCapability().getMemory());
    assertTrue(before == after);

    // case memory is not a multiple of minMemory
    ask.setCapability(Resources.createResource(minMemory + 10));
    before = ask.getCapability();
    SchedulerUtils.normalizeRequest(ask, resourceCalculator, null, minResource);
    after = ask.getCapability();
    assertEquals(2 * minMemory, ask.getCapability().getMemory());
    assertTrue(before == after);

  }
  
  @Test
  public void testNormalizeRequestWithDominantResourceCalculator() {
    ResourceCalculator resourceCalculator = new DominantResourceCalculator();
    
    Resource minResource = Resources.createResource(1024, 1);
    Resource clusterResource = Resources.createResource(10 * 1024, 10);
    
    ResourceRequest ask = new ResourceRequestPBImpl();

    // case negative memory/vcores
    ask.setCapability(Resources.createResource(-1024, -1));
    Resource before = ask.getCapability();
    SchedulerUtils.normalizeRequest(
        ask, resourceCalculator, clusterResource, minResource);
    Resource after = ask.getCapability();
    assertEquals(minResource, ask.getCapability());
    assertTrue(before == after);

    // case zero memory/vcores
    ask.setCapability(Resources.createResource(0, 0));
    before = ask.getCapability();
    SchedulerUtils.normalizeRequest(
        ask, resourceCalculator, clusterResource, minResource);
    after = ask.getCapability();
    assertEquals(minResource, ask.getCapability());
    assertEquals(1, ask.getCapability().getVirtualCores());
    assertEquals(1024, ask.getCapability().getMemory());
    assertTrue(before == after);

    // case non-zero memory & zero cores
    ask.setCapability(Resources.createResource(1536, 0));
    before = ask.getCapability();
    SchedulerUtils.normalizeRequest(
        ask, resourceCalculator, clusterResource, minResource);
    after = ask.getCapability();
    assertEquals(Resources.createResource(2048, 1), ask.getCapability());
    assertEquals(1, ask.getCapability().getVirtualCores());
    assertEquals(2048, ask.getCapability().getMemory());
    assertTrue(before == after);
  }
}
