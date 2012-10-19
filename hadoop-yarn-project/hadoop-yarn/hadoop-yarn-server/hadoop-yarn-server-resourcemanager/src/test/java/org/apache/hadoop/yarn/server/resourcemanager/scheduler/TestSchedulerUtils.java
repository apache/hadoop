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

import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceRequestPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resource;
import org.junit.Test;

public class TestSchedulerUtils {

  @Test
  public void testNormalizeRequest() {
    int minMemory = 1024;
    ResourceRequest ask = new ResourceRequestPBImpl();

    // case negative memory
    ask.setCapability(Resource.createResource(-1024));
    SchedulerUtils.normalizeRequest(ask, minMemory);
    assertEquals(minMemory, ask.getCapability().getMemory());

    // case zero memory
    ask.setCapability(Resource.createResource(0));
    SchedulerUtils.normalizeRequest(ask, minMemory);
    assertEquals(minMemory, ask.getCapability().getMemory());

    // case memory is a multiple of minMemory
    ask.setCapability(Resource.createResource(2 * minMemory));
    SchedulerUtils.normalizeRequest(ask, minMemory);
    assertEquals(2 * minMemory, ask.getCapability().getMemory());

    // case memory is not a multiple of minMemory
    ask.setCapability(Resource.createResource(minMemory + 10));
    SchedulerUtils.normalizeRequest(ask, minMemory);
    assertEquals(2 * minMemory, ask.getCapability().getMemory());

  }

}
