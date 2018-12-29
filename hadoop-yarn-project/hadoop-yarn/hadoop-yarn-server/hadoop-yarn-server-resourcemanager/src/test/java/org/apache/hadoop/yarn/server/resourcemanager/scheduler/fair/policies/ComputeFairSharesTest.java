/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FakeSchedulable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.Schedulable;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

public class ComputeFairSharesTest {

  @Test(timeout = 10000)
  public void testResourceUsedWithWeightToResourceRatio() throws Exception {
    Collection<Schedulable> schedulables = new ArrayList<>();

    schedulables.add(new FakeSchedulable(Integer.MAX_VALUE));
    schedulables.add(new FakeSchedulable(Integer.MAX_VALUE));

    Resource totalResource = Resource.newInstance(Integer.MAX_VALUE, 0);
    ComputeFairShares.computeShares(schedulables, totalResource, ResourceInformation.MEMORY_URI);
  }
}
