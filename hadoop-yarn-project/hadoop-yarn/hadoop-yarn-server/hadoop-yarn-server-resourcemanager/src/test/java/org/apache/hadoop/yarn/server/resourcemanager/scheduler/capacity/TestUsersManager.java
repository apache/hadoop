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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestUsersManager {
  private static final Resource CLUSTER_RESOURCE =
      Resource.newInstance(16384, 16);
  private static final Resource MINIMUM_ALLOCATION =
      Resource.newInstance(1024, 1);
  private static final Resource MAX_RESOURCE_LIMIT =
      Resource.newInstance(9216, 1);
  private static final Resource NON_ZERO_CAPACITY =
      Resource.newInstance(8192, 1);
  private static final String TEST_USER = "test";

  private UsersManager usersManager;

  @Mock
  private AutoCreatedLeafQueue lQueue;

  @Mock
  private RMNodeLabelsManager labelMgr;

  @Mock
  private QueueMetrics metrics;

  @Before
  public void setup() {
    usersManager = new UsersManager(metrics,
        lQueue,
        labelMgr,
        new DefaultResourceCalculator());

    when(lQueue.getMinimumAllocation()).thenReturn(MINIMUM_ALLOCATION);
    when(lQueue.getEffectiveMaxCapacityDown(anyString(), any(Resource.class)))
        .thenReturn(MAX_RESOURCE_LIMIT);
    when(labelMgr.getResourceByLabel(anyString(), any(Resource.class)))
        .thenReturn(CLUSTER_RESOURCE);
    usersManager.setUsageRatio(CommonNodeLabelsManager.NO_LABEL, 0.5f);
    usersManager.setUserLimit(
        CapacitySchedulerConfiguration.DEFAULT_USER_LIMIT);
    usersManager.setUserLimitFactor(
        CapacitySchedulerConfiguration.DEFAULT_USER_LIMIT_FACTOR);
  }

  @Test
  public void testComputeUserLimitWithZeroCapacityQueue() {
    when(lQueue.getEffectiveCapacity(anyString()))
        .thenReturn(Resources.none());

    checkLimit(MAX_RESOURCE_LIMIT);
  }

  @Test
  public void testComputeUserLimitWithNonZeroCapacityQueue() {
    when(lQueue.getEffectiveCapacity(anyString()))
        .thenReturn(NON_ZERO_CAPACITY);

    checkLimit(NON_ZERO_CAPACITY);
  }

  private void checkLimit(Resource expectedLimit) {
    Resource limit = usersManager.computeUserLimit(TEST_USER,
        CLUSTER_RESOURCE,
        CommonNodeLabelsManager.NO_LABEL,
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY,
        true);

    assertEquals("User limit", expectedLimit, limit);
  }
}
