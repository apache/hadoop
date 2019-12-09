/*
 *
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
 *
 */

package org.apache.hadoop.resourceestimator.common.serialization;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.resourceestimator.common.api.RecurrenceId;
import org.apache.hadoop.resourceestimator.common.api.ResourceSkyline;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

/**
 * Test HistorySkylineSerDe.
 */
public class TestHistorySkylineSerDe {
  /**
   * Testing variables.
   */
  private Gson gson;

  private ResourceSkyline resourceSkyline;
  private Resource resource;
  private Resource resource2;
  private TreeMap<Long, Resource> resourceOverTime;
  private RLESparseResourceAllocation skylineList;

  @Before public final void setup() {
    resourceOverTime = new TreeMap<>();
    skylineList = new RLESparseResourceAllocation(resourceOverTime,
        new DefaultResourceCalculator());
    resource = Resource.newInstance(1024 * 100, 100);
    resource2 = Resource.newInstance(1024 * 200, 200);
    gson = new GsonBuilder()
        .registerTypeAdapter(Resource.class, new ResourceSerDe())
        .registerTypeAdapter(RLESparseResourceAllocation.class,
            new RLESparseResourceAllocationSerDe())
        .enableComplexMapKeySerialization().create();
  }

  @Test public final void testSerialization() {
    ReservationInterval riAdd = new ReservationInterval(0, 10);
    skylineList.addInterval(riAdd, resource);
    riAdd = new ReservationInterval(10, 20);
    skylineList.addInterval(riAdd, resource2);
    resourceSkyline =
        new ResourceSkyline("1", 1024.5, 0, 20, resource, skylineList);
    RecurrenceId recurrenceId = new RecurrenceId("FraudDetection", "1");
    List<ResourceSkyline> listSkyline = new ArrayList<>();
    listSkyline.add(resourceSkyline);
    Map<RecurrenceId, List<ResourceSkyline>> historySkyline =
        new HashMap<RecurrenceId, List<ResourceSkyline>>();
    historySkyline.put(recurrenceId, listSkyline);

    final String json = gson.toJson(historySkyline,
        new TypeToken<Map<RecurrenceId, List<ResourceSkyline>>>() {
        }.getType());
    final Map<RecurrenceId, List<ResourceSkyline>> historySkylineDe =
        gson.fromJson(json,
            new TypeToken<Map<RecurrenceId, List<ResourceSkyline>>>() {
            }.getType());
    // check if the recurrenceId is correct
    List<ResourceSkyline> resourceSkylineList =
        historySkylineDe.get(recurrenceId);
    Assert.assertNotNull(resourceSkylineList);
    Assert.assertEquals(1, resourceSkylineList.size());

    // check if the resourceSkyline is correct
    ResourceSkyline resourceSkylineDe = resourceSkylineList.get(0);
    Assert
        .assertEquals(resourceSkylineDe.getJobId(), resourceSkyline.getJobId());
    Assert.assertEquals(resourceSkylineDe.getJobInputDataSize(),
        resourceSkyline.getJobInputDataSize(), 0);
    Assert.assertEquals(resourceSkylineDe.getJobSubmissionTime(),
        resourceSkyline.getJobSubmissionTime());
    Assert.assertEquals(resourceSkylineDe.getJobFinishTime(),
        resourceSkyline.getJobFinishTime());
    Assert.assertEquals(resourceSkylineDe.getContainerSpec().getMemorySize(),
        resourceSkyline.getContainerSpec().getMemorySize());
    Assert.assertEquals(resourceSkylineDe.getContainerSpec().getVirtualCores(),
        resourceSkyline.getContainerSpec().getVirtualCores());
    final RLESparseResourceAllocation skylineList2 =
        resourceSkyline.getSkylineList();
    final RLESparseResourceAllocation skylineListDe =
        resourceSkylineDe.getSkylineList();
    for (int i = 0; i < 20; i++) {
      Assert.assertEquals(skylineList2.getCapacityAtTime(i).getMemorySize(),
          skylineListDe.getCapacityAtTime(i).getMemorySize());
      Assert.assertEquals(skylineList2.getCapacityAtTime(i).getVirtualCores(),
          skylineListDe.getCapacityAtTime(i).getVirtualCores());
    }
  }

  @After public final void cleanUp() {
    gson = null;
    resourceSkyline = null;
    resourceOverTime.clear();
    resourceOverTime = null;
    resource = null;
    resource2 = null;
    skylineList = null;
  }
}
