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

package org.apache.hadoop.resourceestimator.skylinestore.impl;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.resourceestimator.common.api.RecurrenceId;
import org.apache.hadoop.resourceestimator.common.api.ResourceSkyline;
import org.apache.hadoop.resourceestimator.skylinestore.api.SkylineStore;
import org.apache.hadoop.resourceestimator.skylinestore.exceptions.DuplicateRecurrenceIdException;
import org.apache.hadoop.resourceestimator.skylinestore.exceptions.EmptyResourceSkylineException;
import org.apache.hadoop.resourceestimator.skylinestore.exceptions.NullPipelineIdException;
import org.apache.hadoop.resourceestimator.skylinestore.exceptions.NullRLESparseResourceAllocationException;
import org.apache.hadoop.resourceestimator.skylinestore.exceptions.NullRecurrenceIdException;
import org.apache.hadoop.resourceestimator.skylinestore.exceptions.NullResourceSkylineException;
import org.apache.hadoop.resourceestimator.skylinestore.exceptions.RecurrenceIdNotFoundException;
import org.apache.hadoop.resourceestimator.skylinestore.exceptions.SkylineStoreException;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test {@link SkylineStore} class.
 */
public abstract class TestSkylineStore {
  /**
   * Testing variables.
   */
  private SkylineStore skylineStore;

  private TreeMap<Long, Resource> resourceOverTime;
  private RLESparseResourceAllocation skylineList;
  private ReservationInterval riAdd;
  private Resource resource;

  protected abstract SkylineStore createSkylineStore();

  @BeforeEach
  public final void setup() {
    skylineStore = createSkylineStore();
    resourceOverTime = new TreeMap<>();
    resource = Resource.newInstance(1024 * 100, 100);
  }

  private void compare(final ResourceSkyline skyline1,
      final ResourceSkyline skyline2) {
    assertEquals(skyline1.getJobId(), skyline2.getJobId());
    assertEquals(skyline1.getJobInputDataSize(),
        skyline2.getJobInputDataSize(), 0);
    assertEquals(skyline1.getJobSubmissionTime(),
        skyline2.getJobSubmissionTime());
    assertEquals(skyline1.getJobFinishTime(), skyline2.getJobFinishTime());
    assertEquals(skyline1.getContainerSpec().getMemorySize(),
        skyline2.getContainerSpec().getMemorySize());
    assertEquals(skyline1.getContainerSpec().getVirtualCores(),
        skyline2.getContainerSpec().getVirtualCores());
    assertEquals(true,
        skyline2.getSkylineList().equals(skyline1.getSkylineList()));
  }

  private void addToStore(final RecurrenceId recurrenceId,
      final ResourceSkyline resourceSkyline) throws SkylineStoreException {
    final List<ResourceSkyline> resourceSkylines = new ArrayList<>();
    resourceSkylines.add(resourceSkyline);
    skylineStore.addHistory(recurrenceId, resourceSkylines);
    final List<ResourceSkyline> resourceSkylinesGet =
        skylineStore.getHistory(recurrenceId).get(recurrenceId);
    assertTrue(resourceSkylinesGet.contains(resourceSkyline));
  }

  private ResourceSkyline getSkyline(final int n) {
    skylineList = new RLESparseResourceAllocation(resourceOverTime,
        new DefaultResourceCalculator());
    for (int i = 0; i < n; i++) {
      riAdd = new ReservationInterval(i * 10, (i + 1) * 10);
      skylineList.addInterval(riAdd, resource);
    }
    final ResourceSkyline resourceSkyline =
        new ResourceSkyline(Integer.toString(n), 1024.5, 0, 20, resource,
            skylineList);

    return resourceSkyline;
  }

  @Test public final void testGetHistory() throws SkylineStoreException {
    // addHistory first recurring pipeline
    final RecurrenceId recurrenceId1 =
        new RecurrenceId("FraudDetection", "17/06/20 00:00:00");
    final ResourceSkyline resourceSkyline1 = getSkyline(1);
    addToStore(recurrenceId1, resourceSkyline1);
    final ResourceSkyline resourceSkyline2 = getSkyline(2);
    addToStore(recurrenceId1, resourceSkyline2);
    final RecurrenceId recurrenceId2 =
        new RecurrenceId("FraudDetection", "17/06/21 00:00:00");
    final ResourceSkyline resourceSkyline3 = getSkyline(3);
    addToStore(recurrenceId2, resourceSkyline3);
    final ResourceSkyline resourceSkyline4 = getSkyline(4);
    addToStore(recurrenceId2, resourceSkyline4);
    // addHistory second recurring pipeline
    final RecurrenceId recurrenceId3 =
        new RecurrenceId("Random", "17/06/20 00:00:00");
    addToStore(recurrenceId3, resourceSkyline1);
    addToStore(recurrenceId3, resourceSkyline2);
    // test getHistory {pipelineId, runId}
    Map<RecurrenceId, List<ResourceSkyline>> jobHistory =
        skylineStore.getHistory(recurrenceId1);
    assertEquals(1, jobHistory.size());
    for (final Map.Entry<RecurrenceId, List<ResourceSkyline>> entry : jobHistory
        .entrySet()) {
      assertEquals(recurrenceId1, entry.getKey());
      final List<ResourceSkyline> getSkylines = entry.getValue();
      assertEquals(2, getSkylines.size());
      compare(resourceSkyline1, getSkylines.get(0));
      compare(resourceSkyline2, getSkylines.get(1));
    }
    // test getHistory {pipelineId, *}
    RecurrenceId recurrenceIdTest = new RecurrenceId("FraudDetection", "*");
    jobHistory = skylineStore.getHistory(recurrenceIdTest);
    assertEquals(2, jobHistory.size());
    for (final Map.Entry<RecurrenceId, List<ResourceSkyline>> entry : jobHistory
        .entrySet()) {
      assertEquals(recurrenceId1.getPipelineId(),
          entry.getKey().getPipelineId());
      final List<ResourceSkyline> getSkylines = entry.getValue();
      if (entry.getKey().getRunId().equals("17/06/20 00:00:00")) {
        assertEquals(2, getSkylines.size());
        compare(resourceSkyline1, getSkylines.get(0));
        compare(resourceSkyline2, getSkylines.get(1));
      } else {
        assertEquals(entry.getKey().getRunId(), "17/06/21 00:00:00");
        assertEquals(2, getSkylines.size());
        compare(resourceSkyline3, getSkylines.get(0));
        compare(resourceSkyline4, getSkylines.get(1));
      }
    }
    // test getHistory {*, runId}
    recurrenceIdTest = new RecurrenceId("*", "some random runId");
    jobHistory = skylineStore.getHistory(recurrenceIdTest);
    assertEquals(3, jobHistory.size());
    for (final Map.Entry<RecurrenceId, List<ResourceSkyline>> entry : jobHistory
        .entrySet()) {
      if (entry.getKey().getPipelineId().equals("FraudDetection")) {
        final List<ResourceSkyline> getSkylines = entry.getValue();
        if (entry.getKey().getRunId().equals("17/06/20 00:00:00")) {
          assertEquals(2, getSkylines.size());
          compare(resourceSkyline1, getSkylines.get(0));
          compare(resourceSkyline2, getSkylines.get(1));
        } else {
          assertEquals(entry.getKey().getRunId(), "17/06/21 00:00:00");
          assertEquals(2, getSkylines.size());
          compare(resourceSkyline3, getSkylines.get(0));
          compare(resourceSkyline4, getSkylines.get(1));
        }
      } else {
        assertEquals("Random", entry.getKey().getPipelineId());
        assertEquals(entry.getKey().getRunId(), "17/06/20 00:00:00");
        final List<ResourceSkyline> getSkylines = entry.getValue();
        assertEquals(2, getSkylines.size());
        compare(resourceSkyline1, getSkylines.get(0));
        compare(resourceSkyline2, getSkylines.get(1));
      }
    }
    // test getHistory with wrong RecurrenceId
    recurrenceIdTest =
        new RecurrenceId("some random pipelineId", "some random runId");
    assertNull(skylineStore.getHistory(recurrenceIdTest));
  }

  @Test public final void testGetEstimation() throws SkylineStoreException {
    // first, add estimation to the skyline store
    final RLESparseResourceAllocation skylineList2 =
        new RLESparseResourceAllocation(resourceOverTime,
            new DefaultResourceCalculator());
    for (int i = 0; i < 5; i++) {
      riAdd = new ReservationInterval(i * 10, (i + 1) * 10);
      skylineList2.addInterval(riAdd, resource);
    }
    skylineStore.addEstimation("FraudDetection", skylineList2);
    // then, try to get the estimation
    final RLESparseResourceAllocation estimation =
        skylineStore.getEstimation("FraudDetection");
    for (int i = 0; i < 50; i++) {
      assertEquals(skylineList2.getCapacityAtTime(i),
          estimation.getCapacityAtTime(i));
    }
  }

  @Test
  public final void testGetNullRecurrenceId()
      throws SkylineStoreException {
          assertThrows(NullRecurrenceIdException.class, () -> {
              final RecurrenceId recurrenceId1 =
        new RecurrenceId("FraudDetection", "17/06/20 00:00:00");
              final ResourceSkyline resourceSkyline1 = getSkyline(1);
              addToStore(recurrenceId1, resourceSkyline1);
              final ResourceSkyline resourceSkyline2 = getSkyline(2);
              addToStore(recurrenceId1, resourceSkyline2);
              final RecurrenceId recurrenceId2 =
        new RecurrenceId("FraudDetection", "17/06/21 00:00:00");
              final ResourceSkyline resourceSkyline3 = getSkyline(3);
              addToStore(recurrenceId2, resourceSkyline3);
              final ResourceSkyline resourceSkyline4 = getSkyline(4);
              addToStore(recurrenceId2, resourceSkyline4);
              final RecurrenceId recurrenceId3 =
        new RecurrenceId("Random", "17/06/20 00:00:00");
              addToStore(recurrenceId3, resourceSkyline1);
              addToStore(recurrenceId3, resourceSkyline2);
              skylineStore.getHistory(null);
          });


// try to getHistory with null recurringId
}

  @Test
  public final void testGetNullPipelineIdException()
      throws SkylineStoreException {
          assertThrows(NullPipelineIdException.class, () -> {
              skylineStore.getEstimation(null);
          });
      }

  @Test public final void testAddNormal() throws SkylineStoreException {
    // addHistory resource skylines to the in-memory store
    final RecurrenceId recurrenceId =
        new RecurrenceId("FraudDetection", "17/06/20 00:00:00");
    final ResourceSkyline resourceSkyline1 = getSkyline(1);
    addToStore(recurrenceId, resourceSkyline1);
    final ArrayList<ResourceSkyline> resourceSkylines =
        new ArrayList<ResourceSkyline>();
    // the resource skylines to be added contain null
    resourceSkylines.add(null);
    final ResourceSkyline resourceSkyline2 = getSkyline(2);
    resourceSkylines.add(resourceSkyline2);
    skylineStore.addHistory(recurrenceId, resourceSkylines);
    // query the in-memory store
    final Map<RecurrenceId, List<ResourceSkyline>> jobHistory =
        skylineStore.getHistory(recurrenceId);
    assertEquals(1, jobHistory.size());
    for (final Map.Entry<RecurrenceId, List<ResourceSkyline>> entry : jobHistory
        .entrySet()) {
      assertEquals(recurrenceId, entry.getKey());
      final List<ResourceSkyline> getSkylines = entry.getValue();
      assertEquals(2, getSkylines.size());
      compare(resourceSkyline1, getSkylines.get(0));
      compare(resourceSkyline2, getSkylines.get(1));
    }
  }

  @Test
  public final void testAddNullRecurrenceId()
      throws SkylineStoreException {
          assertThrows(NullRecurrenceIdException.class, () -> {
              final RecurrenceId recurrenceIdNull = null;
              final ArrayList<ResourceSkyline> resourceSkylines =
        new ArrayList<ResourceSkyline>();
              final ResourceSkyline resourceSkyline1 = getSkyline(1);
              resourceSkylines.add(resourceSkyline1);
              skylineStore.addHistory(recurrenceIdNull, resourceSkylines);
          });

}

  @Test
  public final void testAddNullResourceSkyline()
      throws SkylineStoreException {
          assertThrows(NullResourceSkylineException.class, () -> {
              final RecurrenceId recurrenceId =
        new RecurrenceId("FraudDetection", "17/06/20 00:00:00");
              final ArrayList<ResourceSkyline> resourceSkylines =
        new ArrayList<ResourceSkyline>();
              final ResourceSkyline resourceSkyline1 = getSkyline(1);
              resourceSkylines.add(resourceSkyline1);
              skylineStore.addHistory(recurrenceId, null);
          });
      // resourceSkylines is null
}

  @Test
  public final void testAddDuplicateRecurrenceId()
      throws SkylineStoreException {
          assertThrows(DuplicateRecurrenceIdException.class, () -> {
              final RecurrenceId recurrenceId =
        new RecurrenceId("FraudDetection", "17/06/20 00:00:00");
              final ArrayList<ResourceSkyline> resourceSkylines =
        new ArrayList<ResourceSkyline>();
              final ResourceSkyline resourceSkyline1 = getSkyline(1);
              resourceSkylines.add(resourceSkyline1);
              skylineStore.addHistory(recurrenceId, resourceSkylines);
              skylineStore.addHistory(recurrenceId, resourceSkylines);
          });

}

  @Test
  public final void testAddNullPipelineIdException()
      throws SkylineStoreException {
          assertThrows(NullPipelineIdException.class, () -> {
              final RLESparseResourceAllocation skylineList2 =
        new RLESparseResourceAllocation(resourceOverTime,
            new DefaultResourceCalculator());
              for (int i = 0; i < 5; i++) {
      riAdd = new ReservationInterval(i * 10, (i + 1) * 10);
      skylineList2.addInterval(riAdd, resource);
    }
              skylineStore.addEstimation(null, skylineList2);
          });
      }

  @Test
  public final void testAddNullRLESparseResourceAllocationExceptionException()
      throws SkylineStoreException {
          assertThrows(NullRLESparseResourceAllocationException.class, () -> {
              skylineStore.addEstimation("FraudDetection", null);
          });
      }

  @Test public final void testDeleteNormal() throws SkylineStoreException {
    // addHistory first recurring pipeline
    final RecurrenceId recurrenceId1 =
        new RecurrenceId("FraudDetection", "17/06/20 00:00:00");
    final ResourceSkyline resourceSkyline1 = getSkyline(1);
    addToStore(recurrenceId1, resourceSkyline1);
    final ResourceSkyline resourceSkyline2 = getSkyline(2);
    addToStore(recurrenceId1, resourceSkyline2);
    // test deleteHistory function of the in-memory store
    skylineStore.deleteHistory(recurrenceId1);
  }

  @Test
  public final void testDeleteNullRecurrenceId()
      throws SkylineStoreException {
          assertThrows(NullRecurrenceIdException.class, () -> {
              final RecurrenceId recurrenceId1 =
        new RecurrenceId("FraudDetection", "17/06/20 00:00:00");
              final ResourceSkyline resourceSkyline1 = getSkyline(1);
              addToStore(recurrenceId1, resourceSkyline1);
              skylineStore.deleteHistory(null);
          });
      // try to deleteHistory with null recurringId
}

  @Test
  public final void testDeleteRecurrenceIdNotFound()
      throws SkylineStoreException {
          assertThrows(RecurrenceIdNotFoundException.class, () -> {
              final RecurrenceId recurrenceId1 =
        new RecurrenceId("FraudDetection", "17/06/20 00:00:00");
              final ResourceSkyline resourceSkyline1 = getSkyline(1);
              addToStore(recurrenceId1, resourceSkyline1);
              final RecurrenceId recurrenceIdInvalid =
        new RecurrenceId("Some random pipelineId", "Some random runId");
              skylineStore.deleteHistory(recurrenceIdInvalid);
          });
      // try to deleteHistory non-existing recurringId
}

  @Test public final void testUpdateNormal() throws SkylineStoreException {
    // addHistory first recurring pipeline
    final RecurrenceId recurrenceId1 =
        new RecurrenceId("FraudDetection", "17/06/20 00:00:00");
    final ResourceSkyline resourceSkyline1 = getSkyline(1);
    addToStore(recurrenceId1, resourceSkyline1);
    final ArrayList<ResourceSkyline> resourceSkylines =
        new ArrayList<ResourceSkyline>();
    final ResourceSkyline resourceSkyline2 = getSkyline(2);
    resourceSkylines.add(resourceSkyline1);
    resourceSkylines.add(resourceSkyline2);
    skylineStore.updateHistory(recurrenceId1, resourceSkylines);
    // query the in-memory store
    final Map<RecurrenceId, List<ResourceSkyline>> jobHistory =
        skylineStore.getHistory(recurrenceId1);
    assertEquals(1, jobHistory.size());
    for (final Map.Entry<RecurrenceId, List<ResourceSkyline>> entry : jobHistory
        .entrySet()) {
      assertEquals(recurrenceId1, entry.getKey());
      final List<ResourceSkyline> getSkylines = entry.getValue();
      assertEquals(2, getSkylines.size());
      compare(resourceSkyline1, getSkylines.get(0));
      compare(resourceSkyline2, getSkylines.get(1));
    }
  }

  @Test
  public final void testUpdateNullRecurrenceId()
      throws SkylineStoreException {
          assertThrows(NullRecurrenceIdException.class, () -> {
              final ArrayList<ResourceSkyline> resourceSkylines =
        new ArrayList<ResourceSkyline>();
              final ResourceSkyline resourceSkyline1 = getSkyline(1);
              resourceSkylines.add(resourceSkyline1);
              final ArrayList<ResourceSkyline> resourceSkylinesInvalid =
        new ArrayList<ResourceSkyline>();
              resourceSkylinesInvalid.add(null);
              skylineStore.updateHistory(null, resourceSkylines);
          });
      // try to updateHistory with null recurringId
}

  @Test
  public final void testUpdateNullResourceSkyline()
      throws SkylineStoreException {
          assertThrows(NullResourceSkylineException.class, () -> {
              final RecurrenceId recurrenceId =
        new RecurrenceId("FraudDetection", "17/06/20 00:00:00");
              final ArrayList<ResourceSkyline> resourceSkylines =
        new ArrayList<ResourceSkyline>();
              final ResourceSkyline resourceSkyline1 = getSkyline(1);
              resourceSkylines.add(resourceSkyline1);
              final ArrayList<ResourceSkyline> resourceSkylinesInvalid =
        new ArrayList<ResourceSkyline>();
              resourceSkylinesInvalid.add(null);
              skylineStore.addHistory(recurrenceId, resourceSkylines);
              skylineStore.updateHistory(recurrenceId, null);
          });

}

  @Test
  public final void testUpdateEmptyRecurrenceId()
      throws SkylineStoreException {
          assertThrows(EmptyResourceSkylineException.class, () -> {
              final RecurrenceId recurrenceId =
        new RecurrenceId("FraudDetection", "17/06/20 00:00:00");
              final ArrayList<ResourceSkyline> resourceSkylines =
        new ArrayList<ResourceSkyline>();
              final ResourceSkyline resourceSkyline1 = getSkyline(1);
              resourceSkylines.add(resourceSkyline1);
              final ArrayList<ResourceSkyline> resourceSkylinesInvalid =
        new ArrayList<ResourceSkyline>();
              resourceSkylinesInvalid.add(null);
              skylineStore.addHistory(recurrenceId, resourceSkylines);
              skylineStore.updateHistory(recurrenceId, resourceSkylinesInvalid);
          });
      // try to updateHistory with empty resourceSkyline
}

  @Test
  public final void testUpdateRecurrenceIdNotFound()
      throws SkylineStoreException {
          assertThrows(RecurrenceIdNotFoundException.class, () -> {
              final ArrayList<ResourceSkyline> resourceSkylines =
        new ArrayList<ResourceSkyline>();
              final ResourceSkyline resourceSkyline1 = getSkyline(1);
              resourceSkylines.add(resourceSkyline1);
              final RecurrenceId recurrenceIdInvalid =
        new RecurrenceId("Some random pipelineId", "Some random runId");
              final ArrayList<ResourceSkyline> resourceSkylinesInvalid =
        new ArrayList<ResourceSkyline>();
              resourceSkylinesInvalid.add(null);
              skylineStore.updateHistory(recurrenceIdInvalid, resourceSkylines);
          });
      // try to updateHistory with non-existing recurringId
}

  @AfterEach
  public final void cleanUp() {
    skylineStore = null;
    resourceOverTime.clear();
    resourceOverTime = null;
    skylineList = null;
    riAdd = null;
    resource = null;
  }
}
