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

package org.apache.hadoop.resourceestimator.translator.api;

import java.text.ParseException;

import org.apache.hadoop.resourceestimator.common.api.RecurrenceId;
import org.apache.hadoop.resourceestimator.translator.impl.LogParserUtil;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test JobMetaData.
 */
public class TestJobMetaData {
  /**
   * TODO: parametrize this test.
   */
  private LogParserUtil logParserUtil = new LogParserUtil();

  private JobMetaData jobMetaData;
  private RecurrenceId recurrenceId;

  @Before public final void setup() throws ParseException {
    recurrenceId = new RecurrenceId("Fraud Detection", "17/07/16 16:27:25");
    jobMetaData = new JobMetaData(
        logParserUtil.stringToUnixTimestamp("17/07/16 16:27:25"));
    jobMetaData.setRecurrenceId(recurrenceId);
    jobMetaData.setContainerStart("C1",
        logParserUtil.stringToUnixTimestamp("17/07/16 16:27:30"));
    jobMetaData.setContainerEnd("C1",
        logParserUtil.stringToUnixTimestamp("17/07/16 16:37:30"));
    jobMetaData.setContainerStart("C2",
        logParserUtil.stringToUnixTimestamp("17/07/16 16:27:40"));
    jobMetaData.setContainerEnd("C2",
        logParserUtil.stringToUnixTimestamp("17/07/16 16:37:40"));
    jobMetaData.setJobFinishTime(
        logParserUtil.stringToUnixTimestamp("17/07/16 16:37:45"));
    final Resource containerAlloc = Resource.newInstance(1, 1);
    jobMetaData.getResourceSkyline().setContainerSpec(containerAlloc);
    jobMetaData.getResourceSkyline().setJobInputDataSize(1024.5);
    jobMetaData.createSkyline();
  }

  @Test public final void testGetContainerSpec() {
    final Resource containerAlloc =
        jobMetaData.getResourceSkyline().getContainerSpec();
    final Resource containerAlloc2 = Resource.newInstance(1, 1);
    Assert.assertEquals(containerAlloc.getMemorySize(),
        containerAlloc2.getMemorySize());
    Assert.assertEquals(containerAlloc.getVirtualCores(),
        containerAlloc2.getVirtualCores());
  }

  @Test public final void testGetJobSize() {
    Assert.assertEquals(jobMetaData.getResourceSkyline().getJobInputDataSize(),
        1024.5, 0);
  }

  @Test public final void testGetRecurrenceeId() {
    final RecurrenceId recurrenceIdTest =
        new RecurrenceId("Fraud Detection", "17/07/16 16:27:25");
    Assert.assertEquals(recurrenceIdTest, jobMetaData.getRecurrenceId());
  }

  @Test public final void testStringToUnixTimestamp() throws ParseException {
    final long submissionTime =
        logParserUtil.stringToUnixTimestamp("17/07/16 16:27:25");
    Assert.assertEquals(jobMetaData.getResourceSkyline().getJobSubmissionTime(),
        submissionTime);
  }

  @Test public final void testResourceSkyline() {
    final RLESparseResourceAllocation skylineList =
        jobMetaData.getResourceSkyline().getSkylineList();
    final int containerCPU =
        jobMetaData.getResourceSkyline().getContainerSpec().getVirtualCores();
    int k;
    for (k = 0; k < 5; k++) {
      Assert.assertEquals(0,
          skylineList.getCapacityAtTime(k).getVirtualCores() / containerCPU);
    }
    for (k = 5; k < 15; k++) {
      Assert.assertEquals(1,
          skylineList.getCapacityAtTime(k).getVirtualCores() / containerCPU);
    }
    for (k = 15; k < 605; k++) {
      Assert.assertEquals(2,
          skylineList.getCapacityAtTime(k).getVirtualCores() / containerCPU);
    }
    for (k = 605; k < 615; k++) {
      Assert.assertEquals(1,
          skylineList.getCapacityAtTime(k).getVirtualCores() / containerCPU);
    }
    Assert.assertEquals(0,
        skylineList.getCapacityAtTime(615).getVirtualCores() / containerCPU);
  }

  @Test public final void testContainerReleaseTimeMissing()
      throws ParseException {
    // create an invalid JobMetaData
    recurrenceId = new RecurrenceId("Fraud Detection", "17/07/16 16:27:25");
    jobMetaData = new JobMetaData(
        logParserUtil.stringToUnixTimestamp("17/07/16 16:27:25"));
    jobMetaData.setRecurrenceId(recurrenceId);
    jobMetaData.setContainerStart("C1",
        logParserUtil.stringToUnixTimestamp("17/07/16 16:27:30"));
    jobMetaData.setContainerEnd("C1",
        logParserUtil.stringToUnixTimestamp("17/07/16 16:37:30"));
    jobMetaData.setContainerStart("C2",
        logParserUtil.stringToUnixTimestamp("17/07/16 16:27:40"));
    jobMetaData.setJobFinishTime(
        logParserUtil.stringToUnixTimestamp("17/07/16 16:37:45"));
    final Resource containerAlloc = Resource.newInstance(1, 1);
    jobMetaData.getResourceSkyline().setContainerSpec(containerAlloc);
    jobMetaData.getResourceSkyline().setJobInputDataSize(1024.5);
    jobMetaData.createSkyline();
    // test the generated ResourceSkyline
    final RLESparseResourceAllocation skylineList =
        jobMetaData.getResourceSkyline().getSkylineList();
    final int containerCPU =
        jobMetaData.getResourceSkyline().getContainerSpec().getVirtualCores();
    int k;
    for (k = 0; k < 5; k++) {
      Assert.assertEquals(0,
          skylineList.getCapacityAtTime(k).getVirtualCores() / containerCPU);
    }
    for (k = 5; k < 605; k++) {
      Assert.assertEquals(1,
          skylineList.getCapacityAtTime(k).getVirtualCores() / containerCPU);
    }
    Assert.assertEquals(0,
        skylineList.getCapacityAtTime(605).getVirtualCores() / containerCPU);
  }

  @After public final void cleanUp() {
    jobMetaData = null;
    recurrenceId = null;
    logParserUtil = null;
  }
}
