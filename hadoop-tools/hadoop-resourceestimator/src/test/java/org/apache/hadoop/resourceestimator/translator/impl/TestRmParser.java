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

package org.apache.hadoop.resourceestimator.translator.impl;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.resourceestimator.common.api.RecurrenceId;
import org.apache.hadoop.resourceestimator.common.api.ResourceSkyline;
import org.apache.hadoop.resourceestimator.common.config.ResourceEstimatorConfiguration;
import org.apache.hadoop.resourceestimator.common.exception.ResourceEstimatorException;
import org.apache.hadoop.resourceestimator.skylinestore.api.SkylineStore;
import org.apache.hadoop.resourceestimator.skylinestore.exceptions.SkylineStoreException;
import org.apache.hadoop.resourceestimator.skylinestore.impl.InMemoryStore;
import org.apache.hadoop.resourceestimator.translator.api.LogParser;
import org.apache.hadoop.resourceestimator.translator.exceptions.DataFieldNotFoundException;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * This sample parser will parse the sample log and extract the resource
 * skyline.
 */
public class TestRmParser {
  private LogParserUtil logParserUtil = new LogParserUtil();
  private SkylineStore skylineStore;

  @Before public final void setup() throws ResourceEstimatorException {
    skylineStore = new InMemoryStore();
    final LogParser rmParser = new BaseLogParser();
    Configuration config = new Configuration();
    config.addResource(new org.apache.hadoop.fs.Path(
        ResourceEstimatorConfiguration.CONFIG_FILE));
    config.set(ResourceEstimatorConfiguration.TRANSLATOR_LINE_PARSER,
        RmSingleLineParser.class.getName());
    rmParser.init(config, skylineStore);
    logParserUtil.setLogParser(rmParser);
  }

  private void parseFile(final String logFile)
      throws SkylineStoreException, IOException, ParseException,
      ResourceEstimatorException, DataFieldNotFoundException {
    logParserUtil.parseLog(logFile);
  }

  @Test public final void testParse()
      throws SkylineStoreException, IOException, ParseException,
      ResourceEstimatorException, DataFieldNotFoundException {
    final String logFile = "src/test/resources/trace/rmLog.txt";
    parseFile(logFile);
    final RecurrenceId recurrenceId = new RecurrenceId("FraudDetection", "1");
    final Map<RecurrenceId, List<ResourceSkyline>> jobSkylineLists =
        skylineStore.getHistory(recurrenceId);
    Assert.assertEquals(1, jobSkylineLists.size());
    final List<ResourceSkyline> jobHistory = jobSkylineLists.get(recurrenceId);
    Assert.assertEquals(1, jobHistory.size());
    final ResourceSkyline resourceSkyline = jobHistory.get(0);
    Assert.assertEquals(0, resourceSkyline.getJobInputDataSize(), 0);
    Assert.assertEquals("application_1497832133857_0330",
        resourceSkyline.getJobId());
    Assert.assertEquals(
        logParserUtil.stringToUnixTimestamp("06/21/2017 16:10:13"),
        resourceSkyline.getJobSubmissionTime());
    Assert.assertEquals(
        logParserUtil.stringToUnixTimestamp("06/21/2017 16:18:35"),
        resourceSkyline.getJobFinishTime());
    final Resource resource = Resource.newInstance(1800, 1);
    Assert.assertEquals(resource.getMemorySize(),
        resourceSkyline.getContainerSpec().getMemorySize());
    Assert.assertEquals(resource.getVirtualCores(),
        resourceSkyline.getContainerSpec().getVirtualCores());
    final RLESparseResourceAllocation skylineLists =
        resourceSkyline.getSkylineList();

    int k;
    for (k = 0; k < 142; k++) {
      Assert.assertEquals(1,
          skylineLists.getCapacityAtTime(k).getMemorySize() / resource
              .getMemorySize());
    }
    for (k = 142; k < 345; k++) {
      Assert.assertEquals(2,
          skylineLists.getCapacityAtTime(k).getMemorySize() / resource
              .getMemorySize());
    }
    for (k = 345; k < 502; k++) {
      Assert.assertEquals(1,
          skylineLists.getCapacityAtTime(k).getMemorySize() / resource
              .getMemorySize());
    }
  }

  @Test(expected = ParseException.class)
  public final void testInvalidDateFormat()
      throws ParseException {
    logParserUtil.stringToUnixTimestamp("2017.07.16 16:37:45");
  }

  @Test public final void testDuplicateJobSubmissionTime()
      throws SkylineStoreException, IOException, ParseException,
      ResourceEstimatorException, DataFieldNotFoundException {
    final String logFile = "src/test/resources/trace/invalidLog1.txt";
    parseFile(logFile);
    final RecurrenceId recurrenceId = new RecurrenceId("Test", "1");
    final Map<RecurrenceId, List<ResourceSkyline>> jobSkylineLists =
        skylineStore.getHistory(recurrenceId);
    Assert.assertEquals(
        logParserUtil.stringToUnixTimestamp("06/21/2017 16:10:23"),
        jobSkylineLists.get(recurrenceId).get(0).getJobSubmissionTime());
  }

  @Test public final void testJobIdNotFoundInJobSubmission()
      throws SkylineStoreException, IOException, ParseException,
      ResourceEstimatorException, DataFieldNotFoundException {
    final String logFile = "src/test/resources/trace/invalidLog2.txt";
    parseFile(logFile);
    final RecurrenceId recurrenceId = new RecurrenceId("Test", "2");
    Assert.assertNull(skylineStore.getHistory(recurrenceId));
  }

  @Test public final void testJobIdNotFoundInContainerAlloc()
      throws SkylineStoreException, IOException, ParseException,
      ResourceEstimatorException, DataFieldNotFoundException {
    final String logFile = "src/test/resources/trace/invalidLog3.txt";
    parseFile(logFile);
    final RecurrenceId recurrenceId = new RecurrenceId("Test", "3");
    final Map<RecurrenceId, List<ResourceSkyline>> jobSkylineLists =
        skylineStore.getHistory(recurrenceId);
    Assert.assertEquals(0,
        jobSkylineLists.get(recurrenceId).get(0).getSkylineList()
            .getCumulative().size());
  }

  @Test public final void testContainerIdNotFoundInContainerAlloc()
      throws SkylineStoreException, IOException, ParseException,
      ResourceEstimatorException, DataFieldNotFoundException {
    final String logFile = "src/test/resources/trace/invalidLog4.txt";
    parseFile(logFile);
    final RecurrenceId recurrenceId = new RecurrenceId("Test", "4");
    final Map<RecurrenceId, List<ResourceSkyline>> jobSkylineLists =
        skylineStore.getHistory(recurrenceId);
    Assert.assertEquals(0,
        jobSkylineLists.get(recurrenceId).get(0).getSkylineList()
            .getCumulative().size());
  }

  @Test public final void testJobIdNotFoundInJobFailure()
      throws SkylineStoreException, IOException, ParseException,
      ResourceEstimatorException, DataFieldNotFoundException {
    final String logFile = "src/test/resources/trace/invalidLog5.txt";
    parseFile(logFile);
    final RecurrenceId recurrenceId = new RecurrenceId("Test", "5");
    final Map<RecurrenceId, List<ResourceSkyline>> jobSkylineLists =
        skylineStore.getHistory(recurrenceId);
    Assert.assertEquals(
        logParserUtil.stringToUnixTimestamp("06/21/2017 16:10:13"),
        jobSkylineLists.get(recurrenceId).get(0).getJobSubmissionTime());
  }

  @Test public final void testJobIdNotFoundInJobFinish()
      throws SkylineStoreException, IOException, ParseException,
      ResourceEstimatorException, DataFieldNotFoundException {
    final String logFile = "src/test/resources/trace/invalidLog6.txt";
    parseFile(logFile);
    final RecurrenceId recurrenceId = new RecurrenceId("Test", "6");
    Assert.assertNull(skylineStore.getHistory(recurrenceId));
  }

  @Test public final void testRecurrenceIdNotFoundInJobFinish()
      throws SkylineStoreException, IOException, ParseException,
      ResourceEstimatorException, DataFieldNotFoundException {
    final String logFile = "src/test/resources/trace/invalidLog7.txt";
    parseFile(logFile);
    final RecurrenceId recurrenceId = new RecurrenceId("Test", "7");
    Assert.assertNull(skylineStore.getHistory(recurrenceId));
  }

  @Test public final void testJobIdNotFoundInResourceSpec()
      throws SkylineStoreException, IOException, ParseException,
      ResourceEstimatorException, DataFieldNotFoundException {
    final String logFile = "src/test/resources/trace/invalidLog8.txt";
    parseFile(logFile);
    final RecurrenceId recurrenceId = new RecurrenceId("Test", "8");
    final Map<RecurrenceId, List<ResourceSkyline>> jobSkylineLists =
        skylineStore.getHistory(recurrenceId);
    Assert.assertEquals(1024,
        jobSkylineLists.get(recurrenceId).get(0).getContainerSpec()
            .getMemorySize());
    Assert.assertEquals(1,
        jobSkylineLists.get(recurrenceId).get(0).getContainerSpec()
            .getVirtualCores());
  }

  @Test public final void testResourceSpecNotFoundInResourceSpec()
      throws SkylineStoreException, IOException, ParseException,
      ResourceEstimatorException, DataFieldNotFoundException {
    final String logFile = "src/test/resources/trace/invalidLog9.txt";
    parseFile(logFile);
    final RecurrenceId recurrenceId = new RecurrenceId("Test", "9");
    final Map<RecurrenceId, List<ResourceSkyline>> jobSkylineLists =
        skylineStore.getHistory(recurrenceId);
    Assert.assertEquals(1024,
        jobSkylineLists.get(recurrenceId).get(0).getContainerSpec()
            .getMemorySize());
    Assert.assertEquals(1,
        jobSkylineLists.get(recurrenceId).get(0).getContainerSpec()
            .getVirtualCores());
  }

  @After public final void cleanUp() {
    skylineStore = null;
    logParserUtil = null;
  }
}
