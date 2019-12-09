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
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * This sample parser will parse the sample log and extract the resource
 * skyline.
 */
public class TestNativeParser {
  private LogParserUtil logParserUtil = new LogParserUtil();
  private SkylineStore skylineStore;

  @Before public final void setup() throws ResourceEstimatorException {
    skylineStore = new InMemoryStore();
    final LogParser nativeParser = new BaseLogParser();
    Configuration config = new Configuration();
    config.addResource(ResourceEstimatorConfiguration.CONFIG_FILE);
    nativeParser.init(config, skylineStore);
    logParserUtil.setLogParser(nativeParser);
  }

  private void parseFile(final String logFile)
      throws SkylineStoreException, IOException, ParseException,
      ResourceEstimatorException, DataFieldNotFoundException {
    logParserUtil.parseLog(logFile);
  }

  @Test public final void testParse()
      throws SkylineStoreException, IOException, ParseException,
      ResourceEstimatorException, DataFieldNotFoundException {
    final String logFile = "src/test/resources/trace/nativeLog.txt";
    parseFile(logFile);
    final RecurrenceId recurrenceId =
        new RecurrenceId("tpch_q12", "tpch_q12_0");
    final Map<RecurrenceId, List<ResourceSkyline>> jobSkylineLists =
        skylineStore.getHistory(recurrenceId);
    Assert.assertEquals(1, jobSkylineLists.size());
    final List<ResourceSkyline> jobHistory = jobSkylineLists.get(recurrenceId);
    Assert.assertEquals(1, jobHistory.size());
    final ResourceSkyline resourceSkyline = jobHistory.get(0);
    Assert.assertEquals(0, resourceSkyline.getJobInputDataSize(), 0);
    Assert.assertEquals("tpch_q12_0", resourceSkyline.getJobId());
    Assert.assertEquals(0, resourceSkyline.getJobSubmissionTime());
    Assert.assertEquals(25, resourceSkyline.getJobFinishTime());
    Assert
        .assertEquals(1024, resourceSkyline.getContainerSpec().getMemorySize());
    Assert
        .assertEquals(1, resourceSkyline.getContainerSpec().getVirtualCores());
    final RLESparseResourceAllocation skylineLists =
        resourceSkyline.getSkylineList();
    int k;
    for (k = 0; k < 10; k++) {
      Assert.assertEquals(1,
          skylineLists.getCapacityAtTime(k).getMemorySize() / 1024);
    }
    for (k = 10; k < 15; k++) {
      Assert.assertEquals(1074,
          skylineLists.getCapacityAtTime(k).getMemorySize() / 1024);
    }
    for (k = 15; k < 20; k++) {
      Assert.assertEquals(2538,
          skylineLists.getCapacityAtTime(k).getMemorySize() / 1024);
    }
    for (k = 20; k < 25; k++) {
      Assert.assertEquals(2468,
          skylineLists.getCapacityAtTime(k).getMemorySize() / 1024);
    }
    Assert.assertEquals(0,
        skylineLists.getCapacityAtTime(25).getMemorySize() / 1024);
  }

  @After public final void cleanUp() {
    skylineStore = null;
    logParserUtil = null;
  }
}
