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

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.resourceestimator.common.api.RecurrenceId;
import org.apache.hadoop.resourceestimator.common.api.ResourceSkyline;
import org.apache.hadoop.resourceestimator.common.config.ResourceEstimatorConfiguration;
import org.apache.hadoop.resourceestimator.translator.api.JobMetaData;
import org.apache.hadoop.resourceestimator.translator.api.SingleLineParser;
import org.apache.hadoop.resourceestimator.translator.exceptions.DataFieldNotFoundException;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This sample parser will parse the sample log and extract the resource
 * skyline.
 * <p> The expected log format is: NormalizedJobName NumInstances SubmitTime
 * StartTime EndTime JobInstanceName memUsage coreUsage
 */
public class NativeSingleLineParser implements SingleLineParser {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(NativeSingleLineParser.class);

  /**
   * Aggregates different jobs' {@link ResourceSkyline}s within the same
   * pipeline together.
   *
   * @param resourceSkyline newly extracted {@link ResourceSkyline}.
   * @param recurrenceId    the {@link RecurrenceId} which the resourceSkyline
   *                        belongs to.
   * @param skylineRecords  a {@link Map} which stores the
   *     {@link ResourceSkyline}s for all pipelines during this parsing.
   */
  private void aggregateSkyline(final ResourceSkyline resourceSkyline,
      final RecurrenceId recurrenceId,
      final Map<RecurrenceId, List<ResourceSkyline>> skylineRecords) {
    List<ResourceSkyline> resourceSkylines = skylineRecords.get(recurrenceId);
    if (resourceSkylines == null) {
      resourceSkylines = new ArrayList<ResourceSkyline>();
      skylineRecords.put(recurrenceId, resourceSkylines);
    }
    resourceSkylines.add(resourceSkyline);
  }

  @Override public void parseLine(String logLine,
      Map<String, JobMetaData> jobMetas,
      Map<RecurrenceId, List<ResourceSkyline>> skylineRecords)
      throws DataFieldNotFoundException, ParseException {
    Configuration config = new Configuration();
    config.addResource(new org.apache.hadoop.fs.Path(
        ResourceEstimatorConfiguration.CONFIG_FILE));
    int timeInterval =
        config.getInt(ResourceEstimatorConfiguration.TIME_INTERVAL_KEY, 5);
    // note that for native log, we assume each container is allocated <1 core,
    // 1GB RAM>
    long containerMemAlloc = 1024;
    int containerCpuAlloc = 1;
    String[] splitString = logLine.split("\\s+");
    String pipelineId = splitString[0];
    String jobId = splitString[5];
    String[] skylineUnits = splitString[7].split("\\|");

    JobMetaData appMeta = new JobMetaData(0);
    RecurrenceId recurrenceId = new RecurrenceId(pipelineId, jobId);
    appMeta.setRecurrenceId(recurrenceId);
    Resource containerAlloc;
    int numContainers;
    ResourceSkyline resourceSkyline = appMeta.getResourceSkyline();
    final TreeMap<Long, Resource> resourceOverTime = new TreeMap<>();
    final RLESparseResourceAllocation skylineList =
        new RLESparseResourceAllocation(resourceOverTime,
            new DefaultResourceCalculator());
    resourceSkyline.setSkylineList(skylineList);
    for (String elem : skylineUnits) {
      numContainers = Integer.parseInt(elem.split("\\:")[0]);
      containerAlloc = Resource.newInstance(containerMemAlloc * numContainers,
          containerCpuAlloc * numContainers);
      final ReservationInterval riAdd =
          new ReservationInterval(Long.parseLong(elem.split("\\:")[1]),
              Long.parseLong(elem.split("\\:")[1]) + timeInterval);
      resourceSkyline.getSkylineList().addInterval(riAdd, containerAlloc);
    }
    resourceSkyline.setContainerSpec(
        Resource.newInstance(containerMemAlloc, containerCpuAlloc));
    appMeta.setJobFinishTime(
        appMeta.getResourceSkyline().getSkylineList().getLatestNonNullTime());
    resourceSkyline.setJobInputDataSize(0);
    resourceSkyline.setJobId(jobId);
    aggregateSkyline(resourceSkyline, recurrenceId, skylineRecords);
  }
}
