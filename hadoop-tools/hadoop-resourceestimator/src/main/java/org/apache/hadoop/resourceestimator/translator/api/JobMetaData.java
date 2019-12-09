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

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.resourceestimator.common.api.RecurrenceId;
import org.apache.hadoop.resourceestimator.common.api.ResourceSkyline;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Job metadata collected when parsing the log file.
 */
public class JobMetaData {
  // containerId, releaseTime
  private static final Logger LOGGER =
      LoggerFactory.getLogger(JobMetaData.class);
  private final ResourceSkyline resourceSkyline = new ResourceSkyline();
  private final Map<String, Long> rawStart = new HashMap<String, Long>();
  // containerId, startTime
  private final Map<String, Long> rawEnd = new HashMap<String, Long>();
  private RecurrenceId recurrenceId;

  /**
   * Constructor.
   *
   * @param jobSubmissionTimeConfig job submission time.
   */
  public JobMetaData(final long jobSubmissionTimeConfig) {
    resourceSkyline.setJobSubmissionTime(jobSubmissionTimeConfig);
  }

  /**
   * Set job finish time.
   *
   * @param jobFinishTimeConfig job finish time.
   * @return the reference to current {@link JobMetaData}.
   */
  public final JobMetaData setJobFinishTime(final long jobFinishTimeConfig) {
    resourceSkyline.setJobFinishTime(jobFinishTimeConfig);
    return this;
  }

  /**
   * Add container launch time.
   *
   * @param containerId id of the container.
   * @param time        container launch time.
   * @return the reference to current {@link JobMetaData}.
   */
  public final JobMetaData setContainerStart(final String containerId,
      final long time) {
    if (rawStart.put(containerId, time) != null) {
      LOGGER.warn("find duplicate container launch time for {}, so we replace"
              + " it with {}.", containerId, time);
    }
    return this;
  }

  /**
   * Add container release time.
   *
   * @param containerId id of the container.
   * @param time        container release time.
   * @return the reference to current {@link JobMetaData}.
   */
  public final JobMetaData setContainerEnd(final String containerId,
      final long time) {
    if (rawEnd.put(containerId, time) != null) {
      LOGGER.warn("find duplicate container release time for {}, so we replace"
          + " it with {}.", containerId, time);
    }
    return this;
  }

  /**
   * Get {@link RecurrenceId}.
   *
   * @return {@link RecurrenceId}.
   */
  public final RecurrenceId getRecurrenceId() {
    return recurrenceId;
  }

  /**
   * Set {@link RecurrenceId}.
   *
   * @param recurrenceIdConfig the {@link RecurrenceId}.
   * @return the reference to current {@link JobMetaData}.
   */
  public final JobMetaData setRecurrenceId(
      final RecurrenceId recurrenceIdConfig) {
    this.recurrenceId = recurrenceIdConfig;
    return this;
  }

  /**
   * Get {@link ResourceSkyline}.
   *
   * @return {@link ResourceSkyline}.
   */
  public final ResourceSkyline getResourceSkyline() {
    return resourceSkyline;
  }

  /**
   * Normalized container launch/release time, and generate the
   * {@link ResourceSkyline}.
   */
  public final void createSkyline() {
    final long jobSubmissionTime = resourceSkyline.getJobSubmissionTime();
    Resource containerSpec = resourceSkyline.getContainerSpec();
    final TreeMap<Long, Resource> resourceOverTime = new TreeMap<>();
    final RLESparseResourceAllocation skylineList =
        new RLESparseResourceAllocation(resourceOverTime,
            new DefaultResourceCalculator());
    resourceSkyline.setSkylineList(skylineList);
    if (containerSpec == null) {
      // if RmParser fails to extract container resource spec from logs, we will
      // statically set
      // it to be <1core, 1GB>
      containerSpec = Resource.newInstance(1024, 1);
    }
    resourceSkyline.setContainerSpec(containerSpec);
    for (final Map.Entry<String, Long> entry : rawStart.entrySet()) {
      final long timeStart = entry.getValue();
      final Long timeEnd = rawEnd.get(entry.getKey());
      if (timeEnd == null) {
        LOGGER.warn("container release time not found for {}.", entry.getKey());
      } else {
        final ReservationInterval riAdd =
            new ReservationInterval((timeStart - jobSubmissionTime) / 1000,
                (timeEnd - jobSubmissionTime) / 1000);
        resourceSkyline.getSkylineList().addInterval(riAdd, containerSpec);
      }
    }
  }
}
