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

package org.apache.hadoop.resourceestimator.solver.preprocess;

import static java.lang.Math.toIntExact;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.resourceestimator.common.api.RecurrenceId;
import org.apache.hadoop.resourceestimator.common.api.ResourceSkyline;
import org.apache.hadoop.resourceestimator.solver.api.Solver;
import org.apache.hadoop.resourceestimator.solver.exceptions.InvalidInputException;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common preprocessing functions for {@link Solver}.
 */
public class SolverPreprocessor {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(SolverPreprocessor.class);

  /**
   * Check if Solver's input parameters are valid.
   *
   * @param jobHistory   the history {@link ResourceSkyline}s of the recurring
   *                     pipeline job.
   * @param timeInterval the time interval which is used to discretize the
   *                     history {@link ResourceSkyline}s.
   * @throws InvalidInputException if: (1) jobHistory is <em>null</em>;
   *     (2) jobHistory is empty; (3) timeout is non-positive;
   *     (4) timeInterval is non-positive;
   */
  public final void validate(
      final Map<RecurrenceId, List<ResourceSkyline>> jobHistory,
      final int timeInterval) throws InvalidInputException {
    if ((jobHistory == null) || (jobHistory.size() == 0)) {
      LOGGER.error(
          "Job resource skyline history is invalid, please try again with"
              + " valid resource skyline history.");
      throw new InvalidInputException("Job ResourceSkyline history", "invalid");
    }

    if (timeInterval <= 0) {
      LOGGER.error(
          "Solver timeInterval {} is invalid, please specify a positive value.",
          timeInterval);
      throw new InvalidInputException("Solver timeInterval", "non-positive");
    }
  }

  /**
   * Return the multi-dimension resource vector consumed by the job at specified
   * time.
   *
   * @param skyList           the list of {@link Resource}s used by the job.
   * @param index             the discretized time index.
   * @param containerMemAlloc the multi-dimension resource vector allocated to
   *                          one container.
   * @return the multi-dimension resource vector consumed by the job.
   */
  public final long getResourceVector(final RLESparseResourceAllocation skyList,
      final int index, final long containerMemAlloc) {
    return skyList.getCapacityAtTime(index).getMemorySize() / containerMemAlloc;
  }

  /**
   * Discretize job's lifespan into intervals, and return the number of
   * containers used by the job within each interval.
   * <p> Note that here we assume all containers allocated to the job have the
   * same {@link Resource}. This is due to the limit of
   * {@link RLESparseResourceAllocation}.
   *
   * @param skyList           the list of {@link Resource}s used by the job.
   * @param timeInterval      the time interval used to discretize the job's
   *                          lifespan.
   * @param containerMemAlloc the amount of memory allocated to each container.
   * @param jobLen            the duration of the job.
   * @return the number of containers allocated to the job within discretized
   * time intervals.
   */
  public final int[] getDiscreteSkyline(
      final RLESparseResourceAllocation skyList, final int timeInterval,
      final long containerMemAlloc, final int jobLen) {
    long jobLifeSpan =
        skyList.getLatestNonNullTime() - skyList.getEarliestStartTime();
    int[] result = new int[jobLen];
    Arrays.fill(result, 0);

    int index = 0;
    long numContainerAt = 0;
    for (int i = 0; i < jobLifeSpan; i++) {
      index = (int) Math.floor((double) i / timeInterval);
      numContainerAt = getResourceVector(skyList, i, containerMemAlloc);
      if (result[index] < numContainerAt) {
        result[index] = (int) numContainerAt;
      }
    }
    return result;
  }

  /**
   * Merge different jobs' resource skylines into one within the same pipeline.
   *
   * @param resourceSkylines different jobs' resource skylines within the same
   *                         pipeline.
   * @return an aggregated resource skyline for the pipeline.
   */
  public final ResourceSkyline mergeSkyline(
      final List<ResourceSkyline> resourceSkylines) {
    // TODO:
    // rewrite this function with shift and merge once YARN-5328 is committed
    /** First, getHistory the pipeline submission time. */
    long pipelineSubmission = Long.MAX_VALUE;
    for (int i = 0; i < resourceSkylines.size(); i++) {
      long jobSubmission = resourceSkylines.get(i).getJobSubmissionTime();
      if (pipelineSubmission > jobSubmission) {
        pipelineSubmission = jobSubmission;
      }
    }
    final TreeMap<Long, Resource> resourceOverTime = new TreeMap<>();
    final RLESparseResourceAllocation skylineListAgg =
        new RLESparseResourceAllocation(resourceOverTime,
            new DefaultResourceCalculator());
    /**
     * Second, adjust different jobs' ResourceSkyline starting time based on
     * pipeline submission time, and merge them into one ResourceSkyline.
     */
    for (int i = 0; i < resourceSkylines.size(); i++) {
      long jobSubmission = resourceSkylines.get(i).getJobSubmissionTime();
      long diff = (jobSubmission - pipelineSubmission) / 1000;
      RLESparseResourceAllocation tmp =
          resourceSkylines.get(i).getSkylineList();
      Object[] timePoints = tmp.getCumulative().keySet().toArray();
      for (int j = 0; j < timePoints.length - 2; j++) {
        ReservationInterval riAdd =
            new ReservationInterval(toIntExact((long) timePoints[j]) + diff,
                toIntExact((long) timePoints[j + 1] + diff));
        skylineListAgg.addInterval(riAdd,
            tmp.getCapacityAtTime(toIntExact((long) timePoints[j])));
      }
    }
    ResourceSkyline skylineAgg =
        new ResourceSkyline(resourceSkylines.get(0).getJobId(),
            resourceSkylines.get(0).getJobInputDataSize(),
            resourceSkylines.get(0).getJobSubmissionTime(),
            resourceSkylines.get(0).getJobFinishTime(),
            resourceSkylines.get(0).getContainerSpec(), skylineListAgg);

    return skylineAgg;
  }

  /**
   * Aggregate all job's {@link ResourceSkyline}s in the one run of recurring
   * pipeline, and return the aggregated {@link ResourceSkyline}s in different
   * runs.
   *
   * @param jobHistory the history {@link ResourceSkyline} of the recurring
   *                   pipeline job.
   * @param minJobRuns the minimum number of job runs required to run the
   *                   solver.
   * @return the aggregated {@link ResourceSkyline}s in different runs.
   * @throws InvalidInputException if: (1) job submission time parsing fails;
   *     (2) jobHistory has less job runs than the minimum requirement;
   */
  public final List<ResourceSkyline> aggregateSkylines(
      final Map<RecurrenceId, List<ResourceSkyline>> jobHistory,
      final int minJobRuns) throws InvalidInputException {
    List<ResourceSkyline> resourceSkylines = new ArrayList<ResourceSkyline>();
    for (Map.Entry<RecurrenceId, List<ResourceSkyline>> entry : jobHistory
        .entrySet()) {
      // TODO: identify different jobs within the same pipeline
      // right now, we do prediction at the granularity of pipeline, i.e., we
      // will merge the
      // resource skylines of jobs within the same pipeline into one aggregated
      // resource skyline
      ResourceSkyline skylineAgg = null;
      skylineAgg = mergeSkyline(entry.getValue());
      resourceSkylines.add(skylineAgg);
    }
    int numJobs = resourceSkylines.size();
    if (numJobs < minJobRuns) {
      LOGGER.error(
          "Solver requires job resource skyline history for at least {} runs,"
              + " but it only receives history info for {}  runs.",
          minJobRuns, numJobs);
      throw new InvalidInputException("Job ResourceSkyline history",
          "containing less job runs" + " than " + minJobRuns);
    }

    return resourceSkylines;
  }
}
