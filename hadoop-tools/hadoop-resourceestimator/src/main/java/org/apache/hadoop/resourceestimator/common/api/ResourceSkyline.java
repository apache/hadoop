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

package org.apache.hadoop.resourceestimator.common.api;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;

/**
 * ResourceSkyline records the job identification information as well as job's
 * requested {@code
 * container}s information during its lifespan.
 */
public class ResourceSkyline {
  /**
   * The auto-generated {@code ApplicationId} in job's one run.
   * <p>
   * <p>
   * For a pipeline job, we assume the {@code jobId} changes each time we run
   * the pipeline job.
   */
  private String jobId;
  /**
   * The input data size of the job.
   */
  private double jobInputDataSize;
  /**
   * Job submission time. Different logs could have different time format, so we
   * store the original string directly extracted from logs.
   */
  private long jobSubmissionTime;
  /**
   * Job finish time. Different logs could have different time format, so we
   * store the original string directly extracted from logs.
   */
  private long jobFinishTime;
  /**
   * The resource spec of containers allocated to the job.
   */
  private Resource containerSpec;
  /**
   * The list of {@link Resource} allocated to the job in its lifespan.
   */
  private RLESparseResourceAllocation skylineList;
  // TODO
  // We plan to record pipeline job's actual resource consumptions in one run
  // here.
  // TODO
  // We might need to addHistory more features to the ResourceSkyline, such as
  // users, VC, etc.

  /**
   * Constructor.
   *
   * @param jobIdConfig             the id of the job.
   * @param jobInputDataSizeConfig  the input data size of the job.
   * @param jobSubmissionTimeConfig the submission time of the job.
   * @param jobFinishTimeConfig     the finish time of the job.
   * @param containerSpecConfig     the resource spec of containers allocated
   *                                to the job.
   * @param skylineListConfig       the list of {@link Resource} allocated in
   *                                one run.
   */
  public ResourceSkyline(final String jobIdConfig,
      final double jobInputDataSizeConfig, final long jobSubmissionTimeConfig,
      final long jobFinishTimeConfig, final Resource containerSpecConfig,
      final RLESparseResourceAllocation skylineListConfig) {
    this.jobId = jobIdConfig;
    this.jobInputDataSize = jobInputDataSizeConfig;
    this.jobSubmissionTime = jobSubmissionTimeConfig;
    this.jobFinishTime = jobFinishTimeConfig;
    this.containerSpec = containerSpecConfig;
    this.skylineList = skylineListConfig;
  }

  /**
   * Empty constructor.
   */
  public ResourceSkyline() {
  }

  /**
   * Get the id of the job.
   *
   * @return the id of this job.
   */
  public final String getJobId() {
    return jobId;
  }

  /**
   * Set jobId.
   *
   * @param jobIdConfig jobId.
   */
  public final void setJobId(final String jobIdConfig) {
    this.jobId = jobIdConfig;
  }

  /**
   * Get the job's input data size.
   *
   * @return job's input data size.
   */
  public final double getJobInputDataSize() {
    return jobInputDataSize;
  }

  /**
   * Set jobInputDataSize.
   *
   * @param jobInputDataSizeConfig jobInputDataSize.
   */
  public final void setJobInputDataSize(final double jobInputDataSizeConfig) {
    this.jobInputDataSize = jobInputDataSizeConfig;
  }

  /**
   * Get the job's submission time.
   *
   * @return job's submission time.
   */
  public final long getJobSubmissionTime() {
    return jobSubmissionTime;
  }

  /**
   * Set jobSubmissionTime.
   *
   * @param jobSubmissionTimeConfig jobSubmissionTime.
   */
  public final void setJobSubmissionTime(final long jobSubmissionTimeConfig) {
    this.jobSubmissionTime = jobSubmissionTimeConfig;
  }

  /**
   * Get the job's finish time.
   *
   * @return job's finish time.
   */
  public final long getJobFinishTime() {
    return jobFinishTime;
  }

  /**
   * Set jobFinishTime.
   *
   * @param jobFinishTimeConfig jobFinishTime.
   */
  public final void setJobFinishTime(final long jobFinishTimeConfig) {
    this.jobFinishTime = jobFinishTimeConfig;
  }

  /**
   * Get the resource spec of the job's allocated {@code container}s.
   * <p> Key assumption: during job's lifespan, its allocated {@code container}s
   * have the same {@link Resource} spec.
   *
   * @return the {@link Resource} spec of the job's allocated
   * {@code container}s.
   */
  public final Resource getContainerSpec() {
    return containerSpec;
  }

  /**
   * Set containerSpec.
   *
   * @param containerSpecConfig containerSpec.
   */
  public final void setContainerSpec(final Resource containerSpecConfig) {
    this.containerSpec = containerSpecConfig;
  }

  /**
   * Get the list of {@link Resource}s allocated to the job.
   *
   * @return the {@link RLESparseResourceAllocation} which contains the list of
   * {@link Resource}s allocated to the job.
   */
  public final RLESparseResourceAllocation getSkylineList() {
    return skylineList;
  }

  /**
   * Set skylineList.
   *
   * @param skylineListConfig skylineList.
   */
  public final void setSkylineList(
      final RLESparseResourceAllocation skylineListConfig) {
    this.skylineList = skylineListConfig;
  }
}
