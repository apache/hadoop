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

package org.apache.hadoop.mapreduce.v2.app.speculate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;



public interface TaskRuntimeEstimator {
  public void enrollAttempt(TaskAttemptStatus reportedStatus, long timestamp);

  public long attemptEnrolledTime(TaskAttemptId attemptID);

  public void updateAttempt(TaskAttemptStatus reportedStatus, long timestamp);

  public void contextualize(Configuration conf, AppContext context);

  /**
   *
   * Find a maximum reasonable execution wallclock time.  Includes the time
   * already elapsed.
   *
   * Find a maximum reasonable execution time.  Includes the time
   * already elapsed.  If the projected total execution time for this task
   * ever exceeds its reasonable execution time, we may speculate it.
   *
   * @param id the {@link TaskId} of the task we are asking about
   * @return the task's maximum reasonable runtime, or MAX_VALUE if
   *         we don't have enough information to rule out any runtime,
   *         however long.
   *
   */
  public long thresholdRuntime(TaskId id);

  /**
   *
   * Estimate a task attempt's total runtime.  Includes the time already
   * elapsed.
   *
   * @param id the {@link TaskAttemptId} of the attempt we are asking about
   * @return our best estimate of the attempt's runtime, or {@code -1} if
   *         we don't have enough information yet to produce an estimate.
   *
   */
  public long estimatedRuntime(TaskAttemptId id);

  /**
   *
   * Estimates how long a new attempt on this task will take if we start
   *  one now
   *
   * @param id the {@link TaskId} of the task we are asking about
   * @return our best estimate of a new attempt's runtime, or {@code -1} if
   *         we don't have enough information yet to produce an estimate.
   *
   */
  public long estimatedNewAttemptRuntime(TaskId id);

  /**
   *
   * Computes the width of the error band of our estimate of the task
   *  runtime as returned by {@link #estimatedRuntime(TaskAttemptId)}
   *
   * @param id the {@link TaskAttemptId} of the attempt we are asking about
   * @return our best estimate of the attempt's runtime, or {@code -1} if
   *         we don't have enough information yet to produce an estimate.
   *
   */
  public long runtimeEstimateVariance(TaskAttemptId id);

  /**
   *
   * Returns true if the estimator has no updates records for a threshold time
   * window. This helps to identify task attempts that are stalled at the
   * beginning of execution.
   *
   * @param id the {@link TaskAttemptId} of the attempt we are asking about
   * @param timeStamp the time of the report we compare with
   * @return true if the task attempt has no progress for a given time window
   *
   */
  default boolean hasStagnatedProgress(TaskAttemptId id, long timeStamp) {
    return false;
  }
}
