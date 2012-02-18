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


/*
 * This class is provided solely as an exemplae of the values that mean
 *  that nothing needs to be computed.  It's not currently used.
 */
public class NullTaskRuntimesEngine implements TaskRuntimeEstimator {
  @Override
  public void enrollAttempt(TaskAttemptStatus status, long timestamp) {
    // no code
  }

  @Override
  public long attemptEnrolledTime(TaskAttemptId attemptID) {
    return Long.MAX_VALUE;
  }

  @Override
  public void updateAttempt(TaskAttemptStatus status, long timestamp) {
    // no code
  }

  @Override
  public void contextualize(Configuration conf, AppContext context) {
    // no code
  }

  @Override
  public long thresholdRuntime(TaskId id) {
    return Long.MAX_VALUE;
  }

  @Override
  public long estimatedRuntime(TaskAttemptId id) {
    return -1L;
  }
  @Override
  public long estimatedNewAttemptRuntime(TaskId id) {
    return -1L;
  }

  @Override
  public long runtimeEstimateVariance(TaskAttemptId id) {
    return -1L;
  }

}
