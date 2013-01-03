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

package org.apache.hadoop.mapreduce.v2.app.job.event;

import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;

public class JobAbortCompletedEvent extends JobEvent {

  private JobStatus.State finalState;

  public JobAbortCompletedEvent(JobId jobID, JobStatus.State finalState) {
    super(jobID, JobEventType.JOB_ABORT_COMPLETED);
    this.finalState = finalState;
  }

  public JobStatus.State getFinalState() {
    return finalState;
  }
}
