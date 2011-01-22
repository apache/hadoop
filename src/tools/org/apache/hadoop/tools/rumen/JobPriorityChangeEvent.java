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

package org.apache.hadoop.tools.rumen;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapreduce.JobID;

/**
 * Event to record the change of priority of a job
 *
 */
public class JobPriorityChangeEvent implements HistoryEvent {
  private JobID jobId;
  private JobPriority priority;

  /** Generate an event to record changes in Job priority
   * @param id Job Id
   * @param priority The new priority of the job
   */
  public JobPriorityChangeEvent(JobID id, JobPriority priority) {
    this.jobId = id;
    this.priority = priority;
  }

  /** Get the Job ID */
  public JobID getJobId() { return jobId; }
  /** Get the job priority */
  public JobPriority getPriority() {
    return priority;
  }
  /** Get the event type */
  public EventType getEventType() {
    return EventType.JOB_PRIORITY_CHANGED;
  }

}
