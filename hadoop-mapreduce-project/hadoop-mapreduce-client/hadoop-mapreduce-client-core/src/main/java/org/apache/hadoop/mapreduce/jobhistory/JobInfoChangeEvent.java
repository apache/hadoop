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

package org.apache.hadoop.mapreduce.jobhistory;

import java.util.Set;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;

/**
 * Event to record changes in the submit and launch time of
 * a job
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobInfoChangeEvent implements HistoryEvent {
  private JobInfoChange datum = new JobInfoChange();

  /** 
   * Create a event to record the submit and launch time of a job
   * @param id Job Id 
   * @param submitTime Submit time of the job
   * @param launchTime Launch time of the job
   */
  public JobInfoChangeEvent(JobID id, long submitTime, long launchTime) {
    datum.setJobid(new Utf8(id.toString()));
    datum.setSubmitTime(submitTime);
    datum.setLaunchTime(launchTime);
  }

  JobInfoChangeEvent() { }

  public Object getDatum() { return datum; }
  public void setDatum(Object datum) {
    this.datum = (JobInfoChange)datum;
  }

  /** Get the Job ID */
  public JobID getJobId() { return JobID.forName(datum.getJobid().toString()); }
  /** Get the Job submit time */
  public long getSubmitTime() { return datum.getSubmitTime(); }
  /** Get the Job launch time */
  public long getLaunchTime() { return datum.getLaunchTime(); }

  public EventType getEventType() {
    return EventType.JOB_INFO_CHANGED;
  }

  @Override
  public TimelineEvent toTimelineEvent() {
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(StringUtils.toUpperCase(getEventType().name()));
    tEvent.addInfo("SUBMIT_TIME", getSubmitTime());
    tEvent.addInfo("LAUNCH_TIME", getLaunchTime());
    return tEvent;
  }

  @Override
  public Set<TimelineMetric> getTimelineMetrics() {
    return null;
  }
}
