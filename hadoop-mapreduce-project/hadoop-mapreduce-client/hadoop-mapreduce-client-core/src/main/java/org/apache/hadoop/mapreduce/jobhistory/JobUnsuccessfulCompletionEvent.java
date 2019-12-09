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

import java.util.Collections;
import java.util.Set;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;

import com.google.common.base.Joiner;

/**
 * Event to record Failed and Killed completion of jobs
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobUnsuccessfulCompletionEvent implements HistoryEvent {
  private static final String NODIAGS = "";
  private static final Iterable<String> NODIAGS_LIST =
      Collections.singletonList(NODIAGS);

  private JobUnsuccessfulCompletion datum
    = new JobUnsuccessfulCompletion();

  /**
   * Create an event to record unsuccessful completion (killed/failed) of jobs
   * @param id Job ID
   * @param finishTime Finish time of the job
   * @param succeededMaps Number of succeeded maps
   * @param succeededReduces Number of succeeded reduces
   * @param failedMaps Number of failed maps
   * @param failedReduces Number of failed reduces
   * @param killedMaps Number of killed maps
   * @param killedReduces Number of killed reduces
   * @param status Status of the job
   */
  public JobUnsuccessfulCompletionEvent(JobID id, long finishTime,
      int succeededMaps,
      int succeededReduces,
      int failedMaps,
      int failedReduces,
      int killedMaps,
      int killedReduces,
      String status) {
    this(id, finishTime, succeededMaps, succeededReduces, failedMaps,
            failedReduces, killedMaps, killedReduces, status, NODIAGS_LIST);
  }

  /**
   * Create an event to record unsuccessful completion (killed/failed) of jobs
   * @param id Job ID
   * @param finishTime Finish time of the job
   * @param succeededMaps Number of finished maps
   * @param succeededReduces Number of finished reduces
   * @param failedMaps Number of failed maps
   * @param failedReduces Number of failed reduces
   * @param killedMaps Number of killed maps
   * @param killedReduces Number of killed reduces
   * @param status Status of the job
   * @param diagnostics job runtime diagnostics
   */
  public JobUnsuccessfulCompletionEvent(JobID id, long finishTime,
      int succeededMaps,
      int succeededReduces,
      int failedMaps,
      int failedReduces,
      int killedMaps,
      int killedReduces,
      String status,
      Iterable<String> diagnostics) {
    datum.setJobid(new Utf8(id.toString()));
    datum.setFinishTime(finishTime);
    // using finishedMaps & finishedReduces in the Avro schema for backward
    // compatibility
    datum.setFinishedMaps(succeededMaps);
    datum.setFinishedReduces(succeededReduces);
    datum.setFailedMaps(failedMaps);
    datum.setFailedReduces(failedReduces);
    datum.setKilledMaps(killedMaps);
    datum.setKilledReduces(killedReduces);
    datum.setJobStatus(new Utf8(status));
    if (diagnostics == null) {
      diagnostics = NODIAGS_LIST;
    }
    datum.setDiagnostics(new Utf8(Joiner.on('\n').skipNulls()
        .join(diagnostics)));
  }

  JobUnsuccessfulCompletionEvent() {}

  public Object getDatum() { return datum; }
  public void setDatum(Object datum) {
    this.datum = (JobUnsuccessfulCompletion)datum;
  }

  /** Get the Job ID */
  public JobID getJobId() {
    return JobID.forName(datum.getJobid().toString());
  }
  /** Get the job finish time */
  public long getFinishTime() { return datum.getFinishTime(); }
  /** Get the number of succeeded maps */
  public int getSucceededMaps() { return datum.getFinishedMaps(); }
  /** Get the number of succeeded reduces */
  public int getSucceededReduces() { return datum.getFinishedReduces(); }
  /** Get the number of failed maps */
  public int getFailedMaps() { return datum.getFailedMaps(); }
  /** Get the number of failed reduces */
  public int getFailedReduces() { return datum.getFailedReduces(); }
  /** Get the number of killed maps */
  public int getKilledMaps() { return datum.getKilledMaps(); }
  /** Get the number of killed reduces */
  public int getKilledReduces() { return datum.getKilledReduces(); }

  /** Get the status */
  public String getStatus() { return datum.getJobStatus().toString(); }
  /** Get the event type */
  public EventType getEventType() {
    if ("FAILED".equals(getStatus())) {
      return EventType.JOB_FAILED;
    } else if ("ERROR".equals(getStatus())) {
      return EventType.JOB_ERROR;
    } else
      return EventType.JOB_KILLED;
  }

  /**
   * Retrieves diagnostics information preserved in the history file
   *
   * @return diagnostics as of the time of job termination
   */
  public String getDiagnostics() {
    final CharSequence diagnostics = datum.getDiagnostics();
    return diagnostics == null ? NODIAGS : diagnostics.toString();
  }

  @Override
  public TimelineEvent toTimelineEvent() {
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(StringUtils.toUpperCase(getEventType().name()));
    tEvent.addInfo("FINISH_TIME", getFinishTime());
    tEvent.addInfo("NUM_MAPS", getSucceededMaps() + getFailedMaps()
        + getKilledMaps());
    tEvent.addInfo("NUM_REDUCES", getSucceededReduces() + getFailedReduces()
        + getKilledReduces());
    tEvent.addInfo("JOB_STATUS", getStatus());
    tEvent.addInfo("DIAGNOSTICS", getDiagnostics());
    tEvent.addInfo("SUCCESSFUL_MAPS", getSucceededMaps());
    tEvent.addInfo("SUCCESSFUL_REDUCES", getSucceededReduces());
    tEvent.addInfo("FAILED_MAPS", getFailedMaps());
    tEvent.addInfo("FAILED_REDUCES", getFailedReduces());
    tEvent.addInfo("KILLED_MAPS", getKilledMaps());
    tEvent.addInfo("KILLED_REDUCES", getKilledReduces());

    return tEvent;
  }

  @Override
  public Set<TimelineMetric> getTimelineMetrics() {
    return null;
  }
}
