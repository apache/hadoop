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

import com.google.common.base.Joiner;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.JobID;

import java.util.Collections;

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
   * @param finishedMaps Number of finished maps
   * @param finishedReduces Number of finished reduces
   * @param status Status of the job
   */
  public JobUnsuccessfulCompletionEvent(JobID id, long finishTime,
      int finishedMaps,
      int finishedReduces, String status) {
    this(id, finishTime, finishedMaps, finishedReduces, status, NODIAGS_LIST);
  }

  /**
   * Create an event to record unsuccessful completion (killed/failed) of jobs
   * @param id Job ID
   * @param finishTime Finish time of the job
   * @param finishedMaps Number of finished maps
   * @param finishedReduces Number of finished reduces
   * @param status Status of the job
   * @param diagnostics job runtime diagnostics
   */
  public JobUnsuccessfulCompletionEvent(JobID id, long finishTime,
      int finishedMaps,
      int finishedReduces,
      String status,
      Iterable<String> diagnostics) {
    datum.setJobid(new Utf8(id.toString()));
    datum.setFinishTime(finishTime);
    datum.setFinishedMaps(finishedMaps);
    datum.setFinishedReduces(finishedReduces);
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
  public JobID getJobId() { return JobID.forName(datum.jobid.toString()); }
  /** Get the job finish time */
  public long getFinishTime() { return datum.getFinishTime(); }
  /** Get the number of finished maps */
  public int getFinishedMaps() { return datum.getFinishedMaps(); }
  /** Get the number of finished reduces */
  public int getFinishedReduces() { return datum.getFinishedReduces(); }
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
}
