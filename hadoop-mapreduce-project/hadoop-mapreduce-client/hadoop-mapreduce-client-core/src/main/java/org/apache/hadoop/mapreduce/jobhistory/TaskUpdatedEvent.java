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
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;

/**
 * Event to record updates to a task
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TaskUpdatedEvent implements HistoryEvent {
  private TaskUpdated datum = new TaskUpdated();

  /**
   * Create an event to record task updates
   * @param id Id of the task
   * @param finishTime Finish time of the task
   */
  public TaskUpdatedEvent(TaskID id, long finishTime) {
    datum.setTaskid(new Utf8(id.toString()));
    datum.setFinishTime(finishTime);
  }

  TaskUpdatedEvent() {}

  public Object getDatum() { return datum; }
  public void setDatum(Object datum) { this.datum = (TaskUpdated)datum; }

  /** Get the task ID */
  public TaskID getTaskId() {
    return TaskID.forName(datum.getTaskid().toString());
  }
  /** Get the task finish time */
  public long getFinishTime() { return datum.getFinishTime(); }
  /** Get the event type */
  public EventType getEventType() {
    return EventType.TASK_UPDATED;
  }

  @Override
  public TimelineEvent toTimelineEvent() {
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(StringUtils.toUpperCase(getEventType().name()));
    tEvent.addInfo("FINISH_TIME", getFinishTime());
    return tEvent;
  }

  @Override
  public Set<TimelineMetric> getTimelineMetrics() {
    return null;
  }

}
