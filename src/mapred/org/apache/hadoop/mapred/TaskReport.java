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
package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/** A report on the state of a task. */
public class TaskReport implements Writable {
  private TaskID taskid;
  private float progress;
  private String state;
  private String[] diagnostics;
  private long startTime; 
  private long finishTime; 
  private Counters counters;

  public TaskReport() {}

  TaskReport(TaskID taskid, float progress, String state,
             String[] diagnostics, long startTime, long finishTime,
             Counters counters) {
    this.taskid = taskid;
    this.progress = progress;
    this.state = state;
    this.diagnostics = diagnostics;
    this.startTime = startTime; 
    this.finishTime = finishTime;
    this.counters = counters;
  }
    
  /** @deprecated use {@link #getTaskID()} instead */
  @Deprecated
  public String getTaskId() { return taskid.toString(); }
  /** The id of the task. */
  public TaskID getTaskID() { return taskid; }
  /** The amount completed, between zero and one. */
  public float getProgress() { return progress; }
  /** The most recent state, reported by a {@link Reporter}. */
  public String getState() { return state; }
  /** A list of error messages. */
  public String[] getDiagnostics() { return diagnostics; }
  /** A table of counters. */
  public Counters getCounters() { return counters; }
  
  /**
   * Get finish time of task. 
   * @return 0, if finish time was not set else returns finish time.
   */
  public long getFinishTime() {
    return finishTime;
  }

  /** 
   * set finish time of task. 
   * @param finishTime finish time of task. 
   */
  void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  /**
   * Get start time of task. 
   * @return 0 if start time was not set, else start time. 
   */
  public long getStartTime() {
    return startTime;
  }

  /** 
   * set start time of the task. 
   */ 
  void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  @Override
  public boolean equals(Object o) {
    if(o == null)
      return false;
    if(o.getClass().equals(TaskReport.class)) {
      TaskReport report = (TaskReport) o;
      return counters.contentEquals(report.getCounters())
             && Arrays.toString(this.diagnostics)
                      .equals(Arrays.toString(report.getDiagnostics()))
             && this.finishTime == report.getFinishTime()
             && this.progress == report.getProgress()
             && this.startTime == report.getStartTime()
             && this.state.equals(report.getState())
             && this.taskid.equals(report.getTaskID());
    }
    return false; 
  }

  @Override
  public int hashCode() {
    return (counters.toString() + Arrays.toString(this.diagnostics) 
            + this.finishTime + this.progress + this.startTime + this.state 
            + this.taskid.toString()).hashCode();
  }
  //////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    taskid.write(out);
    out.writeFloat(progress);
    Text.writeString(out, state);
    out.writeLong(startTime);
    out.writeLong(finishTime);
    WritableUtils.writeStringArray(out, diagnostics);
    counters.write(out);
  }

  public void readFields(DataInput in) throws IOException {
    this.taskid = TaskID.read(in);
    this.progress = in.readFloat();
    this.state = Text.readString(in);
    this.startTime = in.readLong(); 
    this.finishTime = in.readLong();
    
    diagnostics = WritableUtils.readStringArray(in);
    counters = new Counters();
    counters.readFields(in);
  }
}
