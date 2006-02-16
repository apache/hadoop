/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.hadoop.io.*;

import java.io.*;

/** A report on the state of a task. */
public class TaskReport implements Writable {
  private String taskid;
  private float progress;
  private String state;
  private String[] diagnostics;

  public TaskReport() {}

  TaskReport(String taskid, float progress, String state,
             String[] diagnostics) {
    this.taskid = taskid;
    this.progress = progress;
    this.state = state;
    this.diagnostics = diagnostics;
  }
    
  /** The id of the task. */
  public String getTaskId() { return taskid; }
  /** The amount completed, between zero and one. */
  public float getProgress() { return progress; }
  /** The most recent state, reported by a {@link Reporter}. */
  public String getState() { return state; }
  /** A list of error messages. */
  public String[] getDiagnostics() { return diagnostics; }

  //////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    UTF8.writeString(out, taskid);
    out.writeFloat(progress);
    UTF8.writeString(out, state);
    new ObjectWritable(diagnostics).write(out);
  }

  public void readFields(DataInput in) throws IOException {
    this.taskid = UTF8.readString(in);
    this.progress = in.readFloat();
    this.state = UTF8.readString(in);

    ObjectWritable wrapper = new ObjectWritable();
    wrapper.readFields(in);
    diagnostics = (String[])wrapper.get();
  }
}
