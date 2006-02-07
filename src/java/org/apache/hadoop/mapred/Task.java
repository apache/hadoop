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
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

import java.io.*;

/** Base class for tasks. */
abstract class Task implements Writable, Configurable {
  ////////////////////////////////////////////
  // Fields
  ////////////////////////////////////////////

  private String jobFile;                         // job configuration file
  private String taskId;                          // unique, includes job id
  ////////////////////////////////////////////
  // Constructors
  ////////////////////////////////////////////

  public Task() {}

  public Task(String jobFile, String taskId) {
    this.jobFile = jobFile;
    this.taskId = taskId;
  }

  ////////////////////////////////////////////
  // Accessors
  ////////////////////////////////////////////
  public void setJobFile(String jobFile) { this.jobFile = jobFile; }
  public String getJobFile() { return jobFile; }
  public String getTaskId() { return taskId; }

  ////////////////////////////////////////////
  // Writable methods
  ////////////////////////////////////////////

  public void write(DataOutput out) throws IOException {
    UTF8.writeString(out, jobFile);
    UTF8.writeString(out, taskId);
  }
  public void readFields(DataInput in) throws IOException {
    jobFile = UTF8.readString(in);
    taskId = UTF8.readString(in);
  }

  public String toString() { return taskId; }

  /** Run this task as a part of the named job.  This method is executed in the
   * child process and is what invokes user-supplied map, reduce, etc. methods.
   * @param umbilical for progress reports
   */
  public abstract void run(JobConf job, TaskUmbilicalProtocol umbilical)
    throws IOException;


  /** Return an approprate thread runner for this task. */
  public abstract TaskRunner createRunner(TaskTracker tracker);

  /** The number of milliseconds between progress reports. */
  public static final int PROGRESS_INTERVAL = 1000;

  private transient Progress taskProgress = new Progress();
  private transient long nextProgressTime =
    System.currentTimeMillis() + PROGRESS_INTERVAL;

  public abstract boolean isMapTask();

  public Progress getProgress() { return taskProgress; }

  public Reporter getReporter(final TaskUmbilicalProtocol umbilical,
                              final Progress progress) throws IOException {
    return new Reporter() {
        public void setStatus(String status) throws IOException {
          progress.setStatus(status);
          reportProgress(umbilical);
        }
      };
  }

  public void reportProgress(TaskUmbilicalProtocol umbilical, float progress)
    throws IOException {
    taskProgress.set(progress);
    reportProgress(umbilical);
  }

  public void reportProgress(TaskUmbilicalProtocol umbilical)
    throws IOException {
    long now = System.currentTimeMillis();
    if (now > nextProgressTime)  {
      synchronized (this) {
        nextProgressTime = now + PROGRESS_INTERVAL;
        float progress = taskProgress.get();
        String status = taskProgress.toString();
        umbilical.progress(getTaskId(), progress, status);
      }
    }
  }

  public void done(TaskUmbilicalProtocol umbilical)
    throws IOException {
    umbilical.progress(getTaskId(),               // send a final status report
                       taskProgress.get(), taskProgress.toString());
    umbilical.done(getTaskId());
  }
}
