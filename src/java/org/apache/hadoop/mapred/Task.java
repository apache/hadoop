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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.StringUtils;

/** Base class for tasks. */
abstract class Task implements Writable, Configurable {
  private static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.mapred.TaskRunner");

  // Counters used by Task subclasses
  protected static enum Counter { 
    MAP_INPUT_RECORDS, 
    MAP_OUTPUT_RECORDS,
    MAP_INPUT_BYTES, 
    MAP_OUTPUT_BYTES,
    COMBINE_INPUT_RECORDS,
    COMBINE_OUTPUT_RECORDS,
    REDUCE_INPUT_GROUPS,
    REDUCE_INPUT_RECORDS,
    REDUCE_OUTPUT_RECORDS
  }
  
  
  ////////////////////////////////////////////
  // Fields
  ////////////////////////////////////////////

  private String jobFile;                         // job configuration file
  private String taskId;                          // unique, includes job id
  private String jobId;                           // unique jobid
  private String tipId ;
  private int partition;                          // id within job
  private TaskStatus.Phase phase ;                // current phase of the task 
  

  ////////////////////////////////////////////
  // Constructors
  ////////////////////////////////////////////

  public Task() {}

  public Task(String jobId, String jobFile, String tipId, 
      String taskId, int partition) {
    this.jobFile = jobFile;
    this.taskId = taskId;
    this.jobId = jobId;
    this.tipId = tipId; 
    this.partition = partition;
  }

  ////////////////////////////////////////////
  // Accessors
  ////////////////////////////////////////////
  public void setJobFile(String jobFile) { this.jobFile = jobFile; }
  public String getJobFile() { return jobFile; }
  public String getTaskId() { return taskId; }
  public String getTipId(){ return tipId ; }
  public Counters getCounters() { return counters; }
  
  /**
   * Get the job name for this task.
   * @return the job name
   */
  public String getJobId() {
    return jobId;
  }
  
  /**
   * Get the index of this task within the job.
   * @return the integer part of the task id
   */
  public int getPartition() {
    return partition;
  }
  /**
   * Return current phase of the task. 
   * @return
   */
  public TaskStatus.Phase getPhase(){
    return this.phase ; 
  }
  /**
   * Set current phase of the task. 
   * @param p
   */
  protected void setPhase(TaskStatus.Phase p){
    this.phase = p ; 
  }

  ////////////////////////////////////////////
  // Writable methods
  ////////////////////////////////////////////

  public void write(DataOutput out) throws IOException {
    UTF8.writeString(out, jobFile);
    UTF8.writeString(out, tipId); 
    UTF8.writeString(out, taskId);
    UTF8.writeString(out, jobId);
    out.writeInt(partition);
  }
  public void readFields(DataInput in) throws IOException {
    jobFile = UTF8.readString(in);
    tipId = UTF8.readString(in);
    taskId = UTF8.readString(in);
    jobId = UTF8.readString(in);
    partition = in.readInt();
  }

  public String toString() { return taskId; }

  /**
   * Localize the given JobConf to be specific for this task.
   */
  public void localizeConfiguration(JobConf conf) throws IOException {
    conf.set("mapred.tip.id", tipId); 
    conf.set("mapred.task.id", taskId);
    conf.setBoolean("mapred.task.is.map",isMapTask());
    conf.setInt("mapred.task.partition", partition);
    conf.set("mapred.job.id", jobId);
  }
  
  /** Run this task as a part of the named job.  This method is executed in the
   * child process and is what invokes user-supplied map, reduce, etc. methods.
   * @param umbilical for progress reports
   */
  public abstract void run(JobConf job, TaskUmbilicalProtocol umbilical)
    throws IOException;


  /** Return an approprate thread runner for this task. */
  public abstract TaskRunner createRunner(TaskTracker tracker
                                          ) throws IOException;

  /** The number of milliseconds between progress reports. */
  public static final int PROGRESS_INTERVAL = 1000;

  private transient Progress taskProgress = new Progress();
  private transient long nextProgressTime =
    System.currentTimeMillis() + PROGRESS_INTERVAL;

  // Current counters
  private transient Counters counters = new Counters();
  
  public abstract boolean isMapTask();

  public Progress getProgress() { return taskProgress; }

  protected Reporter getReporter(final TaskUmbilicalProtocol umbilical) 
    throws IOException 
  {
    return new Reporter() {
        public void setStatus(String status) throws IOException {
          synchronized (this) {
            taskProgress.setStatus(status);
            progress();
          }
        }
        public void progress() throws IOException {
            reportProgress(umbilical);
        }
        public void incrCounter(Enum key, long amount) {
            Counters counters = getCounters();
            if (counters != null) {
              counters.incrCounter(key, amount);
            }
          }
      };
  }

  public void setProgress(float progress) {
    taskProgress.set(progress);
  }

  public void reportProgress(TaskUmbilicalProtocol umbilical) {
    long now = System.currentTimeMillis();
    synchronized (this) {
      if (now > nextProgressTime)  {
        nextProgressTime = now + PROGRESS_INTERVAL;
        float progress = taskProgress.get();
        String status = taskProgress.toString();
        try {
          umbilical.progress(getTaskId(), progress, status, phase, counters);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();     // interrupt ourself
        } catch (IOException ie) {
          LOG.warn(StringUtils.stringifyException(ie));
        }
      }
    }
  }

  public void done(TaskUmbilicalProtocol umbilical) throws IOException {
    int retries = 10;
    boolean needProgress = true;
    while (true) {
      try {
        if (needProgress) {
          // send a final status report
          try {
            umbilical.progress(getTaskId(), taskProgress.get(), 
                               taskProgress.toString(), phase, counters);
            needProgress = false;
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();       // interrupt ourself
          }
        }
        umbilical.done(getTaskId());
        return;
      } catch (IOException ie) {
        LOG.warn("Failure signalling completion: " + 
                 StringUtils.stringifyException(ie));
        if (--retries == 0) {
          throw ie;
        }
      }
    }
  }
}
