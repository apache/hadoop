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
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;
import java.text.NumberFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.ReflectionUtils;
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

  ///////////////////////////////////////////////////////////
  // Helper methods to construct task-output paths
  ///////////////////////////////////////////////////////////
  
  /** Construct output file names so that, when an output directory listing is
   * sorted lexicographically, positions correspond to output partitions.*/
  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }

  static synchronized String getOutputName(int partition) {
    return "part-" + NUMBER_FORMAT.format(partition);
  }

  ////////////////////////////////////////////
  // Fields
  ////////////////////////////////////////////

  private String jobFile;                         // job configuration file
  private String taskId;                          // unique, includes job id
  private String jobId;                           // unique jobid
  private String tipId;
  private int partition;                          // id within job
  TaskStatus taskStatus; 										      // current status of the task
  private Path taskOutputPath;                    // task-specific output dir
  
  protected JobConf conf;
  protected MapOutputFile mapOutputFile = new MapOutputFile();
  protected LocalDirAllocator lDirAlloc;

  ////////////////////////////////////////////
  // Constructors
  ////////////////////////////////////////////

  public Task() {
    taskStatus = TaskStatus.createTaskStatus(isMapTask());
  }

  public Task(String jobId, String jobFile, String tipId, 
              String taskId, int partition) {
    this.jobFile = jobFile;
    this.taskId = taskId;
    this.jobId = jobId;
    this.tipId = tipId; 
    this.partition = partition;
    this.taskStatus = TaskStatus.createTaskStatus(isMapTask(), this.taskId, 
                                                  0.0f, 
                                                  TaskStatus.State.UNASSIGNED, 
                                                  "", "", "", 
                                                  isMapTask() ? 
                                                    TaskStatus.Phase.MAP : 
                                                    TaskStatus.Phase.SHUFFLE, 
                                                  counters);
  }

  ////////////////////////////////////////////
  // Accessors
  ////////////////////////////////////////////
  public void setJobFile(String jobFile) { this.jobFile = jobFile; }
  public String getJobFile() { return jobFile; }
  public String getTaskId() { return taskId; }
  public String getTipId(){ return tipId; }
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
   * needs to be synchronized as communication thread sends the phase every second
   * @return
   */
  public synchronized TaskStatus.Phase getPhase(){
    return this.taskStatus.getPhase(); 
  }
  /**
   * Set current phase of the task. 
   * @param p
   */
  protected synchronized void setPhase(TaskStatus.Phase phase){
    this.taskStatus.setPhase(phase); 
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
    if (taskOutputPath != null) {
      Text.writeString(out, taskOutputPath.toString());
    } else {
      Text.writeString(out, "");
    }
    taskStatus.write(out);
  }
  public void readFields(DataInput in) throws IOException {
    jobFile = UTF8.readString(in);
    tipId = UTF8.readString(in);
    taskId = UTF8.readString(in);
    jobId = UTF8.readString(in);
    partition = in.readInt();
    String outPath = Text.readString(in);
    if (outPath.length() != 0) {
      taskOutputPath = new Path(outPath);
    } else {
      taskOutputPath = null;
    }
    taskStatus.readFields(in);
  }

  public String toString() { return taskId; }

  private Path getTaskOutputPath(JobConf conf) {
    Path p = new Path(conf.getOutputPath(), ("_temporary" 
                      + Path.SEPARATOR + "_" + taskId));
    try {
      FileSystem fs = p.getFileSystem(conf);
      return p.makeQualified(fs);
    } catch (IOException ie) {
      LOG.warn(StringUtils.stringifyException(ie));
      return p;
    }
  }
  
  /**
   * Localize the given JobConf to be specific for this task.
   */
  public void localizeConfiguration(JobConf conf) throws IOException {
    conf.set("mapred.tip.id", tipId); 
    conf.set("mapred.task.id", taskId);
    conf.setBoolean("mapred.task.is.map", isMapTask());
    conf.setInt("mapred.task.partition", partition);
    conf.set("mapred.job.id", jobId);
    
    // The task-specific output path
    if (conf.getOutputPath() != null) {
      taskOutputPath = getTaskOutputPath(conf);
      conf.setOutputPath(taskOutputPath);
    }
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
  public static final int PROGRESS_INTERVAL = 3000;

  private transient Progress taskProgress = new Progress();

  // Current counters
  private transient Counters counters = new Counters();
  
  /**
   * flag that indicates whether progress update needs to be sent to parent.
   * If true, it has been set. If false, it has been reset. 
   * Using AtomicBoolean since we need an atomic read & reset method. 
   */  
  private AtomicBoolean progressFlag = new AtomicBoolean(false);
  /* flag to track whether task is done */
  private AtomicBoolean taskDone = new AtomicBoolean(false);
  // getters and setters for flag
  private void setProgressFlag() {
    progressFlag.set(true);
  }
  private boolean resetProgressFlag() {
    return progressFlag.getAndSet(false);
  }
  
  public abstract boolean isMapTask();

  public Progress getProgress() { return taskProgress; }

  InputSplit getInputSplit() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Input only available on map");
  }

  /** 
   * The communication thread handles communication with the parent (Task Tracker). 
   * It sends progress updates if progress has been made or if the task needs to 
   * let the parent know that it's alive. It also pings the parent to see if it's alive. 
   */
  protected void startCommunicationThread(final TaskUmbilicalProtocol umbilical) {
    Thread thread = new Thread(new Runnable() {
        public void run() {
          final int MAX_RETRIES = 3;
          int remainingRetries = MAX_RETRIES;
          // get current flag value and reset it as well
          boolean sendProgress = resetProgressFlag();
          while (!taskDone.get()) {
            try {
              boolean taskFound = true; // whether TT knows about this task
              // sleep for a bit
              try {
                Thread.sleep(PROGRESS_INTERVAL);
              } 
              catch (InterruptedException e) {
                LOG.debug(getTaskId() + " Progress/ping thread exiting " +
                                        "since it got interrupted");
                break;
              }
              
              if (sendProgress) {
                // we need to send progress update
                taskStatus.statusUpdate(taskProgress.get(), taskProgress.toString(), 
                        counters);
                taskFound = umbilical.statusUpdate(taskId, taskStatus);
                taskStatus.clearStatus();
              }
              else {
                // send ping 
                taskFound = umbilical.ping(taskId);
              }
              
              // if Task Tracker is not aware of our task ID (probably because it died and 
              // came back up), kill ourselves
              if (!taskFound) {
                LOG.warn("Parent died.  Exiting "+taskId);
                System.exit(66);
              }
              
              sendProgress = resetProgressFlag(); 
              remainingRetries = MAX_RETRIES;
            } 
            catch (Throwable t) {
              LOG.info("Communication exception: " + StringUtils.stringifyException(t));
              remainingRetries -=1;
              if (remainingRetries == 0) {
                ReflectionUtils.logThreadInfo(LOG, "Communication exception", 0);
                LOG.warn("Last retry, killing "+taskId);
                System.exit(65);
              }
            }
          }
        }
      }, "Comm thread for "+taskId);
    thread.setDaemon(true);
    thread.start();
    LOG.debug(getTaskId() + " Progress/ping thread started");
  }

  
  protected Reporter getReporter(final TaskUmbilicalProtocol umbilical) 
    throws IOException 
  {
    return new Reporter() {
        public void setStatus(String status) {
          taskProgress.setStatus(status);
          // indicate that progress update needs to be sent
          setProgressFlag();
        }
        public void progress() {
          // indicate that progress update needs to be sent
          setProgressFlag();
        }
        public void incrCounter(Enum key, long amount) {
          if (counters != null) {
            counters.incrCounter(key, amount);
          }
          setProgressFlag();
        }
        public InputSplit getInputSplit() throws UnsupportedOperationException {
          return Task.this.getInputSplit();
        }
      };
  }

  public void setProgress(float progress) {
    taskProgress.set(progress);
    // indicate that progress update needs to be sent
    setProgressFlag();
  }

  public void done(TaskUmbilicalProtocol umbilical) throws IOException {
    int retries = 10;
    boolean needProgress = true;
    taskDone.set(true);
    while (true) {
      try {
        if (needProgress) {
          // send a final status report
          taskStatus.statusUpdate(taskProgress.get(), taskProgress.toString(), 
                                  counters);
          try {
            if (!umbilical.statusUpdate(getTaskId(), taskStatus)) {
              LOG.warn("Parent died.  Exiting "+taskId);
              System.exit(66);
            }
            taskStatus.clearStatus();
            needProgress = false;
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();       // interrupt ourself
          }
        }
        umbilical.done(taskId);
        LOG.info("Task '" + getTaskId() + "' done.");
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
  
  public void setConf(Configuration conf) {
    if (conf instanceof JobConf) {
      this.conf = (JobConf) conf;

      if (taskId != null && taskOutputPath == null && 
              this.conf.getOutputPath() != null) {
        taskOutputPath = getTaskOutputPath(this.conf);
      }
    } else {
      this.conf = new JobConf(conf);
    }
    this.mapOutputFile.setConf(this.conf);
    this.lDirAlloc = new LocalDirAllocator("mapred.local.dir");
  }

  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Save the task's output on successful completion.
   * 
   * @throws IOException
   */
  void saveTaskOutput() throws IOException {

    if (taskOutputPath != null) {
      FileSystem fs = taskOutputPath.getFileSystem(conf);
      if (fs.exists(taskOutputPath)) {
        Path jobOutputPath = taskOutputPath.getParent().getParent();

        // Move the task outputs to their final place
        moveTaskOutputs(fs, jobOutputPath, taskOutputPath);

        // Delete the temporary task-specific output directory
        if (!fs.delete(taskOutputPath)) {
          LOG.info("Failed to delete the temporary output directory of task: " + 
                  getTaskId() + " - " + taskOutputPath);
        }
        
        LOG.info("Saved output of task '" + getTaskId() + "' to " + jobOutputPath);
      }
    }
  }
  
  private Path getFinalPath(Path jobOutputDir, Path taskOutput) {
    URI relativePath = taskOutputPath.toUri().relativize(taskOutput.toUri());
    if (relativePath.getPath().length() > 0) {
      return new Path(jobOutputDir, relativePath.getPath());
    } else {
      return jobOutputDir;
    }
  }
  
  private void moveTaskOutputs(FileSystem fs, Path jobOutputDir, Path taskOutput) 
  throws IOException {
    if (fs.isFile(taskOutput)) {
      Path finalOutputPath = getFinalPath(jobOutputDir, taskOutput);
      if (!fs.rename(taskOutput, finalOutputPath)) {
        if (!fs.delete(finalOutputPath)) {
          throw new IOException("Failed to delete earlier output of task: " + 
                  getTaskId());
        }
        if (!fs.rename(taskOutput, finalOutputPath)) {
          throw new IOException("Failed to save output of task: " + 
                  getTaskId());
        }
      }
      LOG.debug("Moved " + taskOutput + " to " + finalOutputPath);
    } else if(fs.isDirectory(taskOutput)) {
      Path[] paths = fs.listPaths(taskOutput);
      Path finalOutputPath = getFinalPath(jobOutputDir, taskOutput);
      fs.mkdirs(finalOutputPath);
      if (paths != null) {
        for (Path path : paths) {
          moveTaskOutputs(fs, jobOutputDir, path);
        }
      }
    }
  }
  
  /**
   * Discard the task's output on failure.
   * 
   * @throws IOException
   */
  void discardTaskOutput() throws IOException {
    if (taskOutputPath != null) {
      FileSystem fs = taskOutputPath.getFileSystem(conf);
      if (fs.exists(taskOutputPath)) {
        // Delete the temporary task-specific output directory
        FileUtil.fullyDelete(fs, taskOutputPath);
        LOG.info("Discarded output of task '" + getTaskId() + "' - " 
                + taskOutputPath);
      }
    }
  }

}
