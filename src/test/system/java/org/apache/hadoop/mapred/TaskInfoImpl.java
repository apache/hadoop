package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.test.system.TaskInfo;

/**
 * Concrete class to expose out the task related information to the Clients from
 * the JobTracker.
 * Look at {@link TaskInfo} for further details.
 */
class TaskInfoImpl implements TaskInfo {

  private double progress;
  private TaskID taskID;
  private int killedAttempts;
  private int failedAttempts;
  private int runningAttempts;
  private TaskStatus[] taskStatus;

  public TaskInfoImpl() {
    taskID = new TaskID();
  }
  public TaskInfoImpl(TaskID taskID, double progress, int runningAttempts,
      int killedAttempts, int failedAttempts, TaskStatus[] taskStatus) {
    this.progress = progress;
    this.taskID = taskID;
    this.killedAttempts = killedAttempts;
    this.failedAttempts = failedAttempts;
    this.runningAttempts = runningAttempts;
    if (taskStatus != null) {
      this.taskStatus = taskStatus;
    }
    else { 
      if (taskID.isMap()) {
        this.taskStatus = new MapTaskStatus[]{};
      }
      else {
        this.taskStatus = new ReduceTaskStatus[]{};
      }
    }
    
  }

  @Override
  public double getProgress() {
    return progress;
  }

  @Override
  public TaskID getTaskID() {
    return taskID;
  }

  @Override
  public int numKilledAttempts() {
    return killedAttempts;
  }

  @Override
  public int numFailedAttempts() {
    return failedAttempts;
  }

  @Override
  public int numRunningAttempts() {
    return runningAttempts;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    taskID.readFields(in);
    progress = in.readDouble();
    runningAttempts = in.readInt();
    killedAttempts = in.readInt();
    failedAttempts = in.readInt();
    int size = in.readInt();
    if (taskID.isMap()) {
      taskStatus = new MapTaskStatus[size];
    }
    else {
      taskStatus = new ReduceTaskStatus[size];
    }
    for (int i = 0; i < size; i++) {
      if (taskID.isMap()) {
        taskStatus[i] = new MapTaskStatus();
      }
      else {
        taskStatus[i] = new ReduceTaskStatus();
      }
      taskStatus[i].readFields(in);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    taskID.write(out);
    out.writeDouble(progress);
    out.writeInt(runningAttempts);
    out.writeInt(killedAttempts);
    out.writeInt(failedAttempts);
    out.writeInt(taskStatus.length);
    for (TaskStatus t : taskStatus) {
      t.write(out);
    }
  }
  
  @Override
  public TaskStatus[] getTaskStatus() {
    return taskStatus;
  }
}
